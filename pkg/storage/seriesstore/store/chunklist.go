// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"math/bits"

	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
)

// ChunkMeta is a completed chunk in the chunk files, like Prometheus's `mmappedChunk`.
type ChunkMeta struct {
	Ref     chunks.Ref
	MinTime int64
	MaxTime int64
	Len     uint32
	// The wire encoding: chunks.EncodingXOR and the histogram encodings.
	Encoding uint8
	// Out-of-order chunks sort after in-order ones with the same min time when merging.
	OutOfOrder bool
}

// chunkList is a series' completed chunks, in write order, as varint deltas: most stored series
// are older than the head, and their chunk references are what they keep in memory.
type chunkList []byte

func chunkListFromMetas(metas []ChunkMeta) chunkList {
	if len(metas) == 0 {
		return nil
	}
	// Sized exactly, like Rust's boxed slice: every stored series keeps its list, and growing by
	// appends left up to half of each unused.
	size := 0
	var reference, minTime int64
	for _, meta := range metas {
		size += uvarintLen(zigzag(int64(meta.Ref)-reference)) + uvarintLen(zigzag(meta.MinTime-minTime)) +
			uvarintLen(uint64(meta.MaxTime-meta.MinTime)) + uvarintLen(uint64(meta.Len)) + 1
		reference, minTime = int64(meta.Ref), meta.MinTime
	}
	out := make([]byte, 0, size)
	reference, minTime = 0, 0
	for _, meta := range metas {
		out = putUvarint(out, zigzag(int64(meta.Ref)-reference))
		out = putUvarint(out, zigzag(meta.MinTime-minTime))
		out = putUvarint(out, uint64(meta.MaxTime-meta.MinTime))
		out = putUvarint(out, uint64(meta.Len))
		flags := meta.Encoding & 0x7f
		if meta.OutOfOrder {
			flags |= 0x80
		}
		out = append(out, flags)
		reference = int64(meta.Ref)
		minTime = meta.MinTime
	}
	return out
}

// chunkIter decodes a chunk list.
type chunkIter struct {
	rest      []byte
	reference int64
	minTime   int64
}

func (l chunkList) iter() chunkIter {
	if countersEnabled {
		chunkListDecodes.Add(1)
	}
	return chunkIter{rest: l}
}

func (it *chunkIter) next() (ChunkMeta, bool) {
	if len(it.rest) == 0 {
		return ChunkMeta{}, false
	}
	it.reference += unzigzag(takeUvarint(&it.rest))
	it.minTime += unzigzag(takeUvarint(&it.rest))
	duration := int64(takeUvarint(&it.rest))
	length := uint32(takeUvarint(&it.rest))
	flags := it.rest[0]
	it.rest = it.rest[1:]
	return ChunkMeta{
		Ref:        chunks.Ref(it.reference),
		MinTime:    it.minTime,
		MaxTime:    it.minTime + duration,
		Len:        length,
		Encoding:   flags & 0x7f,
		OutOfOrder: flags&0x80 != 0,
	}, true
}

// appendTo appends the decoded chunks to metas.
func (l chunkList) appendTo(metas []ChunkMeta) []ChunkMeta {
	it := l.iter()
	for meta, ok := it.next(); ok; meta, ok = it.next() {
		metas = append(metas, meta)
	}
	return metas
}

func (l chunkList) toSlice() []ChunkMeta {
	return l.appendTo(nil)
}

func (l chunkList) count() int {
	n := 0
	it := l.iter()
	for _, ok := it.next(); ok; _, ok = it.next() {
		n++
	}
	return n
}

func (l chunkList) isEmpty() bool {
	return len(l) == 0
}

// push adds a chunk after the others. Dense histogram series cut a chunk every few samples, so
// it appends the chunk's delta to the existing bytes instead of decoding the list into chunks and
// encoding it again, which made each cut cost as much as the whole list and fed the GC.
func (l *chunkList) push(meta ChunkMeta) {
	// The deltas are from the last chunk, read without decoding the others into memory.
	last := chunkIter{rest: *l}
	for len(last.rest) > 0 {
		last.next()
	}
	reference := zigzag(int64(meta.Ref) - last.reference)
	minTime := zigzag(meta.MinTime - last.minTime)
	duration := uint64(meta.MaxTime - meta.MinTime)
	size := len(*l) + uvarintLen(reference) + uvarintLen(minTime) + uvarintLen(duration) + uvarintLen(uint64(meta.Len)) + 1
	// Still sized exactly, like chunkListFromMetas: every stored series keeps its list.
	out := make([]byte, len(*l), size)
	copy(out, *l)
	out = putUvarint(out, reference)
	out = putUvarint(out, minTime)
	out = putUvarint(out, duration)
	out = putUvarint(out, uint64(meta.Len))
	flags := meta.Encoding & 0x7f
	if meta.OutOfOrder {
		flags |= 0x80
	}
	*l = append(out, flags)
}

// retain keeps the chunks keep accepts.
func (l *chunkList) retain(keep func(*ChunkMeta) bool) {
	metas := l.toSlice()
	kept := metas[:0]
	all := true
	for index := range metas {
		if keep(&metas[index]) {
			kept = append(kept, metas[index])
		} else {
			all = false
		}
	}
	if all {
		return
	}
	*l = chunkListFromMetas(kept)
}

func zigzag(value int64) uint64 {
	return uint64((value << 1) ^ (value >> 63))
}

func unzigzag(value uint64) int64 {
	return int64(value>>1) ^ -int64(value&1)
}

func putUvarint(out []byte, value uint64) []byte {
	for value >= 0x80 {
		out = append(out, byte(value)|0x80)
		value >>= 7
	}
	return append(out, byte(value))
}

func takeUvarint(bytes *[]byte) uint64 {
	b := *bytes
	var value uint64
	var shift uint
	for i := 0; ; i++ {
		c := b[i]
		value |= uint64(c&0x7f) << shift
		if c < 0x80 {
			*bytes = b[i+1:]
			return value
		}
		shift += 7
	}
}

func takeUvarintString(s *string) uint64 {
	b := *s
	var value uint64
	var shift uint
	for i := 0; ; i++ {
		c := b[i]
		value |= uint64(c&0x7f) << shift
		if c < 0x80 {
			*s = b[i+1:]
			return value
		}
		shift += 7
	}
}

// uvarintLen is how many bytes putUvarint writes for value.
func uvarintLen(value uint64) int {
	return (bits.Len64(value|1) + 6) / 7
}
