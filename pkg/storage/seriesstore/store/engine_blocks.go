// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"

	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
)

// emulatedBlock is a block Prometheus would have written over [minTime, maxTime): label lookups
// overlapping it see every series it holds, whatever their samples' times. The samples themselves
// stay in the store's chunks.
type emulatedBlock struct {
	minTime, maxTime int64
	// Written by an out-of-order compaction, from the out-of-order head's chunks.
	outOfOrder bool
	// Its series' label hashes, sorted: a block holds label sets, whichever series ref wrote them.
	series []uint64
}

// overlaps is Prometheus's Block.OverlapsClosedInterval.
func (b *emulatedBlock) overlaps(start, end int64) bool {
	return b.minTime <= end && start < b.maxTime
}

func (b *emulatedBlock) holds(hash uint64) bool {
	_, found := slices.BinarySearch(b.series, hash)
	return found
}

// writeBlock records the block over [minTime, maxTime) of the head series holds picks, unless it
// picks none: Prometheus writes no empty block, and reports whether it did. No shard lock is held.
func (e *Engine) writeBlock(minTime, maxTime int64, outOfOrder bool, holds func(shard int, series *Series) bool) bool {
	var hashes []uint64
	for index, shard := range e.store.shards {
		shard.RLock()
		if t, ok := shard.tenants[e.tenantID]; ok {
			t.series.forEach(func(entry *seriesEntry) {
				if holds(index, &entry.series) {
					hashes = append(hashes, entry.hash)
				}
			})
		}
		shard.RUnlock()
	}
	if len(hashes) == 0 {
		return false
	}
	slices.Sort(hashes)
	block := emulatedBlock{minTime: minTime, maxTime: maxTime, outOfOrder: outOfOrder, series: slices.Compact(hashes)}
	home, t := e.home()
	home.Lock()
	// Appending never changes the elements a lookup's copy of the slice reads.
	t.blocks = append(t.blocks, block)
	home.Unlock()
	return true
}

// pruneBlocks drops the blocks whose data is all older than cutoff, into a new slice: lookups may
// still read the old one.
func pruneBlocks(blocks []emulatedBlock, cutoff int64) []emulatedBlock {
	var kept []emulatedBlock
	for _, block := range blocks {
		if block.maxTime > cutoff {
			kept = append(kept, block)
		}
	}
	return kept
}

// inOrderIn is whether a series has in-order chunks still in the head overlapping [start, end],
// which a head block of that range holds. Chunks ending before the head's min time were collected
// into earlier blocks.
func inOrderIn(series *Series, start, end, headMin int64) bool {
	it := series.chunks.iter()
	for chunk, more := it.next(); more; chunk, more = it.next() {
		if !chunk.OutOfOrder && chunk.MaxTime >= headMin && chunk.MinTime <= end && chunk.MaxTime >= start {
			return true
		}
	}
	if fh := series.floatHead; fh != nil && fh.minTime <= end && fh.lastTimestamp() >= start {
		return true
	}
	if hh := series.histogramHead; hh != nil && hh.FirstTimestamp() <= end && hh.Last().Timestamp >= start {
		return true
	}
	return false
}

// oooIn is whether a series has out-of-order chunks in the out-of-order head overlapping
// [start, end], which an out-of-order block of that range holds.
func (e *Engine) oooIn(shard int, series *Series, start, end int64) bool {
	watermark := e.oooWatermarks[shard]
	it := series.chunks.iter()
	for chunk, more := it.next(); more; chunk, more = it.next() {
		if chunk.OutOfOrder && uint64(chunk.Ref) >= watermark && chunk.MinTime <= end && chunk.MaxTime >= start {
			return true
		}
	}
	for _, sample := range series.outOfOrder {
		if sample.T >= start && sample.T <= end {
			return true
		}
	}
	return false
}

// The engine's own state beside the store's snapshot: what Prometheus keeps in its blocks and
// out-of-order head.
const (
	engineStateFileName = "engine-state"
	engineStateMagic    = "SSENGINE1"
)

// writeState saves the out-of-order head's watermarks and bounds and the blocks, for the next Open.
func (e *Engine) writeState() error {
	home, t := e.home()
	home.RLock()
	var buf []byte
	buf = append(buf, engineStateMagic...)
	if e.oooWasEnabled.Load() {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = binary.AppendVarint(buf, t.minOOOTime)
	buf = binary.AppendVarint(buf, t.maxOOOTime)
	buf = binary.AppendUvarint(buf, uint64(len(e.oooWatermarks)))
	for _, watermark := range e.oooWatermarks {
		buf = binary.AppendUvarint(buf, watermark)
	}
	buf = binary.AppendUvarint(buf, uint64(len(t.blocks)))
	for _, block := range t.blocks {
		buf = binary.AppendVarint(buf, block.minTime)
		buf = binary.AppendVarint(buf, block.maxTime)
		if block.outOfOrder {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
		buf = binary.AppendUvarint(buf, uint64(len(block.series)))
		for _, hash := range block.series {
			buf = binary.LittleEndian.AppendUint64(buf, hash)
		}
	}
	home.RUnlock()
	buf = binary.LittleEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
	path := filepath.Join(e.dir, engineStateFileName)
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, buf, 0o644); err != nil {
		return fmt.Errorf("write engine state: %w", err)
	}
	return os.Rename(temporary, path)
}

// takeState reads and removes the state writeState saved: like the store's snapshot, it only
// describes the head once.
func takeState(dir string) ([]byte, error) {
	path := filepath.Join(dir, engineStateFileName)
	buf, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := os.Remove(path); err != nil {
		return nil, err
	}
	return buf, nil
}

// restoreState applies a state takeState read; the head was restored with it.
func (e *Engine) restoreState(buf []byte) error {
	if len(buf) < len(engineStateMagic)+4 || !bytes.HasPrefix(buf, []byte(engineStateMagic)) {
		return errors.New("engine state: bad magic")
	}
	body := buf[:len(buf)-4]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(buf[len(buf)-4:]) {
		return errors.New("engine state: bad checksum")
	}
	r := bytes.NewReader(body[len(engineStateMagic):])
	enabled, err := r.ReadByte()
	if err != nil {
		return err
	}
	minOOOTime, err := binary.ReadVarint(r)
	if err != nil {
		return err
	}
	maxOOOTime, err := binary.ReadVarint(r)
	if err != nil {
		return err
	}
	count, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if count != uint64(len(e.oooWatermarks)) {
		return fmt.Errorf("engine state: %d shards, not %d", count, len(e.oooWatermarks))
	}
	watermarks := make([]uint64, count)
	for i := range watermarks {
		if watermarks[i], err = binary.ReadUvarint(r); err != nil {
			return err
		}
	}
	if count, err = binary.ReadUvarint(r); err != nil {
		return err
	}
	var blocks []emulatedBlock
	for range count {
		var block emulatedBlock
		if block.minTime, err = binary.ReadVarint(r); err != nil {
			return err
		}
		if block.maxTime, err = binary.ReadVarint(r); err != nil {
			return err
		}
		kind, err := r.ReadByte()
		if err != nil {
			return err
		}
		block.outOfOrder = kind == 1
		series, err := binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		if series > uint64(r.Len())/8 {
			return errors.New("engine state: truncated block")
		}
		block.series = make([]uint64, series)
		for i := range block.series {
			var raw [8]byte
			if _, err := r.Read(raw[:]); err != nil {
				return err
			}
			block.series[i] = binary.LittleEndian.Uint64(raw[:])
		}
		blocks = append(blocks, block)
	}
	if enabled == 1 {
		e.oooWasEnabled.Store(true)
	}
	copy(e.oooWatermarks, watermarks)
	home, t := e.home()
	home.Lock()
	defer home.Unlock()
	t.blocks = blocks
	t.minOOOTime, t.maxOOOTime = minOOOTime, maxOOOTime
	return nil
}

// splitChunks re-encodes the series' chunks that cuts gives boundaries inside of into one chunk
// per boundary interval, like the pieces Prometheus's compactions write in each block: once a
// chunk leaves the head, a query sees the series through the pieces overlapping its range, not
// the whole chunk. With the shard locked.
func splitChunks(series *Series, disk *chunks.DiskMapper, cuts func(meta ChunkMeta) []int64) error {
	var (
		out     []ChunkMeta
		changed bool
	)
	it := series.chunks.iter()
	for meta, more := it.next(); more; meta, more = it.next() {
		boundaries := cuts(meta)
		if len(boundaries) == 0 {
			out = append(out, meta)
			continue
		}
		samples, err := chunks.Decode(int32(meta.Encoding), disk.Read(meta.Ref, meta.Len))
		if err != nil {
			return err
		}
		changed = true
		for len(samples) > 0 {
			end := len(samples)
			for _, boundary := range boundaries {
				if boundary > samples[0].T {
					end, _ = slices.BinarySearchFunc(samples, boundary, func(sample chunks.Sample, t int64) int { return cmp.Compare(sample.T, t) })
					break
				}
			}
			encoded, err := chunks.EncodeOutOfOrder(samples[:end])
			if err != nil {
				return err
			}
			for _, chunk := range encoded {
				ref, err := disk.Write(chunk.Data, chunk.MaxTime)
				if err != nil {
					return err
				}
				out = append(out, ChunkMeta{Ref: ref, MinTime: chunk.MinTime, MaxTime: chunk.MaxTime, Len: uint32(len(chunk.Data)), Encoding: uint8(chunk.Encoding), OutOfOrder: meta.OutOfOrder})
			}
			samples = samples[end:]
		}
	}
	if changed {
		series.chunks = chunkListFromMetas(out)
	}
	return nil
}

// alignedCuts are the block range starts inside a chunk: out-of-order and selected series blocks
// are aligned on block ranges.
func alignedCuts(meta ChunkMeta) []int64 {
	var cuts []int64
	for boundary := rangeStart(meta.MinTime) + chunkRangeMs; boundary <= meta.MaxTime; boundary += chunkRangeMs {
		cuts = append(cuts, boundary)
	}
	return cuts
}

// inOrderCuts are the in-order blocks' boundaries inside a chunk, sorted.
func inOrderCuts(blocks []emulatedBlock, meta ChunkMeta) []int64 {
	var cuts []int64
	for _, block := range blocks {
		if block.outOfOrder {
			continue
		}
		for _, boundary := range []int64{block.minTime, block.maxTime} {
			if boundary > meta.MinTime && boundary <= meta.MaxTime {
				cuts = append(cuts, boundary)
			}
		}
	}
	slices.Sort(cuts)
	return slices.Compact(cuts)
}
