// SPDX-License-Identifier: AGPL-3.0-only

package indexcache

import (
	"encoding/binary"
	"errors"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"

	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
)

type PostingsOffsetsCacheKey struct {
	tenantID string
	blockID  ulid.ULID
	lbl      labels.Label
}

// typ implements cacheKey for in-memory cache implementations
func (k PostingsOffsetsCacheKey) typ() string {
	return cacheTypePostingsOffset
}

// size implements cacheKey for in-memory cache implementations
func (k PostingsOffsetsCacheKey) size() uint64 {
	return stringSize(k.tenantID) + ulidSize + stringSize(k.lbl.Name) + stringSize(k.lbl.Value)
}

type PostingsOffsetsForMatcherCacheKey struct {
	tenantID   string
	blockID    ulid.ULID
	matcherStr string
	isSubtract bool
}

// typ implements cacheKey for in-memory cache implementations
func (k PostingsOffsetsForMatcherCacheKey) typ() string {
	return cacheTypePostingsOffsetsForMatcher
}

// size implements cacheKey for in-memory cache implementations
func (k PostingsOffsetsForMatcherCacheKey) size() uint64 {
	return stringSize(k.tenantID) + ulidSize + stringSize(k.matcherStr) + 1 // add a byte for boolean isSubtract
}

func encodePostingsOffsets(offsets []streamindex.PostingListOffset) []byte {
	bufLen := binary.MaxVarintLen64 // Leading len field for number of entries
	for _, offset := range offsets {
		bufLen += binary.MaxVarintLen64 + len(offset.LabelValue) // Leading len field for string plus the string itself
		bufLen += 2 * binary.MaxVarintLen64                      // start + end integers for range
	}

	encBuf := encoding.Encbuf{B: make([]byte, 0, bufLen)}
	encBuf.PutUvarint(len(offsets))

	for _, offset := range offsets {
		encBuf.PutUvarintStr(offset.LabelValue)
		encBuf.PutVarint64(offset.Off.Start)
		encBuf.PutVarint64(offset.Off.End)
	}
	return encBuf.Get()
}

func decodePostingsOffsets(buf []byte) ([]streamindex.PostingListOffset, error) {
	decBuf := encoding.Decbuf{B: buf}
	decLen := decBuf.Uvarint()
	offsets := make([]streamindex.PostingListOffset, decLen)

	for i := 0; i < decLen; i++ {
		labelValue := decBuf.UvarintStr()
		rngStart := decBuf.Varint64()
		rngEnd := decBuf.Varint64()

		if err := decBuf.Err(); err != nil {
			return nil, err
		}

		offsets[i] = streamindex.PostingListOffset{
			LabelValue: labelValue,
			Off: index.Range{
				Start: rngStart,
				End:   rngEnd,
			},
		}
	}
	return offsets, nil
}

func encodeSingleRange(rng index.Range) []byte {
	buflen := binary.MaxVarintLen64 + // Leading len field for number of entries - always 1 in this case
		2*binary.MaxVarintLen64 // start + end integers for one entry

	buf := make([]byte, 0, buflen)
	buf = binary.AppendUvarint(buf, uint64(1))

	return encodeRange(buf, rng)
}

func encodeRange(buf []byte, rng index.Range) []byte {
	buf = binary.AppendUvarint(buf, uint64(rng.Start))
	buf = binary.AppendUvarint(buf, uint64(rng.End))
	return buf
}

func decodeSingleRange(buf []byte) (index.Range, error) {
	decLen, n := binary.Uvarint(buf)
	if n <= 0 || decLen != 1 {
		// Number of entries should always be 1 for encodings from EncodeSingleRange
		return index.Range{}, errors.New("invalid single postings offsets encoding")
	}

	startIdx := n
	rng, _, err := decodeRange(buf[startIdx:])
	return rng, err
}

// decodeRange decodes a single start + end integer pair to an index.Range.
// This should only be called after decoding the leading integer
// used to indicate the number of entries in the full list.
func decodeRange(buf []byte) (index.Range, int, error) {
	rng := index.Range{}
	read := 0

	start, n := binary.Uvarint(buf)
	if n <= 0 {
		return rng, n, errors.New("invalid postings offset encoding")
	}
	rng.Start = int64(start)
	read += n

	end, n := binary.Uvarint(buf[n:])
	if n <= 0 {
		return rng, n, errors.New("invalid postings offset encoding")
	}
	rng.End = int64(end)
	read += n

	return rng, read, nil
}
