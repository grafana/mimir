package index

import (
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/index"
)

// PostingsOffsetCacheCodec provides encoding and decoding for PostingsOffsetTableCache values.
//
// Encoding and decoding is strictly to store and retrieve int64 values for index.Range instances.
//
// Implementations must NOT be tied to specific TSDB layout assumptions
// such as the length of Postings entries or relation between consecutive Postings entries -
// This logic is left for the implementations of the PostingOffsetTable interface.
type PostingsOffsetCacheCodec interface {
	Encode(index.Range) []byte
	Decode([]byte) (index.Range, error)
}

type BigEndianPostingsOffsetCodec struct{}

func (c BigEndianPostingsOffsetCodec) Encode(rng index.Range) []byte {
	buf := make([]byte, 0, 2*binary.MaxVarintLen64)
	buf = binary.AppendUvarint(buf, uint64(rng.Start))
	buf = binary.AppendUvarint(buf, uint64(rng.End))
	return buf
}

func (c BigEndianPostingsOffsetCodec) Decode(b []byte) (index.Range, error) {
	rng := index.Range{}

	start, n := binary.Uvarint(b)
	if n <= 0 {
		return rng, errors.New("invalid postings offset encoding")
	}
	rng.Start = int64(start)

	end, n := binary.Uvarint(b[n:])
	if n <= 0 {
		return rng, errors.New("invalid postings offset encoding")
	}
	rng.End = int64(end)

	return rng, nil
}
