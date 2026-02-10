package indexcache

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
	//EncodeRange(index.Range) []byte
	//DecodeRange([]byte) (index.Range, error)
	EncodeRanges([]index.Range) []byte
	DecodeRanges([]byte) ([]index.Range, error)
}

type BigEndianPostingsOffsetCodec struct{}

//func (c BigEndianPostingsOffsetCodec) EncodeRange(rng index.Range) []byte {
//	buf := make([]byte, 0, 2*binary.MaxVarintLen64)
//	buf = binary.AppendUvarint(buf, uint64(rng.Start))
//	buf = binary.AppendUvarint(buf, uint64(rng.End))
//	return buf
//}

func (c BigEndianPostingsOffsetCodec) EncodeRanges(rngs []index.Range) []byte {
	buflen := binary.MaxVarintLen64 + // Leading len field for number of entries
		2*binary.MaxVarintLen64*len(rngs) // start-end for each entry
	buf := make([]byte, 0, buflen)
	for _, rng := range rngs {
		buf = binary.AppendUvarint(buf, uint64(rng.Start))
		buf = binary.AppendUvarint(buf, uint64(rng.End))
	}
	return buf
}

//func (c BigEndianPostingsOffsetCodec) DecodeRange(b []byte) (index.Range, error) {
//	rng, _, err := decodeRange(b)
//	return rng, err
//}

func (c BigEndianPostingsOffsetCodec) DecodeRanges(b []byte) ([]index.Range, error) {
	decLen, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, errors.New("invalid multi postings offsets encoding")
	}

	rngs := make([]index.Range, decLen)
	startIdx := n

	for i := 0; i < int(decLen); i++ {
		rng, read, err := decodeRange(b[startIdx:])
		if err != nil {
			return nil, errors.New("invalid multi postings offsets encoding")
		}
		rngs[i] = rng
		startIdx += read
	}

	return rngs, nil
}

func decodeRange(b []byte) (index.Range, int, error) {
	rng := index.Range{}
	read := 0

	start, n := binary.Uvarint(b)
	if n <= 0 {
		return rng, n, errors.New("invalid postings offset encoding")
	}
	rng.Start = int64(start)
	read += n

	end, n := binary.Uvarint(b[n:])
	if n <= 0 {
		return rng, n, errors.New("invalid postings offset encoding")
	}
	rng.End = int64(end)
	read += n

	return rng, read, nil
}
