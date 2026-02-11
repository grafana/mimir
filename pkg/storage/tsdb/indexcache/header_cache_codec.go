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
	EncodeSingleRange(index.Range) []byte
	DecodeSingleRange([]byte) (index.Range, error)
	EncodeMultiRange([]index.Range) []byte
	DecodeMultiRange([]byte) ([]index.Range, error)
}

type BigEndianPostingsOffsetCodec struct{}

func (c BigEndianPostingsOffsetCodec) EncodeSingleRange(rng index.Range) []byte {
	buflen := binary.MaxVarintLen64 + // Leading len field for number of entries - always 1 in this case
		2*binary.MaxVarintLen64 // start + end integers for one entry

	buf := make([]byte, 0, buflen)
	buf = binary.AppendUvarint(buf, uint64(1))

	return encodeRange(buf, rng)
}

func (c BigEndianPostingsOffsetCodec) EncodeMultiRange(rngs []index.Range) []byte {
	buflen := binary.MaxVarintLen64 + // Leading len field for number of entries
		2*binary.MaxVarintLen64*len(rngs) // start + end integers for each entry

	buf := make([]byte, 0, buflen)
	buf = binary.AppendUvarint(buf, uint64(len(rngs)))

	for _, rng := range rngs {
		buf = encodeRange(buf, rng)
	}
	return buf
}

func encodeRange(b []byte, rng index.Range) []byte {
	b = binary.AppendUvarint(b, uint64(rng.Start))
	b = binary.AppendUvarint(b, uint64(rng.End))
	return b
}

func (c BigEndianPostingsOffsetCodec) DecodeSingleRange(b []byte) (index.Range, error) {
	decLen, n := binary.Uvarint(b)
	if n <= 0 || decLen != 1 {
		// Number of entries should always be 1 for encodings from EncodeSingleRange
		return index.Range{}, errors.New("invalid single postings offsets encoding")
	}

	startIdx := n
	rng, _, err := decodeRange(b[startIdx:])
	return rng, err
}

func (c BigEndianPostingsOffsetCodec) DecodeMultiRange(b []byte) ([]index.Range, error) {
	decLen, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, errors.New("invalid multi postings offsets encoding")
	}

	startIdx := n
	rngs := make([]index.Range, decLen)

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

// decodeRange decodes a single start + end integer pair to an index.Range.
// This should only be called after decoding the leading integer
// used to indicate the number of entries in the full list.
// DecodeSingleRange and DecodeMultiRange handle this before calling.
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
