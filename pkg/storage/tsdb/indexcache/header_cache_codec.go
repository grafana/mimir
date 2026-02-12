package indexcache

import (
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"

	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
)

// PostingsOffsetCacheCodec provides encoding and decoding for PostingsOffsetTableCache values.
//
// Encoding and decoding is strictly to store and retrieve int64 values for index.Range instances.
//
// Implementations must NOT be tied to specific TSDB layout assumptions
// such as the length of Postings entries or relation between consecutive Postings entries -
// This logic is left for the implementations of the PostingOffsetTable interface.
type PostingsOffsetCacheCodec interface {
	EncodePostingsOffsets([]streamindex.PostingListOffset) []byte
	DecodePostingsOffsets([]byte) ([]streamindex.PostingListOffset, error)

	EncodeSingleRange(index.Range) []byte
	DecodeSingleRange([]byte) (index.Range, error)
	EncodeMultiRange([]index.Range) []byte
	DecodeMultiRange([]byte) ([]index.Range, error)
}

type BigEndianPostingsOffsetCodec struct{}

func (c BigEndianPostingsOffsetCodec) EncodePostingsOffsets(offsets []streamindex.PostingListOffset) []byte {
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

func (c BigEndianPostingsOffsetCodec) DecodePostingsOffsets(buf []byte) ([]streamindex.PostingListOffset, error) {
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

func encodeRange(buf []byte, rng index.Range) []byte {
	buf = binary.AppendUvarint(buf, uint64(rng.Start))
	buf = binary.AppendUvarint(buf, uint64(rng.End))
	return buf
}

func (c BigEndianPostingsOffsetCodec) DecodeSingleRange(buf []byte) (index.Range, error) {
	decLen, n := binary.Uvarint(buf)
	if n <= 0 || decLen != 1 {
		// Number of entries should always be 1 for encodings from EncodeSingleRange
		return index.Range{}, errors.New("invalid single postings offsets encoding")
	}

	startIdx := n
	rng, _, err := decodeRange(buf[startIdx:])
	return rng, err
}

func (c BigEndianPostingsOffsetCodec) DecodeMultiRange(buf []byte) ([]index.Range, error) {
	decLen, n := binary.Uvarint(buf)
	if n <= 0 {
		return nil, errors.New("invalid multi postings offsets encoding")
	}

	startIdx := n
	rngs := make([]index.Range, decLen)

	for i := 0; i < int(decLen); i++ {
		rng, read, err := decodeRange(buf[startIdx:])
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
