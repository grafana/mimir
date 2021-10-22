// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/postings_codec.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"bytes"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
)

// This file implements encoding and decoding of postings using diff (or delta) + varint
// number encoding. On top of that, we apply Snappy compression.
//
// On its own, Snappy compressing raw postings doesn't really help, because there is no
// repetition in raw data. Using diff (delta) between postings entries makes values small,
// and Varint is very efficient at encoding small values (values < 128 are encoded as
// single byte, values < 16384 are encoded as two bytes). Diff + varint reduces postings size
// significantly (to about 20% of original), snappy then halves it to ~10% of the original.

const (
	codecHeaderDiffVarintSnappy             = "dvs"   // As in "diff+varint+snappy".
	codecHeaderIndexedDiffVarintSnappy      = "idvs"  // As in "indexed+diff+varint+snappy"
	codecHeaderIndexedSpansDiffVarintSnappy = "isdvs" // As in "indexed+spans+diff+varint+snappy
)

// isDiffVarintSnappyEncodedPostings returns true, if input looks like it has been encoded by diff+varint+snappy codec.
func isDiffVarintSnappyEncodedPostings(input []byte) bool {
	return bytes.HasPrefix(input, []byte(codecHeaderDiffVarintSnappy))
}

// diffVarintSnappyEncode encodes postings into diff+varint representation,
// and applies snappy compression on the result.
// Returned byte slice starts with codecHeaderDiffVarintSnappy header.
// Length argument is expected number of postings, used for preallocating buffer.
func diffVarintSnappyEncode(p index.Postings, length int) ([]byte, error) {
	return snappyEncodePostings(p, length, diffVarintEncodeNoHeader, codecHeaderDiffVarintSnappy)
}

func diffVarintSnappyDecode(input []byte) (index.Postings, error) {
	return snappyDecodePostings(input, func(input []byte) (index.Postings, error) {
		return newDiffVarintPostings(input), nil
	}, codecHeaderDiffVarintSnappy)
}

// diffVarintEncodeNoHeader encodes postings into diff+varint representation.
// It doesn't add any header to the output bytes.
// Length argument is expected number of postings, used for preallocating buffer.
func diffVarintEncodeNoHeader(p index.Postings, length int) ([]byte, error) {
	buf := encoding.Encbuf{}

	// This encoding uses around ~1 bytes per posting, but let's use
	// conservative 1.25 bytes per posting to avoid extra allocations.
	if length > 0 {
		buf.B = make([]byte, 0, 5*length/4)
	}

	prev := uint64(0)
	for p.Next() {
		v := p.At()
		if v < prev {
			return nil, errors.Errorf("postings entries must be in increasing order, current: %d, previous: %d", v, prev)
		}

		// This is the 'diff' part -- compute difference from previous value.
		buf.PutUvarint64(v - prev)
		prev = v
	}
	if p.Err() != nil {
		return nil, p.Err()
	}

	return buf.B, nil
}

func newDiffVarintPostings(input []byte) *diffVarintPostings {
	return &diffVarintPostings{buf: &encoding.Decbuf{B: input}}
}

// diffVarintPostings is an implementation of index.Postings based on diff+varint encoded data.
type diffVarintPostings struct {
	buf *encoding.Decbuf
	cur uint64
}

func (it *diffVarintPostings) At() uint64 {
	return it.cur
}

func (it *diffVarintPostings) Next() bool {
	if it.buf.Err() != nil || it.buf.Len() == 0 {
		return false
	}

	val := it.buf.Uvarint64()
	if it.buf.Err() != nil {
		return false
	}

	it.cur = it.cur + val
	return true
}

func (it *diffVarintPostings) Seek(x uint64) bool {
	if it.cur >= x {
		return true
	}

	// We cannot do any search due to how values are stored,
	// so we simply advance until we find the right value.
	for it.Next() {
		if it.At() >= x {
			return true
		}
	}

	return false
}

func (it *diffVarintPostings) Err() error {
	return it.buf.Err()
}

// isIndexedDiffVarintSnappyEncodedPostings returns true, if input looks like it has been encoded by indexed+diff+varint+snappy codec.
func isIndexedDiffVarintSnappyEncodedPostings(input []byte) bool {
	return bytes.HasPrefix(input, []byte(codecHeaderIndexedDiffVarintSnappy))
}

// indexedDiffVarintSnappyEncode encodes postings into diff+varint representation with an index,
// and applies snappy compression on the result.
// Returned byte slice starts with codecHeaderIndexedDiffVarintSnappy header.
// Length argument is expected number of postings, used for preallocating buffer.
func indexedDiffVarintSnappyEncode(p index.Postings, length int) ([]byte, error) {
	return snappyEncodePostings(p, length, indexedDiffVarintEncodeNoHeader, codecHeaderIndexedDiffVarintSnappy)
}

func indexedDiffVarintSnappyDecode(input []byte) (index.Postings, error) {
	return snappyDecodePostings(input, func(input []byte) (index.Postings, error) {
		return newIndexedDiffVarintPostings(input)
	}, codecHeaderIndexedDiffVarintSnappy)
}

type diffVarintIndexEntry struct {
	offset int
	first  uint64
}

const indexedDiffVarintPageSize = 256
const indexedDiffVarintMaxPages = 4096

// indexedDiffVarintEncodeNoHeader encodes postings into diff+varint representation with an index.
// It doesn't add any header to the output bytes.
// Length argument is expected number of postings, used for preallocating buffer.
// The structure of the output is:
// - varint indexLen
// - (indexLen times):
//   - diffVarint offset (diff with previous nextPage's offset in the slice)
//   - diffVarint first posting (diff with previous pages's first posting)
// diff-varint encoding of each nextPage (offsets above refer to this slice)
func indexedDiffVarintEncodeNoHeader(p index.Postings, length int) ([]byte, error) {
	pageSize := indexedDiffVarintPageSize
	indexCap := 128
	buf := encoding.Encbuf{}

	// This encoding uses around ~1 bytes per posting, but let's use
	// conservative 1.25 bytes per posting to avoid extra allocations.
	if length > 0 {
		if length/pageSize > indexedDiffVarintMaxPages {
			pageSize = length / indexedDiffVarintMaxPages
		}
		buf.B = make([]byte, 0, 5*length/4)
		if length/pageSize > indexCap {
			indexCap = length / pageSize
		}
	}

	index := make([]diffVarintIndexEntry, 0, indexCap)
	count := 0
	prev := uint64(0)
	for p.Next() {
		v := p.At()
		if v < prev {
			return nil, errors.Errorf("postings entries must be in increasing order, current: %d, previous: %d", v, prev)
		}

		// This is the 'diff' part -- compute difference from previous value.
		buf.PutUvarint64(v - prev)
		if count == 0 {
			index = append(index, diffVarintIndexEntry{
				offset: len(buf.B),
				first:  v,
			})
		}

		prev = v

		count++
		if count == pageSize {
			count = 0
		}
	}
	if p.Err() != nil {
		return nil, p.Err()
	}

	indexBuf := encodeDiffVarintIndex(index)
	// TODO: instead of appending here, we can pre-allocate some space in buf.B beforehand, and copy indexBuf instead
	return append(indexBuf.B, buf.B...), nil
}

func newIndexedDiffVarintPostings(input []byte) (*indexedDiffVarintPostings, error) {
	buf := &encoding.Decbuf{B: input}
	index, err := decodeDiffVarintIndex(buf)
	if err != nil {
		return nil, err
	}

	return &indexedDiffVarintPostings{
		buf:   buf,
		raw:   buf.B,
		index: index,
	}, nil
}

// indexedDiffVarintPostings is an implementation of index.Postings based on diff+varint data encoded in index.
type indexedDiffVarintPostings struct {
	buf *encoding.Decbuf
	raw []byte

	page  int
	index []diffVarintIndexEntry

	cur uint64
}

func (it *indexedDiffVarintPostings) At() uint64 {
	return it.cur
}

func (it *indexedDiffVarintPostings) Next() bool {
	if it.buf.Err() != nil || it.buf.Len() == 0 {
		return false
	}

	val := it.buf.Uvarint64()
	if it.buf.Err() != nil {
		return false
	}
	it.cur = it.cur + val

	if next := it.page + 1; next < len(it.index) && it.index[next].first == it.cur {
		it.page = next
	}

	return true
}

func (it *indexedDiffVarintPostings) Seek(x uint64) bool {
	for it.page+1 < len(it.index) && it.index[it.page+1].first < x {
		it.page++
		it.buf.B = it.raw[it.index[it.page].offset:]
		it.cur = it.index[it.page].first
	}

	if it.cur >= x {
		return true
	}

	// We cannot do any search within a nextPage due to how values are stored,
	// so we simply advance until we find the right value.
	for it.Next() {
		if it.At() >= x {
			return true
		}
	}

	return false
}

func (it *indexedDiffVarintPostings) Err() error {
	return it.buf.Err()
}

// isIndexedSpansDiffVarintSnappyEncodedPostings returns true, if input looks like it has been encoded by indexed+spans+diff+varint+snappy codec.
func isIndexedSpansDiffVarintSnappyEncodedPostings(input []byte) bool {
	return bytes.HasPrefix(input, []byte(codecHeaderIndexedSpansDiffVarintSnappy))
}

// indexedSpansDiffVarintSnappyEncode encodes postings into diff+varint representation with an index,
// it summarizes spans of postings with same diff
// and applies snappy compression on the result.
// Returned byte slice starts with codecHeaderIndexedDiffVarintSnappy header.
// Length argument is expected number of postings, used for preallocating buffer.
func indexedSpansDiffVarintSnappyEncode(p index.Postings, length int) ([]byte, error) {
	return snappyEncodePostings(p, length, indexedSpansDiffVarintEncodeNoHeader, codecHeaderIndexedSpansDiffVarintSnappy)
}

func indexedSpansDiffVarintSnappyDecode(input []byte) (index.Postings, error) {
	return snappyDecodePostings(input, func(input []byte) (index.Postings, error) {
		return newIndexedSpansDiffVarintPostings(input)
	}, codecHeaderIndexedSpansDiffVarintSnappy)
}

const indexedSpansDiffVarintPageSize = 256

// indexedSpansDiffVarintEncodeNoHeader encodes postings into diff+varint representation with an index
// it summarizes spans of postings with same diff
// It doesn't add any header to the output bytes.
// Length argument is expected number of postings, used for preallocating buffer.
func indexedSpansDiffVarintEncodeNoHeader(p index.Postings, length int) ([]byte, error) {
	var indexCap = 128
	buf := encoding.Encbuf{}

	// This encoding uses around ~1 bytes per posting, but let's use
	// conservative 1.25 bytes per posting to avoid extra allocations.
	if length > 0 {
		buf.B = make([]byte, 0, 5*length/4)
		if length/indexedSpansDiffVarintPageSize > indexCap {
			indexCap = length / indexedSpansDiffVarintPageSize
		}
	}

	index := make([]diffVarintIndexEntry, 0, indexCap)
	var (
		prev, prevDelta, spanLen uint64
		indexEntrySize           int
	)
	for p.Next() {
		v := p.At()
		if v < prev {
			return nil, errors.Errorf("postings entries must be in increasing order, current: %d, previous: %d", v, prev)
		}
		// count this value towards the index entry size, we'll write an index entry in the first change span after page size have been reached
		indexEntrySize++

		// This is the 'diff' part -- compute difference from previous value.
		delta := v - prev
		if delta == prevDelta {
			spanLen++
		} else {
			if prevDelta > 0 {
				// prevDelta > 0 implies that this is not the first element not first element
				buf.PutUvarint64(spanLen)
			}
			// write index before delta&spanLen, because we'll need to read them
			// this is different from indexed+diff+varint
			if indexEntrySize >= indexedSpansDiffVarintPageSize {
				index = append(index, diffVarintIndexEntry{
					offset: len(buf.B),
					first:  v,
				})
				indexEntrySize = 0
			}
			buf.PutUvarint64(delta)
			prevDelta = delta
			spanLen = 0
		}
		prev = v
	}
	if prevDelta > 0 {
		// prevDelta > 0 implies that we wrote elements
		buf.PutUvarint64(spanLen)
	}

	if p.Err() != nil {
		return nil, p.Err()
	}

	indexBuf := encodeDiffVarintIndex(index)

	return append(indexBuf.B, buf.B...), nil
}

func newIndexedSpansDiffVarintPostings(input []byte) (*indexedSpansDiffVarintPostings, error) {
	buf := &encoding.Decbuf{B: input}
	index, err := decodeDiffVarintIndex(buf)
	if err != nil {
		return nil, err
	}

	return &indexedSpansDiffVarintPostings{
		buf:   buf,
		raw:   buf.B,
		index: index,
	}, nil
}

// indexedSpansDiffVarintPostings is an implementation of index.Postings based on diff+varint data encoded in index.
type indexedSpansDiffVarintPostings struct {
	buf *encoding.Decbuf
	raw []byte

	nextPage int
	index    []diffVarintIndexEntry

	cur   uint64
	delta uint64
	span  uint64
}

func (it *indexedSpansDiffVarintPostings) At() uint64 {
	return it.cur
}

func (it *indexedSpansDiffVarintPostings) Next() bool {
	if it.span > 0 {
		it.cur += it.delta
		it.span--
	} else {
		if it.buf.Err() != nil || it.buf.Len() == 0 {
			return false
		}

		if !it.readDeltaAndSpan() {
			return false
		}

		it.cur = it.cur + it.delta
	}

	if it.nextPage < len(it.index) && it.index[it.nextPage].first == it.cur {
		it.nextPage++
	}

	return true
}

func (it *indexedSpansDiffVarintPostings) readDeltaAndSpan() bool {
	it.delta = it.buf.Uvarint64()
	if it.buf.Err() != nil {
		return false
	}
	it.span = it.buf.Uvarint64()
	if it.buf.Err() != nil {
		return false
	}
	return true
}

func (it *indexedSpansDiffVarintPostings) Seek(x uint64) bool {
	seekPage := -1
	for it.nextPage < len(it.index) && it.index[it.nextPage].first < x {
		seekPage = it.nextPage
		it.nextPage++
	}
	if seekPage >= 0 {
		it.buf.B = it.raw[it.index[seekPage].offset:]
		it.cur = it.index[seekPage].first
		if !it.readDeltaAndSpan() {
			return false
		}
	}

	if it.cur >= x {
		return true
	}

	// We cannot do any search within a nextPage due to how values are stored,
	// so we simply advance until we find the right value.
	for it.Next() {
		if it.At() >= x {
			return true
		}
	}

	return false
}

func (it *indexedSpansDiffVarintPostings) Err() error {
	return it.buf.Err()
}

func snappyEncodePostings(p index.Postings, length int, codec func(p index.Postings, length int) ([]byte, error), codecHeader string) ([]byte, error) {
	buf, err := codec(p, length)
	if err != nil {
		return nil, err
	}

	// Make result buffer large enough to hold our header and compressed block.
	result := make([]byte, len(codecHeader)+snappy.MaxEncodedLen(len(buf)))
	copy(result, codecHeader)

	compressed := snappy.Encode(result[len(codecHeader):], buf)

	// Slice result buffer based on compressed size.
	result = result[:len(codecHeader)+len(compressed)]
	return result, nil
}

func snappyDecodePostings(input []byte, decodec func([]byte) (index.Postings, error), header string) (index.Postings, error) {
	if !bytes.HasPrefix(input, []byte(header)) {
		return nil, errors.New("header not found")
	}

	raw, err := snappy.Decode(nil, input[len(header):])
	if err != nil {
		return nil, errors.Wrap(err, "snappy decode")
	}

	return decodec(raw)
}

func decodeDiffVarintIndex(buf *encoding.Decbuf) ([]diffVarintIndexEntry, error) {
	indexLen := buf.Uvarint()
	if err := buf.Err(); err != nil {
		return nil, errors.Wrap(err, "reading index len")
	}
	index := make([]diffVarintIndexEntry, indexLen)
	prevOffset, prevFirst := 0, uint64(0)
	for i := 0; i < indexLen; i++ {
		index[i].offset = buf.Uvarint() + prevOffset
		if err := buf.Err(); err != nil {
			return nil, errors.Wrapf(err, "reading nextPage offset %d", i)
		}
		prevOffset = index[i].offset

		index[i].first = buf.Uvarint64() + prevFirst
		if err := buf.Err(); err != nil {
			return nil, errors.Wrapf(err, "reading nextPage value %d", i)
		}
		prevFirst = index[i].first
	}
	return index, nil
}

func encodeDiffVarintIndex(index []diffVarintIndexEntry) encoding.Encbuf {
	indexBuf := encoding.Encbuf{
		B: make(
			[]byte,
			0,
			2 /*len(index)*/ +
				2 /*offset*/ *len(index)+
				8 /*first*/ *len(index),
		),
	}
	indexBuf.PutUvarint(len(index))
	prevOffset, prevFirst := 0, uint64(0)
	for i := range index {
		indexBuf.PutUvarint(index[i].offset - prevOffset)
		prevOffset = index[i].offset
		indexBuf.PutUvarint64(index[i].first - prevFirst)
		prevFirst = index[i].first
	}
	return indexBuf
}
