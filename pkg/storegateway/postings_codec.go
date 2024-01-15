// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/postings_codec.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/dennwc/varint"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storegateway/indexcache"
)

// This file implements encoding and decoding of postings using diff (or delta) + varint
// number encoding. On top of that, we apply Snappy compression.
//
// On its own, Snappy compressing raw postings doesn't really help, because there is no
// repetition in raw data. Using diff (delta) between postings entries makes values small,
// and Varint is very efficient at encoding small values (values < 128 are encoded as
// single byte, values < 16384 are encoded as two bytes). Diff + varint reduces postings size
// significantly (to about 20% of original), snappy then halves it to ~10% of the original.

type codec string

const (
	codecHeaderSnappy             codec = "dvs" // As in "diff+varint+snappy".
	codecHeaderSnappyWithMatchers codec = "dm"  // As in "dvs+matchers"
)

// diffVarintSnappyEncode encodes postings into diff+varint representation,
// and applies snappy compression on the result.
// Returned byte slice starts with codecHeaderSnappy header.
// Length argument is expected number of postings, used for preallocating buffer.
func diffVarintSnappyEncode(p index.Postings, length int) ([]byte, error) {
	buf, err := diffVarintEncodeNoHeader(p, length)
	if err != nil {
		return nil, err
	}

	// Make result buffer large enough to hold our header and compressed block.
	result := make([]byte, len(codecHeaderSnappy)+snappy.MaxEncodedLen(len(buf)))
	copy(result, codecHeaderSnappy)

	compressed := snappy.Encode(result[len(codecHeaderSnappy):], buf)

	// Slice result buffer based on compressed size.
	result = result[:len(codecHeaderSnappy)+len(compressed)]
	return result, nil
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

	prev := storage.SeriesRef(0)
	for p.Next() {
		v := p.At()
		if v < prev {
			return nil, errors.Errorf("postings entries must be in increasing order, current: %d, previous: %d", v, prev)
		}

		// This is the 'diff' part -- compute difference from previous value.
		buf.PutUvarint64(uint64(v - prev))
		prev = v
	}
	if p.Err() != nil {
		return nil, p.Err()
	}

	return buf.B, nil
}

// isDiffVarintSnappyEncodedPostings returns true, if input looks like it has been encoded by diff+varint+snappy+matchers codec.
func isDiffVarintSnappyWithMatchersEncodedPostings(input []byte) bool {
	return bytes.HasPrefix(input, []byte(codecHeaderSnappyWithMatchers))
}

// diffVarintSnappyWithMatchersEncode encodes postings into snappy-encoded diff+varint representation,
// prepended with any pending matchers to the result.
// Returned byte slice starts with codecHeaderSnappyWithMatchers header.
// Length argument is expected number of postings, used for preallocating buffer.
func diffVarintSnappyWithMatchersEncode(p index.Postings, length int, requestMatchers indexcache.LabelMatchersKey, pendingMatchers []*labels.Matcher) ([]byte, error) {
	varintPostings, err := diffVarintEncodeNoHeader(p, length)
	if err != nil {
		return nil, err
	}

	// Estimate sizes
	reqMatchersLen := varint.UvarintSize(uint64(len(requestMatchers))) + len(requestMatchers)
	estPendingMatchersLen := encodedMatchersLen(pendingMatchers)
	codecLen := len(codecHeaderSnappyWithMatchers)

	// Preallocate buffer
	result := make([]byte, codecLen+reqMatchersLen+estPendingMatchersLen+snappy.MaxEncodedLen(len(varintPostings)))

	// Codec
	copy(result, codecHeaderSnappyWithMatchers)
	offset := codecLen

	// Request matchers
	offset += binary.PutUvarint(result[offset:], uint64(len(requestMatchers)))
	offset += copy(result[offset:], requestMatchers)

	// Pending matchers size + matchers
	actualMatchersLen, err := encodeMatchers(estPendingMatchersLen, pendingMatchers, result[offset:])
	if err != nil {
		return nil, err
	}
	if actualMatchersLen != estPendingMatchersLen {
		return nil, fmt.Errorf("encoding matchers wrote unexpected number of bytes: wrote %d, expected %d", actualMatchersLen, estPendingMatchersLen)
	}
	offset += actualMatchersLen

	// Compressed postings
	compressedPostings := snappy.Encode(result[offset:], varintPostings)
	offset += len(compressedPostings)

	return result[:offset], nil
}

// encodeMatchers needs to be called with the precomputed length of the encoded matchers from encodedMatchersLen
func encodeMatchers(expectedLen int, matchers []*labels.Matcher, dest []byte) (written int, _ error) {
	if len(dest) < expectedLen {
		return 0, fmt.Errorf("too small buffer to encode matchers: need at least %d, got %d", expectedLen, dest)
	}
	written += binary.PutUvarint(dest, uint64(len(matchers)))
	for _, m := range matchers {
		written += binary.PutUvarint(dest[written:], uint64(len(m.Name)))
		written += copy(dest[written:], m.Name)

		dest[written] = byte(m.Type)
		written++

		written += binary.PutUvarint(dest[written:], uint64(len(m.Value)))
		written += copy(dest[written:], m.Value)
	}
	return written, nil
}

func encodedMatchersLen(matchers []*labels.Matcher) int {
	matchersLen := varint.UvarintSize(uint64(len(matchers)))
	for _, m := range matchers {
		matchersLen += varint.UvarintSize(uint64(len(m.Name)))
		matchersLen += len(m.Name)
		matchersLen++ // 1 byte for the type
		matchersLen += varint.UvarintSize(uint64(len(m.Value)))
		matchersLen += len(m.Value)
	}
	return matchersLen
}

func diffVarintSnappyMatchersDecode(input []byte) (index.Postings, indexcache.LabelMatchersKey, []*labels.Matcher, error) {
	if !isDiffVarintSnappyWithMatchersEncodedPostings(input) {
		return nil, "", nil, errors.New(string(codecHeaderSnappyWithMatchers) + " header not found")
	}

	offset := len(codecHeaderSnappyWithMatchers)

	requestMatchersKeyLen, requestMatchersKeyLenSize := varint.Uvarint(input[offset:])
	offset += requestMatchersKeyLenSize
	requestMatchersKey := indexcache.LabelMatchersKey(input[offset : uint64(offset)+requestMatchersKeyLen])
	offset += int(requestMatchersKeyLen)

	pendingMatchers, pendingMatchersLen, err := decodeMatchers(input[offset:])
	if err != nil {
		return nil, "", nil, errors.Wrap(err, "decoding request matchers")
	}
	offset += pendingMatchersLen
	raw, err := snappy.Decode(nil, input[offset:])
	if err != nil {
		return nil, "", nil, errors.Wrap(err, "snappy decode")
	}

	return newDiffVarintPostings(raw), requestMatchersKey, pendingMatchers, nil
}

func decodeMatchers(src []byte) ([]*labels.Matcher, int, error) {
	initialLength := len(src)
	numMatchers, numMatchersLen := varint.Uvarint(src)
	src = src[numMatchersLen:]
	if numMatchers == 0 {
		return nil, numMatchersLen, nil
	}
	matchers := make([]*labels.Matcher, 0, numMatchers)

	for i := uint64(0); i < numMatchers; i++ {
		n, nLen := varint.Uvarint(src)
		src = src[nLen:]
		// We should copy the string so that we don't retain a reference to the original slice, which may be large.
		labelName := string(src[:n])
		src = src[n:]

		typ := labels.MatchType(src[0])
		src = src[1:]

		n, nLen = varint.Uvarint(src)
		src = src[nLen:]
		// We should copy the string so that we don't retain a reference to the original slice, which may be large.
		value := string(src[:n])
		src = src[n:]

		m, err := labels.NewMatcher(typ, labelName, value)
		if err != nil {
			return nil, 0, err
		}
		matchers = append(matchers, m)
	}

	return matchers, initialLength - len(src), nil
}

func newDiffVarintPostings(input []byte) *diffVarintPostings {
	return &diffVarintPostings{
		buf:   &encoding.Decbuf{B: input},
		bytes: input,
	}
}

// diffVarintPostings is an implementation of index.Postings based on diff+varint encoded data.
type diffVarintPostings struct {
	buf   *encoding.Decbuf
	bytes []byte
	cur   storage.SeriesRef
}

func (it *diffVarintPostings) At() storage.SeriesRef {
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

	it.cur = it.cur + storage.SeriesRef(val)
	return true
}

func (it *diffVarintPostings) Seek(x storage.SeriesRef) bool {
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

func (it *diffVarintPostings) Reset() {
	it.buf = &encoding.Decbuf{B: it.bytes}
}
