// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/io_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"bytes"
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestByteRanges_contiguous(t *testing.T) {
	tests := []struct {
		ranges   byteRanges
		expected bool
	}{
		{
			ranges:   nil,
			expected: true,
		}, {
			ranges:   byteRanges{{offset: 10, length: 5}},
			expected: true,
		}, {
			ranges:   byteRanges{{offset: 10, length: 5}, {offset: 15, length: 3}, {offset: 18, length: 2}},
			expected: true,
		}, {
			ranges:   byteRanges{{offset: 10, length: 3}, {offset: 15, length: 3}, {offset: 18, length: 2}},
			expected: false,
		}, {
			ranges:   byteRanges{{offset: 10, length: 5}, {offset: 15, length: 3}, {offset: 19, length: 2}},
			expected: false,
		},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.ranges.areContiguous())
	}
}

func TestReadByteRanges(t *testing.T) {
	tests := map[string]struct {
		src          []byte
		ranges       byteRanges
		expectedRead []byte
		expectedErr  error
	}{
		"no ranges": {
			src:          []byte(""),
			ranges:       nil,
			expectedRead: nil,
		},
		"single range with offset == 0": {
			src:          []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges:       []byteRange{{offset: 0, length: 21}},
			expectedRead: []byte("ABCDEFGHILMNOPQRSTUVZ"),
		},
		"single range with offset > 0": {
			src:          []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges:       []byteRange{{offset: 10, length: 11}},
			expectedRead: []byte("MNOPQRSTUVZ"),
		},
		"multiple contiguous ranges with first offset == 0": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 0, length: 10},
				{offset: 10, length: 10},
				{offset: 20, length: 1},
			},
			expectedRead: []byte("ABCDEFGHILMNOPQRSTUVZ"),
		},
		"multiple contiguous ranges with first offset > 0": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 5, length: 5},
				{offset: 10, length: 10},
				{offset: 20, length: 1},
			},
			expectedRead: []byte("FGHILMNOPQRSTUVZ"),
		},
		"multiple non-contiguous ranges": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 0, length: 3},
				{offset: 10, length: 5},
				{offset: 16, length: 1},
				{offset: 20, length: 1},
			},
			expectedRead: []byte("ABCMNOPQSZ"),
		},
		"discard bytes before the first range": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 5, length: 16},
			},
			expectedRead: []byte("FGHILMNOPQRSTUVZ"),
		},
		"discard bytes after the last range": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 0, length: 16},
			},
			expectedRead: []byte("ABCDEFGHILMNOPQR"),
		},
		"unexpected EOF while discarding bytes": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 0, length: 16},
				{offset: 25, length: 5},
			},
			expectedErr: io.ErrUnexpectedEOF,
		},
		"unexpected EOF while reading byte range": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 20, length: 10},
				{offset: 40, length: 10},
			},
			expectedErr: io.ErrUnexpectedEOF,
		},
		"unexpected EOF at the beginning of a byte range": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 0, length: 10},
				{offset: 20, length: 1},
				{offset: 21, length: 10},
			},
			expectedErr: io.ErrUnexpectedEOF,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := make([]byte, 0, 1024)
			actual, err := readByteRanges(bytes.NewReader(testData.src), actual, testData.ranges)

			if testData.expectedErr != nil {
				assert.Error(t, err)
				assert.Equal(t, true, errors.Is(err, testData.expectedErr))
			} else {
				assert.NoError(t, err)
				assert.Equal(t, testData.expectedRead, actual)
			}
		})
	}
}
