// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/util_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"crypto/rand"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
)

func TestHashBlockID(t *testing.T) {
	tests := []struct {
		first         ulid.ULID
		second        ulid.ULID
		expectedEqual bool
	}{
		{
			first:         ulid.MustNew(10, nil),
			second:        ulid.MustNew(10, nil),
			expectedEqual: true,
		},
		{
			first:         ulid.MustNew(10, nil),
			second:        ulid.MustNew(20, nil),
			expectedEqual: false,
		},
		{
			first:         ulid.MustNew(10, rand.Reader),
			second:        ulid.MustNew(10, rand.Reader),
			expectedEqual: false,
		},
	}

	for _, testCase := range tests {
		firstHash := HashBlockID(testCase.first)
		secondHash := HashBlockID(testCase.second)
		assert.Equal(t, testCase.expectedEqual, firstHash == secondHash)
	}
}

func TestChunkRefEncode(t *testing.T) {
	tests := []struct {
		segment     uint32
		offset      uint32
		expectedRef chunks.ChunkRef
	}{
		{
			segment:     0,
			offset:      0,
			expectedRef: chunks.ChunkRef(0),
		},
		{
			segment:     0xab,
			offset:      0xcd,
			expectedRef: chunks.ChunkRef(0x000000ab000000cd),
		},
		{
			segment:     0xffffffff,
			offset:      0xffffffff,
			expectedRef: chunks.ChunkRef(0xffffffffffffffff),
		},
	}

	for _, testCase := range tests {
		ref := ChunkRefEncode(testCase.segment, testCase.offset)
		assert.Equal(t, testCase.expectedRef, ref)
	}
}

func TestChunkRefDecode(t *testing.T) {
	tests := []struct {
		ref         chunks.ChunkRef
		expectedSeg uint32
		expectedOff uint32
	}{
		{
			ref:         chunks.ChunkRef(0),
			expectedSeg: 0,
			expectedOff: 0,
		},
		{
			ref:         chunks.ChunkRef(0x000000ab000000cd),
			expectedSeg: 0xab,
			expectedOff: 0xcd,
		},
		{
			ref:         chunks.ChunkRef(0xffffffffffffffff),
			expectedSeg: 0xffffffff,
			expectedOff: 0xffffffff,
		},
	}

	for _, testCase := range tests {
		seg, off := ChunkRefDecode(testCase.ref)
		assert.Equal(t, testCase.expectedSeg, seg)
		assert.Equal(t, testCase.expectedOff, off)
	}
}

func TestChunkRefEncodeDecode(t *testing.T) {
	segment := uint32(0x12345678)
	offset := uint32(0x0fedcba9)
	ref := chunks.ChunkRef(0x1234567890abcdef)

	t.Run("encode decode", func(t *testing.T) {
		ref := ChunkRefEncode(segment, offset)
		seg, off := ChunkRefDecode(ref)
		assert.Equal(t, segment, seg)
		assert.Equal(t, offset, off)
	})

	t.Run("decode encode", func(t *testing.T) {
		seg, off := ChunkRefDecode(ref)
		newRef := ChunkRefEncode(seg, off)
		assert.Equal(t, ref, newRef)
	})
}
