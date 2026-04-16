// SPDX-License-Identifier: AGPL-3.0-only

package block

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMeta_IsOutOfOrder(t *testing.T) {
	t.Run("regular block", func(t *testing.T) {
		meta := &Meta{
			BlockMeta: tsdb.BlockMeta{ULID: ULID(1), MaxTime: 2000, MinTime: 1000, Version: TSDBVersion1},
			Thanos:    ThanosMeta{},
		}
		require.False(t, meta.IsOutOfOrder())
	})

	t.Run("out of order from compaction hint", func(t *testing.T) {
		meta := &Meta{
			BlockMeta: tsdb.BlockMeta{ULID: ULID(1), MaxTime: 2000, MinTime: 1000, Version: TSDBVersion1},
			Thanos:    ThanosMeta{},
		}
		// Set out-of-order hint explicitly.
		meta.Compaction.SetOutOfOrder()

		require.True(t, meta.IsOutOfOrder())
	})

	t.Run("out of order from external label", func(t *testing.T) {
		meta := &Meta{
			BlockMeta: tsdb.BlockMeta{ULID: ULID(1), MaxTime: 2000, MinTime: 1000, Version: TSDBVersion1},
			Thanos: ThanosMeta{
				Version: ThanosVersion1,
				Labels: map[string]string{
					OutOfOrderExternalLabel: OutOfOrderExternalLabelValue,
				},
			},
		}
		require.True(t, meta.IsOutOfOrder())
	})
}

func TestThanosMeta_SourceOffsets(t *testing.T) {
	newMeta := func(offsets map[int32]int64) Meta {
		return Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID:    ulid.MustNew(1, nil),
				MinTime: 1000,
				MaxTime: 2000,
				Version: TSDBVersion1,
				Compaction: tsdb.BlockMetaCompaction{
					Level:   1,
					Sources: []ulid.ULID{ulid.MustNew(1, nil)},
				},
			},
			Thanos: ThanosMeta{
				Version:       ThanosVersion1,
				Labels:        map[string]string{},
				Source:        BlockBuilderSource,
				SourceOffsets: offsets,
			},
		}
	}

	writeMetaJSON := func(t *testing.T, m Meta) string {
		t.Helper()
		var buf bytes.Buffer
		require.NoError(t, m.Write(&buf))
		return buf.String()
	}

	readMetaJSON := func(t *testing.T, s string) *Meta {
		t.Helper()
		m, err := ReadMeta(io.NopCloser(strings.NewReader(s)))
		require.NoError(t, err)
		return m
	}

	t.Run("round-trip with source offsets", func(t *testing.T) {
		original := newMeta(NewSingleSourceOffset(68, 1500))

		encoded := writeMetaJSON(t, original)
		assert.Contains(t, encoded, `"source_offsets"`)

		decoded := readMetaJSON(t, encoded)
		assert.Equal(t, original.Thanos.SourceOffsets, decoded.Thanos.SourceOffsets)
	})

	t.Run("round-trip with multiple partitions", func(t *testing.T) {
		original := newMeta(map[int32]int64{0: 100, 5: 200, 68: 1500})

		encoded := writeMetaJSON(t, original)
		decoded := readMetaJSON(t, encoded)
		assert.Equal(t, original.Thanos.SourceOffsets, decoded.Thanos.SourceOffsets)
	})

	t.Run("omitted when nil", func(t *testing.T) {
		original := newMeta(nil)

		encoded := writeMetaJSON(t, original)
		assert.NotContains(t, encoded, `"source_offsets"`)

		decoded := readMetaJSON(t, encoded)
		assert.Nil(t, decoded.Thanos.SourceOffsets)
	})

	t.Run("backward compatibility with missing field", func(t *testing.T) {
		original := newMeta(nil)
		encoded := writeMetaJSON(t, original)
		require.NotContains(t, encoded, `"source_offsets"`)

		decoded := readMetaJSON(t, encoded)
		assert.Nil(t, decoded.Thanos.SourceOffsets)
	})
}

func TestMergeSourceOffsets(t *testing.T) {
	metaWithOffsets := func(offsets map[int32]int64) *Meta {
		return &Meta{
			BlockMeta: tsdb.BlockMeta{ULID: ULID(1), MinTime: 1000, MaxTime: 2000, Version: TSDBVersion1},
			Thanos:    ThanosMeta{SourceOffsets: offsets},
		}
	}

	tests := map[string]struct {
		blocks   []*Meta
		expected map[int32]int64
	}{
		"no input blocks": {
			blocks:   nil,
			expected: nil,
		},
		"all inputs have nil offsets": {
			blocks:   []*Meta{metaWithOffsets(nil), metaWithOffsets(nil)},
			expected: nil,
		},
		"single input with offsets": {
			blocks:   []*Meta{metaWithOffsets(map[int32]int64{0: 100})},
			expected: map[int32]int64{0: 100},
		},
		"disjoint partitions": {
			blocks:   []*Meta{metaWithOffsets(map[int32]int64{0: 100}), metaWithOffsets(map[int32]int64{1: 200})},
			expected: map[int32]int64{0: 100, 1: 200},
		},
		"overlapping partition takes max": {
			blocks:   []*Meta{metaWithOffsets(map[int32]int64{0: 100}), metaWithOffsets(map[int32]int64{0: 250})},
			expected: map[int32]int64{0: 250},
		},
		"mix of blocks with and without offsets": {
			blocks:   []*Meta{metaWithOffsets(map[int32]int64{0: 100, 1: 200}), metaWithOffsets(nil)},
			expected: map[int32]int64{0: 100, 1: 200},
		},
		"multiple blocks with overlapping and disjoint partitions": {
			blocks:   []*Meta{metaWithOffsets(map[int32]int64{0: 100, 1: 200}), metaWithOffsets(map[int32]int64{1: 150, 2: 300})},
			expected: map[int32]int64{0: 100, 1: 200, 2: 300},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := MergeSourceOffsets(tc.blocks)
			assert.Equal(t, tc.expected, result)
		})
	}
}
