// SPDX-License-Identifier: AGPL-3.0-only

package block

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
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

func TestMeta_Clone(t *testing.T) {
	meta := &Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    ULID(1),
			MaxTime: 2000,
			MinTime: 1000,
			Version: TSDBVersion1,
		},
		Thanos: ThanosMeta{
			Labels:       map[string]string{"foo": "bar"},
			Files:        []File{{RelPath: "a", SizeBytes: 1}},
			SegmentFiles: []string{"000001"},
		},
	}
	meta.Compaction.Sources = []ulid.ULID{ULID(2), ULID(3)}

	clone := meta.Clone()
	require.Equal(t, meta, clone)
	require.NotSame(t, meta, clone)

	// Mutate to ensure it's a deep copy
	clone.Thanos.Labels["foo"] = "baz"
	require.Equal(t, "bar", meta.Thanos.Labels["foo"])

	clone.Compaction.Sources[0] = ULID(4)
	require.Equal(t, ULID(2), meta.Compaction.Sources[0])

	clone.Thanos.Files[0].SizeBytes = 2
	require.Equal(t, int64(1), meta.Thanos.Files[0].SizeBytes)

	clone.Thanos.SegmentFiles[0] = "000002"
	require.Equal(t, "000001", meta.Thanos.SegmentFiles[0])
}
