// SPDX-License-Identifier: AGPL-3.0-only

package block

import (
	"testing"

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
