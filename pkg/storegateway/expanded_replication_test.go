// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestMaxTimeExpandedReplication_Eligible(t *testing.T) {
	// Round "now" to the nearest millisecond since we are using millisecond precision
	// for min/max times for the blocks.
	now := time.Now().Round(time.Millisecond)
	replication := NewMaxTimeExpandedReplication(2 * time.Hour)
	replication.now = func() time.Time { return now }

	t.Run("max time outside limit", func(t *testing.T) {
		b := block.Meta{
			BlockMeta: tsdb.BlockMeta{
				MinTime: now.Add(-5 * time.Hour).UnixMilli(),
				MaxTime: now.Add(-4 * time.Hour).UnixMilli(),
			},
		}
		require.False(t, replication.Eligible(&b))
	})

	t.Run("max time on limit", func(t *testing.T) {
		b := block.Meta{
			BlockMeta: tsdb.BlockMeta{
				MinTime: now.Add(-4 * time.Hour).UnixMilli(),
				MaxTime: now.Add(-2 * time.Hour).UnixMilli(),
			},
		}
		require.True(t, replication.Eligible(&b))
	})

	t.Run("max time inside min time outside limit", func(t *testing.T) {
		b := block.Meta{
			BlockMeta: tsdb.BlockMeta{
				MinTime: now.Add(-3 * time.Hour).UnixMilli(),
				MaxTime: now.Add(-time.Hour).UnixMilli(),
			},
		}
		require.True(t, replication.Eligible(&b))
	})

	t.Run("max and min time inside limit", func(t *testing.T) {
		b := block.Meta{
			BlockMeta: tsdb.BlockMeta{
				MinTime: now.Add(-1 * time.Hour).UnixMilli(),
				MaxTime: now.UnixMilli(),
			},
		}
		require.True(t, replication.Eligible(&b))
	})
}
