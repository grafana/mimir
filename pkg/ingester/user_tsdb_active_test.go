// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
)

func TestNativeActiveSeriesUsesLastPurgeCutoff(t *testing.T) {
	now := time.Unix(3600, 0)
	cfg := Config{}
	cfg.ActiveSeriesMetrics.Enabled = true
	cfg.ActiveSeriesMetrics.IdleTimeout = time.Hour
	head := &testNativeActiveHead{}
	db := &userTSDB{cfg: &cfg, nativeActive: head}

	require.Equal(t, 3, db.activeSeriesCounts(now).Total)
	require.Equal(t, now.Add(-time.Hour), head.cutoff)

	// A count at an earlier time must not bring back already expired series.
	require.Equal(t, 3, db.activeSeriesCounts(now.Add(-time.Minute)).Total)
	require.Equal(t, now.Add(-time.Hour), head.cutoff)

	db.activeRefs(now.Add(time.Minute))
	require.Equal(t, now.Add(-time.Hour), head.cutoff)
}

type testNativeActiveHead struct {
	activeSeriesHead
	cutoff time.Time
}

func (h *testNativeActiveHead) ActiveSeries(cutoff time.Time) activeCounts {
	h.cutoff = cutoff
	return activeCounts{Total: 3}
}

func (h *testNativeActiveHead) ActiveRefs(cutoff time.Time) activeseries.ActiveRefs {
	h.cutoff = cutoff
	return nil
}
