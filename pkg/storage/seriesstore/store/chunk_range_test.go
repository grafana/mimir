// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"testing"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// An engine's chunk range is its head's block range: the head compacts once it spans one and a half, and a series'
// chunks end at its boundaries.
func TestEngineChunkRange(t *testing.T) {
	for _, c := range []struct {
		rangeMs              int64
		compactable          bool
		headMinTime, headEnd int64
	}{
		{0, true, 4 * 3_600_000, 6 * 3_600_000},
		{24 * 3_600_000, false, 0, 24 * 3_600_000},
	} {
		e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 1, SecondaryHashFunction: secondaryHash, ChunkRangeMs: c.rangeMs})
		require.NoError(t, err)
		t.Cleanup(func() { _ = e.Close() })
		lbls := promlabels.FromStrings(model.MetricNameLabel, "requests_total")
		app := e.AppenderV2(context.Background())
		ref := storage.SeriesRef(0)
		// Five hours of a sample every half an hour, too few for a chunk's end to be estimated from its samples.
		for at := int64(0); at <= 5*3_600_000; at += 1_800_000 {
			ref, err = app.Append(ref, lbls, 0, at, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())

		require.Equal(t, c.compactable, e.compactable(), "range %d", c.rangeMs)
		state := e.store.shards[0]
		state.RLock()
		var headMinTime, headEnd int64
		if tenant, ok := e.tenantLocked(0); ok {
			if entry, ok := e.lookupLocked(tenant, uint64(ref)); ok {
				headMinTime, headEnd = entry.series.floatHead.minTime, entry.series.floatHead.nextAt
			}
		}
		state.RUnlock()
		require.Equal(t, [2]int64{c.headMinTime, c.headEnd}, [2]int64{headMinTime, headEnd}, "range %d", c.rangeMs)
	}
}
