// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"math"
	"runtime"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
)

// A count walk holds the refs of the series it resolved, over several sources each series gathered
// by hash, and the groups and hashes it counts. The tenant's memory limit bounds all of them.
func TestIngester_SeriesCounts_TenantMemoryLimit(t *testing.T) {
	const minute = int64(time.Minute / time.Millisecond)
	const hour = 60 * minute
	a1 := labels.FromStrings(model.MetricNameLabel, "a", "pod", "1")
	a2 := labels.FromStrings(model.MetricNameLabel, "a", "pod", "2")
	b1 := labels.FromStrings(model.MetricNameLabel, "b", "pod", "1")

	// Two series in a local block and two in the head, one of them in both.
	const refs, gathered = 4, 3
	counts := func(t *testing.T, maxMemoryBytes uint64, req *client.SeriesCountsRequest) (map[string]int, error) {
		limits := defaultLimitsTestConfig()
		limits.CardinalityEstimateMaxMemoryBytes = maxMemoryBytes
		i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", prometheus.NewRegistry())
		require.NoError(t, err)
		startAndWaitHealthy(t, i, r)
		ctx := user.InjectOrgID(context.Background(), userID)
		push := func(lset labels.Labels, ts int64) {
			_, err := i.Push(ctx, mockWriteRequest(t, lset, 1, ts))
			require.NoError(t, err)
		}
		push(a1, 10*minute)
		push(b1, 10*minute)
		i.compactBlocks(context.Background(), true, math.MaxInt64, nil)
		require.Len(t, i.getTSDB(userID).Blocks(), 1)
		push(a1, 3*hour)
		push(a2, 3*hour)

		resp, _, err := i.seriesCounts(ctx, req)
		if err != nil {
			return nil, err
		}
		out := map[string]int{}
		for _, g := range resp.Groups {
			out[g.Value] = len(g.Counts)
		}
		return out, nil
	}
	requireRefusedByMemory := func(t *testing.T, err error) {
		require.Error(t, err)
		assert.Equal(t, cardinality.LimitMemory, cardinality.RefusedBy(err))
	}

	whole := func() *client.SeriesCountsRequest {
		return &client.SeriesCountsRequest{StartMs: 0, EndMs: 4 * hour}
	}
	held := uint64(refs*seriesRefBytes + gathered*gatheredSeriesBytes)
	// Groups a and b, each with a one-byte value.
	groups := uint64(2 * (countedGroupBytes + 1))

	t.Run("no limit", func(t *testing.T) {
		got, err := counts(t, 0, whole())
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"a": 1, "b": 1}, got)
	})
	t.Run("what the walk holds fits", func(t *testing.T) {
		got, err := counts(t, held+groups, whole())
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"a": 1, "b": 1}, got)
	})
	t.Run("the last group counted does not fit", func(t *testing.T) {
		_, err := counts(t, held+groups-1, whole())
		requireRefusedByMemory(t, err)
	})
	t.Run("the last series gathered does not fit", func(t *testing.T) {
		_, err := counts(t, held-1, whole())
		requireRefusedByMemory(t, err)
	})
	t.Run("the refs do not fit", func(t *testing.T) {
		_, err := counts(t, refs*seriesRefBytes-1, whole())
		requireRefusedByMemory(t, err)
	})
	t.Run("a walk over the head alone holds refs and what it counts", func(t *testing.T) {
		req := &client.SeriesCountsRequest{StartMs: 2 * hour, EndMs: 4 * hour}
		fit := uint64(2*seriesRefBytes + countedGroupBytes + 1)
		got, err := counts(t, fit, req)
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"a": 1}, got)
		_, err = counts(t, fit-1, req)
		requireRefusedByMemory(t, err)
	})
	t.Run("asked for hashes, each hash and its group are held", func(t *testing.T) {
		req := &client.SeriesCountsRequest{StartMs: 2 * hour, EndMs: 4 * hour, Hashes: true}
		fit := uint64(2*seriesRefBytes + cardinality.GroupBytes + 1 + 2*cardinality.HashBytes)
		_, err := counts(t, fit, req)
		require.NoError(t, err)
		_, err = counts(t, fit-1, req)
		requireRefusedByMemory(t, err)
	})
}

// heapBytesPer returns the heap bytes build keeps live per item, building n of them.
func heapBytesPer(n int, build func(n int) any) float64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	kept := build(n)
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(kept)
	return float64(after.HeapAlloc-before.HeapAlloc) / float64(n)
}

// BenchmarkSeriesCountsWalk_HeldBytes measures what a count walk holds per series, the numbers
// behind gatheredSeriesBytes and seriesRefBytes: run it with -benchtime=1x.
func BenchmarkSeriesCountsWalk_HeldBytes(b *testing.B) {
	const n = 1_000_000
	for name, build := range map[string]func(n int) any{
		// As walkSources gathers a series.
		"gathered series": func(n int) any {
			byHash := map[uint64]string{}
			value := "value"
			for i := range n {
				// Spread the keys as hashes are.
				h := uint64(i) * 0x9E3779B97F4A7C15
				byHash[h] = value
			}
			return byHash
		},
		// As expandPostingsUpTo appends them.
		"series ref": func(n int) any {
			var refs []storage.SeriesRef
			for i := range n {
				refs = append(refs, storage.SeriesRef(i))
			}
			return refs
		},
	} {
		b.Run(name, func(b *testing.B) {
			var perItem float64
			for range b.N {
				perItem = heapBytesPer(n, build)
			}
			b.ReportMetric(perItem, "held-B/series")
		})
	}
}
