// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
)

// A head compaction that lands while countSources opens its sources. Listing
// the blocks before opening the head would miss the cut range: the new block
// is not in the list, and the head has already dropped it. countSources opens
// the head first, so the truncation waits for the request, or the new block
// is listed.
func TestIngester_SeriesCounts_HeadCompactionBetweenReads(t *testing.T) {
	const minute = int64(time.Minute / time.Millisecond)
	const hour = 60 * minute

	a1 := labels.FromStrings(model.MetricNameLabel, "a", "pod", "1")
	b1 := labels.FromStrings(model.MetricNameLabel, "b", "pod", "1")

	setup := func(t *testing.T) (*Ingester, context.Context) {
		i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
		ctx := user.InjectOrgID(context.Background(), userID)
		push := func(lset labels.Labels, ts int64) {
			_, err := i.Push(ctx, mockWriteRequest(t, lset, 1, ts))
			require.NoError(t, err)
		}
		// b{pod=1} only before 2h; a{pod=1} before and after. The head spans
		// 10m to 4h, more than 1.5 block ranges, so db.Compact cuts [0, 2h).
		push(a1, 10*minute)
		push(b1, 10*minute)
		push(a1, 4*hour)
		require.Empty(t, i.getTSDB(userID).Blocks())
		return i, ctx
	}
	countsOf := func(resp *client.SeriesCountsResponse) map[string][]int64 {
		out := map[string][]int64{}
		for _, g := range resp.Groups {
			out[g.Value] = g.Counts
		}
		return out
	}
	req := &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 5 * hour}
	want := map[string][]int64{"a": {1}, "b": {1}}

	t.Run("control: compaction before the request", func(t *testing.T) {
		i, ctx := setup(t)
		require.NoError(t, i.getTSDB(userID).Compact())
		require.Len(t, i.getTSDB(userID).Blocks(), 1)
		resp, _, err := i.seriesCounts(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, want, countsOf(resp))
	})

	t.Run("compaction between the head read and the block list", func(t *testing.T) {
		i, ctx := setup(t)
		db := i.getTSDB(userID)
		done := make(chan error, 1)
		countSourcesBetweenReadsHook = func() {
			countSourcesBetweenReadsHook = nil
			go func() { done <- db.Compact() }()
			// Wait for the new block to be listed (reloadBlocks), then give
			// the head truncation up to 3s. It finishes at once when no head
			// reader is registered yet; it waits for us when one is.
			require.Eventually(t, func() bool { return len(db.Blocks()) == 1 }, 5*time.Second, time.Millisecond)
			select {
			case err := <-done:
				require.NoError(t, err)
				done <- nil
			case <-time.After(3 * time.Second):
			}
		}
		t.Cleanup(func() { countSourcesBetweenReadsHook = nil })

		resp, _, err := i.seriesCounts(ctx, req)
		require.NoError(t, err)
		require.NoError(t, <-done)
		assert.Equal(t, want, countsOf(resp), "b{pod=1} lives only in the cut range")
	})
}

// An out-of-order head compaction that lands while a count runs. It writes
// the head's out-of-order chunks to a new block and drops them from the head
// without waiting for the count, which holds no out-of-order isolation. The
// count walks the blocks that appeared since it listed them, so a series
// whose only samples in the window were out of order is still counted, once.
func TestIngester_SeriesCounts_OutOfOrderHeadCompactionDuringTheWalk(t *testing.T) {
	const minute = int64(time.Minute / time.Millisecond)

	a := labels.FromStrings(model.MetricNameLabel, "a")
	b := labels.FromStrings(model.MetricNameLabel, "b")
	c := labels.FromStrings(model.MetricNameLabel, "c")

	compactOutOfOrder := func(t *testing.T, i *Ingester) {
		db := i.getTSDB(userID)
		blocks := len(db.Blocks())
		require.NoError(t, db.db.CompactOOOHead(context.Background()))
		require.Len(t, db.Blocks(), blocks+1)
	}
	// In the first 20m, a has an in-order and an out-of-order sample and b
	// only an out-of-order one. With earlierBlock, c's only sample there is
	// in a block the count lists, so the count walks several sources.
	setup := func(t *testing.T, earlierBlock bool) (*Ingester, context.Context) {
		limits := defaultLimitsTestConfig()
		limits.OutOfOrderTimeWindow = model.Duration(time.Hour)
		i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", prometheus.NewRegistry())
		require.NoError(t, err)
		startAndWaitHealthy(t, i, r)
		ctx := user.InjectOrgID(context.Background(), userID)
		push := func(lset labels.Labels, ts int64) {
			_, err := i.Push(ctx, mockWriteRequest(t, lset, 1, ts))
			require.NoError(t, err)
		}
		push(a, 15*minute)
		push(b, 60*minute)
		push(a, 5*minute)
		if earlierBlock {
			push(c, 60*minute)
			push(c, 8*minute)
			compactOutOfOrder(t, i)
		}
		push(b, 10*minute)
		return i, ctx
	}
	// An answer with hashes has no counts.
	check := func(t *testing.T, req *client.SeriesCountsRequest, earlierBlock bool, resp *client.SeriesCountsResponse) {
		names := []string{"a", "b"}
		if earlierBlock {
			names = append(names, "c")
		}
		want := map[string][]int64{}
		for _, name := range names {
			want[name] = []int64{1}
			if req.Hashes {
				want[name] = nil
			}
		}
		got := map[string][]int64{}
		for _, g := range resp.Groups {
			got[g.Value] = g.Counts
		}
		assert.Equal(t, want, got)
		assert.Equal(t, int64(len(names)), resp.SeriesCounted)
	}

	for name, req := range map[string]*client.SeriesCountsRequest{
		"counts":      {StartTimestampMs: 0, EndTimestampMs: 20 * minute},
		"with hashes": {StartTimestampMs: 0, EndTimestampMs: 20 * minute, Hashes: true, SketchAbove: 1_000_000},
		"per bucket":  {StartTimestampMs: 0, EndTimestampMs: 20 * minute, StepMs: 20 * minute},
	} {
		t.Run(name, func(t *testing.T) {
			t.Run("compaction between the head read and the block list", func(t *testing.T) {
				i, ctx := setup(t, false)
				countSourcesBetweenReadsHook = func() {
					countSourcesBetweenReadsHook = nil
					compactOutOfOrder(t, i)
				}
				t.Cleanup(func() { countSourcesBetweenReadsHook = nil })
				resp, _, err := i.seriesCounts(ctx, req)
				require.NoError(t, err)
				check(t, req, false, resp)
			})

			// b's only sample in the window moves to a block the count did not list.
			for name, earlierBlock := range map[string]bool{
				"compaction after the block list, head only":          false,
				"compaction after the block list, head and one block": true,
			} {
				t.Run(name, func(t *testing.T) {
					i, ctx := setup(t, earlierBlock)
					countSourcesAfterReadsHook = func() {
						countSourcesAfterReadsHook = nil
						compactOutOfOrder(t, i)
					}
					t.Cleanup(func() { countSourcesAfterReadsHook = nil })
					resp, _, err := i.seriesCounts(ctx, req)
					require.NoError(t, err)
					check(t, req, earlierBlock, resp)
				})
			}
		})
	}

	t.Run("the late block counts against max_index_bytes", func(t *testing.T) {
		i, ctx := setup(t, false)
		countSourcesAfterReadsHook = func() {
			countSourcesAfterReadsHook = nil
			compactOutOfOrder(t, i)
		}
		t.Cleanup(func() { countSourcesAfterReadsHook = nil })
		// Enough for the head's 2 series alone.
		_, _, err := i.seriesCounts(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 20 * minute, MaxIndexBytes: 2 * ingesterSeriesCostBytes})
		require.Error(t, err)
		assert.Equal(t, cardinality.LimitMaxIndexBytes, cardinality.RefusedBy(err))
	})
}
