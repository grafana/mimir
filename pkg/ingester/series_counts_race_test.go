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
	req := &client.SeriesCountsRequest{StartMs: 0, EndMs: 5 * hour}
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
		i.countSourcesBetweenReadsHook = func() {
			i.countSourcesBetweenReadsHook = nil
			go func() { done <- db.Compact() }()
			// Wait until the compaction is done, or until its head
			// truncation, which starts once the new block is listed, waits
			// for a head reader. Without one it finishes at once; with
			// one it keeps the head's start where it was until the reader
			// closes, so it is seen waiting on two polls in a row.
			head := db.db.Head()
			waitingPolls := 0
			require.Eventually(t, func() bool {
				if len(done) > 0 {
					return true
				}
				truncating, _, cut := head.IsQuerierCollidingWithTruncation(0, 0)
				if truncating && head.MinTime() < cut {
					waitingPolls++
				} else {
					waitingPolls = 0
				}
				return waitingPolls >= 2
			}, 5*time.Second, 10*time.Millisecond)
			if len(done) == 0 {
				require.Len(t, db.Blocks(), 1, "the truncation starts after the new block is listed")
			}
		}

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
		"counts":      {StartMs: 0, EndMs: 20 * minute},
		"with hashes": {StartMs: 0, EndMs: 20 * minute, Hashes: true},
		"per bucket":  {StartMs: 0, EndMs: 20 * minute, StepMs: 20 * minute},
	} {
		t.Run(name, func(t *testing.T) {
			t.Run("compaction between the head read and the block list", func(t *testing.T) {
				i, ctx := setup(t, false)
				i.countSourcesBetweenReadsHook = func() {
					i.countSourcesBetweenReadsHook = nil
					compactOutOfOrder(t, i)
				}
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
					i.countSourcesAfterReadsHook = func() {
						i.countSourcesAfterReadsHook = nil
						compactOutOfOrder(t, i)
					}
					resp, _, err := i.seriesCounts(ctx, req)
					require.NoError(t, err)
					check(t, req, earlierBlock, resp)
				})
			}
		})
	}

	t.Run("the late block counts against what the head left of max_index_bytes", func(t *testing.T) {
		req := func(maxIndexBytes int64) *client.SeriesCountsRequest {
			return &client.SeriesCountsRequest{StartMs: 0, EndMs: 20 * minute, MaxIndexBytes: maxIndexBytes}
		}
		compactDuringTheWalk := func(t *testing.T, i *Ingester) {
			i.countSourcesAfterReadsHook = func() {
				i.countSourcesAfterReadsHook = nil
				compactOutOfOrder(t, i)
			}
		}
		// The head's 2 series and the late block's 2, at the price of a walk over one source.
		i, ctx := setup(t, false)
		compactDuringTheWalk(t, i)
		resp, walked, err := i.seriesCounts(ctx, req(4*ingesterSeriesCostBytes))
		require.NoError(t, err)
		check(t, req(0), false, resp)
		assert.Equal(t, int64(4), walked, "the head's first walk is not added to its second")

		// Enough for the head's 2 series alone.
		i, ctx = setup(t, false)
		compactDuringTheWalk(t, i)
		_, _, err = i.seriesCounts(ctx, req(2*ingesterSeriesCostBytes))
		require.Error(t, err)
		assert.Equal(t, cardinality.LimitMaxIndexBytes, cardinality.RefusedBy(err))
	})

	// The truncation of an in-order head compaction waits for the count's head
	// reader, so the head still holds the new block's data and the block is
	// not walked: a request that fits the head alone is answered.
	t.Run("an in-order head compaction after the block list", func(t *testing.T) {
		const hour = 60 * minute
		i, ctx := setup(t, false)
		_, err := i.Push(ctx, mockWriteRequest(t, a, 1, 4*hour))
		require.NoError(t, err)
		db := i.getTSDB(userID)
		done := make(chan error, 1)
		i.countSourcesAfterReadsHook = func() {
			i.countSourcesAfterReadsHook = nil
			// The head spans more than 1.5 block ranges, so this cuts [0, 2h).
			go func() { done <- db.Compact() }()
			require.Eventually(t, func() bool { return len(db.Blocks()) == 1 }, 5*time.Second, time.Millisecond)
			meta := db.Blocks()[0].Meta()
			require.False(t, meta.Compaction.FromOutOfOrder())
		}

		req := &client.SeriesCountsRequest{StartMs: 0, EndMs: 5 * hour, MaxIndexBytes: 2 * ingesterSeriesCostBytes}
		resp, walked, err := i.seriesCounts(ctx, req)
		require.NoError(t, err)
		require.NoError(t, <-done)
		check(t, req, false, resp)
		assert.Equal(t, int64(2), walked, "the head only, once")
	})
}
