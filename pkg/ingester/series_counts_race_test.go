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
		resp, err := i.seriesCountsWithStatus(ctx, req)
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

		resp, err := i.seriesCountsWithStatus(ctx, req)
		require.NoError(t, err)
		require.NoError(t, <-done)
		assert.Equal(t, want, countsOf(resp), "b{pod=1} lives only in the cut range")
	})
}
