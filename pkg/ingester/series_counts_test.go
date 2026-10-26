// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
)

// Native histogram series, integer and float, are counted like float
// series, including when the end of an open head chunk is read from its
// last sample.
func TestIngester_SeriesCounts_NativeHistograms(t *testing.T) {
	const minute = int64(time.Minute / time.Millisecond)
	const hour = 60 * minute

	hist := labels.FromStrings(model.MetricNameLabel, "h", "kind", "int")
	floatHist := labels.FromStrings(model.MetricNameLabel, "h", "kind", "float")
	float := labels.FromStrings(model.MetricNameLabel, "f")

	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	ctx := user.InjectOrgID(context.Background(), "test")
	for _, ts := range []int64{10 * minute, 20 * minute, 70 * minute} {
		if ts != 70*minute {
			_, err := i.Push(ctx, mockHistogramWriteRequest(hist, ts, int(ts/minute), false))
			require.NoError(t, err)
		}
		if ts != 20*minute {
			_, err := i.Push(ctx, mockHistogramWriteRequest(floatHist, ts, int(ts/minute), true))
			require.NoError(t, err)
		}
		if ts == 10*minute {
			_, err := i.Push(ctx, mockWriteRequest(t, float, 1, ts))
			require.NoError(t, err)
		}
	}
	countsOf := func(resp *client.SeriesCountsResponse) map[string][]int64 {
		out := map[string][]int64{}
		for _, g := range resp.Groups {
			out[g.Value] = g.Counts
		}
		return out
	}

	resp, walked, err := i.seriesCountsWalked(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour})
	require.NoError(t, err)
	assert.Equal(t, map[string][]int64{"h": {2}, "f": {1}}, countsOf(resp))
	assert.Equal(t, int64(3), walked)

	resp, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour, StepMs: hour, GroupBy: "kind"})
	require.NoError(t, err)
	assert.Equal(t, map[string][]int64{"int": {1, 0}, "float": {1, 1}, "": {1, 0}}, countsOf(resp), "a histogram series' chunk ends at its last sample")

	// The window starts after every chunk's first sample, so each open
	// chunk's end is read from its last sample: the integer histogram's at
	// 20m, the float histogram's at 70m and the float series' at 10m.
	resp, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 15 * minute, EndTimestampMs: 2 * hour, GroupBy: "kind"})
	require.NoError(t, err)
	assert.Equal(t, map[string][]int64{"int": {1}, "float": {1}}, countsOf(resp))
}

func TestIngester_SeriesCounts(t *testing.T) {
	const minute = int64(time.Minute / time.Millisecond)
	const hour = 60 * minute

	a1 := labels.FromStrings(model.MetricNameLabel, "a", "pod", "1")
	a2 := labels.FromStrings(model.MetricNameLabel, "a", "pod", "2")
	b1 := labels.FromStrings(model.MetricNameLabel, "b", "pod", "1")
	samples := []struct {
		lset labels.Labels
		ts   []int64
	}{
		{a1, []int64{10 * minute, 20 * minute}},
		{a2, []int64{70 * minute, 80 * minute}},
		{b1, []int64{10 * minute, 70 * minute}},
	}

	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	ctx := user.InjectOrgID(context.Background(), "test")
	// Push in time order across series: the head refuses samples older than
	// its range allows once a later sample is in.
	for _, ts := range []int64{10 * minute, 20 * minute, 70 * minute, 80 * minute} {
		for _, s := range samples {
			if slices.Contains(s.ts, ts) {
				_, err := i.Push(ctx, mockWriteRequest(t, s.lset, 1, ts))
				require.NoError(t, err)
			}
		}
	}

	countsOf := func(resp *client.SeriesCountsResponse) map[string][]int64 {
		out := map[string][]int64{}
		for _, g := range resp.Groups {
			out[g.Value] = g.Counts
		}
		return out
	}

	// A matcher that selects every series makes the ingester walk them.
	everySeries := []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".+"}}

	t.Run("one count per metric over the window", func(t *testing.T) {
		resp, walked, err := i.seriesCountsWalked(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 2 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {2}, "b": {1}}, countsOf(resp))
		assert.Equal(t, int64(3), walked)
		assert.Equal(t, int64(3), resp.SeriesCounted)
	})

	t.Run("the head is walked even when it sits inside the window", func(t *testing.T) {
		resp, walked, err := i.seriesCountsWalked(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {2}, "b": {1}}, countsOf(resp))
		assert.Equal(t, int64(3), walked)
	})

	t.Run("a series whose chunks end before the window is not counted", func(t *testing.T) {
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: hour, EndTimestampMs: 2 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {1}, "b": {1}}, countsOf(resp), "a{pod=1} has samples only before 1h")
	})

	t.Run("per-hour buckets, grouped by pod, with matchers", func(t *testing.T) {
		resp, walked, err := i.seriesCountsWalked(ctx, &client.SeriesCountsRequest{
			Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "a"}},
			StartTimestampMs: 0, EndTimestampMs: 2 * hour, StepMs: hour, GroupBy: "pod",
		})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"1": {1, 0}, "2": {0, 1}}, countsOf(resp))
		assert.Equal(t, int64(2), walked)
	})

	t.Run("buckets wider than the step look back from each bucket's end", func(t *testing.T) {
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{
			StartTimestampMs: 0, EndTimestampMs: 2 * hour, StepMs: 30 * minute, WindowMs: hour, GroupBy: "pod",
		})
		require.NoError(t, err)
		// Buckets [0, 1h), [30m, 90m) and [1h, 2h): pod 1 has samples at 10m, 20m and 70m, pod 2 at 70m and 80m.
		assert.Equal(t, map[string][]int64{"1": {2, 1, 1}, "2": {0, 1, 1}}, countsOf(resp))
		assert.Equal(t, hour, resp.WindowMs, "the window is echoed")
	})

	t.Run("hashes match the label-set hashes other sources send", func(t *testing.T) {
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour, Hashes: true, GroupBy: "pod"})
		require.NoError(t, err)
		require.Len(t, resp.Groups, 2)
		assert.Equal(t, "1", resp.Groups[0].Value)
		assert.ElementsMatch(t, []uint64{labels.StableHash(a1), labels.StableHash(b1)}, resp.Groups[0].Hashes)
		assert.Equal(t, []uint64{labels.StableHash(a2)}, resp.Groups[1].Hashes)
	})

	t.Run("refused before the walk when over max_index_bytes", func(t *testing.T) {
		_, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 2 * hour, MaxIndexBytes: 100})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok, err)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
		assert.Contains(t, st.Message(), "max_index_bytes 100")
	})

	t.Run("with no matchers the limit is checked from the series each source holds", func(t *testing.T) {
		_, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour, MaxIndexBytes: 100})
		st, ok := status.FromError(err)
		require.True(t, ok, err)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
		assert.Contains(t, st.Message(), "would walk 3 series in 1 head and local blocks")
		assert.Contains(t, st.Message(), "more than max_index_bytes 100")
	})

	t.Run("a request is refused when its series cost more than max_index_bytes, and not at the limit", func(t *testing.T) {
		// Three series at ingesterSeriesCostBytes each.
		for name, matchers := range map[string][]*client.LabelMatcher{"no matchers": nil, "a matcher": everySeries} {
			resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: matchers, StartTimestampMs: 0, EndTimestampMs: 2 * hour, MaxIndexBytes: 3 * ingesterSeriesCostBytes})
			require.NoError(t, err, name)
			assert.Equal(t, int64(3), resp.SeriesCounted, name)

			_, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: matchers, StartTimestampMs: 0, EndTimestampMs: 2 * hour, MaxIndexBytes: 3*ingesterSeriesCostBytes - 1})
			st, ok := status.FromError(err)
			require.True(t, ok, name)
			assert.Equal(t, codes.ResourceExhausted, st.Code(), name)
		}
	})

	t.Run("a cancelled request stops the walk", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := i.seriesCountsWithStatus(cancelled, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 2 * hour})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("an invalid request is InvalidArgument", func(t *testing.T) {
		_, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour, StepMs: 7 * minute})
		st, ok := status.FromError(err)
		require.True(t, ok, err)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("a tenant with no data is empty", func(t *testing.T) {
		resp, err := i.seriesCountsWithStatus(user.InjectOrgID(context.Background(), "other"), &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: hour, StepMs: 30 * minute, WindowMs: hour})
		require.NoError(t, err)
		assert.Empty(t, resp.Groups)
		assert.Equal(t, hour, resp.WindowMs, "the window is echoed with nothing counted")
	})
}

func TestIngester_SeriesCounts_LocalBlocksAndHead(t *testing.T) {
	const minute = int64(time.Minute / time.Millisecond)
	const hour = 60 * minute

	a1 := labels.FromStrings(model.MetricNameLabel, "a", "pod", "1")
	a2 := labels.FromStrings(model.MetricNameLabel, "a", "pod", "2")
	b1 := labels.FromStrings(model.MetricNameLabel, "b", "pod", "1")

	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	ctx := user.InjectOrgID(context.Background(), userID)
	push := func(lset labels.Labels, ts int64) {
		_, err := i.Push(ctx, mockWriteRequest(t, lset, 1, ts))
		require.NoError(t, err)
	}

	// a{pod=1} and b{pod=1} go into a local block; a{pod=1} keeps writing
	// into the new head with a{pod=2}.
	push(a1, 10*minute)
	push(b1, 10*minute)
	i.compactBlocks(context.Background(), true, math.MaxInt64, nil)
	require.Len(t, i.getTSDB(userID).Blocks(), 1)
	push(a1, 3*hour)
	push(a2, 3*hour)

	countsOf := func(resp *client.SeriesCountsResponse) map[string][]int64 {
		out := map[string][]int64{}
		for _, g := range resp.Groups {
			out[g.Value] = g.Counts
		}
		return out
	}

	t.Run("a series in the block and the head counts once", func(t *testing.T) {
		resp, walked, err := i.seriesCountsWalked(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 4 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {2}, "b": {1}}, countsOf(resp))
		assert.Equal(t, int64(4), walked, "two series in the block, two in the head")
		assert.Equal(t, int64(3), resp.SeriesCounted)
		assert.Equal(t, 10*minute, resp.MinTimeMs)
	})

	t.Run("buckets take each series' chunks from every source", func(t *testing.T) {
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 4 * hour, StepMs: 2 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {1, 2}, "b": {1, 0}}, countsOf(resp))
	})

	t.Run("a window after the block reads only the head", func(t *testing.T) {
		resp, walked, err := i.seriesCountsWalked(ctx, &client.SeriesCountsRequest{StartTimestampMs: 2 * hour, EndTimestampMs: 4 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {2}}, countsOf(resp))
		assert.Equal(t, int64(2), walked)
	})

	t.Run("a local block inside the window is counted from postings lengths", func(t *testing.T) {
		// The head starts where the block ends, whatever its first sample, so a window ending there reads
		// only the block.
		resp, walked, err := i.seriesCountsWalked(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 10*minute + 1, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"1": {2}}, countsOf(resp))
		assert.Zero(t, walked, "only the block overlaps the window")
		assert.Equal(t, int64(2), resp.SeriesCounted)

		// The same counts as the walk, which a matcher selecting every series forces.
		everySeries := []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".+"}}
		for _, groupBy := range []string{"", "pod", "missing"} {
			fromPostings, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 10*minute + 1, GroupBy: groupBy})
			require.NoError(t, err)
			walked, nWalked, err := i.seriesCountsWalked(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 10*minute + 1, GroupBy: groupBy})
			require.NoError(t, err)
			assert.Positive(t, nWalked)
			assert.Equal(t, countsOf(walked), countsOf(fromPostings), "group by %q", groupBy)
			assert.Equal(t, walked.SeriesCounted, fromPostings.SeriesCounted, "group by %q", groupBy)
			assert.Equal(t, walked.MinTimeMs, fromPostings.MinTimeMs, "group by %q", groupBy)
		}
	})

	t.Run("hashes are unique across block and head", func(t *testing.T) {
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 4 * hour, Hashes: true})
		require.NoError(t, err)
		require.Len(t, resp.Groups, 2)
		assert.ElementsMatch(t, []uint64{labels.StableHash(a1), labels.StableHash(a2)}, resp.Groups[0].Hashes)
	})

	t.Run("max_index_bytes prices the series of every source", func(t *testing.T) {
		_, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 4 * hour, MaxIndexBytes: 3 * ingesterSeriesCostBytes})
		st, ok := status.FromError(err)
		require.True(t, ok, err)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
		assert.Contains(t, st.Message(), "walk 4 series in 2 head and local blocks")
	})

	t.Run("a walk over two sources prices each series at the gathering cost", func(t *testing.T) {
		everySeries := []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".+"}}
		// Four series over the block and the head fit at the two-source price,
		// and one byte less refuses before the walk.
		_, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 4 * hour, MaxIndexBytes: 4 * ingesterMultiSourceSeriesCostBytes})
		require.NoError(t, err)
		_, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 4 * hour, MaxIndexBytes: 4*ingesterMultiSourceSeriesCostBytes - 1})
		st, ok := status.FromError(err)
		require.True(t, ok, err)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
		assert.Contains(t, st.Message(), fmt.Sprintf("at %d bytes each", ingesterMultiSourceSeriesCostBytes))
		_, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 4 * hour, MaxIndexBytes: 4*ingesterMultiSourceSeriesCostBytes - 1})
		assert.Equal(t, codes.ResourceExhausted, status.Code(err), "with no matchers too")
		assert.Contains(t, status.Convert(err).Message(), fmt.Sprintf("about %d bytes at %d bytes each", 4*ingesterMultiSourceSeriesCostBytes, ingesterMultiSourceSeriesCostBytes))

		// The head alone is one source, priced at the single-source cost.
		_, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 2 * hour, EndTimestampMs: 4 * hour, MaxIndexBytes: 2 * ingesterSeriesCostBytes})
		require.NoError(t, err)
	})

	t.Run("a head series with no chunk is not counted", func(t *testing.T) {
		// Last, since the series stays. A rolled-back append leaves the series in the head until the next garbage collection.
		app := i.getTSDB(userID).Head().Appender(ctx)
		_, err := app.Append(0, labels.FromStrings(model.MetricNameLabel, "rolled_back"), 3*hour, 1)
		require.NoError(t, err)
		require.NoError(t, app.Rollback())
		// The window holds the whole head and nothing else, so only walking the head keeps the series out.
		resp, walked, err := i.seriesCountsWalked(ctx, &client.SeriesCountsRequest{StartTimestampMs: 10*minute + 1, EndTimestampMs: 4 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {2}}, countsOf(resp))
		assert.Equal(t, int64(3), walked, "the head's two series and the rolled-back one")
	})
}

func TestIngester_SeriesCounts_TenantIndexBytesLimit(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.CardinalityEstimateMaxIndexBytes = 100
	i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", prometheus.NewRegistry())
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)
	ctx := user.InjectOrgID(context.Background(), "test")
	for _, pod := range []string{"1", "2", "3"} {
		_, err := i.Push(ctx, mockWriteRequest(t, labels.FromStrings(model.MetricNameLabel, "a", "pod", pod), 1, 1000))
		require.NoError(t, err)
	}

	// Three series at ingesterSeriesCostBytes each are over the tenant's 100 bytes,
	// whether the request asks for no bound or a larger one. The matcher
	// makes the ingester walk them rather than count postings lengths.
	everySeries := []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".+"}}
	for _, asked := range []int64{0, 1 << 20} {
		_, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 2000, MaxIndexBytes: asked})
		require.Error(t, err, "max_index_bytes %d", asked)
		st, ok := status.FromError(err)
		require.True(t, ok, err)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
		assert.Contains(t, st.Message(), "more than max_index_bytes 100")
	}

	limits.CardinalityEstimateMaxIndexBytes = 0
	i2, r2, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", prometheus.NewRegistry())
	require.NoError(t, err)
	startAndWaitHealthy(t, i2, r2)
	for _, pod := range []string{"1", "2", "3"} {
		_, err := i2.Push(ctx, mockWriteRequest(t, labels.FromStrings(model.MetricNameLabel, "a", "pod", pod), 1, 1000))
		require.NoError(t, err)
	}
	resp, err := i2.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 2000})
	require.NoError(t, err, "without a tenant limit an unbounded request is walked")
	assert.Equal(t, int64(3), resp.SeriesCounted)
}

func TestIngester_SeriesCounts_OutOfOrderHead(t *testing.T) {
	const minute = int64(time.Minute / time.Millisecond)
	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = model.Duration(time.Hour)
	i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", prometheus.NewRegistry())
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)
	ctx := user.InjectOrgID(context.Background(), "test")

	a := labels.FromStrings(model.MetricNameLabel, "a")
	b := labels.FromStrings(model.MetricNameLabel, "b")
	_, err = i.Push(ctx, mockWriteRequest(t, a, 1, 60*minute))
	require.NoError(t, err)
	_, err = i.Push(ctx, mockWriteRequest(t, b, 1, 60*minute))
	require.NoError(t, err)
	// Out of order is per series: these are older than a's and b's 60m samples.
	_, err = i.Push(ctx, mockWriteRequest(t, a, 1, 30*minute))
	require.NoError(t, err)
	_, err = i.Push(ctx, mockWriteRequest(t, b, 1, 10*minute))
	require.NoError(t, err)
	require.Equal(t, 10*minute, i.getTSDB("test").Head().MinOOOTime())

	counts := func(t *testing.T, start, end, step int64) map[string][]int64 {
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: start, EndTimestampMs: end, StepMs: step})
		require.NoError(t, err)
		out := map[string][]int64{}
		for _, g := range resp.Groups {
			out[g.Value] = g.Counts
		}
		return out
	}
	assert.Equal(t, map[string][]int64{"a": {1}}, counts(t, 20*minute, 40*minute, 0), "a's only sample in the window is out of order")
	assert.Equal(t, map[string][]int64{"b": {1}}, counts(t, 0, 20*minute, 0), "a window before the in-order head")
	assert.Equal(t, map[string][]int64{"a": {1}, "b": {1}}, counts(t, 0, 120*minute, 0), "in order and out of order, once each")
	assert.Equal(t, map[string][]int64{"a": {0, 1, 1, 0}, "b": {1, 0, 1, 0}}, counts(t, 0, 120*minute, 30*minute), "per bucket")

}

func TestIngester_SeriesCounts_ShardSizeUnchangedBefore(t *testing.T) {
	i, r, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, prometheus.NewRegistry())
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)
	ctx := user.InjectOrgID(context.Background(), "test")

	_, err = i.Push(ctx, mockWriteRequest(t, labels.FromStrings(model.MetricNameLabel, "a"), 1, 1000))
	require.NoError(t, err)
	req := func(before int64) *client.SeriesCountsRequest {
		return &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2000, ShardSizeUnchangedBeforeMs: before}
	}
	now := time.Now()

	// Owned-series tracking is off in this ingester, so the shard size is not known.
	resp, err := i.seriesCountsWithStatus(ctx, req(0))
	require.NoError(t, err)
	require.Zero(t, resp.ShardSizeUnchangedSinceMs)
	require.False(t, resp.ShardSizeChanged, "not asked")
	require.Len(t, resp.Groups, 1)
	resp, err = i.seriesCountsWithStatus(ctx, req(now.UnixMilli()))
	require.NoError(t, err)
	require.True(t, resp.ShardSizeChanged, "not known")
	require.Empty(t, resp.Groups)

	i.getTSDB("test").observeShardSize(1, now.Add(-2*time.Hour), i.logger)
	resp, err = i.seriesCountsWithStatus(ctx, req(now.Add(-time.Hour).UnixMilli()))
	require.NoError(t, err)
	require.False(t, resp.ShardSizeChanged)
	require.Equal(t, now.Add(-2*time.Hour).UnixMilli(), resp.ShardSizeUnchangedSinceMs)
	require.Len(t, resp.Groups, 1)

	resp, walked, err := i.seriesCountsWalked(ctx, req(now.Add(-3*time.Hour).UnixMilli()))
	require.NoError(t, err)
	require.True(t, resp.ShardSizeChanged, "changed after the time asked")
	require.Empty(t, resp.Groups)
	require.Zero(t, walked, "nothing walked")
}

func TestExpandPostingsUpTo(t *testing.T) {
	refs := func(n int) []storage.SeriesRef {
		out := make([]storage.SeriesRef, n)
		for i := range out {
			out[i] = storage.SeriesRef(i + 1)
		}
		return out
	}

	t.Run("holds every ref up to the limit", func(t *testing.T) {
		got, exceeded, err := expandPostingsUpTo(context.Background(), index.NewListPostings(refs(10)), 10)
		require.NoError(t, err)
		assert.False(t, exceeded)
		assert.Equal(t, refs(10), got)
	})

	t.Run("stops without holding the refs once the limit is passed", func(t *testing.T) {
		counting := &countingPostings{Postings: index.NewListPostings(refs(1_000_000))}
		got, exceeded, err := expandPostingsUpTo(context.Background(), counting, 10)
		require.NoError(t, err)
		assert.True(t, exceeded)
		assert.Nil(t, got)
		assert.Equal(t, 11, counting.next, "it reads one ref past the limit and no more")
	})

	t.Run("a limit of zero refuses the first ref and accepts none", func(t *testing.T) {
		_, exceeded, err := expandPostingsUpTo(context.Background(), index.NewListPostings(refs(1)), 0)
		require.NoError(t, err)
		assert.True(t, exceeded)

		got, exceeded, err := expandPostingsUpTo(context.Background(), index.EmptyPostings(), 0)
		require.NoError(t, err)
		assert.False(t, exceeded)
		assert.Empty(t, got)
	})

	t.Run("stops when the context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _, err := expandPostingsUpTo(ctx, index.NewListPostings(refs(1<<17)), math.MaxInt64)
		require.ErrorIs(t, err, context.Canceled)
	})
}

// countingPostings counts how many refs were read from it.
type countingPostings struct {
	index.Postings
	next int
}

func (p *countingPostings) Next() bool {
	p.next++
	return p.Postings.Next()
}

// notFoundAfter answers the first series with one out-of-order chunk and,
// like the head's reader for a series removed by a head compaction, returns
// ErrNotFound for any other without touching the metas.
type notFoundAfter struct {
	tsdb.IndexReader
	first storage.SeriesRef
}

func (r notFoundAfter) Series(ref storage.SeriesRef, _ *labels.ScratchBuilder, metas *[]chunks.Meta) error {
	if ref != r.first {
		return storage.ErrNotFound
	}
	*metas = append(*metas, chunks.Meta{MinTime: 10, MaxTime: 20})
	return nil
}

func TestAppendOutOfOrderRanges_SeriesRemovedSinceTheInOrderRead(t *testing.T) {
	r := notFoundAfter{first: 1}
	var (
		builder labels.ScratchBuilder
		metas   []chunks.Meta
	)
	got, err := appendOutOfOrderRanges(nil, r, 1, &builder, &metas)
	require.NoError(t, err)
	assert.Equal(t, []cardinality.ChunkRange{{MinTime: 10, MaxTime: 20}}, got)

	got, err = appendOutOfOrderRanges(nil, r, 2, &builder, &metas)
	require.NoError(t, err)
	assert.Empty(t, got, "the first series' out-of-order chunk is not taken for the second")
}
