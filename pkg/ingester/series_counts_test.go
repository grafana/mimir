// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
)

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

	t.Run("one count per metric over the window", func(t *testing.T) {
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {2}, "b": {1}}, countsOf(resp))
		assert.Equal(t, int64(3), resp.SeriesWalked)
		assert.Equal(t, int64(3), resp.SeriesCounted)
		assert.Equal(t, 10*minute, resp.HeadMinTimeMs)
		assert.Equal(t, 80*minute, resp.HeadMaxTimeMs)
	})

	t.Run("a series whose chunks end before the window is not counted", func(t *testing.T) {
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: hour, EndTimestampMs: 2 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {1}, "b": {1}}, countsOf(resp), "a{pod=1} has samples only before 1h")
	})

	t.Run("per-hour buckets, grouped by pod, with matchers", func(t *testing.T) {
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{
			Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "a"}},
			StartTimestampMs: 0, EndTimestampMs: 2 * hour, StepMs: hour, GroupBy: "pod",
		})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"1": {1, 0}, "2": {0, 1}}, countsOf(resp))
		assert.Equal(t, int64(2), resp.SeriesWalked)
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
		_, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour, MaxIndexBytes: 1000})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok, err)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
		assert.Contains(t, st.Message(), "max_index_bytes 1000")
	})

	t.Run("an invalid request is InvalidArgument", func(t *testing.T) {
		_, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour, StepMs: 7 * minute})
		st, ok := status.FromError(err)
		require.True(t, ok, err)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("a tenant with no data is empty", func(t *testing.T) {
		resp, err := i.seriesCountsWithStatus(user.InjectOrgID(context.Background(), "other"), &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: hour})
		require.NoError(t, err)
		assert.Empty(t, resp.Groups)
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
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 4 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {2}, "b": {1}}, countsOf(resp))
		assert.Equal(t, int64(4), resp.SeriesWalked, "two series in the block, two in the head")
		assert.Equal(t, int64(3), resp.SeriesCounted)
		assert.Equal(t, 10*minute, resp.MinTimeMs)
	})

	t.Run("buckets take each series' chunks from every source", func(t *testing.T) {
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 4 * hour, StepMs: 2 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {1, 2}, "b": {1, 0}}, countsOf(resp))
	})

	t.Run("a window after the block reads only the head", func(t *testing.T) {
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 2 * hour, EndTimestampMs: 4 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {2}}, countsOf(resp))
		assert.Equal(t, int64(2), resp.SeriesWalked)
	})

	t.Run("hashes are unique across block and head", func(t *testing.T) {
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 4 * hour, Hashes: true})
		require.NoError(t, err)
		require.Len(t, resp.Groups, 2)
		assert.ElementsMatch(t, []uint64{labels.StableHash(a1), labels.StableHash(a2)}, resp.Groups[0].Hashes)
	})

	t.Run("max_index_bytes prices the series of every source", func(t *testing.T) {
		_, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 4 * hour, MaxIndexBytes: 3 * 512})
		st, ok := status.FromError(err)
		require.True(t, ok, err)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
		assert.Contains(t, st.Message(), "walk 4 series in 2 head and local blocks")
	})
}

func TestIngester_SeriesCounts_TenantIndexBytesLimit(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.CardinalityEstimateMaxIndexBytes = 1000
	i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", prometheus.NewRegistry())
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)
	ctx := user.InjectOrgID(context.Background(), "test")
	for _, pod := range []string{"1", "2", "3"} {
		_, err := i.Push(ctx, mockWriteRequest(t, labels.FromStrings(model.MetricNameLabel, "a", "pod", pod), 1, 1000))
		require.NoError(t, err)
	}

	// Three series at 512 bytes each are over the tenant's 1000 bytes,
	// whether the request asks for no bound or a larger one.
	for _, asked := range []int64{0, 1 << 20} {
		_, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2000, MaxIndexBytes: asked})
		require.Error(t, err, "max_index_bytes %d", asked)
		st, ok := status.FromError(err)
		require.True(t, ok, err)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
		assert.Contains(t, st.Message(), "more than max_index_bytes 1000")
	}

	limits.CardinalityEstimateMaxIndexBytes = 0
	i2, r2, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", prometheus.NewRegistry())
	require.NoError(t, err)
	startAndWaitHealthy(t, i2, r2)
	for _, pod := range []string{"1", "2", "3"} {
		_, err := i2.Push(ctx, mockWriteRequest(t, labels.FromStrings(model.MetricNameLabel, "a", "pod", pod), 1, 1000))
		require.NoError(t, err)
	}
	resp, err := i2.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2000})
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

	resp, err = i.seriesCountsWithStatus(ctx, req(now.Add(-3*time.Hour).UnixMilli()))
	require.NoError(t, err)
	require.True(t, resp.ShardSizeChanged, "changed after the time asked")
	require.Empty(t, resp.Groups)
	require.Zero(t, resp.SeriesWalked, "nothing walked")
}
