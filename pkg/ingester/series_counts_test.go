// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
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
		resp, err := i.SeriesCounts(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {2}, "b": {1}}, countsOf(resp))
		assert.Equal(t, int64(3), resp.SeriesWalked)
		assert.Equal(t, int64(3), resp.SeriesCounted)
		assert.Equal(t, 10*minute, resp.HeadMinTimeMs)
		assert.Equal(t, 80*minute, resp.HeadMaxTimeMs)
	})

	t.Run("a series whose chunks end before the window is not counted", func(t *testing.T) {
		resp, err := i.SeriesCounts(ctx, &client.SeriesCountsRequest{StartTimestampMs: hour, EndTimestampMs: 2 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"a": {1}, "b": {1}}, countsOf(resp), "a{pod=1} has samples only before 1h")
	})

	t.Run("per-hour buckets, grouped by pod, with matchers", func(t *testing.T) {
		resp, err := i.SeriesCounts(ctx, &client.SeriesCountsRequest{
			Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "a"}},
			StartTimestampMs: 0, EndTimestampMs: 2 * hour, StepMs: hour, GroupBy: "pod",
		})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"1": {1, 0}, "2": {0, 1}}, countsOf(resp))
		assert.Equal(t, int64(2), resp.SeriesWalked)
	})

	t.Run("hashes match the label-set hashes other sources send", func(t *testing.T) {
		resp, err := i.SeriesCounts(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour, Hashes: true, GroupBy: "pod"})
		require.NoError(t, err)
		require.Len(t, resp.Groups, 2)
		assert.Equal(t, "1", resp.Groups[0].Value)
		assert.ElementsMatch(t, []uint64{labels.StableHash(a1), labels.StableHash(b1)}, resp.Groups[0].Hashes)
		assert.Equal(t, []uint64{labels.StableHash(a2)}, resp.Groups[1].Hashes)
	})

	t.Run("refused before the walk when over max_index_bytes", func(t *testing.T) {
		_, err := i.SeriesCounts(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour, MaxIndexBytes: 1000})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok, err)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
		assert.Contains(t, st.Message(), "max_index_bytes 1000")
	})

	t.Run("an invalid request is InvalidArgument", func(t *testing.T) {
		_, err := i.SeriesCounts(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour, StepMs: 7 * minute})
		st, ok := status.FromError(err)
		require.True(t, ok, err)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("a tenant with no data is empty", func(t *testing.T) {
		resp, err := i.SeriesCounts(user.InjectOrgID(context.Background(), "other"), &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: hour})
		require.NoError(t, err)
		assert.Empty(t, resp.Groups)
	})
}
