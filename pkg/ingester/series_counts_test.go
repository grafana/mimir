// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/failsafe-go/failsafe-go/adaptivelimiter"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
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

	t.Run("asked for hashes, refused before the walk when the matching series are more than max_hashes", func(t *testing.T) {
		for _, matchers := range [][]*client.LabelMatcher{everySeries, nil} {
			// The head may hold series with no chunk in the window, so it is
			// priced by the walk, which refuses at the third series counted.
			_, _, err := i.seriesCountsWalked(ctx, &client.SeriesCountsRequest{Matchers: matchers, StartTimestampMs: 0, EndTimestampMs: 2 * hour, Hashes: true, MaxHashes: 2})
			assert.Equal(t, codes.ResourceExhausted, status.Code(err), err)
			n, ok := cardinality.HashesNeeded(err)
			require.True(t, ok, err)
			assert.Equal(t, int64(3), n)
		}
		_, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 2 * hour, Hashes: true, MaxHashes: 3})
		require.NoError(t, err)
		_, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 2 * hour, MaxHashes: 1})
		require.NoError(t, err, "counts hold no hashes")
		_, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2 * hour, Hashes: true, MaxHashes: -1})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
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
		assert.Zero(t, resp.MinTimeMs, "nothing held for the tenant")
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

	t.Run("min_time_ms is the oldest time held, whether or not it is in the window", func(t *testing.T) {
		// Nothing between the block (10m) and the head's samples (3h).
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: hour, EndTimestampMs: 2 * hour})
		require.NoError(t, err)
		assert.Empty(t, resp.Groups)
		assert.Equal(t, 10*minute, resp.MinTimeMs, "nothing in the window, but the tenant's data starts at 10m")
		// A window before everything held, and one after it.
		resp, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 5 * minute})
		require.NoError(t, err)
		assert.Equal(t, 10*minute, resp.MinTimeMs)
		resp, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 5 * hour, EndTimestampMs: 6 * hour})
		require.NoError(t, err)
		assert.Equal(t, 10*minute, resp.MinTimeMs)
		// A window the head alone answers still reports the block's start.
		resp, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 2 * hour, EndTimestampMs: 4 * hour})
		require.NoError(t, err)
		assert.Equal(t, 10*minute, resp.MinTimeMs)
		// A request answered without counting carries it too.
		resp, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 4 * hour, ShardSizeUnchangedBeforeMs: 1})
		require.NoError(t, err)
		assert.True(t, resp.ShardSizeChanged)
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

	t.Run("asked for hashes, max_hashes counts a series in the block and the head once", func(t *testing.T) {
		everySeries := []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".+"}}
		// The block holds a1 and b1, the head a1 and a2: three distinct.
		_, walked, err := i.seriesCountsWalked(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 4 * hour, Hashes: true, MaxHashes: 1})
		assert.Equal(t, codes.ResourceExhausted, status.Code(err), err)
		assert.Zero(t, walked, "the local block inside the window alone matches two, so it is refused before the walk")
		_, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 4 * hour, Hashes: true, MaxHashes: 2})
		assert.Equal(t, codes.ResourceExhausted, status.Code(err), err)
		n, ok := cardinality.HashesNeeded(err)
		require.True(t, ok)
		assert.Equal(t, int64(3), n, "refused during the walk at the third distinct series")
		_, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 4 * hour, Hashes: true, MaxHashes: 3})
		require.NoError(t, err)
	})

	t.Run("max_groups refuses before counting where a block gives a lower bound, otherwise while counting", func(t *testing.T) {
		everySeries := []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".+"}}
		m := i.seriesCountsMetrics
		// A refused request returns no walked count, so the series walked
		// are read from the metric.
		walkedBy := func(f func()) float64 {
			before := testutil.ToFloat64(m.seriesWalked)
			f()
			return testutil.ToFloat64(m.seriesWalked) - before
		}
		refused := func(req *client.SeriesCountsRequest, msg string) float64 {
			return walkedBy(func() {
				_, err := i.seriesCountsWithStatus(ctx, req)
				assert.Equal(t, codes.ResourceExhausted, status.Code(err), err)
				assert.Contains(t, status.Convert(err).Message(), msg)
			})
		}

		// The block alone, counted from postings: two names, one pod.
		assert.Zero(t, refused(&client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 10*minute + 1, MaxGroups: 1}, "hold at least 2 distinct values of __name__, more than max_groups 1"))
		resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 10*minute + 1, MaxGroups: 2})
		require.NoError(t, err)
		assert.Len(t, resp.Groups, 2)
		_, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 10*minute + 1, GroupBy: "pod", MaxGroups: 1})
		require.NoError(t, err, "one pod in the block")

		// The block and the head, with no matchers: the block inside the
		// window holds two names, so the request is refused before the walk.
		assert.Zero(t, refused(&client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 4 * hour, MaxGroups: 1}, "more than max_groups 1"), "refused before the walk")
		resp, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 4 * hour, MaxGroups: 2})
		require.NoError(t, err)
		assert.Len(t, resp.Groups, 2)

		// With a matcher the block's values say nothing about the groups, so
		// the cap is checked while gathering: the block holds pod 1 and the
		// head pods 1 and 2.
		assert.Positive(t, refused(&client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 4 * hour, GroupBy: "pod", MaxGroups: 1}, "hold at least 2 distinct values of pod, more than max_groups 1"), "refused during the walk")
		// One source, the head, is counted as it is walked.
		assert.Positive(t, refused(&client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 2 * hour, EndTimestampMs: 4 * hour, GroupBy: "pod", MaxGroups: 1}, "more than max_groups 1"))
		_, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 4 * hour, GroupBy: "pod", MaxGroups: 2})
		require.NoError(t, err)
		assert.Equal(t, 4.0, testutil.ToFloat64(m.refusals.WithLabelValues(seriesCountsRefusedByMaxGroups)), "every refusal is counted")

		_, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 4 * hour, MaxGroups: -1})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("a count from a block's postings takes a slot of the walk gate", func(t *testing.T) {
		for range i.cfg.CardinalityEstimateMaxConcurrent {
			done, err := i.startSeriesCounts(ctx)
			require.NoError(t, err)
			defer done()
		}
		_, walked, err := i.seriesCountsWalked(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 10*minute + 1})
		assert.Equal(t, codes.Unavailable, status.Code(err), err)
		assert.Zero(t, walked)
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

// A series with many chunks across a local block and the head counts once
// per bucket it touches, whatever the number of chunks, and the gather holds
// no chunk.
func TestIngester_SeriesCounts_ManyChunksPerSeries(t *testing.T) {
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
	// The block holds a{pod=1} and b{pod=1} with several chunks each.
	for ts := 10 * minute; ts < 12*minute; ts += 1000 {
		push(a1, ts)
		push(b1, ts)
	}
	i.compactBlocks(context.Background(), true, math.MaxInt64, nil)
	require.Len(t, i.getTSDB(userID).Blocks(), 1)
	// The head holds a{pod=1} in every 2h range from 2h to 10h, more than
	// 120 samples in the first so it has several chunks there too, and
	// a{pod=2} in two of them.
	for ts := 3 * hour; ts < 3*hour+130*1000; ts += 1000 {
		push(a1, ts)
	}
	push(a2, 3*hour)
	for _, h := range []int64{5, 7, 9} {
		push(a1, h*hour)
	}
	push(a2, 9*hour)
	head := i.getTSDB(userID).Head()
	idx, err := head.Index()
	require.NoError(t, err)
	t.Cleanup(func() { _ = idx.Close() })
	p, err := idx.Postings(ctx, model.MetricNameLabel, "a")
	require.NoError(t, err)
	var builder labels.ScratchBuilder
	var metas []chunks.Meta
	chunksOf := map[string]int{}
	for p.Next() {
		require.NoError(t, idx.Series(p.At(), &builder, &metas))
		chunksOf[builder.Labels().Get("pod")] = len(metas)
	}
	assert.Greater(t, chunksOf["1"], 4, "a{pod=1} has many head chunks")
	assert.Greater(t, chunksOf["2"], 1)

	countsOf := func(resp *client.SeriesCountsResponse) map[string][]int64 {
		out := map[string][]int64{}
		for _, g := range resp.Groups {
			out[g.Value] = g.Counts
		}
		return out
	}
	// Buckets of 2h over [0, 10h): the block's samples are in the first,
	// a{pod=1} is in every bucket, a{pod=2} in the second and the last.
	resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 10 * hour, StepMs: 2 * hour})
	require.NoError(t, err)
	assert.Equal(t, map[string][]int64{"a": {1, 2, 1, 1, 2}, "b": {1, 0, 0, 0, 0}}, countsOf(resp))
	assert.Equal(t, int64(3), resp.SeriesCounted)
	// Buckets of 4h every 2h, the last ending at 10h: [0, 4h), [2h, 6h),
	// [4h, 8h) and [6h, 10h).
	resp, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 10 * hour, StepMs: 2 * hour, WindowMs: 4 * hour})
	require.NoError(t, err)
	assert.Equal(t, map[string][]int64{"a": {2, 2, 1, 2}, "b": {1, 0, 0, 0}}, countsOf(resp))
	// One count over the window, by count and by hash, and a window the
	// series' chunks are all outside.
	resp, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 10 * hour})
	require.NoError(t, err)
	assert.Equal(t, map[string][]int64{"a": {2}, "b": {1}}, countsOf(resp))
	resp, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 10 * hour, Hashes: true})
	require.NoError(t, err)
	require.Len(t, resp.Groups, 2)
	assert.ElementsMatch(t, []uint64{labels.StableHash(a1), labels.StableHash(a2)}, resp.Groups[0].Hashes)
	assert.Equal(t, []uint64{labels.StableHash(b1)}, resp.Groups[1].Hashes)
	resp, err = i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{StartTimestampMs: 4 * hour, EndTimestampMs: 5 * hour})
	require.NoError(t, err)
	assert.Empty(t, resp.Groups, "no chunk in [4h, 5h)")
	assert.Zero(t, resp.SeriesCounted)
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

func TestIngester_SeriesCounts_TenantHashesLimit(t *testing.T) {
	limits := defaultLimitsTestConfig()
	// Two hashes' worth of memory.
	limits.CardinalityEstimateMaxMemoryBytes = 2 * cardinality.HashBytes
	i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", prometheus.NewRegistry())
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)
	ctx := user.InjectOrgID(context.Background(), "test")
	for _, pod := range []string{"1", "2", "3"} {
		_, err := i.Push(ctx, mockWriteRequest(t, labels.FromStrings(model.MetricNameLabel, "a", "pod", pod), 1, 1000))
		require.NoError(t, err)
	}

	// Three series are more than the two hashes the tenant's memory allows,
	// whether the request asks for no bound or a larger one.
	everySeries := []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".+"}}
	for _, asked := range []int64{0, 1 << 20} {
		_, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 2000, Hashes: true, MaxHashes: asked})
		require.Error(t, err, "max_hashes %d", asked)
		st, ok := status.FromError(err)
		require.True(t, ok, err)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
		assert.Contains(t, st.Message(), "more than max_hashes 2")
	}
	resp, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 2000})
	require.NoError(t, err, "counts hold no hashes")
	assert.Equal(t, int64(3), resp.SeriesCounted)

	limits.CardinalityEstimateMaxMemoryBytes = 0
	i2, r2, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", prometheus.NewRegistry())
	require.NoError(t, err)
	startAndWaitHealthy(t, i2, r2)
	for _, pod := range []string{"1", "2", "3"} {
		_, err := i2.Push(ctx, mockWriteRequest(t, labels.FromStrings(model.MetricNameLabel, "a", "pod", pod), 1, 1000))
		require.NoError(t, err)
	}
	resp, err = i2.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 2000, Hashes: true})
	require.NoError(t, err, "without a tenant limit an unbounded request is hashed")
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

// openChunkIndex answers every series with one open head chunk, whose meta
// has no end, starting at 10.
type openChunkIndex struct {
	tsdb.IndexReader
}

func (openChunkIndex) Series(_ storage.SeriesRef, _ *labels.ScratchBuilder, metas *[]chunks.Meta) error {
	*metas = append((*metas)[:0], chunks.Meta{MinTime: 10, MaxTime: math.MaxInt64})
	return nil
}

// failingChunkReader fails every chunk read with err.
type failingChunkReader struct {
	tsdb.ChunkReader
	err error
}

func (r failingChunkReader) ChunkOrIterable(chunks.Meta) (chunkenc.Chunk, chunkenc.Iterable, error) {
	return nil, nil, r.err
}

// A series whose open chunk is gone when its last sample is read was removed
// by a head compaction since its index entry was read, like one whose index
// entry is gone, and is skipped rather than failing the count.
func TestAppendSeriesRanges_OpenChunkRemovedSinceTheIndexRead(t *testing.T) {
	var (
		builder, oooBuilder labels.ScratchBuilder
		metas, oooMetas     []chunks.Meta
	)
	// The chunk starts before the window, so its end is read.
	req := &client.SeriesCountsRequest{StartTimestampMs: 100, EndTimestampMs: 200}
	src := &countSource{idx: openChunkIndex{}, chunkr: failingChunkReader{err: storage.ErrNotFound}}
	ranges, found, err := appendSeriesRanges(nil, src, 1, req, &builder, &metas, &oooBuilder, &oooMetas)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, ranges)

	src = &countSource{idx: openChunkIndex{}, chunkr: failingChunkReader{err: errors.New("disk")}}
	_, _, err = appendSeriesRanges(nil, src, 1, req, &builder, &metas, &oooBuilder, &oooMetas)
	require.ErrorContains(t, err, "disk", "any other failure is one")

	// A chunk that starts in the window of a count with no step needs no end.
	req = &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 200}
	src = &countSource{idx: openChunkIndex{}, chunkr: failingChunkReader{err: storage.ErrNotFound}}
	ranges, found, err = appendSeriesRanges(nil, src, 1, req, &builder, &metas, &oooBuilder, &oooMetas)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []cardinality.ChunkRange{{MinTime: 10, MaxTime: 199}}, ranges)
}

// recordingLimiter is a read reactive limiter whose permits record whether
// they were recorded or dropped.
type recordingLimiter struct {
	recorded, dropped atomic.Int64
}

func (l *recordingLimiter) AcquirePermit(context.Context) (adaptivelimiter.Permit, error) {
	return recordingPermit{l}, nil
}
func (*recordingLimiter) CanAcquirePermit() bool           { return true }
func (*recordingLimiter) Metrics() adaptivelimiter.Metrics { return nil }
func (*recordingLimiter) Reset()                           {}

type recordingPermit struct{ l *recordingLimiter }

func (p recordingPermit) Record() { p.l.recorded.Inc() }
func (p recordingPermit) Drop()   { p.l.dropped.Inc() }

// A count walk takes the read circuit breaker's permit and the adaptive
// read limiter's, so an overloaded ingester refuses it, but however long it
// runs it is not a breaker timeout and leaves no latency sample with the
// limiter; an ordinary read of the same length is both.
func TestIngester_StartReadRequest_SeriesCountsWalk(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.ReadCircuitBreaker = CircuitBreakerConfig{Enabled: true, CooldownPeriod: 10 * time.Second, RequestTimeout: 10 * time.Millisecond}
	ing := newFailingIngester(t, cfg, nil, nil)
	ing.startWaitAndCheck(context.Background(), t)
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), ing) })
	acquired, successes, failures := atomic.NewInt64(0), atomic.NewInt64(0), atomic.NewInt64(0)
	ing.circuitBreaker.read.cb = &mockedCircuitBreaker{acquiredPermitCount: acquired, recordSuccessCount: successes, recordFailureCount: failures, CircuitBreaker: ing.circuitBreaker.read.cb}
	ing.circuitBreaker.read.cb.Close()
	limiter := &recordingLimiter{}
	ing.reactiveLimiter.read = limiter

	for _, tc := range []struct {
		name                      string
		ctx                       context.Context
		wantFailures, wantDropped int64
	}{
		{name: "a count walk", ctx: client.ContextWithSeriesCounts(context.Background()), wantFailures: 0, wantDropped: 1},
		{name: "an ordinary read", ctx: context.Background(), wantFailures: 1, wantDropped: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			acquired.Store(0)
			successes.Store(0)
			failures.Store(0)
			limiter.recorded.Store(0)
			limiter.dropped.Store(0)
			ctx, err := ing.StartReadRequest(tc.ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(1), acquired.Load(), "the breaker's permit is taken")
			finish, err := ing.PrepareReadRequest(ctx)
			require.NoError(t, err)
			time.Sleep(3 * cfg.ReadCircuitBreaker.RequestTimeout)
			finish(nil)
			assert.Equal(t, tc.wantFailures, failures.Load(), "breaker failures")
			assert.Equal(t, 1-tc.wantFailures, successes.Load(), "breaker successes")
			assert.Equal(t, tc.wantDropped, limiter.dropped.Load(), "permits dropped")
			assert.Equal(t, 1-tc.wantDropped, limiter.recorded.Load(), "permits recorded")
		})
	}

	// An open breaker refuses a count walk like any read.
	ing.circuitBreaker.read.cb.Open()
	_, err := ing.StartReadRequest(client.ContextWithSeriesCounts(context.Background()))
	require.ErrorAs(t, err, &circuitBreakerOpenError{})
}

func TestIngester_SeriesCounts_ConcurrencyLimit(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	assert.Equal(t, 8, cfg.CardinalityEstimateMaxConcurrent, "the default")
	cfg.CardinalityEstimateMaxConcurrent = 1
	reg := prometheus.NewPedanticRegistry()
	i := requireActiveIngesterWithBlocksStorage(t, cfg, reg)
	ctx := user.InjectOrgID(context.Background(), "test")
	for _, job := range []string{"a", "b"} {
		_, err := i.Push(ctx, mockWriteRequest(t, labels.FromStrings(model.MetricNameLabel, "up", "job", job), 1, 1000))
		require.NoError(t, err)
	}
	req := &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2000}

	done, err := i.startSeriesCounts(ctx)
	require.NoError(t, err)
	// A busy ingester refuses to walk.
	err = i.SeriesCounts(req, &seriesCountsServerMock{ctx: ctx})
	assert.Equal(t, codes.Unavailable, status.Code(err), err)
	assert.Contains(t, err.Error(), "-ingester.cardinality-estimate-max-concurrent 1")
	m := i.seriesCountsMetrics
	assert.Equal(t, 1.0, testutil.ToFloat64(m.refusals.WithLabelValues(seriesCountsRefusedByGate)))
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_gate_queries_in_flight Number of queries that are currently in flight.
		# TYPE cortex_ingester_gate_queries_in_flight gauge
		cortex_ingester_gate_queries_in_flight{gate="cardinality_estimate"} 1
	`), "cortex_ingester_gate_queries_in_flight"), "the gate exports the walks in flight")
	// Answers that walk nothing don't take a slot.
	err = i.SeriesCounts(&client.SeriesCountsRequest{StartTimestampMs: 2000, EndTimestampMs: 0}, &seriesCountsServerMock{ctx: ctx})
	assert.Equal(t, codes.InvalidArgument, status.Code(err), err)
	srv := &seriesCountsServerMock{ctx: ctx}
	require.NoError(t, i.SeriesCounts(&client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2000, ShardSizeUnchangedBeforeMs: 1}, srv))
	require.Len(t, srv.sent, 1)
	assert.True(t, srv.sent[0].ShardSizeChanged)
	err = i.SeriesCounts(req, &seriesCountsServerMock{ctx: user.InjectOrgID(context.Background(), "no-data")})
	assert.NoError(t, err, "a tenant with no data")

	done()
	srv = &seriesCountsServerMock{ctx: ctx}
	require.NoError(t, i.SeriesCounts(req, srv), "the slot is free again")
	require.NotEmpty(t, srv.sent)
	// A walk the limit refuses frees its slot too.
	err = i.SeriesCounts(&client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2000, MaxIndexBytes: 1}, &seriesCountsServerMock{ctx: ctx})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), err)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.refusals.WithLabelValues(seriesCountsRefusedByMaxIndexBytes)))
	err = i.SeriesCounts(&client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2000, Hashes: true, MaxHashes: 1}, &seriesCountsServerMock{ctx: ctx})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), err)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.refusals.WithLabelValues(seriesCountsRefusedByMaxHashes)))
	assert.Positive(t, testutil.ToFloat64(m.seriesWalked), "the answered walk's series")
	walks := &dto.Metric{}
	require.NoError(t, m.walkDuration.WithLabelValues("head").(prometheus.Metric).Write(walks))
	assert.Equal(t, uint64(2), walks.GetHistogram().GetSampleCount(), "the answered walk and the one refused during it, not the one refused before it")
	done, err = i.startSeriesCounts(ctx)
	require.NoError(t, err, "a finished request frees its slot")
	done()

	cfg.CardinalityEstimateMaxConcurrent = 0
	unlimited := requireActiveIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	for range 3 {
		_, err := unlimited.startSeriesCounts(ctx)
		require.NoError(t, err, "0 means no limit")
	}
}

type seriesCountsServerMock struct {
	grpc.ServerStream
	ctx  context.Context
	sent []*client.SeriesCountsResponse
}

func (m *seriesCountsServerMock) Context() context.Context { return m.ctx }
func (m *seriesCountsServerMock) Send(resp *client.SeriesCountsResponse) error {
	m.sent = append(m.sent, resp)
	return nil
}

func TestIngester_SeriesCounts_SpansPerPhase(t *testing.T) {
	spans := testSpanExporter(t)

	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	ctx := user.InjectOrgID(context.Background(), "test")
	for _, pod := range []string{"1", "2"} {
		_, err := i.Push(ctx, mockWriteRequest(t, labels.FromStrings(model.MetricNameLabel, "up", "pod", pod), 1, 1000))
		require.NoError(t, err)
	}
	everySeries := []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".+"}}
	_, err := i.seriesCountsWithStatus(ctx, &client.SeriesCountsRequest{Matchers: everySeries, StartTimestampMs: 0, EndTimestampMs: 2000})
	require.NoError(t, err)

	attrs := map[string]map[string]int64{}
	for _, s := range spans.GetSpans() {
		m := map[string]int64{}
		for _, a := range s.Attributes {
			m[string(a.Key)] = a.Value.AsInt64()
		}
		attrs[s.Name] = m
	}
	require.Contains(t, attrs, "Ingester.SeriesCounts postings")
	assert.Equal(t, int64(2), attrs["Ingester.SeriesCounts postings"]["series_matched_summed_over_sources"])
	assert.Equal(t, int64(1), attrs["Ingester.SeriesCounts postings"]["sources"])
	require.Contains(t, attrs, "Ingester.SeriesCounts walk")
	assert.Equal(t, int64(2), attrs["Ingester.SeriesCounts walk"]["series_walked"])
}
