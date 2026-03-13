// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"math"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// collectSearchLabelNames calls SearchLabelNames and collects all results into a flat slice.
func collectSearchLabelNames(i *Ingester, ctx context.Context, req *client.SearchLabelValuesRequest) ([]string, error) {
	srv := &testSearchLabelNamesServer{ctx: ctx}
	if err := i.SearchLabelNames(req, srv); err != nil {
		return nil, err
	}
	return srv.names, nil
}

// testSearchLabelNamesServer is a test implementation of Ingester_SearchLabelNamesServer.
type testSearchLabelNamesServer struct {
	grpc.ServerStream
	ctx      context.Context
	names    []string
	warnings []string
}

func (s *testSearchLabelNamesServer) Send(resp *client.SearchLabelValuesResponse) error {
	for _, r := range resp.Results {
		s.names = append(s.names, r.Value)
	}
	s.warnings = append(s.warnings, resp.Warnings...)
	return nil
}

func (s *testSearchLabelNamesServer) Context() context.Context     { return s.ctx }
func (s *testSearchLabelNamesServer) SetHeader(metadata.MD) error  { return nil }
func (s *testSearchLabelNamesServer) SendHeader(metadata.MD) error { return nil }
func (s *testSearchLabelNamesServer) SetTrailer(metadata.MD)       {}
func (s *testSearchLabelNamesServer) SendMsg(interface{}) error    { return nil }
func (s *testSearchLabelNamesServer) RecvMsg(interface{}) error    { return nil }

// collectSearchLabelValues calls SearchLabelValues and collects all results into a flat slice.
func collectSearchLabelValues(i *Ingester, ctx context.Context, req *client.SearchLabelValuesRequest) ([]string, error) {
	srv := &testSearchLabelValuesServer{ctx: ctx}
	if err := i.SearchLabelValues(req, srv); err != nil {
		return nil, err
	}
	return srv.values, nil
}

// collectSearchLabelNamesWithWarnings calls SearchLabelNames and collects results and warnings.
func collectSearchLabelNamesWithWarnings(i *Ingester, ctx context.Context, req *client.SearchLabelValuesRequest) (names []string, warnings []string, err error) {
	srv := &testSearchLabelNamesServer{ctx: ctx}
	if err := i.SearchLabelNames(req, srv); err != nil {
		return nil, nil, err
	}
	return srv.names, srv.warnings, nil
}

// collectSearchLabelValuesWithWarnings calls SearchLabelValues and collects results and warnings.
func collectSearchLabelValuesWithWarnings(i *Ingester, ctx context.Context, req *client.SearchLabelValuesRequest) (values []string, warnings []string, err error) {
	srv := &testSearchLabelValuesServer{ctx: ctx}
	if err := i.SearchLabelValues(req, srv); err != nil {
		return nil, nil, err
	}
	return srv.values, srv.warnings, nil
}

// testSearchLabelValuesServer is a test implementation of Ingester_SearchLabelValuesServer.
type testSearchLabelValuesServer struct {
	grpc.ServerStream
	ctx      context.Context
	values   []string
	warnings []string
}

func (s *testSearchLabelValuesServer) Send(resp *client.SearchLabelValuesResponse) error {
	for _, r := range resp.Results {
		s.values = append(s.values, r.Value)
	}
	s.warnings = append(s.warnings, resp.Warnings...)
	return nil
}

func (s *testSearchLabelValuesServer) Context() context.Context     { return s.ctx }
func (s *testSearchLabelValuesServer) SetHeader(metadata.MD) error  { return nil }
func (s *testSearchLabelValuesServer) SendHeader(metadata.MD) error { return nil }
func (s *testSearchLabelValuesServer) SetTrailer(metadata.MD)       {}
func (s *testSearchLabelValuesServer) SendMsg(interface{}) error    { return nil }
func (s *testSearchLabelValuesServer) RecvMsg(interface{}) error    { return nil }

func Test_Ingester_SearchLabelNames(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(model.MetricNameLabel, "test_1", "status", "200", "route", "get_user"), 1, 100000},
		{labels.FromStrings(model.MetricNameLabel, "test_1", "status", "500", "route", "get_user"), 1, 110000},
		{labels.FromStrings(model.MetricNameLabel, "test_2"), 2, 200000},
		{labels.FromStrings(model.MetricNameLabel, "test_3", "status", "500"), 2, 200000},
	}

	registry := prometheus.NewRegistry()

	i, r, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, registry)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	ctx := user.InjectOrgID(context.Background(), "test")

	for _, s := range series {
		req := mockWriteRequest(t, s.lbls, s.value, s.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	t.Run("no filter returns all names", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"__name__", "status", "route"}, names)
	})

	t.Run("filter matching subset", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"status"}},
		}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.Equal(t, []string{"status"}, names)
	})

	t.Run("filter matching AND - no match", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"status", "route"}, Operator: 1},
		}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.Empty(t, names)
	})

	t.Run("filter matching AND - match", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"sta", "tus"}, Operator: 1},
		}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.Equal(t, []string{"status"}, names)
	})

	t.Run("filter matching OR - match", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"status", "route"}, Operator: 0},
		}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.Equal(t, []string{"route", "status"}, names)
	})

	t.Run("filter matching nothing", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"zzz"}},
		}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.Empty(t, names)
	})

	t.Run("limit applied after filter", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Limit:          1,
		}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.Len(t, names, 1)
	})

	t.Run("filter then limit", func(t *testing.T) {
		// filter matches "status" and "route"; limit=1 keeps just one
		req := &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"t"}},
			Limit:          1,
		}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.Len(t, names, 1)
		require.True(t, names[0] == "status" || names[0] == "route")
	})

	t.Run("case insensitive filter", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"STATUS"}, CaseInsensitive: true},
		}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.Equal(t, []string{"status"}, names)
	})

	t.Run("no TSDB returns empty response", func(t *testing.T) {
		// Use a different tenant that has no data.
		emptyCtx := user.InjectOrgID(context.Background(), "no-data-tenant")
		req := &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64}
		names, err := collectSearchLabelNames(i, emptyCtx, req)
		require.NoError(t, err)
		assert.Empty(t, names)
	})

	t.Run("sort alpha ascending", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SortBy: 1, SortOrder: 0},
		}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.Equal(t, []string{"__name__", "route", "status"}, names)
	})

	t.Run("sort alpha descending", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SortBy: 1, SortOrder: 1},
		}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.Equal(t, []string{"status", "route", "__name__"}, names)
	})

	t.Run("sort alpha descending limit 2", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SortBy: 1, SortOrder: 1},
			Limit:          2,
		}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.Equal(t, []string{"status", "route"}, names)
	})

	t.Run("sort score descending (best match first)", func(t *testing.T) {
		// "__name__" contains "ame" as a substring → FilterContains match, score = -1.
		// "status" does not contain "ame" but Jaro("ame","status") ≈ 0.5 > threshold → score = 0.5.
		// "route" does not match either filter.
		// Score DESC (SortOrder=0): status(0.5) before __name__(-1).
		req := &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Filter: &client.SearchLabelValuesFilter{
				SearchTerms:   []string{"ame"},
				FuzzThreshold: 0.4,
				SortBy:        2,
				SortOrder:     0,
			},
		}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.Equal(t, []string{"status", "__name__"}, names)
	})

	t.Run("sort score ascending (worst match first)", func(t *testing.T) {
		// Same filter as above; SortOrder=1 reverses: __name__(-1) before status(0.5).
		req := &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Filter: &client.SearchLabelValuesFilter{
				SearchTerms:   []string{"ame"},
				FuzzThreshold: 0.4,
				SortBy:        2,
				SortOrder:     1,
			},
		}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.Equal(t, []string{"__name__", "status"}, names)
	})
}

func Test_Ingester_SearchLabelValues(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(model.MetricNameLabel, "test_1", "status", "200", "route", "get_user"), 1, 100000},
		{labels.FromStrings(model.MetricNameLabel, "test_1", "status", "500", "route", "get_user"), 1, 110000},
		{labels.FromStrings(model.MetricNameLabel, "test_2"), 2, 200000},
	}

	registry := prometheus.NewRegistry()

	i, r, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, registry)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	ctx := user.InjectOrgID(context.Background(), "test")

	for _, s := range series {
		req := mockWriteRequest(t, s.lbls, s.value, s.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	t.Run("no filter returns all values", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			LabelName:      "__name__",
			EndTimestampMs: math.MaxInt64,
		}
		values, err := collectSearchLabelValues(i, ctx, req)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"test_1", "test_2"}, values)
	})

	t.Run("filter matching one value", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			LabelName:      "status",
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"200"}},
		}
		values, err := collectSearchLabelValues(i, ctx, req)
		require.NoError(t, err)
		assert.Equal(t, []string{"200"}, values)
	})

	t.Run("filter matching nothing", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			LabelName:      "status",
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"zzz"}},
		}
		values, err := collectSearchLabelValues(i, ctx, req)
		require.NoError(t, err)
		assert.Empty(t, values)
	})

	t.Run("limit applied", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			LabelName:      "status",
			EndTimestampMs: math.MaxInt64,
			Limit:          1,
		}
		values, err := collectSearchLabelValues(i, ctx, req)
		require.NoError(t, err)
		assert.Len(t, values, 1)
	})

	t.Run("unknown label returns empty", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			LabelName:      "unknown",
			EndTimestampMs: math.MaxInt64,
		}
		values, err := collectSearchLabelValues(i, ctx, req)
		require.NoError(t, err)
		assert.Empty(t, values)
	})

	t.Run("no TSDB returns empty response", func(t *testing.T) {
		emptyCtx := user.InjectOrgID(context.Background(), "no-data-tenant")
		req := &client.SearchLabelValuesRequest{
			LabelName:      "__name__",
			EndTimestampMs: math.MaxInt64,
		}
		values, err := collectSearchLabelValues(i, emptyCtx, req)
		require.NoError(t, err)
		assert.Empty(t, values)
	})

	t.Run("streaming early-exit: limit stops iteration before all values are read", func(t *testing.T) {
		// "status" has values {"200", "500"} sorted. With Limit=1, only one value
		// should be returned, confirming that iteration stops after the first match.
		req := &client.SearchLabelValuesRequest{
			LabelName:      "status",
			EndTimestampMs: math.MaxInt64,
			Limit:          1,
		}
		values, err := collectSearchLabelValues(i, ctx, req)
		require.NoError(t, err)
		require.Len(t, values, 1)
		// TSDB returns values in sorted order; the first alpha value must be "200".
		assert.Equal(t, "200", values[0])
	})

	t.Run("streaming with filter and limit: filter applied before limit", func(t *testing.T) {
		// Only "200" matches the filter; limit=1 should return exactly that value.
		req := &client.SearchLabelValuesRequest{
			LabelName:      "status",
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"2"}},
			Limit:          1,
		}
		values, err := collectSearchLabelValues(i, ctx, req)
		require.NoError(t, err)
		require.Len(t, values, 1)
		assert.Equal(t, "200", values[0])
	})
}

func Test_Ingester_SearchLabelNames_MaxLimitWarning(t *testing.T) {
	// Series produce 3 distinct label names: __name__, status, route.
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(model.MetricNameLabel, "test_1", "status", "200", "route", "get_user"), 1, 100000},
		{labels.FromStrings(model.MetricNameLabel, "test_2"), 2, 200000},
	}

	ctx := user.InjectOrgID(context.Background(), "test")

	pushSeries := func(i *Ingester) {
		for _, s := range series {
			req := mockWriteRequest(t, s.lbls, s.value, s.timestamp)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
	}

	t.Run("limit=0 (unlimited): no warning emitted", func(t *testing.T) {
		limits := defaultLimitsTestConfig()
		limits.MaxLabelNamesLimit = 0
		i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", nil)
		require.NoError(t, err)
		startAndWaitHealthy(t, i, r)
		pushSeries(i)

		names, warnings, err := collectSearchLabelNamesWithWarnings(i, ctx, &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"__name__", "status", "route"}, names)
		assert.Empty(t, warnings)
	})

	t.Run("limit=2: warning emitted, results truncated", func(t *testing.T) {
		limits := defaultLimitsTestConfig()
		limits.MaxLabelNamesLimit = 2
		i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", nil)
		require.NoError(t, err)
		startAndWaitHealthy(t, i, r)
		pushSeries(i)

		names, warnings, err := collectSearchLabelNamesWithWarnings(i, ctx, &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64})
		require.NoError(t, err)
		assert.Len(t, names, 2)
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "the label names search has exceeded the allowed number of records")
	})

	t.Run("limit=10: no warning (fewer names than limit)", func(t *testing.T) {
		limits := defaultLimitsTestConfig()
		limits.MaxLabelNamesLimit = 10
		i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", nil)
		require.NoError(t, err)
		startAndWaitHealthy(t, i, r)
		pushSeries(i)

		names, warnings, err := collectSearchLabelNamesWithWarnings(i, ctx, &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"__name__", "status", "route"}, names)
		assert.Empty(t, warnings)
	})
}

func Test_Ingester_SearchLabelValues_MaxLimitWarning(t *testing.T) {
	// Series produce 2 distinct values for "status": "200", "500".
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(model.MetricNameLabel, "test_1", "status", "200"), 1, 100000},
		{labels.FromStrings(model.MetricNameLabel, "test_1", "status", "500"), 1, 110000},
	}

	ctx := user.InjectOrgID(context.Background(), "test")

	pushSeries := func(i *Ingester) {
		for _, s := range series {
			req := mockWriteRequest(t, s.lbls, s.value, s.timestamp)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
	}

	req := func() *client.SearchLabelValuesRequest {
		return &client.SearchLabelValuesRequest{LabelName: "status", EndTimestampMs: math.MaxInt64}
	}

	t.Run("limit=0 (unlimited): no warning emitted", func(t *testing.T) {
		limits := defaultLimitsTestConfig()
		limits.MaxLabelValuesLimit = 0
		i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", nil)
		require.NoError(t, err)
		startAndWaitHealthy(t, i, r)
		pushSeries(i)

		values, warnings, err := collectSearchLabelValuesWithWarnings(i, ctx, req())
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"200", "500"}, values)
		assert.Empty(t, warnings)
	})

	t.Run("limit=1: warning emitted, results truncated", func(t *testing.T) {
		limits := defaultLimitsTestConfig()
		limits.MaxLabelValuesLimit = 1
		i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", nil)
		require.NoError(t, err)
		startAndWaitHealthy(t, i, r)
		pushSeries(i)

		values, warnings, err := collectSearchLabelValuesWithWarnings(i, ctx, req())
		require.NoError(t, err)
		assert.Len(t, values, 1)
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "the label values search has exceeded the allowed number of records")
	})

	t.Run("limit=10: no warning (fewer values than limit)", func(t *testing.T) {
		limits := defaultLimitsTestConfig()
		limits.MaxLabelValuesLimit = 10
		i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", nil)
		require.NoError(t, err)
		startAndWaitHealthy(t, i, r)
		pushSeries(i)

		values, warnings, err := collectSearchLabelValuesWithWarnings(i, ctx, req())
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"200", "500"}, values)
		assert.Empty(t, warnings)
	})
}

func Test_Ingester_SearchLabelNames_MaxBytesLimit(t *testing.T) {
	// Series produce 3 distinct label names: __name__ (8 bytes), status (6 bytes), route (5 bytes).
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(model.MetricNameLabel, "test_1", "status", "200", "route", "get_user"), 1, 100000},
	}

	ctx := user.InjectOrgID(context.Background(), "test")

	setup := func(t *testing.T, maxBytes int) *Ingester {
		limits := defaultLimitsTestConfig()
		limits.LabelNamesAndValuesResultsMaxSizeBytes = maxBytes
		i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", nil)
		require.NoError(t, err)
		startAndWaitHealthy(t, i, r)
		for _, s := range series {
			req := mockWriteRequest(t, s.lbls, s.value, s.timestamp)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
		return i
	}

	t.Run("limit=0 (unlimited): no error", func(t *testing.T) {
		i := setup(t, 0)
		names, err := collectSearchLabelNames(i, ctx, &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"__name__", "status", "route"}, names)
	})

	t.Run("no-sort path: limit smaller than first name triggers error", func(t *testing.T) {
		// "__name__" is 8 bytes; a limit of 4 is exceeded on the first value.
		i := setup(t, 4)
		_, err := collectSearchLabelNames(i, ctx, &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64})
		require.Error(t, err)
		assert.ErrorContains(t, err, "buffer exceeded max size")
	})

	t.Run("sort path: cumulative bytes limit exceeded during buffering", func(t *testing.T) {
		// With sort, all values are buffered before sending. Total bytes = 8+6+5 = 19;
		// a limit of 10 is exceeded when buffering the third name.
		i := setup(t, 10)
		_, err := collectSearchLabelNames(i, ctx, &client.SearchLabelValuesRequest{
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SortBy: 1},
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "buffer exceeded max size")
	})

	t.Run("limit=1000: no error", func(t *testing.T) {
		i := setup(t, 1000)
		names, err := collectSearchLabelNames(i, ctx, &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"__name__", "status", "route"}, names)
	})
}

func Test_Ingester_SearchLabelValues_MaxBytesLimit(t *testing.T) {
	// Series produce 2 distinct values for "status": "200" (3 bytes), "500" (3 bytes).
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(model.MetricNameLabel, "test_1", "status", "200"), 1, 100000},
		{labels.FromStrings(model.MetricNameLabel, "test_1", "status", "500"), 1, 110000},
	}

	ctx := user.InjectOrgID(context.Background(), "test")

	setup := func(t *testing.T, maxBytes int) *Ingester {
		limits := defaultLimitsTestConfig()
		limits.LabelNamesAndValuesResultsMaxSizeBytes = maxBytes
		i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, "", nil)
		require.NoError(t, err)
		startAndWaitHealthy(t, i, r)
		for _, s := range series {
			req := mockWriteRequest(t, s.lbls, s.value, s.timestamp)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
		return i
	}

	statusReq := func(filter *client.SearchLabelValuesFilter) *client.SearchLabelValuesRequest {
		return &client.SearchLabelValuesRequest{LabelName: "status", EndTimestampMs: math.MaxInt64, Filter: filter}
	}

	t.Run("limit=0 (unlimited): no error", func(t *testing.T) {
		i := setup(t, 0)
		values, err := collectSearchLabelValues(i, ctx, statusReq(nil))
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"200", "500"}, values)
	})

	t.Run("no-sort path: limit smaller than first value triggers error", func(t *testing.T) {
		// "200" is 3 bytes; a limit of 2 is exceeded on the first value.
		i := setup(t, 2)
		_, err := collectSearchLabelValues(i, ctx, statusReq(nil))
		require.Error(t, err)
		assert.ErrorContains(t, err, "buffer exceeded max size")
	})

	t.Run("sort path: cumulative bytes limit exceeded during buffering", func(t *testing.T) {
		// With sort, both values are buffered. Total = 3+3 = 6 bytes; limit of 4 is exceeded
		// after buffering the second value.
		i := setup(t, 4)
		_, err := collectSearchLabelValues(i, ctx, statusReq(&client.SearchLabelValuesFilter{SortBy: 1}))
		require.Error(t, err)
		assert.ErrorContains(t, err, "buffer exceeded max size")
	})

	t.Run("limit=1000: no error", func(t *testing.T) {
		i := setup(t, 1000)
		values, err := collectSearchLabelValues(i, ctx, statusReq(nil))
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"200", "500"}, values)
	})
}
