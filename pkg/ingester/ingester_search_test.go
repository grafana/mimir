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
	ctx   context.Context
	names []string
}

func (s *testSearchLabelNamesServer) Send(resp *client.SearchResponse) error {
	for _, r := range resp.Results {
		s.names = append(s.names, r.Value)
	}
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

// testSearchLabelValuesServer is a test implementation of Ingester_SearchLabelValuesServer.
type testSearchLabelValuesServer struct {
	grpc.ServerStream
	ctx    context.Context
	values []string
}

func (s *testSearchLabelValuesServer) Send(resp *client.SearchResponse) error {
	for _, r := range resp.Results {
		s.values = append(s.values, r.Value)
	}
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
}
