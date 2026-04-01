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

func (s *testSearchLabelNamesServer) Send(resp *client.SearchLabelValuesResponse) error {
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

func (s *testSearchLabelValuesServer) Send(resp *client.SearchLabelValuesResponse) error {
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

	tests := []struct {
		name        string
		req         *client.SearchLabelValuesRequest
		useEmptyCtx bool
		// wantNames is the expected set of names. When ordered=false, ElementsMatch is used
		// (any order); when ordered=true, assert.Equal is used (exact order).
		// When nil, wantLen is checked instead (0 means assert empty).
		wantNames []string
		ordered   bool
		wantLen   int
	}{
		{
			name:      "no filter returns all names",
			req:       &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64},
			wantNames: []string{"__name__", "status", "route"},
		},
		{
			name: "filter matching subset",
			req: &client.SearchLabelValuesRequest{
				EndTimestampMs: math.MaxInt64,
				Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"status"}},
			},
			wantNames: []string{"status"},
			ordered:   true,
		},
		{
			name: "filter matching multiple",
			req: &client.SearchLabelValuesRequest{
				EndTimestampMs: math.MaxInt64,
				Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"status", "route"}},
			},
			wantNames: []string{"route", "status"},
			ordered:   true,
		},
		{
			name: "filter matching nothing",
			req: &client.SearchLabelValuesRequest{
				EndTimestampMs: math.MaxInt64,
				Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"zzz"}},
			},
		},
		{
			name: "limit applied",
			req: &client.SearchLabelValuesRequest{
				EndTimestampMs: math.MaxInt64,
				Limit:          1,
			},
			wantLen: 1,
		},
		{
			// filter matches "status" and "route"; limit=1 keeps just one
			name: "filter then limit",
			req: &client.SearchLabelValuesRequest{
				EndTimestampMs: math.MaxInt64,
				Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"t"}},
				Limit:          1,
			},
			wantLen: 1,
		},
		{
			// 3 names exist: __name__, route, status (alpha order).
			// Filter for "status" matches only "status" — the last name alphabetically.
			// If the TSDB limit were applied before filtering (the bug), a limit=1 would
			// return only "__name__" (first alpha), which doesn't match, yielding 0 results.
			// The correct behaviour is: filter first, then limit → ["status"].
			name: "limit is not applied before filter",
			req: &client.SearchLabelValuesRequest{
				EndTimestampMs: math.MaxInt64,
				Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"status"}},
				Limit:          1,
			},
			wantNames: []string{"status"},
			ordered:   true,
		},
		{
			name: "case insensitive filter",
			req: &client.SearchLabelValuesRequest{
				EndTimestampMs: math.MaxInt64,
				Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"STATUS"}, CaseInsensitive: true},
			},
			wantNames: []string{"status"},
			ordered:   true,
		},
		{
			name:        "no TSDB returns empty response",
			req:         &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64},
			useEmptyCtx: true,
		},
		{
			name: "sort alpha ascending",
			req: &client.SearchLabelValuesRequest{
				EndTimestampMs: math.MaxInt64,
				Filter:         &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA, SortOrder: client.SORT_ORDER_ASC},
			},
			wantNames: []string{"__name__", "route", "status"},
			ordered:   true,
		},
		{
			name: "sort alpha descending",
			req: &client.SearchLabelValuesRequest{
				EndTimestampMs: math.MaxInt64,
				Filter:         &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA, SortOrder: client.SORT_ORDER_DESC},
			},
			wantNames: []string{"status", "route", "__name__"},
			ordered:   true,
		},
		{
			name: "sort alpha descending limit 2",
			req: &client.SearchLabelValuesRequest{
				EndTimestampMs: math.MaxInt64,
				Filter:         &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA, SortOrder: client.SORT_ORDER_DESC},
				Limit:          2,
			},
			wantNames: []string{"status", "route"},
			ordered:   true,
		},
		{
			// "__name__" contains "ame" as a substring → FilterContains match, score = -1.
			// "status" does not contain "ame" but Jaro("ame","status") ≈ 0.5 > threshold → score = 0.5.
			// "route" does not match either filter.
			// Score DESC (SortOrder=ASC): status(0.5) before __name__(-1).
			name: "sort score best match first",
			req: &client.SearchLabelValuesRequest{
				EndTimestampMs: math.MaxInt64,
				Filter: &client.SearchLabelValuesFilter{
					SearchTerms:   []string{"ame"},
					FuzzThreshold: 0.4,
					SortBy:        client.SORT_BY_SCORE,
					SortOrder:     client.SORT_ORDER_ASC,
				},
			},
			wantNames: []string{"status", "__name__"},
			ordered:   true,
		},
		{
			// Same filter; SortOrder=DESC reverses: __name__(-1) before status(0.5).
			name: "sort score worst match first",
			req: &client.SearchLabelValuesRequest{
				EndTimestampMs: math.MaxInt64,
				Filter: &client.SearchLabelValuesFilter{
					SearchTerms:   []string{"ame"},
					FuzzThreshold: 0.4,
					SortBy:        client.SORT_BY_SCORE,
					SortOrder:     client.SORT_ORDER_DESC,
				},
			},
			wantNames: []string{"__name__", "status"},
			ordered:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reqCtx := ctx
			if tc.useEmptyCtx {
				reqCtx = user.InjectOrgID(context.Background(), "no-data-tenant")
			}
			names, err := collectSearchLabelNames(i, reqCtx, tc.req)
			require.NoError(t, err)
			switch {
			case tc.wantNames != nil && tc.ordered:
				assert.Equal(t, tc.wantNames, names)
			case tc.wantNames != nil:
				assert.ElementsMatch(t, tc.wantNames, names)
			case tc.wantLen > 0:
				assert.Len(t, names, tc.wantLen)
			default:
				assert.Empty(t, names)
			}
		})
	}
}

// Test_Ingester_Search_PersistBlocks verifies that SearchLabelNames and SearchLabelValues
// work correctly when data has been flushed from the TSDB head into persisted blocks.
// This exercises the tsdb.DB.Querier() path used after the compaction-race fix and
// confirms no ErrClosing (or similar) propagates when querying through the querier.
func Test_Ingester_Search_PersistBlocks(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(model.MetricNameLabel, "metric_a", "env", "prod", "region", "us-east"), 1, 100000},
		{labels.FromStrings(model.MetricNameLabel, "metric_b", "env", "staging"), 1, 110000},
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

	// Flush compacts the head into a persisted block. After this, data is no longer in
	// the head — queries must go through the persisted block index via the querier.
	i.Flush()

	t.Run("SearchLabelNames returns names from persisted block", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64}
		names, err := collectSearchLabelNames(i, ctx, req)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"__name__", "env", "region"}, names)
	})

	t.Run("SearchLabelValues returns values from persisted block", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			LabelName:      "env",
			EndTimestampMs: math.MaxInt64,
		}
		values, err := collectSearchLabelValues(i, ctx, req)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"prod", "staging"}, values)
	})

	t.Run("filter works against persisted block", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			LabelName:      "__name__",
			EndTimestampMs: math.MaxInt64,
			Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"metric_a"}},
		}
		values, err := collectSearchLabelValues(i, ctx, req)
		require.NoError(t, err)
		assert.Equal(t, []string{"metric_a"}, values)
	})

	t.Run("limit works against persisted block", func(t *testing.T) {
		req := &client.SearchLabelValuesRequest{
			LabelName:      "__name__",
			EndTimestampMs: math.MaxInt64,
			Limit:          1,
		}
		values, err := collectSearchLabelValues(i, ctx, req)
		require.NoError(t, err)
		assert.Len(t, values, 1)
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

	tests := []struct {
		name        string
		req         *client.SearchLabelValuesRequest
		useEmptyCtx bool
		wantValues  []string
		ordered     bool
		wantLen     int
	}{
		{
			name:       "no filter returns all values",
			req:        &client.SearchLabelValuesRequest{LabelName: "__name__", EndTimestampMs: math.MaxInt64},
			wantValues: []string{"test_1", "test_2"},
		},
		{
			name: "filter matching one value",
			req: &client.SearchLabelValuesRequest{
				LabelName:      "status",
				EndTimestampMs: math.MaxInt64,
				Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"200"}},
			},
			wantValues: []string{"200"},
			ordered:    true,
		},
		{
			name: "filter matching nothing",
			req: &client.SearchLabelValuesRequest{
				LabelName:      "status",
				EndTimestampMs: math.MaxInt64,
				Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"zzz"}},
			},
		},
		{
			name:    "limit applied",
			req:     &client.SearchLabelValuesRequest{LabelName: "status", EndTimestampMs: math.MaxInt64, Limit: 1},
			wantLen: 1,
		},
		{
			name: "unknown label returns empty",
			req:  &client.SearchLabelValuesRequest{LabelName: "unknown", EndTimestampMs: math.MaxInt64},
		},
		{
			name:        "no TSDB returns empty response",
			req:         &client.SearchLabelValuesRequest{LabelName: "__name__", EndTimestampMs: math.MaxInt64},
			useEmptyCtx: true,
		},
		{
			// "status" has values {"200", "500"} sorted. Limit=1 must stop after the first
			// alpha value; TSDB returns values sorted so "200" comes first.
			name:       "limit stops at first value",
			req:        &client.SearchLabelValuesRequest{LabelName: "status", EndTimestampMs: math.MaxInt64, Limit: 1},
			wantValues: []string{"200"},
			ordered:    true,
		},
		{
			// Only "200" matches the filter; limit=1 must return exactly that value.
			name: "filter applied before limit",
			req: &client.SearchLabelValuesRequest{
				LabelName:      "status",
				EndTimestampMs: math.MaxInt64,
				Filter:         &client.SearchLabelValuesFilter{SearchTerms: []string{"2"}},
				Limit:          1,
			},
			wantValues: []string{"200"},
			ordered:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reqCtx := ctx
			if tc.useEmptyCtx {
				reqCtx = user.InjectOrgID(context.Background(), "no-data-tenant")
			}
			values, err := collectSearchLabelValues(i, reqCtx, tc.req)
			require.NoError(t, err)
			switch {
			case tc.wantValues != nil && tc.ordered:
				assert.Equal(t, tc.wantValues, values)
			case tc.wantValues != nil:
				assert.ElementsMatch(t, tc.wantValues, values)
			case tc.wantLen > 0:
				assert.Len(t, values, tc.wantLen)
			default:
				assert.Empty(t, values)
			}
		})
	}
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
		limits.IngesterSearchLabelsValuesMaxSizeBytes = maxBytes
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

	tests := []struct {
		name       string
		maxBytes   int
		req        *client.SearchLabelValuesRequest
		wantErr    string
		wantNames  []string
	}{
		{
			name:      "unlimited",
			maxBytes:  0,
			req:       &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64},
			wantNames: []string{"__name__", "status", "route"},
		},
		{
			// "__name__" is 8 bytes; a limit of 4 is exceeded on the first value.
			name:     "no-sort path: limit smaller than first name triggers error",
			maxBytes: 4,
			req:      &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64},
			wantErr:  "buffer exceeded max size",
		},
		{
			// With sort, all values are buffered. Total bytes = 8+6+5 = 19;
			// a limit of 10 is exceeded when buffering the third name.
			name:     "sort path: cumulative bytes limit exceeded during buffering",
			maxBytes: 10,
			req: &client.SearchLabelValuesRequest{
				EndTimestampMs: math.MaxInt64,
				Filter:         &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA},
			},
			wantErr: "buffer exceeded max size",
		},
		{
			name:      "within limit",
			maxBytes:  1000,
			req:       &client.SearchLabelValuesRequest{EndTimestampMs: math.MaxInt64},
			wantNames: []string{"__name__", "status", "route"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			i := setup(t, tc.maxBytes)
			names, err := collectSearchLabelNames(i, ctx, tc.req)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				assert.ElementsMatch(t, tc.wantNames, names)
			}
		})
	}
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
		limits.IngesterSearchLabelsValuesMaxSizeBytes = maxBytes
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

	tests := []struct {
		name       string
		maxBytes   int
		filter     *client.SearchLabelValuesFilter
		wantErr    string
		wantValues []string
	}{
		{
			name:       "unlimited",
			maxBytes:   0,
			wantValues: []string{"200", "500"},
		},
		{
			// "200" is 3 bytes; a limit of 2 is exceeded on the first value.
			name:     "no-sort path: limit smaller than first value triggers error",
			maxBytes: 2,
			wantErr:  "buffer exceeded max size",
		},
		{
			// With sort, both values are buffered. Total = 3+3 = 6 bytes; limit of 4 is
			// exceeded after buffering the second value.
			name:     "sort path: cumulative bytes limit exceeded during buffering",
			maxBytes: 4,
			filter:   &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA},
			wantErr:  "buffer exceeded max size",
		},
		{
			name:       "within limit",
			maxBytes:   1000,
			wantValues: []string{"200", "500"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			i := setup(t, tc.maxBytes)
			req := &client.SearchLabelValuesRequest{LabelName: "status", EndTimestampMs: math.MaxInt64, Filter: tc.filter}
			values, err := collectSearchLabelValues(i, ctx, req)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				assert.ElementsMatch(t, tc.wantValues, values)
			}
		})
	}
}
