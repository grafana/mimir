// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	util_test "github.com/grafana/mimir/pkg/util/test"
)

type mockSearchLabelNamesStream struct {
	client.Ingester_SearchLabelNamesServer
	ctx  context.Context
	sent []*client.SearchResultBatch
}

func (m *mockSearchLabelNamesStream) Send(b *client.SearchResultBatch) error {
	m.sent = append(m.sent, b)
	return nil
}
func (m *mockSearchLabelNamesStream) Context() context.Context { return m.ctx }

func TestIngesterSearchLabelNames(t *testing.T) {
	series := []util_test.Series{
		{Labels: labels.FromStrings(model.MetricNameLabel, "metric_a", "status", "200"), Samples: []util_test.Sample{{TS: 100000, Val: 1}}},
		{Labels: labels.FromStrings(model.MetricNameLabel, "metric_b", "env", "prod"), Samples: []util_test.Sample{{TS: 110000, Val: 1}}},
	}
	registry := prometheus.NewRegistry()
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), registry)
	ctx := user.InjectOrgID(context.Background(), "test")
	require.NoError(t, pushSeriesToIngester(ctx, t, i, series))

	tests := []struct {
		name        string
		filterTerms []string
		caseInsens  bool
		ordering    client.SearchOrdering
		limit       int64
		wantValues  []string
	}{
		{name: "no filter returns all", wantValues: []string{"__name__", "env", "status"}},
		{name: "substring 'env' case-sensitive", filterTerms: []string{"env"}, wantValues: []string{"env"}},
		{name: "case-insensitive 'ENV' matches", filterTerms: []string{"ENV"}, caseInsens: true, wantValues: []string{"env"}},
		{name: "limit 1", limit: 1, wantValues: []string{"__name__"}},
		{name: "value desc", ordering: client.ORDER_BY_VALUE_DESC, wantValues: []string{"status", "env", "__name__"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &client.SearchLabelNamesRequest{
				StartTimestampMs: 0,
				EndTimestampMs:   200_000,
				Filter:           &client.SearchFilter{Terms: tc.filterTerms, CaseInsensitive: tc.caseInsens},
				Ordering:         tc.ordering,
				Limit:            tc.limit,
			}
			s := &mockSearchLabelNamesStream{ctx: ctx}
			require.NoError(t, i.SearchLabelNames(req, s))

			var got []string
			for _, b := range s.sent {
				for _, r := range b.Results {
					got = append(got, r.Value)
				}
			}
			assert.Equal(t, tc.wantValues, got)
		})
	}
}

type mockSearchLabelValuesStream struct {
	client.Ingester_SearchLabelValuesServer
	ctx  context.Context
	sent []*client.SearchResultBatch
}

func (m *mockSearchLabelValuesStream) Send(b *client.SearchResultBatch) error {
	m.sent = append(m.sent, b)
	return nil
}
func (m *mockSearchLabelValuesStream) Context() context.Context { return m.ctx }

func TestIngesterSearchLabelValues(t *testing.T) {
	series := []util_test.Series{
		{Labels: labels.FromStrings(model.MetricNameLabel, "metric", "status", "200"), Samples: []util_test.Sample{{TS: 100000, Val: 1}}},
		{Labels: labels.FromStrings(model.MetricNameLabel, "metric", "status", "300"), Samples: []util_test.Sample{{TS: 110000, Val: 1}}},
		{Labels: labels.FromStrings(model.MetricNameLabel, "metric", "status", "500"), Samples: []util_test.Sample{{TS: 120000, Val: 1}}},
	}
	registry := prometheus.NewRegistry()
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), registry)
	ctx := user.InjectOrgID(context.Background(), "test")
	require.NoError(t, pushSeriesToIngester(ctx, t, i, series))

	tests := []struct {
		name          string
		labelName     string
		filterTerms   []string
		caseInsens    bool
		fuzzAlg       client.SearchFilter_FuzzAlg
		fuzzThreshold int32
		ordering      client.SearchOrdering
		limit         int64
		want          []string
	}{
		{name: "all status values", labelName: "status", want: []string{"200", "300", "500"}},
		{name: "substring '0'", labelName: "status", filterTerms: []string{"0"}, want: []string{"200", "300", "500"}},
		{name: "substring '20'", labelName: "status", filterTerms: []string{"20"}, want: []string{"200"}},
		{name: "limit 2", labelName: "status", limit: 2, want: []string{"200", "300"}},
		{name: "value desc", labelName: "status", ordering: client.ORDER_BY_VALUE_DESC, want: []string{"500", "300", "200"}},
		{name: "missing label returns empty", labelName: "missing"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &client.SearchLabelValuesRequest{
				StartTimestampMs: 0,
				EndTimestampMs:   200_000,
				Name:             tc.labelName,
				Filter: &client.SearchFilter{
					Terms:           tc.filterTerms,
					CaseInsensitive: tc.caseInsens,
					FuzzAlg:         tc.fuzzAlg,
					FuzzThreshold:   tc.fuzzThreshold,
				},
				Ordering: tc.ordering,
				Limit:    tc.limit,
			}
			s := &mockSearchLabelValuesStream{ctx: ctx}
			require.NoError(t, i.SearchLabelValues(req, s))

			var got []string
			for _, b := range s.sent {
				for _, r := range b.Results {
					got = append(got, r.Value)
				}
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIngesterSearchLabelValuesWithMetricMatcher(t *testing.T) {
	const (
		bigMetric   = "big_metric"
		smallMetric = "small_metric"
	)
	var series []util_test.Series
	add := func(metric, tenant string) {
		series = append(series, util_test.Series{
			Labels:  labels.FromStrings(model.MetricNameLabel, metric, "cluster", "c", "tenant", tenant),
			Samples: []util_test.Sample{{TS: 100000, Val: 1}},
		})
	}
	// bigMetric matches far more series than 1/100 of the tenant values, so
	// TSDB takes the per-value postings path; smallMetric takes the series path.
	for n := 0; n < 300; n++ {
		add(bigMetric, fmt.Sprintf("tenant-%03d", n))
	}
	for _, v := range []string{"Example-1", "Example-2", "Example-3"} {
		add(bigMetric, v)
	}
	add(smallMetric, "tenant-001")
	add(smallMetric, "Example-1")
	// A value that matches the term but is not on bigMetric must not be returned.
	add("other_metric", "example-elsewhere")

	registry := prometheus.NewRegistry()
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), registry)
	ctx := user.InjectOrgID(context.Background(), "test")
	require.NoError(t, pushSeriesToIngester(ctx, t, i, series))

	tests := []struct {
		name       string
		metric     string
		filter     *client.SearchFilter
		ordering   client.SearchOrdering
		limit      int64
		wantValues []string
	}{
		{
			name:       "postings path, selective term",
			metric:     bigMetric,
			filter:     &client.SearchFilter{Terms: []string{"example"}, CaseInsensitive: true, FuzzAlg: client.FUZZ_ALG_SUBSTRING_LEFT},
			wantValues: []string{"Example-1", "Example-2", "Example-3"},
		},
		{
			name:       "postings path, expression with NOT",
			metric:     bigMetric,
			filter:     &client.SearchFilter{Expression: "example and not 2", CaseInsensitive: true, FuzzAlg: client.FUZZ_ALG_SUBSTRING_LEFT},
			wantValues: []string{"Example-1", "Example-3"},
		},
		{
			name:   "postings path, no value passes the filter",
			metric: bigMetric,
			filter: &client.SearchFilter{Terms: []string{"nomatch"}, FuzzAlg: client.FUZZ_ALG_SUBSTRING_LEFT},
		},
		{
			name:       "postings path, limit",
			metric:     bigMetric,
			filter:     &client.SearchFilter{Terms: []string{"example"}, CaseInsensitive: true, FuzzAlg: client.FUZZ_ALG_SUBSTRING_LEFT},
			limit:      2,
			wantValues: []string{"Example-1", "Example-2"},
		},
		{
			name:       "postings path, score ordering",
			metric:     bigMetric,
			filter:     &client.SearchFilter{Terms: []string{"tenant-29"}, FuzzAlg: client.FUZZ_ALG_SUBSTRING_LEFT},
			ordering:   client.ORDER_BY_SCORE_DESC,
			wantValues: []string{"tenant-290", "tenant-291", "tenant-292", "tenant-293", "tenant-294", "tenant-295", "tenant-296", "tenant-297", "tenant-298", "tenant-299"},
		},
		{
			name:       "series path, selective term",
			metric:     smallMetric,
			filter:     &client.SearchFilter{Terms: []string{"example"}, CaseInsensitive: true, FuzzAlg: client.FUZZ_ALG_SUBSTRING_LEFT},
			wantValues: []string{"Example-1"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &client.SearchLabelValuesRequest{
				StartTimestampMs: 0,
				EndTimestampMs:   200_000,
				Name:             "tenant",
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: tc.metric},
					{Type: client.EQUAL, Name: "cluster", Value: "c"},
				},
				Filter:   tc.filter,
				Ordering: tc.ordering,
				Limit:    tc.limit,
			}
			s := &mockSearchLabelValuesStream{ctx: ctx}
			require.NoError(t, i.SearchLabelValues(req, s))

			var got []string
			for _, b := range s.sent {
				for _, r := range b.Results {
					got = append(got, r.Value)
				}
			}
			assert.Equal(t, tc.wantValues, got)
		})
	}
}

type mockSearchMetricsMetadataStream struct {
	client.Ingester_SearchMetricsMetadataServer
	ctx  context.Context
	sent []*client.SearchResultBatch
}

func (m *mockSearchMetricsMetadataStream) Send(b *client.SearchResultBatch) error {
	m.sent = append(m.sent, b)
	return nil
}
func (m *mockSearchMetricsMetadataStream) Context() context.Context { return m.ctx }

// pushMetadataToIngester pushes metric metadata only (no series).
func pushMetadataToIngester(ctx context.Context, t testing.TB, i *Ingester, metadata []*mimirpb.MetricMetadata) {
	t.Helper()
	_, err := i.Push(ctx, mimirpb.ToWriteRequest(nil, nil, nil, metadata, mimirpb.API))
	require.NoError(t, err)
}

// collectSearchMetricsMetadataValues drains a mockSearchMetricsMetadataStream
// into a slice of result values, in the order batches and results were sent.
func collectSearchMetricsMetadataValues(s *mockSearchMetricsMetadataStream) []string {
	var got []string
	for _, b := range s.sent {
		for _, r := range b.Results {
			got = append(got, r.Value)
		}
	}
	return got
}

func TestIngesterSearchMetricsMetadata(t *testing.T) {
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	ctx := user.InjectOrgID(context.Background(), "test")
	pushMetadataToIngester(ctx, t, i, []*mimirpb.MetricMetadata{
		{MetricFamilyName: "cpu_seconds_total", Help: "Total CPU time", Type: mimirpb.COUNTER},
		{MetricFamilyName: "memory_bytes", Help: "Current memory usage in bytes", Type: mimirpb.GAUGE},
		{MetricFamilyName: "disk_io_time", Help: "Cumulative disk IO time", Type: mimirpb.COUNTER},
	})

	tests := []struct {
		name        string
		filterTerms []string
		caseInsens  bool
		ordering    client.SearchOrdering
		limit       int64
		want        []string
	}{
		{name: "substring 'memory' matches HELP text", filterTerms: []string{"memory"}, want: []string{"memory_bytes"}},
		{name: "case-insensitive 'cpu' matches HELP text", filterTerms: []string{"cpu"}, caseInsens: true, want: []string{"cpu_seconds_total"}},
		{name: "term in every HELP text returns all names", filterTerms: []string{"e"}, want: []string{"cpu_seconds_total", "disk_io_time", "memory_bytes"}},
		{name: "limit 1", filterTerms: []string{"e"}, limit: 1, want: []string{"cpu_seconds_total"}},
		{name: "value desc", filterTerms: []string{"e"}, ordering: client.ORDER_BY_VALUE_DESC, want: []string{"memory_bytes", "disk_io_time", "cpu_seconds_total"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &client.SearchMetricsMetadataRequest{
				Filter:   &client.SearchFilter{Terms: tc.filterTerms, CaseInsensitive: tc.caseInsens},
				Ordering: tc.ordering,
				Limit:    tc.limit,
			}
			s := &mockSearchMetricsMetadataStream{ctx: ctx}
			require.NoError(t, i.SearchMetricsMetadata(req, s))
			assert.Equal(t, tc.want, collectSearchMetricsMetadataValues(s))
		})
	}
}

// TestIngesterSearchMetricsMetadataRejectsScoreOrderingAsInvalidArgument
// locks in that ORDER_BY_SCORE_DESC is rejected server-side, not just at the
// HTTP layer: every FuzzAlgSubstring match scores identically, so score
// ordering carries no information for this endpoint.
func TestIngesterSearchMetricsMetadataRejectsScoreOrderingAsInvalidArgument(t *testing.T) {
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	ctx := user.InjectOrgID(context.Background(), "test")
	pushMetadataToIngester(ctx, t, i, []*mimirpb.MetricMetadata{
		{MetricFamilyName: "cpu_seconds_total", Help: "Total CPU time", Type: mimirpb.COUNTER},
	})

	req := &client.SearchMetricsMetadataRequest{
		Filter:   &client.SearchFilter{Terms: []string{"cpu"}},
		Ordering: client.ORDER_BY_SCORE_DESC,
	}
	s := &mockSearchMetricsMetadataStream{ctx: ctx}
	err := i.SearchMetricsMetadata(req, s)
	require.Error(t, err)
	st, ok := grpcutil.ErrorToStatus(err)
	require.True(t, ok, "expected gRPC status error, got %T: %v", err, err)
	assert.Equal(t, codes.InvalidArgument, st.Code(), "ORDER_BY_SCORE_DESC must be rejected as codes.InvalidArgument")
}

func TestIngesterSearchMetricsMetadataWordPrefix(t *testing.T) {
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	ctx := user.InjectOrgID(context.Background(), "test")
	pushMetadataToIngester(ctx, t, i, []*mimirpb.MetricMetadata{
		{MetricFamilyName: "compression_ratio", Help: "Compression ratio of requests.", Type: mimirpb.HISTOGRAM},
		{MetricFamilyName: "discarded_series_ratio", Help: "Ratio of dropped series in the ingester.", Type: mimirpb.HISTOGRAM},
		{MetricFamilyName: "sync_duration_seconds", Help: "Duration of the sync operations.", Type: mimirpb.HISTOGRAM},
	})

	tests := map[string]struct {
		filter *client.SearchFilter
		want   []string
	}{
		"term matches only at the start of a word": {
			filter: &client.SearchFilter{Terms: []string{"ratio"}, CaseInsensitive: true, FuzzAlg: client.FUZZ_ALG_WORD_PREFIX},
			want:   []string{"compression_ratio", "discarded_series_ratio"},
		},
		"case-sensitive term matches only the same case": {
			filter: &client.SearchFilter{Terms: []string{"Ratio"}, FuzzAlg: client.FUZZ_ALG_WORD_PREFIX},
			want:   []string{"discarded_series_ratio"},
		},
		"AND of two words": {
			filter: &client.SearchFilter{Expression: "ingester AND ratio", CaseInsensitive: true, FuzzAlg: client.FUZZ_ALG_WORD_PREFIX},
			want:   []string{"discarded_series_ratio"},
		},
		"term matches a word in the metric name": {
			filter: &client.SearchFilter{Terms: []string{"seconds"}, CaseInsensitive: true, FuzzAlg: client.FUZZ_ALG_WORD_PREFIX},
			want:   []string{"sync_duration_seconds"},
		},
		"AND across name and HELP": {
			filter: &client.SearchFilter{Expression: "compression AND requests", CaseInsensitive: true, FuzzAlg: client.FUZZ_ALG_WORD_PREFIX},
			want:   []string{"compression_ratio"},
		},
		"NOT excludes a word found only in the name": {
			filter: &client.SearchFilter{Expression: "ratio AND NOT discarded", CaseInsensitive: true, FuzzAlg: client.FUZZ_ALG_WORD_PREFIX},
			want:   []string{"compression_ratio"},
		},
		"substring matches inside words": {
			filter: &client.SearchFilter{Terms: []string{"ratio"}, CaseInsensitive: true, FuzzAlg: client.FUZZ_ALG_SUBSTRING},
			want:   []string{"compression_ratio", "discarded_series_ratio", "sync_duration_seconds"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := &mockSearchMetricsMetadataStream{ctx: ctx}
			require.NoError(t, i.SearchMetricsMetadata(&client.SearchMetricsMetadataRequest{Filter: tc.filter}, s))
			assert.Equal(t, tc.want, collectSearchMetricsMetadataValues(s))
		})
	}
}

func TestIngesterSearchMetricsMetadataRejectsMissingSearchTermsAsInvalidArgument(t *testing.T) {
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	ctx := user.InjectOrgID(context.Background(), "test")
	pushMetadataToIngester(ctx, t, i, []*mimirpb.MetricMetadata{
		{MetricFamilyName: "cpu_seconds_total", Help: "Total CPU time", Type: mimirpb.COUNTER},
	})

	tests := map[string]*client.SearchFilter{
		"nil filter":            nil,
		"empty filter":          {},
		"resume cursor only":    {ResumeAfter: "a"},
		"case insensitive only": {CaseInsensitive: true},
	}
	for name, filter := range tests {
		t.Run(name, func(t *testing.T) {
			s := &mockSearchMetricsMetadataStream{ctx: ctx}
			err := i.SearchMetricsMetadata(&client.SearchMetricsMetadataRequest{Filter: filter}, s)
			st, ok := grpcutil.ErrorToStatus(err)
			require.True(t, ok, "expected gRPC status error, got %T: %v", err, err)
			assert.Equal(t, codes.InvalidArgument, st.Code())
			assert.Equal(t, "metric metadata search requires search terms or an expression", st.Message())
			assert.Empty(t, collectSearchMetricsMetadataValues(s))
		})
	}
}

// TestIngesterSearchMetricsMetadataWithNoMetadataReturnsEmptyStream covers
// the case where getUserMetadata(userID) returns nil because the tenant has
// pushed series but no metadata yet: this must behave identically to a
// search that matched nothing, not as an error or a nil-pointer panic.
func TestIngesterSearchMetricsMetadataWithNoMetadataReturnsEmptyStream(t *testing.T) {
	series := []util_test.Series{
		{Labels: labels.FromStrings(model.MetricNameLabel, "metric_a"), Samples: []util_test.Sample{{TS: 100000, Val: 1}}},
	}
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	ctx := user.InjectOrgID(context.Background(), "test")
	require.NoError(t, pushSeriesToIngester(ctx, t, i, series))

	req := &client.SearchMetricsMetadataRequest{Filter: &client.SearchFilter{Terms: []string{"metric"}}}
	s := &mockSearchMetricsMetadataStream{ctx: ctx}
	require.NoError(t, i.SearchMetricsMetadata(req, s))
	assert.Empty(t, collectSearchMetricsMetadataValues(s))
}

// TestIngesterSearchMetricsMetadataResumeCursorPagination exercises
// resume-cursor pagination through the full RPC handler, not just
// userMetricsMetadata.searchHelp in isolation (that's covered by
// user_metrics_metadata_search_test.go). Paging by metric name, one page at
// a time, must reconstruct the same sequence as a single unlimited search.
func TestIngesterSearchMetricsMetadataResumeCursorPagination(t *testing.T) {
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	ctx := user.InjectOrgID(context.Background(), "test")
	pushMetadataToIngester(ctx, t, i, []*mimirpb.MetricMetadata{
		{MetricFamilyName: "metric_a", Help: "alpha help", Type: mimirpb.COUNTER},
		{MetricFamilyName: "metric_b", Help: "beta help", Type: mimirpb.COUNTER},
		{MetricFamilyName: "metric_c", Help: "gamma help", Type: mimirpb.COUNTER},
		{MetricFamilyName: "metric_d", Help: "delta help", Type: mimirpb.COUNTER},
	})

	fullReq := &client.SearchMetricsMetadataRequest{Filter: &client.SearchFilter{Terms: []string{"help"}}}
	fullStream := &mockSearchMetricsMetadataStream{ctx: ctx}
	require.NoError(t, i.SearchMetricsMetadata(fullReq, fullStream))
	want := collectSearchMetricsMetadataValues(fullStream)
	require.Equal(t, []string{"metric_a", "metric_b", "metric_c", "metric_d"}, want)

	var got []string
	resumeAfter := ""
	for {
		req := &client.SearchMetricsMetadataRequest{
			Filter: &client.SearchFilter{Terms: []string{"help"}, ResumeAfter: resumeAfter},
			Limit:  1,
		}
		s := &mockSearchMetricsMetadataStream{ctx: ctx}
		require.NoError(t, i.SearchMetricsMetadata(req, s))
		page := collectSearchMetricsMetadataValues(s)
		if len(page) == 0 {
			break
		}
		require.Len(t, page, 1, "limit=1 must return at most one result per page")
		got = append(got, page...)
		resumeAfter = page[len(page)-1]
	}
	assert.Equal(t, want, got)
}

// fakeSearchResultSet is a test SearchResultSet backed by an in-memory slice
// plus an optional terminal error and pre-populated annotations. Used to
// exercise streamSearchResults without spinning up a real TSDB.
type fakeSearchResultSet struct {
	results []storage.SearchResult
	idx     int
	err     error
	warns   annotations.Annotations
}

func (s *fakeSearchResultSet) Next() bool {
	if s.idx >= len(s.results) {
		return false
	}
	s.idx++
	return true
}
func (s *fakeSearchResultSet) At() storage.SearchResult          { return s.results[s.idx-1] }
func (s *fakeSearchResultSet) Warnings() annotations.Annotations { return s.warns }
func (s *fakeSearchResultSet) Err() error                        { return s.err }
func (s *fakeSearchResultSet) Close() error                      { return nil }

func TestStreamSearchResultsPropagatesWarnings(t *testing.T) {
	tests := []struct {
		name     string
		results  []storage.SearchResult
		warns    annotations.Annotations
		wantSent []*client.SearchResultBatch
	}{
		{
			name:    "results with warnings: warnings ride on the final batch",
			results: []storage.SearchResult{{Value: "a", Score: 1.0}, {Value: "b", Score: 0.5}},
			warns:   addAnnotation(nil, "limit reached"),
			wantSent: []*client.SearchResultBatch{
				{
					Results: []client.SearchResultBatch_Result{
						{Value: "a", Score: 1.0},
						{Value: "b", Score: 0.5},
					},
					Warnings: []string{"limit reached"},
				},
			},
		},
		{
			name:    "warnings only, no results: still sends a batch",
			results: nil,
			warns:   addAnnotation(nil, "no series matched"),
			wantSent: []*client.SearchResultBatch{
				{Results: []client.SearchResultBatch_Result{}, Warnings: []string{"no series matched"}},
			},
		},
		{
			name:     "no results, no warnings: sends nothing",
			results:  nil,
			warns:    nil,
			wantSent: nil,
		},
		{
			name:    "results, no warnings: warnings field stays nil",
			results: []storage.SearchResult{{Value: "x", Score: 1.0}},
			warns:   nil,
			wantSent: []*client.SearchResultBatch{
				{Results: []client.SearchResultBatch_Result{{Value: "x", Score: 1.0}}},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rs := &fakeSearchResultSet{results: tc.results, warns: tc.warns}
			var sent []*client.SearchResultBatch
			send := func(b *client.SearchResultBatch) error {
				sent = append(sent, b)
				return nil
			}
			require.NoError(t, streamSearchResults(context.Background(), rs, send))
			assert.Equal(t, tc.wantSent, sent)
		})
	}
}

func TestStreamSearchResultsPropagatesErrInsteadOfWarnings(t *testing.T) {
	// Use exactly searchBatchSize results so the producer flushes one full
	// batch mid-iteration before rs.Err() is observed. Without the error,
	// streamSearchResults would then emit a trailer batch carrying the
	// warnings (since len(batch.Warnings) > 0 alone triggers a send); with
	// the error, that trailer must be suppressed. So len(sent)==1 is the
	// direct observation of suppression, and the single sent batch — being
	// a mid-iteration flush — must not carry the warnings either.
	const total = searchBatchSize
	results := make([]storage.SearchResult, total)
	for i := 0; i < total; i++ {
		results[i] = storage.SearchResult{Value: fmt.Sprintf("v%04d", i), Score: 1.0}
	}
	want := errors.New("boom")
	rs := &fakeSearchResultSet{
		results: results,
		err:     want,
		warns:   addAnnotation(nil, "should not appear"),
	}
	var sent []*client.SearchResultBatch
	send := func(b *client.SearchResultBatch) error {
		// Defensive copy: the producer reuses the batch struct between sends.
		out := make([]client.SearchResultBatch_Result, len(b.Results))
		copy(out, b.Results)
		sent = append(sent, &client.SearchResultBatch{Results: out, Warnings: b.Warnings})
		return nil
	}
	require.ErrorIs(t, streamSearchResults(context.Background(), rs, send), want)
	require.Len(t, sent, 1, "trailer batch carrying warnings must be suppressed when rs.Err() is non-nil")
	assert.Len(t, sent[0].Results, searchBatchSize, "the mid-iteration flush carried the full batch")
	assert.Empty(t, sent[0].Warnings, "warnings must never ride on mid-iteration batches")
}

func addAnnotation(a annotations.Annotations, msg string) annotations.Annotations {
	return a.Add(errors.New(msg))
}

func TestStreamSearchResultsBatching(t *testing.T) {
	// 257 results forces a batch boundary at 256, producing exactly two batches:
	// a full one of 256 followed by a trailer of 1.
	const total = searchBatchSize + 1
	results := make([]storage.SearchResult, total)
	for i := 0; i < total; i++ {
		results[i] = storage.SearchResult{Value: fmt.Sprintf("v%04d", i), Score: 1.0}
	}
	rs := &fakeSearchResultSet{results: results}
	var sent []*client.SearchResultBatch
	send := func(b *client.SearchResultBatch) error {
		// Defensive copy: the producer reuses the batch struct between sends.
		results := make([]client.SearchResultBatch_Result, len(b.Results))
		copy(results, b.Results)
		sent = append(sent, &client.SearchResultBatch{Results: results, Warnings: b.Warnings})
		return nil
	}
	require.NoError(t, streamSearchResults(context.Background(), rs, send))
	require.Len(t, sent, 2, "257 results must split into 2 batches at the searchBatchSize=256 boundary")
	assert.Len(t, sent[0].Results, searchBatchSize, "first batch is full")
	assert.Len(t, sent[1].Results, 1, "second batch carries the remainder")
	// Order preserved end-to-end.
	assert.Equal(t, "v0000", sent[0].Results[0].Value)
	assert.Equal(t, fmt.Sprintf("v%04d", searchBatchSize-1), sent[0].Results[searchBatchSize-1].Value)
	assert.Equal(t, fmt.Sprintf("v%04d", searchBatchSize), sent[1].Results[0].Value)
}

// TestIngesterSearchLabelValuesRejectsInvalidFuzzThresholdAsInvalidArgument
// locks in the contract that wire-shape errors surface as
// codes.InvalidArgument rather than codes.Internal. Without this test,
// moving the validation back below the deferred mapReadErrorToErrorWithStatus
// would silently downgrade the error code and no other test would notice.
func TestIngesterSearchLabelValuesRejectsInvalidFuzzThresholdAsInvalidArgument(t *testing.T) {
	series := []util_test.Series{
		{Labels: labels.FromStrings(model.MetricNameLabel, "metric"), Samples: []util_test.Sample{{TS: 100000, Val: 1}}},
	}
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	ctx := user.InjectOrgID(context.Background(), "test")
	require.NoError(t, pushSeriesToIngester(ctx, t, i, series))

	req := &client.SearchLabelValuesRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   200_000,
		Name:             "status",
		Filter:           &client.SearchFilter{Terms: []string{"x"}, FuzzThreshold: 200},
	}
	s := &mockSearchLabelValuesStream{ctx: ctx}
	err := i.SearchLabelValues(req, s)
	require.Error(t, err)
	st, ok := grpcutil.ErrorToStatus(err)
	require.True(t, ok, "expected gRPC status error, got %T: %v", err, err)
	assert.Equal(t, codes.InvalidArgument, st.Code(), "wire-shape errors must surface as codes.InvalidArgument, not codes.Internal")
}

func TestProtoToParams(t *testing.T) {
	t.Run("nil filter returns nil params", func(t *testing.T) {
		params, err := protoToParams(nil)
		require.NoError(t, err)
		assert.Nil(t, params)
	})

	t.Run("terms only builds Params via NewParams", func(t *testing.T) {
		params, err := protoToParams(&client.SearchFilter{Terms: []string{"foo"}})
		require.NoError(t, err)
		require.NotNil(t, params)
		assert.Equal(t, []string{"foo"}, params.Terms)
		assert.Empty(t, params.Expression())
	})

	t.Run("expression only builds Params via NewExpressionParams", func(t *testing.T) {
		params, err := protoToParams(&client.SearchFilter{Expression: "foo AND NOT bar"})
		require.NoError(t, err)
		require.NotNil(t, params)
		assert.Equal(t, "foo AND NOT bar", params.Expression())
		assert.Empty(t, params.Terms)
	})

	t.Run("terms and expression are rejected when both are set on the wire", func(t *testing.T) {
		params, err := protoToParams(&client.SearchFilter{Terms: []string{"foo"}, Expression: "bar"})
		require.EqualError(t, err, "search terms and search expression are mutually exclusive")
		require.ErrorIs(t, err, streaminglabelvalues.ErrTermsAndExpression)
		assert.Nil(t, params)
	})

	t.Run("invalid expression returns an error", func(t *testing.T) {
		_, err := protoToParams(&client.SearchFilter{Expression: "foo AND"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "search expression:")
	})

	t.Run("respects fuzz alg and threshold for expression params", func(t *testing.T) {
		params, err := protoToParams(&client.SearchFilter{
			Expression:      "foo",
			CaseInsensitive: true,
			FuzzAlg:         client.FUZZ_ALG_JARO_WINKLER,
			FuzzThreshold:   50,
		})
		require.NoError(t, err)
		require.NotNil(t, params)
		assert.False(t, params.CaseSensitive)
		assert.Equal(t, streaminglabelvalues.FuzzAlgJaroWinkler, params.FuzzAlg)
		assert.Equal(t, 50, params.FuzzThreshold)
	})

	t.Run("maps FUZZ_ALG_SUBSTRING_LEFT", func(t *testing.T) {
		params, err := protoToParams(&client.SearchFilter{Terms: []string{"foo"}, FuzzAlg: client.FUZZ_ALG_SUBSTRING_LEFT})
		require.NoError(t, err)
		require.NotNil(t, params)
		assert.Equal(t, streaminglabelvalues.FuzzAlgSubstringLeft, params.FuzzAlg)
	})

	t.Run("maps FUZZ_ALG_SUBSTRING", func(t *testing.T) {
		params, err := protoToParams(&client.SearchFilter{Terms: []string{"foo"}, FuzzAlg: client.FUZZ_ALG_SUBSTRING})
		require.NoError(t, err)
		require.NotNil(t, params)
		assert.Equal(t, streaminglabelvalues.FuzzAlgSubstring, params.FuzzAlg)
	})

	t.Run("carries ResumeAfter", func(t *testing.T) {
		params, err := protoToParams(&client.SearchFilter{Terms: []string{"foo"}, ResumeAfter: "bar"})
		require.NoError(t, err)
		require.NotNil(t, params)
		assert.Equal(t, "bar", params.ResumeAfter)
	})
}

// TestIngesterSearchLabelValuesRejectsInvalidExpressionAsInvalidArgument
// mirrors TestIngesterSearchLabelValuesRejectsInvalidFuzzThresholdAsInvalidArgument
// for the search_expr wire path.
func TestIngesterSearchLabelValuesRejectsInvalidExpressionAsInvalidArgument(t *testing.T) {
	series := []util_test.Series{
		{Labels: labels.FromStrings(model.MetricNameLabel, "metric"), Samples: []util_test.Sample{{TS: 100000, Val: 1}}},
	}
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	ctx := user.InjectOrgID(context.Background(), "test")
	require.NoError(t, pushSeriesToIngester(ctx, t, i, series))

	req := &client.SearchLabelValuesRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   200_000,
		Name:             "status",
		Filter:           &client.SearchFilter{Expression: "foo AND"},
	}
	s := &mockSearchLabelValuesStream{ctx: ctx}
	err := i.SearchLabelValues(req, s)
	require.Error(t, err)
	st, ok := grpcutil.ErrorToStatus(err)
	require.True(t, ok, "expected gRPC status error, got %T: %v", err, err)
	assert.Equal(t, codes.InvalidArgument, st.Code(), "wire-shape errors must surface as codes.InvalidArgument, not codes.Internal")
}

// TestStreamSearchResults covers streamSearchResults' batching and warnings
// behaviour.
func TestStreamSearchResults(t *testing.T) {
	t.Run("splits into fixed-size wire batches", func(t *testing.T) {
		const total = searchBatchSize + 1
		results := make([]storage.SearchResult, total)
		for i := 0; i < total; i++ {
			results[i] = storage.SearchResult{Value: fmt.Sprintf("metric_%04d", i), Score: 1.0}
		}
		rs := &fakeSearchResultSet{results: results}
		var sent []*client.SearchResultBatch
		send := func(b *client.SearchResultBatch) error {
			out := make([]client.SearchResultBatch_Result, len(b.Results))
			copy(out, b.Results)
			sent = append(sent, &client.SearchResultBatch{Results: out, Warnings: b.Warnings})
			return nil
		}
		require.NoError(t, streamSearchResults(context.Background(), rs, send))
		require.Len(t, sent, 2)
		require.Len(t, sent[0].Results, searchBatchSize)
		require.Len(t, sent[1].Results, 1)
	})

	t.Run("warnings-only batch is sent", func(t *testing.T) {
		rs := &fakeSearchResultSet{warns: addAnnotation(nil, "all clamped")}
		var sent []*client.SearchResultBatch
		send := func(b *client.SearchResultBatch) error {
			sent = append(sent, &client.SearchResultBatch{Results: append([]client.SearchResultBatch_Result(nil), b.Results...), Warnings: b.Warnings})
			return nil
		}
		require.NoError(t, streamSearchResults(context.Background(), rs, send))
		require.Len(t, sent, 1)
		assert.Empty(t, sent[0].Results)
		assert.Equal(t, []string{"all clamped"}, sent[0].Warnings)
	})
}

func TestStreamSearchResultsHonoursCtxCancellation(t *testing.T) {
	// Drive the loop with results, cancel ctx between iterations, and assert
	// streamSearchResults returns the cancellation error before draining the
	// rest of the iterator.
	results := make([]storage.SearchResult, 10)
	for i := range results {
		results[i] = storage.SearchResult{Value: fmt.Sprintf("v%d", i), Score: 1.0}
	}
	ctx, cancel := context.WithCancel(context.Background())
	rs := &fakeSearchResultSet{results: results}
	send := func(_ *client.SearchResultBatch) error { return nil }
	cancel()
	err := streamSearchResults(ctx, rs, send)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBuildSearchHintsLimitGuard(t *testing.T) {
	tests := []struct {
		name    string
		limit   int64
		wantErr bool
		want    int
	}{
		{name: "zero is no-limit", limit: 0, want: 0},
		{name: "positive passes through", limit: 1000, want: 1000},
		{name: "negative is rejected", limit: -1, wantErr: true},
		{name: "min int64 is rejected", limit: math.MinInt64, wantErr: true},
		{name: "max int64 clamps to math.MaxInt", limit: math.MaxInt64, want: math.MaxInt},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hints, _, err := buildSearchHints(nil, client.ORDER_BY_VALUE_ASC, tc.limit, nil)
			if tc.wantErr {
				require.Error(t, err)
				require.Nil(t, hints)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, hints)
			assert.Equal(t, tc.want, hints.Limit)
		})
	}
}

func TestBuildSearchHintsExcludesValuesAtOrBeforeResumeAfter(t *testing.T) {
	hints, matchers, err := buildSearchHints(
		&client.SearchFilter{Terms: []string{"pod"}, ResumeAfter: "kube_pod_info"},
		client.ORDER_BY_VALUE_ASC,
		10,
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, matchers)
	require.NotNil(t, hints.Filter)

	accepted, _ := hints.Filter.Accept("kube_pod_info")
	assert.False(t, accepted, "the resume-after value itself must not be re-returned")

	accepted, _ = hints.Filter.Accept("kube_pod_container_status_pod")
	assert.False(t, accepted, "alphabetically before the resume-after value")

	accepted, _ = hints.Filter.Accept("kube_pod_status_ready")
	assert.True(t, accepted, "alphabetically after the resume-after value, and matches the term")
}

func TestBuildSearchHintsExcludesValuesAtOrAfterResumeAfterDescending(t *testing.T) {
	hints, _, err := buildSearchHints(
		&client.SearchFilter{Terms: []string{"pod"}, ResumeAfter: "kube_pod_status_ready"},
		client.ORDER_BY_VALUE_DESC,
		10,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, hints.Filter)

	accepted, _ := hints.Filter.Accept("kube_pod_status_ready")
	assert.False(t, accepted, "the resume-after value itself must not be re-returned")

	accepted, _ = hints.Filter.Accept("kube_pod_container_status_pod")
	assert.True(t, accepted, "alphabetically before the resume-after value, so it comes after it in descending order")
}

func TestProtoToParams_CarriesScoreAfter(t *testing.T) {
	wf := &client.SearchFilter{Terms: []string{"foo"}, ResumeAfter: "bar", ScoreAfter: 0.75}
	params, err := protoToParams(wf)
	require.NoError(t, err)
	assert.Equal(t, "bar", params.ResumeAfter)
	assert.Equal(t, 0.75, params.ScoreAfter)
}

func TestBuildSearchHintsExcludesScoreAfterUnderScoreOrdering(t *testing.T) {
	wf := &client.SearchFilter{Terms: []string{"foo"}, ResumeAfter: "bar", ScoreAfter: 0.75}
	hints, _, err := buildSearchHints(wf, client.ORDER_BY_SCORE_DESC, 10, nil)
	require.NoError(t, err)
	require.NotNil(t, hints.Filter)
	// "foo" as a value scores 1.0 under FuzzAlgSubsequence's prefix rule
	// (term == value), which is > afterScore 0.75, so it must be excluded —
	// this candidate was already returned on an earlier page.
	accepted, _ := hints.Filter.Accept("foo")
	assert.False(t, accepted, "score 1.0 exceeds afterScore 0.75, already returned")
}

func TestBuildSearchHintsDoesNotApplyScoreResumeAfterUnderAlphaOrdering(t *testing.T) {
	// Guard against the score branch leaking into the alpha path: a filter
	// built for OrderByValueAsc must use ApplyResumeAfter's value-based
	// rule, not the score-based one, even if ScoreAfter happens to be set
	// (which toSearchRequest/decode should never actually produce for an
	// alpha cursor, but buildSearchHints itself must not assume that).
	//
	// These inputs must discriminate the two rules: with ResumeAfter="eee",
	// ScoreAfter=0.9, the value-rule accepts "foo" (not <= "eee") while a
	// wrongly-applied score-rule would reject it (score 1.0 > afterScore
	// 0.9).
	wf := &client.SearchFilter{Terms: []string{"foo"}, ResumeAfter: "eee", ScoreAfter: 0.9}
	hints, _, err := buildSearchHints(wf, client.ORDER_BY_VALUE_ASC, 10, nil)
	require.NoError(t, err)
	require.NotNil(t, hints.Filter)
	accepted, _ := hints.Filter.Accept("foo")
	assert.True(t, accepted, "value-based resume: 'foo' is not <= after 'eee', so it is accepted — if the score-rule leaked in here instead, it would wrongly reject since score 1.0 > ScoreAfter 0.9")
}

// Benchmark fixture shape mirrors BenchmarkIngester_LabelValuesCardinality
// (pkg/ingester/label_names_and_values_test.go) so cardinality buckets line
// up across the legacy and search benchmarks. Prime moduli ensure each
// labelset is a distinct series and produce 10 / 77 / 4199 unique values
// for the mod_10 / mod_77 / mod_4199 labels respectively.
const (
	searchBenchUserID      = "test"
	searchBenchNumSeries   = 10e6
	searchBenchMetricName  = "metric_name"
	searchBenchEndTimeMs   = 200_000
	searchBenchLimitLarge  = 10_000
	searchBenchLimitSmall  = 100
	searchBenchFuzzPercent = 70
)

// prepareSearchBenchmarkIngester builds a healthy ingester and pushes
// searchBenchNumSeries series. The 10M-series push dominates fixture cost
// (~tens of seconds); it runs once per Benchmark* invocation and feeds all
// sub-benchmarks of that invocation.
func prepareSearchBenchmarkIngester(b *testing.B) (*Ingester, context.Context) {
	in := prepareHealthyIngester(b, nil)
	ctx := user.InjectOrgID(context.Background(), searchBenchUserID)

	samples := []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}
	writeReq := &mimirpb.WriteRequest{Source: mimirpb.API}
	for s := 0; s < searchBenchNumSeries; s++ {
		writeReq.Timeseries = append(writeReq.Timeseries, mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(
					model.MetricNameLabel, searchBenchMetricName,
					"mod_10", strconv.Itoa(s%(2*5)),
					"mod_77", strconv.Itoa(s%(7*11)),
					"mod_4199", strconv.Itoa(s%(13*17*19)))),
				Samples: samples,
			},
		})
	}
	_, err := in.Push(ctx, writeReq)
	require.NoError(b, err)
	return in, ctx
}

// BenchmarkIngester_SearchLabelValues exercises the new streaming
// SearchLabelValues RPC across representative axes: label cardinality,
// filter (none / substring / fuzzy Jaro-Winkler), ordering (alpha asc /
// alpha desc / score desc), and limit. The matrix is intentionally pruned
// to representative combinations rather than a full cross-product to keep
// benchmark wall time bounded; the helper is reused by the parity
// benchmark below.
func BenchmarkIngester_SearchLabelValues(b *testing.B) {
	in, ctx := prepareSearchBenchmarkIngester(b)

	runOnce := func(b *testing.B, req *client.SearchLabelValuesRequest) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s := &mockSearchLabelValuesStream{ctx: ctx}
			if err := in.SearchLabelValues(req, s); err != nil {
				b.Fatal(err)
			}
		}
	}

	// Baseline (no filter): pure scan cost per cardinality bucket.
	for _, c := range []struct {
		name  string
		label string
	}{
		{"mod_10__10_values", "mod_10"},
		{"mod_77__77_values", "mod_77"},
		{"mod_4199__4199_values", "mod_4199"},
		{"__name__1_value", model.MetricNameLabel},
	} {
		b.Run(fmt.Sprintf("card=%s/filter=none/order=alpha_asc/limit=%d", c.name, searchBenchLimitLarge), func(b *testing.B) {
			runOnce(b, &client.SearchLabelValuesRequest{
				StartTimestampMs: 0,
				EndTimestampMs:   searchBenchEndTimeMs,
				Name:             c.label,
				Ordering:         client.ORDER_BY_VALUE_ASC,
				Limit:            searchBenchLimitLarge,
			})
		})
	}

	// Filter / ordering / limit variation on the highest-cardinality label.
	// The "1" term matches a large fraction of mod_4199 values so the filter
	// path is exercised non-trivially without becoming pathological.
	const fuzzyTerm, substringTerm = "11", "1"
	const heavyCardLabel = "mod_4199"

	b.Run(fmt.Sprintf("card=mod_4199__4199_values/filter=substring/order=alpha_asc/limit=%d", searchBenchLimitLarge), func(b *testing.B) {
		runOnce(b, &client.SearchLabelValuesRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   searchBenchEndTimeMs,
			Name:             heavyCardLabel,
			Filter:           &client.SearchFilter{Terms: []string{substringTerm}},
			Ordering:         client.ORDER_BY_VALUE_ASC,
			Limit:            searchBenchLimitLarge,
		})
	})
	b.Run(fmt.Sprintf("card=mod_4199__4199_values/filter=substring/order=alpha_desc/limit=%d", searchBenchLimitLarge), func(b *testing.B) {
		runOnce(b, &client.SearchLabelValuesRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   searchBenchEndTimeMs,
			Name:             heavyCardLabel,
			Filter:           &client.SearchFilter{Terms: []string{substringTerm}},
			Ordering:         client.ORDER_BY_VALUE_DESC,
			Limit:            searchBenchLimitLarge,
		})
	})
	b.Run(fmt.Sprintf("card=mod_4199__4199_values/filter=substring/order=score_desc/limit=%d", searchBenchLimitLarge), func(b *testing.B) {
		runOnce(b, &client.SearchLabelValuesRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   searchBenchEndTimeMs,
			Name:             heavyCardLabel,
			Filter:           &client.SearchFilter{Terms: []string{substringTerm}},
			Ordering:         client.ORDER_BY_SCORE_DESC,
			Limit:            searchBenchLimitLarge,
		})
	})
	b.Run(fmt.Sprintf("card=mod_4199__4199_values/filter=substring/order=alpha_asc/limit=%d", searchBenchLimitSmall), func(b *testing.B) {
		runOnce(b, &client.SearchLabelValuesRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   searchBenchEndTimeMs,
			Name:             heavyCardLabel,
			Filter:           &client.SearchFilter{Terms: []string{substringTerm}},
			Ordering:         client.ORDER_BY_VALUE_ASC,
			Limit:            searchBenchLimitSmall,
		})
	})
	b.Run(fmt.Sprintf("card=mod_4199__4199_values/filter=fuzzy_jw%d/order=score_desc/limit=%d", searchBenchFuzzPercent, searchBenchLimitLarge), func(b *testing.B) {
		runOnce(b, &client.SearchLabelValuesRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   searchBenchEndTimeMs,
			Name:             heavyCardLabel,
			Filter: &client.SearchFilter{
				Terms:         []string{fuzzyTerm},
				FuzzAlg:       client.FUZZ_ALG_JARO_WINKLER,
				FuzzThreshold: searchBenchFuzzPercent,
			},
			Ordering: client.ORDER_BY_SCORE_DESC,
			Limit:    searchBenchLimitLarge,
		})
	})

	// The single-value __name__ bucket has the shortest per-call body, so
	// the streaming path's fixed overhead shows up most clearly here.
	// (Metric metadata enrichment is no longer done at the ingester; it
	// happens on the querier — see pkg/querier/multi_searcher.go.)
	b.Run(fmt.Sprintf("card=__name__1_value/order=alpha_asc/limit=%d", searchBenchLimitLarge), func(b *testing.B) {
		runOnce(b, &client.SearchLabelValuesRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   searchBenchEndTimeMs,
			Name:             model.MetricNameLabel,
			Ordering:         client.ORDER_BY_VALUE_ASC,
			Limit:            searchBenchLimitLarge,
		})
	})
}

// BenchmarkIngester_SearchLabelNames exercises the SearchLabelNames RPC.
// Label-name cardinality on this fixture is fixed at four (__name__,
// mod_10, mod_77, mod_4199), so the per-call cost is dominated by the
// scan + filter setup rather than result-set size.
func BenchmarkIngester_SearchLabelNames(b *testing.B) {
	in, ctx := prepareSearchBenchmarkIngester(b)

	run := func(b *testing.B, req *client.SearchLabelNamesRequest) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s := &mockSearchLabelNamesStream{ctx: ctx}
			if err := in.SearchLabelNames(req, s); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("filter=none", func(b *testing.B) {
		run(b, &client.SearchLabelNamesRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   searchBenchEndTimeMs,
			Ordering:         client.ORDER_BY_VALUE_ASC,
			Limit:            searchBenchLimitLarge,
		})
	})
	b.Run("filter=substring_mod", func(b *testing.B) {
		run(b, &client.SearchLabelNamesRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   searchBenchEndTimeMs,
			Filter:           &client.SearchFilter{Terms: []string{"mod"}},
			Ordering:         client.ORDER_BY_VALUE_ASC,
			Limit:            searchBenchLimitLarge,
		})
	})
	b.Run("matcher=__name__=metric_name", func(b *testing.B) {
		run(b, &client.SearchLabelNamesRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   searchBenchEndTimeMs,
			Matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: searchBenchMetricName},
			},
			Ordering: client.ORDER_BY_VALUE_ASC,
			Limit:    searchBenchLimitLarge,
		})
	})
}

// BenchmarkIngester_LegacyVsSearchLabelValues is the load-bearing parity
// comparison for the new RPC. Sub-case names are keyed on /impl=legacy and
// /impl=new so benchstat can render the comparison directly:
//
//	go test ./pkg/ingester -run='^$' -bench='BenchmarkIngester_LegacyVsSearchLabelValues' \
//	    -count=10 -benchmem > parity.txt
//	benchstat -col '/impl' parity.txt
//
// Both sub-cases run on the same head, with no filter, alpha-asc ordering,
// and a limit large enough not to clamp the result set. This isolates the
// streaming-RPC overhead vs the unary RPC at functional parity.
func BenchmarkIngester_LegacyVsSearchLabelValues(b *testing.B) {
	in, ctx := prepareSearchBenchmarkIngester(b)

	cards := []struct {
		name  string
		label string
	}{
		{"mod_10__10_values", "mod_10"},
		{"mod_4199__4199_values", "mod_4199"},
	}

	for _, c := range cards {
		// Build the legacy LabelValuesRequest once per cardinality bucket.
		legacyReq, err := client.ToLabelValuesRequest(
			model.LabelName(c.label),
			0,
			model.Time(searchBenchEndTimeMs),
			&storage.LabelHints{Limit: searchBenchLimitLarge},
			nil,
		)
		require.NoError(b, err)

		b.Run(fmt.Sprintf("card=%s/impl=legacy", c.name), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := in.LabelValues(ctx, legacyReq); err != nil {
					b.Fatal(err)
				}
			}
		})

		newReq := &client.SearchLabelValuesRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   searchBenchEndTimeMs,
			Name:             c.label,
			Ordering:         client.ORDER_BY_VALUE_ASC,
			Limit:            searchBenchLimitLarge,
		}
		b.Run(fmt.Sprintf("card=%s/impl=new", c.name), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s := &mockSearchLabelValuesStream{ctx: ctx}
				if err := in.SearchLabelValues(newReq, s); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
