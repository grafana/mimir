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
