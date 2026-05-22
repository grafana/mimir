// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
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
			require.NoError(t, streamSearchResults(context.Background(), rs, send, nil))
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
	require.ErrorIs(t, streamSearchResults(context.Background(), rs, send, nil), want)
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
	require.NoError(t, streamSearchResults(context.Background(), rs, send, nil))
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

// TestStreamSearchResultsDecorator covers the contract between
// streamSearchResults and the metadataBatchDecoratorFunc it accepts.
func TestStreamSearchResultsDecorator(t *testing.T) {
	t.Run("decorates each batch and is invoked once per batch", func(t *testing.T) {
		// Force a two-batch split so the per-batch invocation count is
		// observable. The lock-amortisation contract is the whole point of
		// the batch-shaped decorator.
		const total = searchBatchSize + 1
		results := make([]storage.SearchResult, total)
		for i := 0; i < total; i++ {
			results[i] = storage.SearchResult{Value: fmt.Sprintf("metric_%04d", i), Score: 1.0}
		}
		known := map[string]mimirpb.MetricMetadata{
			results[0].Value:       {Type: mimirpb.COUNTER, Help: "first"},
			results[total-1].Value: {Type: mimirpb.GAUGE, Help: "last", Unit: "s"},
		}
		decoratorCalls := 0
		decorator := func(batch *client.SearchResultBatch) {
			decoratorCalls++
			for i := range batch.Results {
				if md, ok := known[batch.Results[i].Value]; ok {
					mCopy := md
					batch.Results[i].Metadata = &mCopy
				}
			}
		}

		rs := &fakeSearchResultSet{results: results}
		var sent []*client.SearchResultBatch
		send := func(b *client.SearchResultBatch) error {
			out := make([]client.SearchResultBatch_Result, len(b.Results))
			copy(out, b.Results)
			sent = append(sent, &client.SearchResultBatch{Results: out, Warnings: b.Warnings})
			return nil
		}
		require.NoError(t, streamSearchResults(context.Background(), rs, send, decorator))
		require.Len(t, sent, 2)
		assert.Equal(t, 2, decoratorCalls, "decorator must be called once per wire batch, not per record")

		require.Len(t, sent[0].Results, searchBatchSize)
		first := sent[0].Results[0]
		require.NotNil(t, first.Metadata)
		assert.Equal(t, mimirpb.COUNTER, first.Metadata.Type)
		assert.Equal(t, "first", first.Metadata.Help)
		assert.Nil(t, sent[0].Results[1].Metadata, "values without metadata stay un-decorated")

		require.Len(t, sent[1].Results, 1)
		last := sent[1].Results[0]
		require.NotNil(t, last.Metadata)
		assert.Equal(t, mimirpb.GAUGE, last.Metadata.Type)
		assert.Equal(t, "s", last.Metadata.Unit)
	})

	t.Run("skipped on warnings-only batch", func(t *testing.T) {
		// Warnings-only trailers shouldn't acquire the metadata-store RLock,
		// so the decorator must not be called when there are no results.
		rs := &fakeSearchResultSet{warns: addAnnotation(nil, "all clamped")}
		calls := 0
		decorator := func(_ *client.SearchResultBatch) { calls++ }
		var sent []*client.SearchResultBatch
		send := func(b *client.SearchResultBatch) error {
			sent = append(sent, &client.SearchResultBatch{Results: append([]client.SearchResultBatch_Result(nil), b.Results...), Warnings: b.Warnings})
			return nil
		}
		require.NoError(t, streamSearchResults(context.Background(), rs, send, decorator))
		require.Len(t, sent, 1)
		assert.Empty(t, sent[0].Results)
		assert.Equal(t, []string{"all clamped"}, sent[0].Warnings)
		assert.Equal(t, 0, calls, "decorator must be skipped on a warnings-only batch")
	})
}

// TestNewMetadataBatchDecoratorFunc covers the factory's gating logic — when
// it should return a non-nil decorator and when it should short-circuit so
// streamSearchResults can skip the decoration step entirely.
func TestNewMetadataBatchDecoratorFunc(t *testing.T) {
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	// Wire metadata for the "test" tenant so a non-nil userMetricsMetadata
	// exists; rows that target a different userID exercise the "never
	// pushed" branch.
	require.NoError(t, i.getOrCreateUserMetadata("test").add("metric_a",
		&mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "metric_a", Help: "h"}))

	tests := []struct {
		name    string
		userID  string
		req     *client.SearchLabelValuesRequest
		wantSet bool
	}{
		{name: "nil request", userID: "test", req: nil},
		{name: "include_metadata false", userID: "test", req: &client.SearchLabelValuesRequest{Name: model.MetricNameLabel, IncludeMetadata: false}},
		{name: "include_metadata true but non-__name__ label", userID: "test", req: &client.SearchLabelValuesRequest{Name: "env", IncludeMetadata: true}},
		{name: "tenant has never pushed metadata", userID: "never-pushed", req: &client.SearchLabelValuesRequest{Name: model.MetricNameLabel, IncludeMetadata: true}},
		{name: "include_metadata true and __name__ label and tenant has metadata", userID: "test", req: &client.SearchLabelValuesRequest{Name: model.MetricNameLabel, IncludeMetadata: true}, wantSet: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := i.newMetadataBatchDecoratorFunc(tc.userID, tc.req)
			if tc.wantSet {
				assert.NotNil(t, got, "decorator must be wired up for valid metric-names enrichment requests")
			} else {
				assert.Nil(t, got, "decorator must be nil when enrichment is not applicable")
			}
		})
	}
}

// TestIngesterSearchLabelValuesMetadataEnrichment covers the end-to-end
// metric-name enrichment path through SearchLabelValues. Each case wires
// the series + metadata + request shape it needs, runs the RPC, and
// asserts per-record Metadata on the wire batches.
func TestIngesterSearchLabelValuesMetadataEnrichment(t *testing.T) {
	type metadataEntry struct {
		metric string
		md     *mimirpb.MetricMetadata
	}
	tests := map[string]struct {
		series        []util_test.Series
		metadataAdded []metadataEntry
		req           *client.SearchLabelValuesRequest
		// wantMetadata is keyed by result Value. A non-nil expected entry is
		// matched field-by-field; an explicitly nil entry asserts the result
		// must come back un-decorated.
		wantMetadata map[string]*mimirpb.MetricMetadata
	}{
		"include_metadata enriches matching metrics and leaves unrecorded ones alone": {
			series: []util_test.Series{
				{Labels: labels.FromStrings(model.MetricNameLabel, "metric_a"), Samples: []util_test.Sample{{TS: 100000, Val: 1}}},
				{Labels: labels.FromStrings(model.MetricNameLabel, "metric_b"), Samples: []util_test.Sample{{TS: 100000, Val: 1}}},
			},
			metadataAdded: []metadataEntry{
				{"metric_a", &mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "metric_a", Help: "Total a.", Unit: "s"}},
			},
			req: &client.SearchLabelValuesRequest{StartTimestampMs: 0, EndTimestampMs: 200_000, Name: model.MetricNameLabel, IncludeMetadata: true},
			wantMetadata: map[string]*mimirpb.MetricMetadata{
				"metric_a": {Type: mimirpb.COUNTER, Help: "Total a.", Unit: "s"},
				"metric_b": nil,
			},
		},
		"include_metadata is ignored for non-__name__ labels": {
			series: []util_test.Series{
				{Labels: labels.FromStrings(model.MetricNameLabel, "metric_a", "env", "prod"), Samples: []util_test.Sample{{TS: 100000, Val: 1}}},
			},
			metadataAdded: []metadataEntry{
				{"metric_a", &mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "metric_a", Help: "ignored"}},
			},
			req: &client.SearchLabelValuesRequest{StartTimestampMs: 0, EndTimestampMs: 200_000, Name: "env", IncludeMetadata: true},
			wantMetadata: map[string]*mimirpb.MetricMetadata{
				"prod": nil,
			},
		},
		"most recently added metadata wins when multiple entries exist": {
			series: []util_test.Series{
				{Labels: labels.FromStrings(model.MetricNameLabel, "metric_a"), Samples: []util_test.Sample{{TS: 100000, Val: 1}}},
			},
			// time.Now() is monotonically increasing across the two add()
			// calls below, so the second one is unambiguously "most recent".
			metadataAdded: []metadataEntry{
				{"metric_a", &mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "metric_a", Help: "old", Unit: ""}},
				{"metric_a", &mimirpb.MetricMetadata{Type: mimirpb.GAUGE, MetricFamilyName: "metric_a", Help: "new", Unit: "s"}},
			},
			req: &client.SearchLabelValuesRequest{StartTimestampMs: 0, EndTimestampMs: 200_000, Name: model.MetricNameLabel, IncludeMetadata: true},
			wantMetadata: map[string]*mimirpb.MetricMetadata{
				"metric_a": {Type: mimirpb.GAUGE, Help: "new", Unit: "s"},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ing := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
			ctx := user.InjectOrgID(context.Background(), "test")
			require.NoError(t, pushSeriesToIngester(ctx, t, ing, tc.series))
			um := ing.getOrCreateUserMetadata("test")
			for _, m := range tc.metadataAdded {
				require.NoError(t, um.add(m.metric, m.md))
			}

			s := &mockSearchLabelValuesStream{ctx: ctx}
			require.NoError(t, ing.SearchLabelValues(tc.req, s))

			got := map[string]*client.SearchResultBatch_Result{}
			for _, b := range s.sent {
				for idx := range b.Results {
					r := b.Results[idx]
					got[r.Value] = &r
				}
			}
			for value, want := range tc.wantMetadata {
				require.Contains(t, got, value, "result %q must be present on the wire", value)
				if want == nil {
					assert.Nil(t, got[value].Metadata, "%q must come back un-decorated", value)
					continue
				}
				require.NotNil(t, got[value].Metadata, "%q must come back decorated", value)
				assert.Equal(t, want.Type, got[value].Metadata.Type, "%q Type", value)
				assert.Equal(t, want.Help, got[value].Metadata.Help, "%q Help", value)
				assert.Equal(t, want.Unit, got[value].Metadata.Unit, "%q Unit", value)
			}
		})
	}
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
	err := streamSearchResults(ctx, rs, send, nil)
	require.ErrorIs(t, err, context.Canceled)
}
