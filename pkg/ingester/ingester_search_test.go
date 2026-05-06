// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
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
	want := errors.New("boom")
	rs := &fakeSearchResultSet{
		results: []storage.SearchResult{{Value: "a", Score: 1.0}},
		err:     want,
		warns:   addAnnotation(nil, "should not appear"),
	}
	var sent []*client.SearchResultBatch
	send := func(b *client.SearchResultBatch) error {
		sent = append(sent, b)
		return nil
	}
	require.ErrorIs(t, streamSearchResults(context.Background(), rs, send), want)
	// The single result was sent before iteration ended; the trailer batch
	// (which would carry warnings) is suppressed because rs.Err is non-nil.
	for _, b := range sent {
		assert.Empty(t, b.Warnings, "warnings must not be sent when iteration errored")
	}
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
	st, ok := status.FromError(err)
	require.True(t, ok, "expected gRPC status error, got %T: %v", err, err)
	assert.Equal(t, codes.InvalidArgument, st.Code(), "wire-shape errors must surface as codes.InvalidArgument, not codes.Internal")
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
