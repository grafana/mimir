// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		caseSens    bool
		ordering    client.SearchOrdering
		limit       int64
		wantValues  []string
	}{
		{name: "no filter returns all", caseSens: true, wantValues: []string{"__name__", "env", "status"}},
		{name: "substring 'env' case-sensitive", filterTerms: []string{"env"}, caseSens: true, wantValues: []string{"env"}},
		{name: "case-insensitive 'ENV' matches", filterTerms: []string{"ENV"}, caseSens: false, wantValues: []string{"env"}},
		{name: "limit 1", caseSens: true, limit: 1, wantValues: []string{"__name__"}},
		{name: "value desc", caseSens: true, ordering: client.ORDER_BY_VALUE_DESC, wantValues: []string{"status", "env", "__name__"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &client.SearchLabelNamesRequest{
				StartTimestampMs: 0,
				EndTimestampMs:   200_000,
				Filter:           &client.SearchFilter{Terms: tc.filterTerms, CaseSensitive: tc.caseSens},
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
		caseSens      bool
		fuzzAlg       client.SearchFilter_FuzzAlg
		fuzzThreshold int32
		ordering      client.SearchOrdering
		limit         int64
		want          []string
	}{
		{name: "all status values", labelName: "status", caseSens: true, want: []string{"200", "300", "500"}},
		{name: "substring '0'", labelName: "status", filterTerms: []string{"0"}, caseSens: true, want: []string{"200", "300", "500"}},
		{name: "substring '20'", labelName: "status", filterTerms: []string{"20"}, caseSens: true, want: []string{"200"}},
		{name: "limit 2", labelName: "status", caseSens: true, limit: 2, want: []string{"200", "300"}},
		{name: "value desc", labelName: "status", caseSens: true, ordering: client.ORDER_BY_VALUE_DESC, want: []string{"500", "300", "200"}},
		{name: "missing label returns empty", labelName: "missing", caseSens: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &client.SearchLabelValuesRequest{
				StartTimestampMs: 0,
				EndTimestampMs:   200_000,
				Name:             tc.labelName,
				Filter: &client.SearchFilter{
					Terms:         tc.filterTerms,
					CaseSensitive: tc.caseSens,
					FuzzAlg:       tc.fuzzAlg,
					FuzzThreshold: tc.fuzzThreshold,
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
			require.NoError(t, streamSearchResults(rs, send))
			assert.Equal(t, tc.wantSent, sent)
		})
	}
}

func TestStreamSearchResultsPropagatesErrInsteadOfWarnings(t *testing.T) {
	want := errors.New("boom")
	// Generate enough results to trigger at least one mid-iteration batch send.
	results := make([]storage.SearchResult, searchBatchSize)
	for i := 0; i < searchBatchSize; i++ {
		results[i] = storage.SearchResult{Value: "a", Score: 1.0}
	}
	rs := &fakeSearchResultSet{results: results, err: want, warns: addAnnotation(nil, "should not appear")}
	var sent []*client.SearchResultBatch
	send := func(b *client.SearchResultBatch) error {
		sent = append(sent, b)
		return nil
	}
	require.ErrorIs(t, streamSearchResults(rs, send), want)
	// A batch was sent before iteration ended; the trailer batch
	// (which would carry warnings) is suppressed because rs.Err is non-nil.
	for _, b := range sent {
		assert.Empty(t, b.Warnings, "warnings must not be sent when iteration errored")
	}
}

func addAnnotation(a annotations.Annotations, msg string) annotations.Annotations {
	return a.Add(errors.New(msg))
}
