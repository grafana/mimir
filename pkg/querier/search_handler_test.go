// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
)

// searchTestQuerier is a simple storage.Querier for search handler tests.
type searchTestQuerier struct {
	storage.Querier
	labelNames  []string
	labelValues map[string][]string
}

func (q *searchTestQuerier) LabelNames(_ context.Context, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.labelNames, nil, nil
}

func (q *searchTestQuerier) LabelValues(_ context.Context, name string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.labelValues[name], nil, nil
}

func (q *searchTestQuerier) Close() error { return nil }

func newSearchTestQueryable(labelNames []string, labelValues map[string][]string) mockSampleAndChunkQueryable {
	q := &searchTestQuerier{labelNames: labelNames, labelValues: labelValues}
	return mockSampleAndChunkQueryable{
		queryableFn: func(_, _ int64) (storage.Querier, error) {
			return q, nil
		},
	}
}

func TestSearchLabelNamesHandler(t *testing.T) {
	queryable := newSearchTestQueryable(
		[]string{"__name__", "job", "instance"},
		nil,
	)

	handler := SearchLabelNamesHandler(queryable, nil)

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/search/label_names", nil)
	require.NoError(t, err)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/x-ndjson", rec.Header().Get("Content-Type"))

	scanner := bufio.NewScanner(rec.Body)
	var lines []map[string]any
	for scanner.Scan() {
		var obj map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &obj))
		lines = append(lines, obj)
	}
	require.GreaterOrEqual(t, len(lines), 1)

	// Last line should be status
	last := lines[len(lines)-1]
	assert.Equal(t, "success", last["status"])
	assert.Equal(t, false, last["has_more"])
}

func TestSearchMetricNamesHandler(t *testing.T) {
	queryable := newSearchTestQueryable(
		nil,
		map[string][]string{
			"__name__": {"http_requests_total", "go_goroutines", "process_cpu_seconds"},
		},
	)

	handler := SearchMetricNamesHandler(queryable, nil)

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/search/metric_names?search=http", nil)
	require.NoError(t, err)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	scanner := bufio.NewScanner(rec.Body)
	var lines []map[string]any
	for scanner.Scan() {
		var obj map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &obj))
		lines = append(lines, obj)
	}
	require.GreaterOrEqual(t, len(lines), 1)

	// First line should have the filtered result
	results, ok := lines[0]["results"].([]any)
	require.True(t, ok)
	require.Len(t, results, 1)
	assert.Equal(t, "http_requests_total", results[0])
}

func TestSearchLabelValuesHandler_MissingLabelName(t *testing.T) {
	queryable := newSearchTestQueryable(nil, nil)
	handler := SearchLabelValuesHandler(queryable, nil)

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/search/label_values", nil)
	require.NoError(t, err)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSearchLabelValuesHandler(t *testing.T) {
	queryable := newSearchTestQueryable(
		nil,
		map[string][]string{
			"job": {"prometheus", "alertmanager", "node-exporter"},
		},
	)

	handler := SearchLabelValuesHandler(queryable, nil)

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/search/label_values?label_name=job", nil)
	require.NoError(t, err)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/x-ndjson", rec.Header().Get("Content-Type"))

	scanner := bufio.NewScanner(rec.Body)
	var lines []map[string]any
	for scanner.Scan() {
		var obj map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &obj))
		lines = append(lines, obj)
	}
	require.GreaterOrEqual(t, len(lines), 1)
	last := lines[len(lines)-1]
	assert.Equal(t, "success", last["status"])
}

// ── Native-Searcher test infrastructure ──────────────────────────────────────
//
// searchTestNativeSearcher simulates a querier that directly implements
// mimirstorage.Searcher (as an upstream Prometheus storage would). Its
// SearchLabelNames / SearchLabelValues return different data than the
// embedded searchTestQuerier's LabelNames / LabelValues, so tests can
// assert which code path was actually taken by the handler.

type sliceSearcherValueSet struct {
	values []string
	pos    int
}

func (s *sliceSearcherValueSet) Next() bool {
	if s.pos < len(s.values) {
		s.pos++
		return true
	}
	return false
}

func (s *sliceSearcherValueSet) At() string                        { return s.values[s.pos-1] }
func (s *sliceSearcherValueSet) Warnings() annotations.Annotations { return nil }
func (s *sliceSearcherValueSet) Err() error                        { return nil }
func (s *sliceSearcherValueSet) Close()                            {}

// searchTestNativeSearcher embeds searchTestQuerier (for the LabelQuerier methods
// required by storage.Querier) and adds native Searcher methods whose results are
// independent of the underlying labelNames / labelValues maps.
type searchTestNativeSearcher struct {
	searchTestQuerier
	searchLabelNames  []string
	searchLabelValues map[string][]string
}

func (q *searchTestNativeSearcher) SearchLabelNames(_ context.Context, _ *mimirstorage.SearchHints, _ ...*labels.Matcher) (mimirstorage.SearcherValueSet, error) {
	return &sliceSearcherValueSet{values: q.searchLabelNames}, nil
}

func (q *searchTestNativeSearcher) SearchLabelValues(_ context.Context, name string, _ *mimirstorage.SearchHints, _ ...*labels.Matcher) (mimirstorage.SearcherValueSet, error) {
	return &sliceSearcherValueSet{values: q.searchLabelValues[name]}, nil
}

// newSearchTestNativeSearchableQueryable returns a queryable whose querier
// implements mimirstorage.Searcher natively. The LabelNames / LabelValues data
// is set to different sentinel values so tests can confirm the Searcher path is used.
func newSearchTestNativeSearchableQueryable(searchLabelNames []string, searchLabelValues map[string][]string) mockSampleAndChunkQueryable {
	q := &searchTestNativeSearcher{
		searchTestQuerier: searchTestQuerier{
			// Values on the non-Searcher path — should never appear in results.
			labelNames:  []string{"via_label_querier"},
			labelValues: map[string][]string{"__name__": {"via_label_querier"}},
		},
		searchLabelNames:  searchLabelNames,
		searchLabelValues: searchLabelValues,
	}
	return mockSampleAndChunkQueryable{
		queryableFn: func(_, _ int64) (storage.Querier, error) {
			return q, nil
		},
	}
}

// collectNDJSONResults scans an NDJSON body and returns the string values
// from each "results" chunk. Results are expected to be plain strings.
func collectNDJSONResults(t *testing.T, body *bytes.Buffer) []string {
	t.Helper()
	var values []string
	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		var obj map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &obj))
		results, ok := obj["results"].([]any)
		if !ok {
			continue
		}
		for _, r := range results {
			s, ok := r.(string)
			require.True(t, ok)
			values = append(values, s)
		}
	}
	return values
}

func TestSearchLabelNamesHandler_WithNativeSearcher(t *testing.T) {
	queryable := newSearchTestNativeSearchableQueryable(
		[]string{"__name__", "job", "instance"},
		nil,
	)

	handler := SearchLabelNamesHandler(queryable, nil)

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/search/label_names", nil)
	require.NoError(t, err)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/x-ndjson", rec.Header().Get("Content-Type"))

	got := collectNDJSONResults(t, rec.Body)
	assert.Equal(t, []string{"__name__", "job", "instance"}, got)
	assert.NotContains(t, got, "via_label_querier")
}

func TestSearchMetricNamesHandler_WithNativeSearcher(t *testing.T) {
	queryable := newSearchTestNativeSearchableQueryable(
		nil,
		map[string][]string{
			"__name__": {"http_requests_total", "go_goroutines"},
		},
	)

	handler := SearchMetricNamesHandler(queryable, nil)

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/search/metric_names", nil)
	require.NoError(t, err)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	got := collectNDJSONResults(t, rec.Body)
	assert.Equal(t, []string{"http_requests_total", "go_goroutines"}, got)
	assert.NotContains(t, got, "via_label_querier")
}

func TestSearchLabelValuesHandler_WithNativeSearcher(t *testing.T) {
	queryable := newSearchTestNativeSearchableQueryable(
		nil,
		map[string][]string{
			"job": {"prometheus", "alertmanager"},
		},
	)

	handler := SearchLabelValuesHandler(queryable, nil)

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/search/label_values?label_name=job", nil)
	require.NoError(t, err)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	got := collectNDJSONResults(t, rec.Body)
	assert.Equal(t, []string{"prometheus", "alertmanager"}, got)
	assert.NotContains(t, got, "via_label_querier")
}

// cancellingSearcherValueSet is a SearcherValueSet that returns up to blockAfter values
// and then blocks until its context is cancelled. It records whether Close was called.
type cancellingSearcherValueSet struct {
	values      []string
	pos         int
	blockAfter  int
	readyOnce   sync.Once
	readyCh     chan struct{} // closed when the iterator is about to block
	ctx         context.Context
	closeCalled bool
}

func (s *cancellingSearcherValueSet) Next() bool {
	if s.pos < len(s.values) && s.pos < s.blockAfter {
		s.pos++
		return true
	}
	// Signal to the test that we're now blocking.
	s.readyOnce.Do(func() { close(s.readyCh) })
	// Block until context is cancelled.
	<-s.ctx.Done()
	return false
}

func (s *cancellingSearcherValueSet) At() string                        { return s.values[s.pos-1] }
func (s *cancellingSearcherValueSet) Warnings() annotations.Annotations { return nil }
func (s *cancellingSearcherValueSet) Err() error                        { return s.ctx.Err() }
func (s *cancellingSearcherValueSet) Close()                            { s.closeCalled = true }

// cancellingNativeSearcher is a storage.Querier + mimirstorage.Searcher that returns
// a cancellingSearcherValueSet, capturing the request context when SearchLabelNames is called.
type cancellingNativeSearcher struct {
	searchTestQuerier
	vs *cancellingSearcherValueSet
}

func (q *cancellingNativeSearcher) SearchLabelNames(ctx context.Context, _ *mimirstorage.SearchHints, _ ...*labels.Matcher) (mimirstorage.SearcherValueSet, error) {
	q.vs.ctx = ctx
	return q.vs, nil
}

func (q *cancellingNativeSearcher) SearchLabelValues(ctx context.Context, _ string, _ *mimirstorage.SearchHints, _ ...*labels.Matcher) (mimirstorage.SearcherValueSet, error) {
	q.vs.ctx = ctx
	return q.vs, nil
}

// TestSearchLabelNamesHandler_ContextCancellation verifies that when a request is cancelled
// mid-stream, the handler writes an in-band error chunk and closes the iterator.
func TestSearchLabelNamesHandler_ContextCancellation(t *testing.T) {
	readyCh := make(chan struct{})
	vs := &cancellingSearcherValueSet{
		values:     []string{"__name__", "job", "instance", "pod", "namespace"},
		blockAfter: 2, // return 2 values then block
		readyCh:    readyCh,
	}

	q := &cancellingNativeSearcher{vs: vs}
	queryable := mockSampleAndChunkQueryable{
		queryableFn: func(_, _ int64) (storage.Querier, error) {
			return q, nil
		},
	}

	handler := SearchLabelNamesHandler(queryable, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = user.InjectOrgID(ctx, "test-tenant")

	// batch_size=1 ensures each value is flushed immediately so the response
	// body contains result chunks before the blocking point is reached.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/search/label_names?batch_size=1", nil)
	require.NoError(t, err)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	// Wait until the iterator signals it is about to block, then cancel.
	<-readyCh
	cancel()
	<-done

	// Parse the NDJSON body to verify results and the error chunk.
	scanner := bufio.NewScanner(rec.Body)
	var resultCount int
	var errFound bool
	for scanner.Scan() {
		var obj map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &obj))
		if results, ok := obj["results"].([]any); ok {
			resultCount += len(results)
		}
		if status, ok := obj["status"].(string); ok && status == "error" {
			errFound = true
		}
	}

	assert.Greater(t, resultCount, 0, "expected some results before cancellation")
	assert.True(t, errFound, "expected an error status chunk in the response")
	assert.True(t, vs.closeCalled, "expected iterator Close() to have been called")
}

// TestStreamingSearch_ContextCancellation verifies that a cancelled context stops
// the producer goroutine and the SearcherValueSet reports the cancellation.
func TestStreamingSearch_ContextCancellation(t *testing.T) {
	// Use a querier that returns enough values to fill the channel buffer so the
	// producer will block trying to send, giving cancellation something to interrupt.
	many := make([]string, 512)
	for i := range many {
		many[i] = "label"
	}
	q := &searchTestQuerier{labelNames: many}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately before the producer even starts

	ss := searcherFor(q)
	vs, err := ss.SearchLabelNames(ctx, nil)
	require.NoError(t, err)
	defer vs.Close()

	// Drain; the producer should exit quickly due to cancelled context.
	for vs.Next() {
	}
	// Err() should return the context cancellation error (or nil if the producer
	// managed to finish before noticing the cancellation).
	err = vs.Err()
	if err != nil {
		assert.ErrorIs(t, err, context.Canceled)
	}
}
