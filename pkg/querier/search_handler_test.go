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

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
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

func TestFilterChain(t *testing.T) {
	tcs := []struct {
		name     string
		params   searchParams
		input    []string
		expected map[string]struct {
			accepted bool
			score    float64
		}
	}{
		{
			name:   "no filters - everything is a match",
			params: searchParams{},
			input:  []string{"foo", "bar"},
			expected: map[string]struct {
				accepted bool
				score    float64
			}{
				// no filters were applied - no score was recorded
				"foo": {true, -1},
				"bar": {true, -1},
			},
		},

		{
			name:   "single string",
			params: searchParams{search: []string{"foo"}},
			input:  []string{"foo", "bar", "_foo", "foo_", "_foo_"},
			expected: map[string]struct {
				accepted bool
				score    float64
			}{
				// input string contains foo - accepted and no score was recorded
				"foo":   {true, -1},
				"_foo":  {true, -1},
				"foo_":  {true, -1},
				"_foo_": {true, -1},
				"bar":   {false, 0},
			},
		},

		{
			name:   "string disjunction",
			params: searchParams{search: []string{"foo", "bar"}},
			input:  []string{"foo", "bar", "cat"},
			expected: map[string]struct {
				accepted bool
				score    float64
			}{
				// inputs were filtered against foo or bar - those accepted have no score recorded
				"foo": {true, -1},
				"bar": {true, -1},
				"cat": {false, 0},
			},
		},

		{
			name:   "string conjunction",
			params: searchParams{search: []string{"foo", "bar"}, operation: streaminglabelvalues.And},
			input:  []string{"foo", "bar", "cat", "foobar", "foo_bar"},
			expected: map[string]struct {
				accepted bool
				score    float64
			}{
				// inputs were filtered against foo AND bar - those accepted have no score recorded
				"foo":     {false, 0},
				"bar":     {false, 0},
				"cat":     {false, 0},
				"foobar":  {true, -1},
				"foo_bar": {true, -1},
			},
		},

		{
			name:   "single string with fuzz",
			params: searchParams{search: []string{"foo"}, fuzzThreshold: 70},
			input:  []string{"foo", "bar", "boo", "some_metric_ending_with_foo", "bar_foo"},
			expected: map[string]struct {
				accepted bool
				score    float64
			}{
				// inputs were filtered as containing foo or the fuzz similarity > 0.7
				// Only one input did not contain foo but had a fuzz score above the threshold
				"foo":                         {true, -1},   // string contains match - no need to calculate a fuzz score here
				"bar":                         {false, 0},   // no match at all
				"boo":                         {true, 0.77}, // fuzz match only
				"some_metric_ending_with_foo": {true, -1},   // string contains match - no need to calculate a fuzz score here
				"bar_foo":                     {true, -1},   // string contains match - no need to calculate a fuzz score here
			},
		},

		{
			name:   "multiple strings with fuzz",
			params: searchParams{search: []string{"foo", "bar"}, fuzzThreshold: 70},
			input:  []string{"foo", "bar", "boo", "some_metric_ending_with_foo", "bar_foo", "tar", "foobar", "barfoo", "tarloo", "loo_tar"},
			expected: map[string]struct {
				accepted bool
				score    float64
			}{
				"foo":                         {true, -1},   // string contains match - no need to calculate a fuzz score here
				"bar":                         {true, -1},   // string contains match - no need to calculate a fuzz score here
				"boo":                         {true, 0.77}, // fuzz match on foo
				"some_metric_ending_with_foo": {true, -1},   // string contains match - no need to calculate a fuzz score here
				"bar_foo":                     {true, -1},   // string contains match - no need to calculate a fuzz score here
				"tar":                         {true, 0.77}, // fuzz match on bar
				"foobar":                      {true, -1},   // string contains match - no need to calculate a fuzz score here
				"barfoo":                      {true, -1},   // string contains match - no need to calculate a fuzz score here
				"loo_tar":                     {false, 0},   // input does not contain the given search strings, and the fuzz matching does not meet the necessary threshold
				"tarloo":                      {false, 0},   // input does not contain the given search strings, and the fuzz matching does not meet the necessary threshold
			},
		},

		{
			name:   "multiple strings with fuzz - lower threshold",
			params: searchParams{search: []string{"foo", "bar"}, fuzzThreshold: 1},
			input:  []string{"foo", "bar", "boo", "some_metric_ending_with_foo", "bar_foo", "tar", "foobar", "barfoo", "tarloo", "loo_tar"},
			expected: map[string]struct {
				accepted bool
				score    float64
			}{
				"foo":                         {true, -1},   // string contains match - no need to calculate a fuzz score here
				"bar":                         {true, -1},   // string contains match - no need to calculate a fuzz score here
				"boo":                         {true, 0.77}, // fuzz match on foo
				"some_metric_ending_with_foo": {true, -1},   // string contains match - no need to calculate a fuzz score here
				"bar_foo":                     {true, -1},   // string contains match - no need to calculate a fuzz score here
				"tar":                         {true, 0.77}, // fuzz match on bar
				"foobar":                      {true, -1},   // string contains match - no need to calculate a fuzz score here
				"barfoo":                      {true, -1},   // string contains match - no need to calculate a fuzz score here
				"loo_tar":                     {true, 0.65}, //  fuzz match
				"tarloo":                      {true, 0.66}, // fuzz match on bar
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			hints := buildSearchHints(&tc.params)
			for _, input := range tc.input {
				accepted, score := hints.Filter.Accept(input)
				require.Equalf(t, tc.expected[input].accepted, accepted, "%s should have accepted %v but got %v", input, tc.expected[input].accepted, score)
				require.InDeltaf(t, tc.expected[input].score, score, 0.02, "%s should have score %v but got %v", input, tc.expected[input].score, score)
			}
		})
	}
}

func TestSearchLabelNamesHandler(t *testing.T) {
	queryable := newSearchTestQueryable(
		[]string{"__name__", "job", "instance"},
		nil,
	)

	handler := SearchLabelNamesHandler(queryable, nil, log.NewNopLogger())

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

	handler := SearchMetricNamesHandler(queryable, nil, log.NewNopLogger())

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
	result, ok := results[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "http_requests_total", result["name"])
}

func TestSearchLabelValuesHandler_MissingLabelName(t *testing.T) {
	queryable := newSearchTestQueryable(nil, nil)
	handler := SearchLabelValuesHandler(queryable, nil, log.NewNopLogger())

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

	handler := SearchLabelValuesHandler(queryable, nil, log.NewNopLogger())

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

func (s *sliceSearcherValueSet) At() mimirstorage.FilteredResult {
	return mimirstorage.FilteredResult{Value: s.values[s.pos-1]}
}
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

// collectNDJSONResults scans an NDJSON body and returns the Value field from
// each FilteredResult in "results" chunks.
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
			m, ok := r.(map[string]any)
			require.True(t, ok, "expected result to be a JSON object, got %T", r)
			s, ok := m["name"].(string)
			require.True(t, ok, "expected name field in result object")
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

	handler := SearchLabelNamesHandler(queryable, nil, log.NewNopLogger())

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

	handler := SearchMetricNamesHandler(queryable, nil, log.NewNopLogger())

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

	handler := SearchLabelValuesHandler(queryable, nil, log.NewNopLogger())

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

func (s *cancellingSearcherValueSet) At() mimirstorage.FilteredResult {
	return mimirstorage.FilteredResult{Value: s.values[s.pos-1]}
}
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

	handler := SearchLabelNamesHandler(queryable, nil, log.NewNopLogger())

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
