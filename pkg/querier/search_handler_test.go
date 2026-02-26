// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestFilterSearchValues(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		params   *searchParams
		expected []string
	}{
		{
			name:     "no filter returns all",
			values:   []string{"http_requests_total", "go_goroutines"},
			params:   &searchParams{},
			expected: []string{"http_requests_total", "go_goroutines"},
		},
		{
			name:     "case-insensitive substring match",
			values:   []string{"http_requests_total", "go_goroutines", "HTTP_errors"},
			params:   &searchParams{search: []string{"http"}},
			expected: []string{"http_requests_total", "HTTP_errors"},
		},
		{
			name:     "case-sensitive match",
			values:   []string{"http_requests_total", "go_goroutines", "HTTP_errors"},
			params:   &searchParams{search: []string{"http"}, caseSensitive: true},
			expected: []string{"http_requests_total"},
		},
		{
			name:     "multiple search terms OR'd",
			values:   []string{"http_requests_total", "go_goroutines", "tcp_packets"},
			params:   &searchParams{search: []string{"http", "tcp"}},
			expected: []string{"http_requests_total", "tcp_packets"},
		},
		{
			name:     "no match returns empty",
			values:   []string{"http_requests_total", "go_goroutines"},
			params:   &searchParams{search: []string{"xyz"}},
			expected: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := filterSearchValues(tc.values, tc.params)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestWriteSearchNDJSON(t *testing.T) {
	values := []string{"a", "b", "c", "d", "e"}

	rec := httptest.NewRecorder()
	writeSearchNDJSON(rec, values, 2, func(v string) any {
		return map[string]string{"metric_name": v}
	})

	assert.Equal(t, "application/x-ndjson", rec.Header().Get("Content-Type"))

	var buf bytes.Buffer
	buf.Write(rec.Body.Bytes())

	scanner := bufio.NewScanner(&buf)
	var lines []map[string]any
	for scanner.Scan() {
		var obj map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &obj))
		lines = append(lines, obj)
	}

	// 5 values / batchSize=2 => ceil(5/2)=3 result chunks + 1 status chunk = 4 lines
	require.Len(t, lines, 4)

	// Check first batch has 2 results
	results0, ok := lines[0]["results"].([]any)
	require.True(t, ok)
	assert.Len(t, results0, 2)

	// Check last chunk is status
	lastLine := lines[len(lines)-1]
	assert.Equal(t, "success", lastLine["status"])
	assert.Equal(t, false, lastLine["has_more"])
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
	resultMap, ok := results[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "http_requests_total", resultMap["metric_name"])
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
