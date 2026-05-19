// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

// searchMockQuerier satisfies storage.Querier and mimirSearcher. The search
// methods return whatever ResultSet was wired in via the closure-based
// fields, letting each test express exactly the iterator behaviour it wants
// (results, mid-stream error, warnings, etc.).
type searchMockQuerier struct {
	namesFn      func(params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet
	valuesFn     func(name string, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet
	lastName     string
	lastParams   *streaminglabelvalues.Params
	lastHints    *storage.SearchHints
	lastMatchers []*labels.Matcher
}

func (s *searchMockQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}
func (s *searchMockQuerier) LabelValues(_ context.Context, _ string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (s *searchMockQuerier) LabelNames(_ context.Context, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (s *searchMockQuerier) Close() error { return nil }

func (s *searchMockQuerier) SearchLabelNames(_ context.Context, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet {
	s.lastParams = params
	s.lastHints = hints
	s.lastMatchers = matchers
	if s.namesFn == nil {
		return storage.EmptySearchResultSet()
	}
	return s.namesFn(params, hints, matchers...)
}

func (s *searchMockQuerier) SearchLabelValues(_ context.Context, name string, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet {
	s.lastName = name
	s.lastParams = params
	s.lastHints = hints
	s.lastMatchers = matchers
	if s.valuesFn == nil {
		return storage.EmptySearchResultSet()
	}
	return s.valuesFn(name, params, hints, matchers...)
}

type searchMockQueryable struct {
	q *searchMockQuerier
}

func (q *searchMockQueryable) Querier(_, _ int64) (storage.Querier, error) {
	return q.q, nil
}

func newSearchMockQueryable(q *searchMockQuerier) *searchMockQueryable {
	return &searchMockQueryable{q: q}
}

func sr(v string, score float64) storage.SearchResult {
	return storage.SearchResult{Value: v, Score: score}
}

// erroringQueryable returns an error from Querier(); used to exercise the
// searcherForRequest open-error branch.
type erroringQueryable struct{ err error }

func (e *erroringQueryable) Querier(_, _ int64) (storage.Querier, error) {
	return nil, e.err
}

// plainQueryable returns a storage.Querier that deliberately does NOT
// implement mimirSearcher; exercises searcherForRequest's type-assertion
// failure branch.
type plainQueryable struct{}

func (plainQueryable) Querier(_, _ int64) (storage.Querier, error) {
	return plainQuerier{}, nil
}

type plainQuerier struct{}

func (plainQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}
func (plainQuerier) LabelValues(_ context.Context, _ string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (plainQuerier) LabelNames(_ context.Context, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (plainQuerier) Close() error { return nil }

// tooManySearchTerms returns a "search[]=tN&..." query fragment with n terms.
// Used to drive the maxSearchTermsPerRequest cap test.
func tooManySearchTerms(n int) string {
	parts := make([]string, 0, n)
	for i := 0; i < n; i++ {
		parts = append(parts, fmt.Sprintf("search[]=t%d", i))
	}
	return strings.Join(parts, "&")
}

func newSearchHandlerRequest(t *testing.T, target string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, target, nil)
	return r.WithContext(user.InjectOrgID(r.Context(), "test"))
}

func enabledSearchConfig() Config {
	return Config{ExperimentalSearchAPIEnabled: true}
}

// drainNDJSON returns each emitted JSON object as a map[string]any, in
// emission order.
func drainNDJSON(t *testing.T, body string) []map[string]any {
	t.Helper()
	if body == "" {
		return nil
	}
	scanner := bufio.NewScanner(strings.NewReader(body))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var out []map[string]any
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m), "decoding NDJSON line: %s", line)
		out = append(out, m)
	}
	require.NoError(t, scanner.Err())
	return out
}

func TestSearchLabelNamesHandler_FlagOff_Returns404(t *testing.T) {
	mq := &searchMockQuerier{}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), Config{ExperimentalSearchAPIEnabled: false}, nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names"))

	assert.Equal(t, http.StatusNotFound, w.Code)
	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 1)
	assert.Equal(t, "error", lines[0]["status"])
	assert.Equal(t, "feature_not_enabled", lines[0]["errorType"])
}

func TestSearchLabelNamesHandler_HappyPath(t *testing.T) {
	results := []storage.SearchResult{sr("a", 1.0), sr("b", 0.9)}
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice(results, nil)
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names"))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, searchAPIContentType, w.Header().Get("Content-Type"))
	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 2, "one batch + one trailer")
	// batch line
	batchResults := lines[0]["results"].([]any)
	require.Len(t, batchResults, 2)
	assert.Equal(t, "a", batchResults[0].(map[string]any)["name"])
	_, hasScore := batchResults[0].(map[string]any)["score"]
	assert.False(t, hasScore, "score must be omitted by default (include_score=false)")
	// trailer
	assert.Equal(t, "success", lines[1]["status"])
}

func TestSearchLabelNamesHandler_IncludeScoreEmitsScore(t *testing.T) {
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("foo", 0.75)}, nil)
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names?include_score=true"))

	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 2)
	rec := lines[0]["results"].([]any)[0].(map[string]any)
	assert.Equal(t, "foo", rec["name"])
	require.Contains(t, rec, "score")
	assert.InDelta(t, 0.75, rec["score"], 1e-9)
}

func TestSearchLabelNamesHandler_BatchBoundaries(t *testing.T) {
	// 5 results, batch_size=2 → 3 batch lines + 1 trailer = 4 lines.
	results := []storage.SearchResult{
		sr("a", 1.0), sr("b", 1.0), sr("c", 1.0),
		sr("d", 1.0), sr("e", 1.0),
	}
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice(results, nil)
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names?batch_size=2"))

	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 4)
	// batches
	for i, expectLen := range []int{2, 2, 1} {
		batch := lines[i]["results"].([]any)
		assert.Lenf(t, batch, expectLen, "batch %d", i)
	}
	assert.Equal(t, "success", lines[3]["status"])
}

func TestSearchLabelNamesHandler_HasMoreWhenLimitHit(t *testing.T) {
	results := []storage.SearchResult{sr("a", 1.0), sr("b", 1.0)}
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice(results, nil)
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names?limit=2"))

	lines := drainNDJSON(t, w.Body.String())
	require.NotEmpty(t, lines)
	trailer := lines[len(lines)-1]
	assert.Equal(t, "success", trailer["status"])
	assert.Equal(t, true, trailer["has_more"])
}

func TestSearchLabelNamesHandler_HasMoreFalseWhenUnderLimit(t *testing.T) {
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("a", 1.0)}, nil)
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names?limit=10"))

	lines := drainNDJSON(t, w.Body.String())
	trailer := lines[len(lines)-1]
	assert.Equal(t, "success", trailer["status"])
	assert.Equal(t, false, trailer["has_more"])
}

func TestSearchLabelNamesHandler_WarningsRideOnTrailer(t *testing.T) {
	var warns annotations.Annotations
	warns.Add(errors.New("source-warning"))
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("a", 1.0)}, warns)
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names"))

	lines := drainNDJSON(t, w.Body.String())
	trailer := lines[len(lines)-1]
	warnsField := trailer["warnings"].([]any)
	require.Len(t, warnsField, 1)
	assert.Equal(t, "source-warning", warnsField[0])
}

// errSearchResultSet emits one result then surfaces an error from Err().
type errSearchResultSet struct {
	results []storage.SearchResult
	idx     int
	err     error
}

func (e *errSearchResultSet) Next() bool {
	if e.idx >= len(e.results) {
		return false
	}
	e.idx++
	return true
}
func (e *errSearchResultSet) At() storage.SearchResult          { return e.results[e.idx-1] }
func (e *errSearchResultSet) Warnings() annotations.Annotations { return nil }
func (e *errSearchResultSet) Err() error                        { return e.err }
func (e *errSearchResultSet) Close() error                      { return nil }

func TestSearchLabelNamesHandler_MidStreamErrorRendersErrorTrailer(t *testing.T) {
	wantErr := errors.New("midstream boom")
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return &errSearchResultSet{
				results: []storage.SearchResult{sr("a", 1.0)},
				err:     wantErr,
			}
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names"))

	// HTTP 200 because headers were already on the wire when the error
	// surfaced; the failure is in the NDJSON trailer instead.
	assert.Equal(t, http.StatusOK, w.Code)
	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 2)
	trailer := lines[1]
	assert.Equal(t, "error", trailer["status"])
	assert.Equal(t, "internal", trailer["errorType"])
	assert.Contains(t, trailer["error"], "midstream boom")
}

func TestSearchLabelNamesHandler_PreFlushErrorReturns500(t *testing.T) {
	wantErr := errors.New("pre-flush boom")
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			// No results so nothing flushes before Err() is read.
			return &errSearchResultSet{err: wantErr}
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names"))

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestSearchLabelNamesHandler_BadParams_Return400(t *testing.T) {
	mq := &searchMockQuerier{}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	tests := []struct {
		name  string
		query string
	}{
		{name: "invalid fuzz_alg", query: "fuzz_alg=banana"},
		{name: "invalid fuzz_threshold (non-integer)", query: "fuzz_threshold=abc"},
		{name: "invalid fuzz_threshold (out of range)", query: "fuzz_threshold=200"},
		{name: "invalid sort_by", query: "sort_by=cosine"},
		{name: "sort_dir with sort_by=score rejected", query: "sort_by=score&sort_dir=asc"},
		{name: "invalid sort_dir", query: "sort_dir=sideways"},
		{name: "negative limit", query: "limit=-1"},
		{name: "negative batch_size", query: "batch_size=-1"},
		{name: "non-integer batch_size", query: "batch_size=abc"},
		{name: "batch_size above maximum", query: "batch_size=1000000000"},
		{name: "sort_by=score without search[]", query: "sort_by=score"},
		{name: "too many search[] terms", query: tooManySearchTerms(maxSearchTermsPerRequest + 1)},
		{name: "invalid include_score", query: "include_score=maybe"},
		{name: "invalid case_sensitive", query: "case_sensitive=maybe"},
		{name: "unparseable start", query: "start=not-a-time"},
		{name: "invalid match selector", query: "match[]=foo%7Bbar"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names?"+tc.query))
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestSearchLabelValuesHandler_RequiresLabel(t *testing.T) {
	mq := &searchMockQuerier{}
	h := SearchLabelValuesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_values"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 1)
	assert.Contains(t, lines[0]["error"], `"label"`)
}

func TestSearchLabelValuesHandler_ForwardsLabel(t *testing.T) {
	mq := &searchMockQuerier{
		valuesFn: func(_ string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("prod", 1.0)}, nil)
		},
	}
	h := SearchLabelValuesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_values?label=env"))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "env", mq.lastName)
}

func TestSearchMetricNamesHandler_ForwardsMetricNameLabel(t *testing.T) {
	mq := &searchMockQuerier{
		valuesFn: func(_ string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("metric_a", 1.0)}, nil)
		},
	}
	h := SearchMetricNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/metric_names"))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, model.MetricNameLabel, mq.lastName, "metric_names endpoint must search the __name__ label")
}

func TestParseSortOrder_Cases(t *testing.T) {
	tests := []struct {
		name, sortBy, sortDir string
		dirExplicit           bool
		want                  storage.Ordering
		wantErr               bool
	}{
		{name: "alpha asc default", sortBy: "alpha", sortDir: "asc", want: storage.OrderByValueAsc},
		{name: "alpha dsc", sortBy: "alpha", sortDir: "dsc", want: storage.OrderByValueDesc},
		{name: "alpha desc (alias)", sortBy: "alpha", sortDir: "desc", want: storage.OrderByValueDesc},
		{name: "score with no explicit dir", sortBy: "score", sortDir: "", want: storage.OrderByScoreDesc},
		{name: "score with explicit dir rejected", sortBy: "score", sortDir: "asc", dirExplicit: true, wantErr: true},
		{name: "unknown sort_by rejected", sortBy: "magic", sortDir: "asc", wantErr: true},
		{name: "unknown sort_dir under alpha rejected", sortBy: "alpha", sortDir: "sideways", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSortOrder(tc.sortBy, tc.sortDir, tc.dirExplicit)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestParseSearchRequest_DefaultTimeRange pins the one-hour default window
// matched against Prometheus PR #18573. If either default ever drifts (e.g.
// back to model.Earliest/Latest), this test fails: an unspecified start/end
// must return endMs == "now" and startMs == "now - 1h".
func TestParseSearchRequest_DefaultTimeRange(t *testing.T) {
	r := newSearchHandlerRequest(t, "/api/v1/search/label_names?search[]=foo")
	before := time.Now().UnixMilli()
	req, err := parseSearchRequest(r, false)
	after := time.Now().UnixMilli()
	require.NoError(t, err)

	assert.Equal(t, int64(3600000), req.endMs-req.startMs, "default window must be exactly one hour")
	// endMs should fall in [before, after] modulo the small slack of the
	// internal model.Now() call between us reading "before" and the handler
	// stamping the default. Add a 1s tolerance either side.
	assert.GreaterOrEqual(t, req.endMs, before-1000, "endMs must not predate the test start")
	assert.LessOrEqual(t, req.endMs, after+1000, "endMs must not exceed the test end")
}

// TestParseSearchRequest_RejectsInvertedTimeRange pins the upstream behaviour
// from Prometheus PR #18573: end < start is HTTP 400 with the exact error
// message clients are documented to expect, but end == start is fine.
func TestParseSearchRequest_RejectsInvertedTimeRange(t *testing.T) {
	t.Run("end before start is rejected", func(t *testing.T) {
		r := newSearchHandlerRequest(t, "/api/v1/search/label_names?start=7200&end=3600")
		_, err := parseSearchRequest(r, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "end timestamp must not be before start timestamp")
	})
	t.Run("end equal to start is permitted", func(t *testing.T) {
		r := newSearchHandlerRequest(t, "/api/v1/search/label_names?start=3600&end=3600")
		req, err := parseSearchRequest(r, false)
		require.NoError(t, err)
		assert.Equal(t, req.startMs, req.endMs)
	})
}

func TestParseSearchRequest_ParamRoundTrip(t *testing.T) {
	r := newSearchHandlerRequest(t, "/api/v1/search/label_names?search[]=foo&search[]=bar&case_sensitive=false&fuzz_alg=jarowinkler&fuzz_threshold=75&sort_by=alpha&sort_dir=dsc&include_score=true&limit=42&batch_size=7&match[]={job=\"prom\"}")
	req, err := parseSearchRequest(r, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"foo", "bar"}, req.params.Terms)
	assert.False(t, req.params.CaseSensitive)
	assert.Equal(t, streaminglabelvalues.FuzzAlgJaroWinkler, req.params.FuzzAlg)
	assert.Equal(t, 75, req.params.FuzzThreshold)
	assert.Equal(t, storage.OrderByValueDesc, req.hints.OrderBy)
	assert.True(t, req.includeScore)
	assert.Equal(t, 42, req.hints.Limit)
	assert.Equal(t, 7, req.batchSize)
	require.Len(t, req.matchers, 1, "one match[] entry → one matcher set")
	require.Len(t, req.matchers[0], 1)
	assert.Equal(t, "job", req.matchers[0][0].Name)
	assert.Equal(t, "prom", req.matchers[0][0].Value)
}

// TestParseSearchRequest_BatchSizeZeroKeepsDefault pins the post-rename
// contract: batch_size=0 means "server-determined" and falls back to
// searchDefaultBatchSize. Previously Mimir rejected 0; upstream accepts it.
func TestParseSearchRequest_BatchSizeZeroKeepsDefault(t *testing.T) {
	r := newSearchHandlerRequest(t, "/api/v1/search/label_names?batch_size=0")
	req, err := parseSearchRequest(r, false)
	require.NoError(t, err)
	assert.Equal(t, searchDefaultBatchSize, req.batchSize)
}

// TestSearchLabelNamesHandler_AcceptsPOSTForm covers the spec parity case:
// the routes are registered for POST and the query parameters can ride on
// the form body. r.ParseForm() should reach them.
func TestSearchLabelNamesHandler_AcceptsPOSTForm(t *testing.T) {
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("a", 1.0)}, nil)
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	body := strings.NewReader("search[]=foo&include_score=true")
	r := httptest.NewRequest(http.MethodPost, "/api/v1/search/label_names", body)
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r = r.WithContext(user.InjectOrgID(r.Context(), "test"))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, mq.lastParams)
	assert.Equal(t, []string{"foo"}, mq.lastParams.Terms, "POST form body must reach the parser")
}

func TestSearchLabelNamesHandler_MissingTenantReturns400(t *testing.T) {
	// Deliberately do not inject org ID — searcherForRequest must reject.
	mq := &searchMockQuerier{}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)
	r := httptest.NewRequest(http.MethodGet, "/api/v1/search/label_names", nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestSearchLabelNamesHandler_QueryableOpenError(t *testing.T) {
	wantErr := errors.New("open boom")
	h := SearchLabelNamesHandler(&erroringQueryable{err: wantErr}, enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 1)
	assert.Contains(t, lines[0]["error"], "open querier")
}

func TestSearchLabelNamesHandler_QuerierNotMimirSearcherReturns400(t *testing.T) {
	h := SearchLabelNamesHandler(plainQueryable{}, enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 1)
	assert.Contains(t, lines[0]["error"], "does not support search")
}

// TestSearchLabelNamesHandler_EmptyResultsEmitsTrailerOnly pins that the
// handler does not emit an empty {"results":[]} batch line when the iterator
// has nothing to yield — only the success trailer reaches the wire.
func TestSearchLabelNamesHandler_EmptyResultsEmitsTrailerOnly(t *testing.T) {
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.EmptySearchResultSet()
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names"))
	assert.Equal(t, http.StatusOK, w.Code)
	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 1, "no batch line; only the success trailer")
	assert.Equal(t, "success", lines[0]["status"])
}

// TestSearchLabelNamesHandler_RepeatedMatchUnionsSelectors pins the OR
// semantics for repeated match[] selectors: each selector is run independently
// and the resulting SearchResultSets are merged (with dedup), matching the
// upstream /api/v1/labels and Prometheus PR #18573 behaviour.
func TestSearchLabelNamesHandler_RepeatedMatchUnionsSelectors(t *testing.T) {
	// The mock returns different results per matcher set so we can verify
	// both calls happened AND the union was emitted.
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet {
			require.Len(t, matchers, 1, "each call sees exactly one selector's matchers")
			switch matchers[0].Name {
			case "job":
				return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("a", 1.0), sr("shared", 1.0)}, nil)
			case "env":
				return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("b", 1.0), sr("shared", 1.0)}, nil)
			default:
				t.Fatalf("unexpected matcher %s=%q", matchers[0].Name, matchers[0].Value)
				return nil
			}
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names?match[]={job=%22api%22}&match[]={env=%22prod%22}"))
	assert.Equal(t, http.StatusOK, w.Code)

	lines := drainNDJSON(t, w.Body.String())
	require.NotEmpty(t, lines)
	// Collect every result value across all batch lines (everything but the trailer).
	var got []string
	for _, line := range lines[:len(lines)-1] {
		for _, r := range line["results"].([]any) {
			got = append(got, r.(map[string]any)["name"].(string))
		}
	}
	assert.ElementsMatch(t, []string{"a", "b", "shared"}, got, "selectors unioned and duplicates collapsed")
}
