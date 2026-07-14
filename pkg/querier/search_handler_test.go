// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/querier/worker"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util/validation"
)

// searchMockQuerier satisfies storage.Querier and mimirSearcher. The search
// methods return whatever ResultSet was wired in via the closure-based
// fields, letting each test express exactly the iterator behaviour it wants
// (results, mid-stream error, warnings, etc.).
//
// When metadata is non-nil the mock also satisfies metricMetadataFetcher, so
// the metric-names handler can enrich results from it (mirroring the querier's
// ingester metadata fan-out). metadataFetchCalls counts the fetches.
type searchMockQuerier struct {
	namesFn      func(params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet
	valuesFn     func(name string, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet
	metadata     map[string]metadata.Metadata
	fetchedNames [][]string
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

// FetchMetricMetadata is only present (as far as the metricMetadataFetcher
// type-assert is concerned) to let the handler enrich; it returns metadata for
// the requested names found in the configured map.
func (s *searchMockQuerier) FetchMetricMetadata(_ context.Context, names []string) (map[string]metadata.Metadata, error) {
	s.fetchedNames = append(s.fetchedNames, append([]string(nil), names...))
	out := make(map[string]metadata.Metadata, len(names))
	for _, n := range names {
		if md, ok := s.metadata[n]; ok {
			out[n] = md
		}
	}
	return out, nil
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

// srMD constructs a SearchResult decorated with metadata — the fixture
// that simulates what the wire-decode boundary in distributor_search.go
// produces when the ingester returns a result with inline MetricMetadata.
func srMD(v string, score float64, mt model.MetricType, help, unit string) storage.SearchResult {
	return storage.SearchResult{
		Value: v,
		Score: score,
		Metadata: &metadata.Metadata{
			Type: mt,
			Help: help,
			Unit: unit,
		},
	}
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

// TestSearchLabelNamesHandler_HasMoreWhenLimitHit pins the limit+1 probe:
// the data has more records than the user's limit, so the iterator's
// (limit+1)-th step lets the handler signal has_more=true while the
// wire response is still capped at exactly limit records.
func TestSearchLabelNamesHandler_HasMoreWhenLimitHit(t *testing.T) {
	results := []storage.SearchResult{sr("a", 1.0), sr("b", 1.0), sr("c", 1.0)}
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
	// The wire response holds at most limit records — the (limit+1)-th
	// probe is dropped before encoding.
	emitted := 0
	for _, ln := range lines[:len(lines)-1] {
		emitted += len(ln["results"].([]any))
	}
	assert.Equal(t, 2, emitted, "wire output must be capped at the user limit; probe record is dropped")
	trailer := lines[len(lines)-1]
	assert.Equal(t, "success", trailer["status"])
	assert.Equal(t, true, trailer["has_more"])
}

// TestSearchLabelNamesHandler_HasMoreFalseWhenDataExactlyFillsLimit
// pins the precision improvement from the limit+1 probe: when the data
// has *exactly* req.limit records the iterator runs out without
// producing a probe record, so has_more must be false. Without the +1
// probe this case would read as has_more=true (the old "emitted >=
// limit" approximation).
func TestSearchLabelNamesHandler_HasMoreFalseWhenDataExactlyFillsLimit(t *testing.T) {
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
	assert.Equal(t, false, trailer["has_more"],
		"data length == limit must not be reported as has_more (limit+1 probe distinguishes this from a real overflow)")
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

// TestSearchLabelNamesHandler_HintsLimitIsLimitPlusOne pins that the
// downstream Searcher sees hints.Limit set to userLimit+1 so the +1
// probe survives all the way to the iterator. limit=0 ("no limit") is
// passed through as hints.Limit=0.
func TestSearchLabelNamesHandler_HintsLimitIsLimitPlusOne(t *testing.T) {
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.EmptySearchResultSet()
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names?limit=50"))
	assert.Equal(t, 51, mq.lastHints.Limit, "user limit must arrive at the searcher as limit+1 (the has_more probe)")

	w = httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names?limit=0"))
	assert.Equal(t, 0, mq.lastHints.Limit, "limit=0 (no limit) must pass through unchanged — no probe added")
}

// TestSearchLabelNamesHandler_HintsLimitAtMaxIntDoesNotOverflow pins
// the guard against the +1 probe wrapping to a negative number when
// the caller supplies math.MaxInt. A negative hints.Limit would be
// treated as "no limit" by the merger, defeating the cap.
func TestSearchLabelNamesHandler_HintsLimitAtMaxIntDoesNotOverflow(t *testing.T) {
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.EmptySearchResultSet()
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, fmt.Sprintf("/api/v1/search/label_names?limit=%d", math.MaxInt)))
	assert.Equal(t, math.MaxInt, mq.lastHints.Limit,
		"limit=math.MaxInt must not wrap to a negative hints.Limit")
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
	// An arbitrary warning must NOT flip has_more — only the
	// MaxLimitError clamp warning signals truncation.
	assert.Equal(t, false, trailer["has_more"])
}

// TestSearchLabelNamesHandler_HasMoreWhenClampFires pins the fix for the
// case where a per-tenant max-label-names ceiling clamps the request
// limit below what the client asked for: emitted < req.hints.Limit but
// the result set was still truncated, so has_more must be true.
func TestSearchLabelNamesHandler_HasMoreWhenClampFires(t *testing.T) {
	var warns annotations.Annotations
	warns.Add(NewMaxLimitError(10_000, 5, validation.MaxLabelNamesLimitFlag))
	results := []storage.SearchResult{sr("a", 1), sr("b", 1), sr("c", 1), sr("d", 1), sr("e", 1)}
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice(results, warns)
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names?limit=10000"))

	lines := drainNDJSON(t, w.Body.String())
	trailer := lines[len(lines)-1]
	assert.Equal(t, "success", trailer["status"])
	assert.Equal(t, true, trailer["has_more"],
		"clamp warning must flip has_more even when emitted (%d) < requested limit", len(results))
	warnsField := trailer["warnings"].([]any)
	require.Len(t, warnsField, 1)
	assert.Contains(t, warnsField[0], validation.MaxLabelNamesLimitFlag)
}

// TestSearchLabelValuesHandler_HasMoreWhenClampFires mirrors the label-
// names test for the label-values path.
func TestSearchLabelValuesHandler_HasMoreWhenClampFires(t *testing.T) {
	var warns annotations.Annotations
	warns.Add(NewMaxLimitError(10_000, 5, validation.MaxLabelValuesLimitFlag))
	results := []storage.SearchResult{sr("a", 1), sr("b", 1), sr("c", 1), sr("d", 1), sr("e", 1)}
	mq := &searchMockQuerier{
		valuesFn: func(_ string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice(results, warns)
		},
	}
	h := SearchLabelValuesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_values?label=instance&limit=10000"))

	lines := drainNDJSON(t, w.Body.String())
	trailer := lines[len(lines)-1]
	assert.Equal(t, "success", trailer["status"])
	assert.Equal(t, true, trailer["has_more"],
		"clamp warning must flip has_more even when emitted (%d) < requested limit", len(results))
	warnsField := trailer["warnings"].([]any)
	require.Len(t, warnsField, 1)
	assert.Contains(t, warnsField[0], validation.MaxLabelValuesLimitFlag)
}

// TestSearchLabelNamesHandler_UnrelatedLimitErrorDoesNotFlipHasMore pins
// that LimitErrors *other* than the MaxLimitError search clamps (e.g. a
// series-query length limit) do not flip has_more — only the search
// clamps signal label-list truncation.
func TestSearchLabelNamesHandler_UnrelatedLimitErrorDoesNotFlipHasMore(t *testing.T) {
	var warns annotations.Annotations
	warns.Add(NewMaxLimitError(10_000, 5, validation.MaxSeriesQueryLimitFlag))
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("a", 1.0)}, warns)
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names?limit=100"))

	lines := drainNDJSON(t, w.Body.String())
	trailer := lines[len(lines)-1]
	assert.Equal(t, false, trailer["has_more"],
		"non-search MaxLimitError must not be treated as a search-result truncation signal")
}

// TestSearchHandlers_ResultJSONKey verifies that the wire-format JSON response conforms to spec.
// The label_names and metric_names return a result with `name` as the key. Where-as label_values
// returns a result with `value` as the key. This test confirms that the expected result shape is returned
// for each API.
func TestSearchHandlers_ResultJSONKey(t *testing.T) {
	resultSet := func() storage.SearchResultSet {
		return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("prod", 0.9)}, nil)
	}
	tests := []struct {
		name       string
		newHandler func(storage.Queryable) http.Handler
		url        string
		wantKey    string // key that must be present
		absentKey  string // key that must not be present
	}{
		{
			name: "label_names emits name",
			newHandler: func(q storage.Queryable) http.Handler {
				return SearchLabelNamesHandler(q, enabledSearchConfig(), nil)
			},
			url:       "/api/v1/search/label_names?include_score=true",
			wantKey:   "name",
			absentKey: "value",
		},
		{
			name: "label_values emits value",
			newHandler: func(q storage.Queryable) http.Handler {
				return SearchLabelValuesHandler(q, enabledSearchConfig(), nil)
			},
			url:       "/api/v1/search/label_values?label=env&include_score=true",
			wantKey:   "value",
			absentKey: "name",
		},
		{
			name: "metric_names emits name",
			newHandler: func(q storage.Queryable) http.Handler {
				return SearchMetricNamesHandler(q, enabledSearchConfig(), nil, log.NewNopLogger())
			},
			url:       "/api/v1/search/metric_names?include_score=true",
			wantKey:   "name",
			absentKey: "value",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mq := &searchMockQuerier{
				namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
					return resultSet()
				},
				valuesFn: func(_ string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
					return resultSet()
				},
			}
			h := tc.newHandler(newSearchMockQueryable(mq))

			w := httptest.NewRecorder()
			h.ServeHTTP(w, newSearchHandlerRequest(t, tc.url))
			require.Equal(t, http.StatusOK, w.Code)

			lines := drainNDJSON(t, w.Body.String())
			require.Len(t, lines, 2, "one batch + one trailer")
			rec := lines[0]["results"].([]any)[0].(map[string]any)
			require.Contains(t, rec, tc.wantKey, "endpoint must emit %q as the JSON key per upstream contract", tc.wantKey)
			assert.Equal(t, "prod", rec[tc.wantKey])
			_, hasAbsent := rec[tc.absentKey]
			assert.False(t, hasAbsent, "endpoint must not emit %q; that is a different endpoint's contract", tc.absentKey)
			assert.InDelta(t, 0.9, rec["score"], 1e-9)
		})
	}
}

// TestSearchLabelValuesHandler_HasMoreVsClampWarning exercises the full
// decision matrix for `has_more` against the per-tenant clamp warning.
func TestSearchLabelValuesHandler_HasMoreVsClampWarning(t *testing.T) {
	const resultCount = 5

	tests := []struct {
		name        string
		urlLimit    string
		emitWarning bool
		tenantCap   int // only meaningful when emitWarning is true
		wantHasMore bool
		wantWarning bool
		why         string
	}{
		{
			name:        "limit=0 result count below tenant cap",
			urlLimit:    "0",
			emitWarning: true,
			tenantCap:   10_000,
			wantHasMore: false,
			wantWarning: true,
			why:         "requested unlimited but although tenant cap exists the result count is below the cap",
		},
		{
			name:        "requested limit under tenant cap",
			urlLimit:    "100",
			emitWarning: false,
			wantHasMore: false,
			wantWarning: false,
			why:         "requested limit is below the tenant cap",
		},
		{
			name:        "requested limit over tenant cap",
			urlLimit:    "200",
			emitWarning: true,
			tenantCap:   resultCount,
			wantHasMore: true,
			wantWarning: true,
			//   clampEnforcedMin >= 0 && emitted >= clampEnforcedMin - hasMore=true
			why: "requested 200 which is above the tenant cap of 5 and there were 5 results.",
		},
		{
			name:        "limit=0 with no tenant cap configured",
			urlLimit:    "0",
			emitWarning: false,
			wantHasMore: false,
			wantWarning: false,
			why:         "requested unlimited but there is no tenant cap configured",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			results := make([]storage.SearchResult, 0, resultCount)
			for i := 0; i < resultCount; i++ {
				results = append(results, sr(fmt.Sprintf("v%d", i), 1))
			}
			var warns annotations.Annotations
			if tc.emitWarning {
				warns.Add(NewMaxLimitError(0, tc.tenantCap, validation.MaxLabelValuesLimitFlag))
			}
			mq := &searchMockQuerier{
				valuesFn: func(_ string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
					return storage.NewSearchResultSetFromSlice(results, warns)
				},
			}
			h := SearchLabelValuesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

			w := httptest.NewRecorder()
			h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_values?label=instance&limit="+tc.urlLimit))

			lines := drainNDJSON(t, w.Body.String())
			trailer := lines[len(lines)-1]
			assert.Equal(t, "success", trailer["status"])
			assert.Equal(t, tc.wantHasMore, trailer["has_more"], tc.why)

			warnsField, _ := trailer["warnings"].([]any)
			if tc.wantWarning {
				require.Len(t, warnsField, 1, "the clamp warning must always be relayed so clients know a cap was applied")
				assert.Contains(t, warnsField[0], validation.MaxLabelValuesLimitFlag)
			} else {
				assert.Empty(t, warnsField, "no clamp adjustment occurred; trailer must carry no warning")
			}
		})
	}
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

// TestSearchLabelNamesHandler_PreFlushErrorReturnsJSONEnvelope pins the
// pre-flush error response shape. Headers haven't been sent at the
// point the error fires, so the response uses Content-Type:
// application/json and carries the same envelope the post-flush path
// emits as an NDJSON trailer (status / errorType / error). NDJSON
// headers must NOT leak onto the response — that was the original bug
// (http.Error overriding Content-Type with text/plain).
func TestSearchLabelNamesHandler_PreFlushErrorReturnsJSONEnvelope(t *testing.T) {
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
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"),
		"pre-flush error must not advertise the streaming NDJSON content type")
	assert.Empty(t, w.Header().Get(worker.ResponseStreamingEnabledHeader),
		"pre-flush error must not advertise streaming-enabled")

	var env map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
	assert.Equal(t, "error", env["status"])
	assert.Equal(t, "internal", env["errorType"])
	assert.Contains(t, env["error"], "pre-flush boom")
}

// TestSearchLabelNamesHandler_PreFlushContextCanceledReturns499 pins
// that a client-cancelled iterator surfaces as Prometheus' 499 status
// with errorType=canceled, matching getDefaultErrorCode(errorCanceled).
func TestSearchLabelNamesHandler_PreFlushContextCanceledReturns499(t *testing.T) {
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return &errSearchResultSet{err: context.Canceled}
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names"))

	assert.Equal(t, 499, w.Code)
	var env map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
	assert.Equal(t, "canceled", env["errorType"])
}

// TestSearchLabelNamesHandler_PreFlushContextDeadlineReturns503 pins
// that an iterator failing with context.DeadlineExceeded maps to a
// 503 with errorType=timeout, matching getDefaultErrorCode(errorTimeout).
func TestSearchLabelNamesHandler_PreFlushContextDeadlineReturns503(t *testing.T) {
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return &errSearchResultSet{err: context.DeadlineExceeded}
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names"))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	var env map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
	assert.Equal(t, "timeout", env["errorType"])
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
		// Include a search[] term so parseSortOrder validates the sort_dir+score combination.
		{name: "sort_dir with sort_by=score rejected", query: "search[]=foo&sort_by=score&sort_dir=asc"},
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
	h := SearchMetricNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil, log.NewNopLogger())

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
	req, err := parseSearchRequest(r, false, false)
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
		_, err := parseSearchRequest(r, false, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "end timestamp must not be before start timestamp")
	})
	t.Run("end equal to start is permitted", func(t *testing.T) {
		r := newSearchHandlerRequest(t, "/api/v1/search/label_names?start=3600&end=3600")
		req, err := parseSearchRequest(r, false, false)
		require.NoError(t, err)
		assert.Equal(t, req.startMs, req.endMs)
	})
}

func TestParseSearchRequest_ParamRoundTrip(t *testing.T) {
	r := newSearchHandlerRequest(t, "/api/v1/search/label_names?search[]=foo&search[]=bar&case_sensitive=false&fuzz_alg=jarowinkler&fuzz_threshold=75&sort_by=alpha&sort_dir=dsc&include_score=true&limit=42&batch_size=7&match[]={job=\"prom\"}")
	req, err := parseSearchRequest(r, false, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"foo", "bar"}, req.params.Terms)
	assert.False(t, req.params.CaseSensitive)
	assert.Equal(t, streaminglabelvalues.FuzzAlgJaroWinkler, req.params.FuzzAlg)
	assert.Equal(t, 75, req.params.FuzzThreshold)
	assert.Equal(t, storage.OrderByValueDesc, req.hints.OrderBy)
	assert.True(t, req.includeScore)
	assert.Equal(t, 42, req.limit, "user-facing limit is the URL value")
	assert.Equal(t, 43, req.hints.Limit, "downstream hint is limit+1 (the has_more probe)")
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
	req, err := parseSearchRequest(r, false, false)
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
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 1)
	assert.Equal(t, "error", lines[0]["status"])
	assert.Equal(t, "bad_data", lines[0]["errorType"])
}

func TestSearchLabelNamesHandler_QueryableOpenErrorReturns500(t *testing.T) {
	wantErr := errors.New("open boom")
	h := SearchLabelNamesHandler(&erroringQueryable{err: wantErr}, enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names"))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 1)
	assert.Equal(t, "error", lines[0]["status"])
	assert.Equal(t, "internal", lines[0]["errorType"])
	assert.Contains(t, lines[0]["error"], "open querier")
}

func TestSearchLabelNamesHandler_QuerierNotMimirSearcherReturns500(t *testing.T) {
	h := SearchLabelNamesHandler(plainQueryable{}, enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names"))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 1)
	assert.Equal(t, "error", lines[0]["status"])
	assert.Equal(t, "internal", lines[0]["errorType"])
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

func TestSearchMetricNamesHandler_MetadataEnrichesRecords(t *testing.T) {
	// The search returns bare metric names; the handler enriches them from the
	// querier's FetchMetricMetadata (the metadata fan-out) above every merge.
	mq := &searchMockQuerier{
		valuesFn: func(_ string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice([]storage.SearchResult{
				sr("http_requests_total", 1.0),
				sr("cpu_usage_seconds", 1.0),
			}, nil)
		},
		metadata: map[string]metadata.Metadata{
			"http_requests_total": {Type: model.MetricTypeCounter, Help: "Total HTTP requests."},
			"cpu_usage_seconds":   {Type: model.MetricTypeGauge, Help: "CPU usage.", Unit: "seconds"},
		},
	}
	h := SearchMetricNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil, log.NewNopLogger())

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/metric_names?include_metadata=true"))
	assert.Equal(t, http.StatusOK, w.Code)
	require.NotEmpty(t, mq.fetchedNames, "the handler must fetch metadata for the returned names")
	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 2)
	batch := lines[0]["results"].([]any)
	require.Len(t, batch, 2)
	first := batch[0].(map[string]any)
	assert.Equal(t, "http_requests_total", first["name"])
	assert.Equal(t, "counter", first["type"])
	assert.Equal(t, "Total HTTP requests.", first["help"])
	_, hasUnit := first["unit"]
	assert.False(t, hasUnit, "empty unit must omit field")
	second := batch[1].(map[string]any)
	assert.Equal(t, "cpu_usage_seconds", second["name"])
	assert.Equal(t, "gauge", second["type"])
	assert.Equal(t, "seconds", second["unit"])
}

func TestSearchMetricNamesHandler_MissingMetadataLeavesFieldsEmpty(t *testing.T) {
	// Result without inline metadata — simulates store-gateway-sourced
	// hits (no metadata available) or metrics the ingester didn't have
	// recorded metadata for. The handler must leave Type/Help/Unit absent.
	mq := &searchMockQuerier{
		valuesFn: func(_ string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("orphan_metric", 1.0)}, nil)
		},
	}
	h := SearchMetricNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil, log.NewNopLogger())

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/metric_names?include_metadata=true"))
	assert.Equal(t, http.StatusOK, w.Code)
	lines := drainNDJSON(t, w.Body.String())
	require.Len(t, lines, 2)
	rec := lines[0]["results"].([]any)[0].(map[string]any)
	assert.Equal(t, "orphan_metric", rec["name"])
	_, hasType := rec["type"]
	assert.False(t, hasType, "missing metadata leaves enrichment fields absent")
	_, hasWarns := lines[1]["warnings"]
	assert.False(t, hasWarns)
}

func TestSearchMetricNamesHandler_IncludeScoreAndMetadataCompose(t *testing.T) {
	mq := &searchMockQuerier{
		valuesFn: func(_ string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("http_requests_total", 0.92)}, nil)
		},
		metadata: map[string]metadata.Metadata{"http_requests_total": {Type: model.MetricTypeCounter, Help: "h"}},
	}
	h := SearchMetricNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil, log.NewNopLogger())

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/metric_names?include_metadata=true&include_score=true"))
	lines := drainNDJSON(t, w.Body.String())
	rec := lines[0]["results"].([]any)[0].(map[string]any)
	assert.Equal(t, "http_requests_total", rec["name"])
	assert.InDelta(t, 0.92, rec["score"], 1e-9)
	assert.Equal(t, "counter", rec["type"])
	assert.Equal(t, "h", rec["help"])
}

func TestSearchMetricNamesHandler_ShouldFetchMetadataFromIngesters(t *testing.T) {
	makeQueryable := func(dist *mockDistributor) storage.Queryable {
		var cfg Config
		flagext.DefaultValues(&cfg)

		limits := defaultLimitsConfig()
		limits.QueryIngestersWithin = 0 // Always query the ingester leaf, regardless of time range.
		overrides := validation.NewOverrides(limits, nil)

		distQueryable := NewDistributorQueryable(dist, newMockConfigProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
		return newMultiQueryable(cfg, distQueryable, nil, overrides, stats.NewQueryMetrics(nil), log.NewNopLogger())
	}

	searchValues := func(results ...string) func(context.Context, model.Time, model.Time, string, *streaminglabelvalues.Params, *storage.SearchHints, []*labels.Matcher) storage.SearchResultSet {
		set := make([]storage.SearchResult, len(results))
		for i, v := range results {
			set[i] = storage.SearchResult{Value: v, Score: 1.0}
		}
		return func(context.Context, model.Time, model.Time, string, *streaminglabelvalues.Params, *storage.SearchHints, []*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice(set, nil)
		}
	}

	t.Run("enriches metric names via the metadata fan-out across the full querier stack", func(t *testing.T) {
		var gotReq *client.MetricsMetadataRequest
		// Results are ascending (the merge assumes each source is pre-sorted).
		dist := &mockDistributor{searchLabelValuesFn: searchValues("cpu_usage_seconds", "http_requests_total")}
		// Metadata is produced only by the metadata fan-out RPC, never alongside
		// the series results: enrichment working therefore proves the handler
		// does not rely on metadata riding along with series (the original bug).
		dist.On("MetricsMetadata", mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) { gotReq = args.Get(1).(*client.MetricsMetadataRequest) }).
			Return([]scrape.MetricMetadata{
				{MetricFamily: "cpu_usage_seconds", Type: model.MetricTypeGauge, Help: "CPU usage.", Unit: "seconds"},
				{MetricFamily: "http_requests_total", Type: model.MetricTypeCounter, Help: "Total HTTP requests."},
			}, nil)

		h := SearchMetricNamesHandler(makeQueryable(dist), enabledSearchConfig(), nil, log.NewNopLogger())
		w := httptest.NewRecorder()
		h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/metric_names?include_metadata=true"))

		require.Equal(t, http.StatusOK, w.Code)
		lines := drainNDJSON(t, w.Body.String())
		require.Len(t, lines, 2)
		batch := lines[0]["results"].([]any)
		require.Len(t, batch, 2)

		cpuRec := batch[0].(map[string]any)
		assert.Equal(t, "cpu_usage_seconds", cpuRec["name"])
		assert.Equal(t, "gauge", cpuRec["type"])
		assert.Equal(t, "CPU usage.", cpuRec["help"])
		assert.Equal(t, "seconds", cpuRec["unit"])

		httpRec := batch[1].(map[string]any)
		assert.Equal(t, "http_requests_total", httpRec["name"])
		assert.Equal(t, "counter", httpRec["type"])
		assert.Equal(t, "Total HTTP requests.", httpRec["help"])

		require.NotNil(t, gotReq, "the metadata fan-out RPC must be invoked")
		assert.Equal(t, []string{"cpu_usage_seconds", "http_requests_total"}, gotReq.MetricNames)
	})

	t.Run("leaves results un-enriched when the metadata fetch fails", func(t *testing.T) {
		dist := &mockDistributor{searchLabelValuesFn: searchValues("http_requests_total")}
		dist.On("MetricsMetadata", mock.Anything, mock.Anything).Return([]scrape.MetricMetadata(nil), errors.New("boom"))

		h := SearchMetricNamesHandler(makeQueryable(dist), enabledSearchConfig(), nil, log.NewNopLogger())
		w := httptest.NewRecorder()
		h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/metric_names?include_metadata=true"))

		require.Equal(t, http.StatusOK, w.Code, "a metadata fetch failure must not fail the request")
		lines := drainNDJSON(t, w.Body.String())
		require.Len(t, lines, 2)
		rec := lines[0]["results"].([]any)[0].(map[string]any)
		assert.Equal(t, "http_requests_total", rec["name"])
		_, hasType := rec["type"]
		assert.False(t, hasType, "a failed metadata fetch leaves enrichment fields absent")
	})
}

func TestSearchLabelNamesHandler_MetadataParamSilentlyIgnored(t *testing.T) {
	// include_metadata=true on the label-names endpoint is accepted (no 400) but the
	// supplier is never wired into label-names — the param is metric-specific
	// and silently ignored for forward-compatibility.
	mq := &searchMockQuerier{
		namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("env", 1.0)}, nil)
		},
	}
	h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_names?include_metadata=true"))
	assert.Equal(t, http.StatusOK, w.Code)
	lines := drainNDJSON(t, w.Body.String())
	rec := lines[0]["results"].([]any)[0].(map[string]any)
	_, hasType := rec["type"]
	assert.False(t, hasType, "label_names must never emit type/help/unit even with include_metadata=true")
}

func TestSearchMetricNamesHandler_InvalidMetadataParamReturns400(t *testing.T) {
	h := SearchMetricNamesHandler(newSearchMockQueryable(&searchMockQuerier{}), enabledSearchConfig(), nil, log.NewNopLogger())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/metric_names?include_metadata=maybe"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestSearchLabelValuesHandler_NameLabelNotEnrichedWithMetadata(t *testing.T) {
	// label_values?label=__name__ looks like metric_names, but its record shape
	// can't carry Type/Help/Unit, so include_metadata must be ignored. Only the
	// metric-names handler enriches, so the label-values handler must never call
	// the metadata fetcher.
	mq := &searchMockQuerier{
		valuesFn: func(_ string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
			return storage.NewSearchResultSetFromSlice([]storage.SearchResult{sr("metric_a", 1.0)}, nil)
		},
		metadata: map[string]metadata.Metadata{"metric_a": {Type: model.MetricTypeCounter, Help: "h"}},
	}
	h := SearchLabelValuesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_values?label=__name__&include_metadata=true"))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "__name__", mq.lastName, "sanity: the searcher was invoked for __name__")
	assert.Empty(t, mq.fetchedNames, "label_values must not fetch metadata, even for label=__name__")
	rec := drainNDJSON(t, w.Body.String())[0]["results"].([]any)[0].(map[string]any)
	_, hasType := rec["type"]
	assert.False(t, hasType, "label_values records must never carry type/help/unit")
}

func TestSearchLabelValuesHandler_MalformedMetadataParamIgnored(t *testing.T) {
	// include_metadata is not parsed for endpoints that can't carry metadata,
	// so a malformed value is ignored rather than rejected (unlike metric_names).
	h := SearchLabelValuesHandler(newSearchMockQueryable(&searchMockQuerier{}), enabledSearchConfig(), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, newSearchHandlerRequest(t, "/api/v1/search/label_values?label=env&include_metadata=maybe"))
	assert.Equal(t, http.StatusOK, w.Code, "malformed include_metadata must be ignored, not 400, on endpoints that don't support it")
}

// TestDefaultSuccessTrailer_MatchesEncoderOutput pins the byte-for-byte
// equivalence between the hand-rolled defaultSuccessTrailer constant and
// what json.Encoder produces for a zero-warning success trailer. If
// searchTrailerEnvelope's JSON tags, field order, or default-value
// rendering ever change, this test forces a deliberate update to
// defaultSuccessTrailer rather than letting the two paths drift silently.
func TestDefaultSuccessTrailer_MatchesEncoderOutput(t *testing.T) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	require.NoError(t, enc.Encode(searchTrailerEnvelope{Status: "success"}))
	assert.Equal(t, buf.String(), string(defaultSuccessTrailer),
		"defaultSuccessTrailer must match json.Encoder output for the zero-warning success trailer")
}

// newBenchmarkSearchResults builds n SearchResults with uniform-length names
// and a non-zero score. When withMetadata is true each result also carries
// the same Type/Help/Unit fixture so the metadata-encoding cost shows up
// across all batches.
func newBenchmarkSearchResults(n int, withMetadata bool) []storage.SearchResult {
	out := make([]storage.SearchResult, n)
	for i := 0; i < n; i++ {
		if withMetadata {
			out[i] = srMD(fmt.Sprintf("v%07d", i), 0.5, model.MetricTypeCounter, "help text", "seconds")
		} else {
			out[i] = sr(fmt.Sprintf("v%07d", i), 0.5)
		}
	}
	return out
}

// benchmarkSearchHandlerRequest builds an authenticated GET request for the
// given handler path; the benchmark counterpart of newSearchHandlerRequest.
func benchmarkSearchHandlerRequest(target string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	return r.WithContext(user.InjectOrgID(r.Context(), "test"))
}

// BenchmarkSearchLabelNamesHandler_Encoding isolates the NDJSON encoding +
// batching cost of the streaming handler using an in-process mock searcher.
// Results are pre-built once per sub-case and recycled per request via a
// fresh NewSearchResultSetFromSlice iterator, so the timed loop reflects the
// per-record encoder + per-batch flush path, not data generation.
func BenchmarkSearchLabelNamesHandler_Encoding(b *testing.B) {
	type benchCase struct {
		results      int
		batchSize    int
		includeScore bool
	}
	cases := []benchCase{
		{10, 100, false},
		{100, 100, false},
		{1000, 100, false},
		{10_000, 100, false},
		{1000, 1, false},    // smallest batch_size: per-batch flush dominates
		{1000, 1000, false}, // single big batch
		{1000, 100, true},   // include_score adds a JSON field per record
	}

	for _, c := range cases {
		c := c
		fixture := newBenchmarkSearchResults(c.results, false)
		mq := &searchMockQuerier{
			namesFn: func(_ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
				return storage.NewSearchResultSetFromSlice(fixture, nil)
			},
		}
		h := SearchLabelNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil)

		// limit must be >= len(fixture) so the iterator emits every result.
		target := fmt.Sprintf("/api/v1/search/label_names?batch_size=%d&limit=%d", c.batchSize, c.results)
		if c.includeScore {
			target += "&include_score=true"
		}
		name := fmt.Sprintf("results=%d/batch=%d/include_score=%t", c.results, c.batchSize, c.includeScore)

		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, benchmarkSearchHandlerRequest(target))
				if w.Code != http.StatusOK {
					b.Fatalf("unexpected status %d", w.Code)
				}
			}
		})
	}
}

// BenchmarkSearchMetricNamesHandler_MetadataEncoding pins the marginal cost
// of the optional include_metadata=true path on /search/metric_names. The
// underlying mock returns results carrying inline metadata; the handler
// must serialise Type/Help/Unit when include_metadata=true and elide them
// when it is false.
func BenchmarkSearchMetricNamesHandler_MetadataEncoding(b *testing.B) {
	for _, results := range []int{100, 1000} {
		for _, withMetadata := range []bool{false, true} {
			results, withMetadata := results, withMetadata
			fixture := newBenchmarkSearchResults(results, withMetadata)
			mq := &searchMockQuerier{
				valuesFn: func(_ string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
					return storage.NewSearchResultSetFromSlice(fixture, nil)
				},
			}
			h := SearchMetricNamesHandler(newSearchMockQueryable(mq), enabledSearchConfig(), nil, log.NewNopLogger())

			target := fmt.Sprintf("/api/v1/search/metric_names?limit=%d&include_metadata=%t", results, withMetadata)
			name := fmt.Sprintf("results=%d/include_metadata=%t", results, withMetadata)

			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					w := httptest.NewRecorder()
					h.ServeHTTP(w, benchmarkSearchHandlerRequest(target))
					if w.Code != http.StatusOK {
						b.Fatalf("unexpected status %d", w.Code)
					}
				}
			})
		}
	}
}
