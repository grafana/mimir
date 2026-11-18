// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"cmp"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

var promParser = promqlext.NewPromQLParser()

// fakeFrontend answers the estimate, PromQL and label names requests from one fixed set of series. A series is present at every time unless spans gives it samples.
type fakeFrontend struct {
	t      *testing.T
	series []labels.Labels
	// spans holds the samples of some series, by their labels' String. The estimate counts such a series when its one chunk, from its first sample to its last or its staleness marker, overlaps the window; PromQL counts it when a sample other than the marker is in the range.
	spans map[string]seriesSpan

	// eventualEstimates expects estimates without X-Read-Consistency: strong.
	eventualEstimates bool

	mu              sync.Mutex
	estimates       int
	labelValueCalls int
	queries         []string
	// estimateHook, when set, edits the estimate's answer; n counts the estimate calls so far, from 1.
	estimateHook func(n int, r *estimateResponse)
	// estimateError, when set and returning a non-zero status, is sent instead of the estimate.
	estimateError func() (int, string)
	// estimateMatchHook, when set and returning a non-zero status for the request's match[], is sent instead of the estimate.
	estimateMatchHook func(selector string) (int, string)
	// truthHook, when set, edits the counts of one PromQL query, or refuses it with a status.
	truthHook func(query string, rangeLen time.Duration, counts map[string]int64) (status int)

	// rangeLen is the length of the block ranges bounds mode answers, aligned to the epoch; 2h when 0.
	rangeLen time.Duration
	// boundsSlack widens every bounds group: lower is the count minus it and upper the count plus it.
	boundsSlack int64
	// headFrom, when > 0, is the time from which only the heads hold data: bounds mode answers the block ranges before it and a head row set for a window that reaches it.
	headFrom int64
	// boundsHook, when set, edits the bounds answer; n counts the bounds calls so far, from 1.
	boundsHook  func(n int, r *boundsResponse)
	boundsCalls int
	// labelValuesCardinalityLimit is the most rows the label values cardinality endpoint answers, 500 when 0.
	labelValuesCardinalityLimit int
}

// seriesSpan is one series' samples, in milliseconds, and its staleness marker's time, or 0 for none.
type seriesSpan struct {
	samples []int64
	stale   int64
}

// chunkOverlaps reports whether the series' chunk overlaps [start, end).
func (f *fakeFrontend) chunkOverlaps(s labels.Labels, start, end int64) bool {
	sp, ok := f.spans[s.String()]
	if !ok {
		return true
	}
	first, last := slices.Min(sp.samples), max(slices.Max(sp.samples), sp.stale)
	return first < end && last >= start
}

// sampled reports whether the series has a sample other than its staleness marker in (from, to].
func (f *fakeFrontend) sampled(s labels.Labels, from, to int64) bool {
	sp, ok := f.spans[s.String()]
	if !ok {
		return true
	}
	return slices.ContainsFunc(sp.samples, func(t int64) bool { return t > from && t <= to })
}

func newFakeFrontend(t *testing.T, series ...labels.Labels) (*fakeFrontend, *httptest.Server) {
	f := &fakeFrontend{t: t, series: series, spans: map[string]seriesSpan{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeFrontend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	require.Equal(f.t, "test-tenant", r.Header.Get("X-Scope-OrgID"))
	require.Equal(f.t, userAgent, r.Header.Get("User-Agent"))
	require.Equal(f.t, "no-store", r.Header.Get("Cache-Control"))
	require.NoError(f.t, r.ParseForm())
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.URL.Path {
	case "/prometheus/api/v1/cardinality/estimate":
		wantConsistency := querierapi.ReadConsistencyStrong
		if f.eventualEstimates {
			wantConsistency = ""
		}
		require.Equal(f.t, wantConsistency, r.Header.Get(querierapi.ReadConsistencyHeader))
		f.estimate(w, r)
	case "/prometheus/api/v1/query":
		require.Equal(f.t, querierapi.ReadConsistencyStrong, r.Header.Get(querierapi.ReadConsistencyHeader))
		f.query(w, r)
	case "/prometheus/api/v1/cardinality/label_names":
		f.labelNames(w, r)
	case "/prometheus/api/v1/cardinality/label_values":
		f.labelValuesCardinality(w, r)
	default:
		label, ok := strings.CutPrefix(r.URL.Path, "/prometheus/api/v1/label/")
		if label, ok = strings.CutSuffix(label, "/values"); !ok {
			http.NotFound(w, r)
			return
		}
		require.Equal(f.t, querierapi.ReadConsistencyStrong, r.Header.Get(querierapi.ReadConsistencyHeader))
		f.labelValues(w, r, label)
	}
}

// labelValues answers the label values API from every series, as the index does, with limit.
func (f *fakeFrontend) labelValues(w http.ResponseWriter, r *http.Request, label string) {
	f.labelValueCalls++
	seen := map[string]bool{}
	var values []string
	for _, s := range f.series {
		if v := s.Get(label); v != "" && !seen[v] {
			seen[v] = true
			values = append(values, v)
		}
	}
	slices.Sort(values)
	if l, _ := strconv.Atoi(r.FormValue("limit")); l > 0 && l < len(values) {
		values = values[:l]
	}
	require.NoError(f.t, json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": values}))
}

func (f *fakeFrontend) matching(ms []*labels.Matcher) []labels.Labels {
	var out []labels.Labels
	for _, s := range f.series {
		ok := true
		for _, m := range ms {
			if !m.Matches(s.Get(m.Name)) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, s)
		}
	}
	return out
}

// countBy counts by label the series matching ms that keep keeps.
func (f *fakeFrontend) countBy(ms []*labels.Matcher, label string, keep func(labels.Labels) bool) map[string]int64 {
	out := map[string]int64{}
	for _, s := range f.matching(ms) {
		if keep(s) {
			out[s.Get(label)]++
		}
	}
	return out
}

func parseMS(t *testing.T, s string) int64 {
	v, err := strconv.ParseFloat(s, 64)
	require.NoError(t, err)
	return int64(math.Round(v * 1000))
}

func (f *fakeFrontend) estimate(w http.ResponseWriter, r *http.Request) {
	if r.FormValue("mode") == "bounds" {
		f.bounds(w, r)
		return
	}
	f.estimates++
	if f.estimateMatchHook != nil {
		if status, msg := f.estimateMatchHook(r.FormValue("match[]")); status != 0 {
			writeAPIError(w, status, msg)
			return
		}
	}
	if f.estimateError != nil {
		if status, msg := f.estimateError(); status != 0 {
			writeAPIError(w, status, msg)
			return
		}
	}
	ms, err := promParser.ParseMetricSelector(r.FormValue("match[]"))
	require.NoError(f.t, err)
	res := &estimateResponse{GroupBy: r.FormValue("group_by")}
	res.Window.Start, res.Window.End = parseMS(f.t, r.FormValue("start")), parseMS(f.t, r.FormValue("end"))
	in := func(start, end int64) func(labels.Labels) bool {
		return func(s labels.Labels) bool { return f.chunkOverlaps(s, start, end) }
	}
	for v, c := range f.countBy(ms, res.GroupBy, in(res.Window.Start, res.Window.End)) {
		res.Groups = append(res.Groups, estimateGroup{Value: v, Count: c})
		res.Total += c
	}
	res.GroupCount = len(res.Groups)
	slices.SortFunc(res.Groups, func(a, b estimateGroup) int {
		if a.Count != b.Count {
			return int(b.Count - a.Count)
		}
		if a.Value < b.Value {
			return -1
		}
		return 1
	})
	if l, _ := strconv.Atoi(r.FormValue("limit")); l > 0 && l < len(res.Groups) {
		res.Groups, res.HasMore = res.Groups[:l], true
	}
	if f.estimateHook != nil {
		f.estimateHook(f.estimates, res)
	}
	require.NoError(f.t, json.NewEncoder(w).Encode(res))
}

// writeAPIError answers status with msg in a Prometheus API error body, with the error type the route gives status (writeEstimateErrorStatus in pkg/querier). A msg that is already a JSON body is sent as it is.
func writeAPIError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if strings.HasPrefix(msg, "{") {
		_, _ = fmt.Fprint(w, msg)
		return
	}
	errorType := "internal"
	switch status {
	case http.StatusBadRequest:
		errorType = "bad_data"
	case http.StatusUnprocessableEntity:
		errorType = "execution"
	case http.StatusServiceUnavailable:
		errorType = "timeout"
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "error", "errorType": errorType, "error": msg})
}

// query answers count by (<label>) (last_over_time(<selector>[<range>])), and the edge probe's count by (<label>) (last_over_time(<selector>[<slack>] offset <range>) unless last_over_time(<selector>[<range>])). truthHook gets the first range.
func (f *fakeFrontend) query(w http.ResponseWriter, r *http.Request) {
	q := r.FormValue("query")
	f.queries = append(f.queries, q)
	expr, err := promParser.ParseExpr(q)
	require.NoError(f.t, err)
	agg, ok := expr.(*parser.AggregateExpr)
	require.True(f.t, ok, q)
	require.Equal(f.t, parser.ItemType(parser.COUNT), agg.Op, q)
	lastOverTime := func(e parser.Expr) *parser.MatrixSelector {
		call := e.(*parser.Call)
		require.Equal(f.t, "last_over_time", call.Func.Name, q)
		return call.Args[0].(*parser.MatrixSelector)
	}
	at := parseMS(f.t, r.FormValue("time"))
	// selected is the series a last_over_time(<selector>[<range>] offset <offset>) at the query time keeps.
	selected := func(m *parser.MatrixSelector) map[string]labels.Labels {
		vs := m.VectorSelector.(*parser.VectorSelector)
		to := at - vs.OriginalOffset.Milliseconds()
		out := map[string]labels.Labels{}
		for _, s := range f.matching(vs.LabelMatchers) {
			if f.sampled(s, to-m.Range.Milliseconds(), to) {
				out[s.String()] = s
			}
		}
		return out
	}
	inner := agg.Expr
	var unless map[string]labels.Labels
	if bin, ok := inner.(*parser.BinaryExpr); ok {
		require.Equal(f.t, parser.ItemType(parser.LUNLESS), bin.Op, q)
		inner, unless = bin.LHS, selected(lastOverTime(bin.RHS))
	}
	matrix := lastOverTime(inner)
	label := ""
	if len(agg.Grouping) == 1 {
		label = agg.Grouping[0]
	}
	counts := map[string]int64{}
	for k, s := range selected(matrix) {
		if _, ok := unless[k]; !ok {
			counts[s.Get(label)]++
		}
	}
	if f.truthHook != nil {
		if status := f.truthHook(q, matrix.Range, counts); status != 0 {
			w.WriteHeader(status)
			_, _ = fmt.Fprint(w, `{"status":"error","errorType":"execution","error":"the query exceeded the maximum number of series (limit: 10) (err-mimir-max-series-per-query)"}`)
			return
		}
	}
	type sample struct {
		Metric map[string]string `json:"metric"`
		Value  [2]any            `json:"value"`
	}
	var result []sample
	for v, c := range counts {
		m := map[string]string{}
		if label != "" && v != "" {
			m[label] = v
		}
		result = append(result, sample{Metric: m, Value: [2]any{r.FormValue("time"), strconv.FormatInt(c, 10)}})
	}
	require.NoError(f.t, json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": result}}))
}

func (f *fakeFrontend) labelNames(w http.ResponseWriter, r *http.Request) {
	var ms []*labels.Matcher
	if s := r.FormValue("selector"); s != "" {
		var err error
		ms, err = promParser.ParseMetricSelector(s)
		require.NoError(f.t, err)
	}
	values := map[string]map[string]bool{}
	for _, s := range f.matching(ms) {
		s.Range(func(l labels.Label) {
			if values[l.Name] == nil {
				values[l.Name] = map[string]bool{}
			}
			values[l.Name][l.Value] = true
		})
	}
	var out querierapi.LabelNamesCardinalityResponse
	for n, vs := range values {
		out.Cardinality = append(out.Cardinality, &querierapi.LabelNamesCardinalityItem{LabelName: n, LabelValuesCount: len(vs)})
	}
	slices.SortFunc(out.Cardinality, func(a, b *querierapi.LabelNamesCardinalityItem) int {
		if a.LabelValuesCount != b.LabelValuesCount {
			return b.LabelValuesCount - a.LabelValuesCount
		}
		if a.LabelName < b.LabelName {
			return -1
		}
		return 1
	})
	require.NoError(f.t, json.NewEncoder(w).Encode(out))
}

// testSeries is a small tenant: two jobs, three metrics, one high-cardinality label, and a status label only some http_requests_total series have.
func testSeries() []labels.Labels {
	var out []labels.Labels
	for i := range 6 {
		out = append(out, labels.FromStrings("__name__", "http_requests_total", "job", "api", "pod", fmt.Sprintf("api-%d", i)))
	}
	out = append(out, labels.FromStrings("__name__", "http_requests_total", "job", "api", "pod", "api-0", "status", "500"))
	out = append(out, labels.FromStrings("__name__", "http_requests_total", "job", "api", "pod", "api-1", "status", "404"))
	for i := range 3 {
		out = append(out, labels.FromStrings("__name__", "up", "job", "api", "pod", fmt.Sprintf("api-%d", i)))
		out = append(out, labels.FromStrings("__name__", "up", "job", "db", "pod", fmt.Sprintf("db-%d", i)))
	}
	out = append(out, labels.FromStrings("__name__", "build_info", "version", "1"))
	return out
}

func newTestRunner(srv *httptest.Server) (*runner, *metrics) {
	m := newMetrics(prometheus.NewRegistry(), time.Minute)
	return &runner{
		client:        &client{baseURL: srv.URL, tenant: "test-tenant", http: srv.Client()},
		m:             m,
		logger:        log.NewNopLogger(),
		edgeSlack:     time.Minute,
		fallbackNames: 5,
	}, m
}

var testWindow = window{kind: windowBlocks, start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), end: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC).UnixMilli()}

func metricPair(groupBy string) pair {
	return pair{query: queryMetricByLabel, matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}, groupBy: groupBy, window: testWindow}
}

// bounds answers mode=bounds as the route does: no matchers, the window widened to block ranges of rangeLen, each answered on its own from the series with a chunk in it, and a head row set when the window reaches headFrom.
func (f *fakeFrontend) bounds(w http.ResponseWriter, r *http.Request) {
	f.boundsCalls++
	if f.estimateError != nil {
		if status, msg := f.estimateError(); status != 0 {
			writeAPIError(w, status, msg)
			return
		}
	}
	if sel := r.Form["match[]"]; len(sel) > 0 {
		ms, err := promParser.ParseMetricSelector(sel[0])
		require.NoError(f.t, err)
		if ms = slices.DeleteFunc(ms, func(m *labels.Matcher) bool {
			return m.Name == "__name__" && m.Type == labels.MatchRegexp && m.Value == ".+"
		}); len(ms) > 0 {
			writeAPIError(w, http.StatusBadRequest, "match[] is not allowed in bounds mode")
			return
		}
	}
	groupBy := r.FormValue("group_by")
	limit, _ := strconv.Atoi(r.FormValue("limit"))
	start, end := parseMS(f.t, r.FormValue("start")), parseMS(f.t, r.FormValue("end"))
	res := &boundsResponse{Mode: "bounds", GroupBy: groupBy, Ranges: []boundsRange{}}
	res.Window.Start, res.Window.End = start, end

	length := f.rangeLen.Milliseconds()
	if length == 0 {
		length = 2 * time.Hour.Milliseconds()
	}
	blocksEnd := end
	if f.headFrom > 0 {
		blocksEnd = min(end, f.headFrom)
	}
	for rs := start - start%length; rs < blocksEnd; rs += length {
		rng := boundsRange{Start: rs, End: rs + length, MainSet: true, MainBlocks: 1, Groups: []boundsGroup{}}
		counts := f.countBy(nil, groupBy, func(s labels.Labels) bool { return f.chunkOverlaps(s, rs, rs+length) })
		for v, c := range counts {
			g := boundsGroup{Value: v, Lower: max(c-f.boundsSlack, 0), Upper: c + f.boundsSlack}
			rng.LowerTotal += g.Lower
			rng.UpperTotal += g.Upper
			rng.Groups = append(rng.Groups, g)
		}
		rng.GroupCount = len(rng.Groups)
		slices.SortFunc(rng.Groups, func(a, b boundsGroup) int {
			if a.Upper != b.Upper {
				return int(b.Upper - a.Upper)
			}
			return strings.Compare(a.Value, b.Value)
		})
		if limit > 0 && limit < len(rng.Groups) {
			rng.Groups, rng.HasMore = rng.Groups[:limit], true
		}
		res.Ranges = append(res.Ranges, rng)
	}
	if f.headFrom > 0 && end > f.headFrom {
		res.NotCovered = append(res.NotCovered, struct {
			Start int64 `json:"start"`
			End   int64 `json:"end"`
		}{max(start, f.headFrom), end})
		head := &boundsHead{Groups: []boundsHeadGroup{}}
		counts := f.countBy(nil, groupBy, func(s labels.Labels) bool { return f.chunkOverlaps(s, max(start, f.headFrom), end) })
		for v, c := range counts {
			head.InMemoryTotal += c
			head.ActiveTotal += c
			head.Groups = append(head.Groups, boundsHeadGroup{Value: v, InMemory: c, Active: c})
		}
		head.GroupCount = len(head.Groups)
		slices.SortFunc(head.Groups, func(a, b boundsHeadGroup) int {
			if a.InMemory != b.InMemory {
				return int(b.InMemory - a.InMemory)
			}
			return strings.Compare(a.Value, b.Value)
		})
		if limit > 0 && limit < len(head.Groups) {
			head.Groups, head.HasMore = head.Groups[:limit], true
		}
		res.Head = head
	}
	if f.boundsHook != nil {
		f.boundsHook(f.boundsCalls, res)
	}
	require.NoError(f.t, json.NewEncoder(w).Encode(res))
}

// labelValuesCardinality answers the label values cardinality endpoint from every series, as the ingesters' heads would, with the tenant's cap on limit.
func (f *fakeFrontend) labelValuesCardinality(w http.ResponseWriter, r *http.Request) {
	label := r.FormValue("label_names[]")
	maxLimit := cmp.Or(f.labelValuesCardinalityLimit, 500)
	limit, err := strconv.Atoi(r.FormValue("limit"))
	if err != nil || limit > maxLimit {
		http.Error(w, fmt.Sprintf("'limit' param cannot be greater than '%d'", maxLimit), http.StatusBadRequest)
		return
	}
	counts := f.countBy(nil, label, func(labels.Labels) bool { return true })
	delete(counts, "")
	item := querierapi.LabelNamesCardinality{LabelName: label, LabelValuesCount: uint64(len(counts))}
	for v, c := range counts {
		item.SeriesCount += uint64(c)
		item.Cardinality = append(item.Cardinality, querierapi.LabelValuesCardinality{LabelValue: v, SeriesCount: uint64(c)})
	}
	slices.SortFunc(item.Cardinality, func(a, b querierapi.LabelValuesCardinality) int {
		if a.SeriesCount != b.SeriesCount {
			return int(b.SeriesCount) - int(a.SeriesCount)
		}
		return strings.Compare(a.LabelValue, b.LabelValue)
	})
	item.Cardinality = item.Cardinality[:min(limit, len(item.Cardinality))]
	require.NoError(f.t, json.NewEncoder(w).Encode(querierapi.LabelValuesCardinalityResponse{SeriesCountTotal: uint64(len(f.series)), Labels: []querierapi.LabelNamesCardinality{item}}))
}

func testTargets() targetConfig {
	return targetConfig{ranks: []int{1, 2, 3}, labels: 3, matcherLabel: "job", source: targetSourceBounds, labelValuesLimit: 500}
}

// testTargetWindows puts the block window and the recent window on the test window.
var testTargetWindows = targetWindows{blocks: testWindow, recent: testWindow}
