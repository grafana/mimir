// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

var promParser = promqlext.NewPromQLParser()

// fakeFrontend answers the estimate, PromQL and label names requests from one fixed set of series, every series present over every window.
type fakeFrontend struct {
	t      *testing.T
	series []labels.Labels

	mu        sync.Mutex
	estimates int
	queries   []string
	// estimateHook, when set, edits the estimate's answer; n counts the estimate calls so far, from 1.
	estimateHook func(n int, r *estimateResponse)
	// estimateError, when set, is sent instead of the estimate.
	estimateError func() (int, string)
	// truthHook, when set, edits the counts of one PromQL query, or refuses it with a status.
	truthHook func(query string, rangeLen time.Duration, counts map[string]int64) (status int)
}

func newFakeFrontend(t *testing.T, series ...labels.Labels) (*fakeFrontend, *httptest.Server) {
	f := &fakeFrontend{t: t, series: series}
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
		f.estimate(w, r)
	case "/prometheus/api/v1/query":
		f.query(w, r)
	case "/prometheus/api/v1/cardinality/label_names":
		f.labelNames(w, r)
	default:
		http.NotFound(w, r)
	}
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

func (f *fakeFrontend) countBy(ms []*labels.Matcher, label string) map[string]int64 {
	out := map[string]int64{}
	for _, s := range f.matching(ms) {
		out[s.Get(label)]++
	}
	return out
}

func parseMS(t *testing.T, s string) int64 {
	v, err := strconv.ParseFloat(s, 64)
	require.NoError(t, err)
	return int64(math.Round(v * 1000))
}

func (f *fakeFrontend) estimate(w http.ResponseWriter, r *http.Request) {
	f.estimates++
	if f.estimateError != nil {
		status, body := f.estimateError()
		http.Error(w, body, status)
		return
	}
	ms, err := promParser.ParseMetricSelector(r.FormValue("match[]"))
	require.NoError(f.t, err)
	res := &estimateResponse{GroupBy: r.FormValue("group_by")}
	res.Window.Start, res.Window.End = parseMS(f.t, r.FormValue("start")), parseMS(f.t, r.FormValue("end"))
	buckets := 0
	if s := r.FormValue("step"); s != "" {
		d, err := model.ParseDuration(s)
		require.NoError(f.t, err)
		res.StepMS = time.Duration(d).Milliseconds()
		buckets = int((res.Window.End - res.Window.Start) / res.StepMS)
	}
	for v, c := range f.countBy(ms, res.GroupBy) {
		g := estimateGroup{Value: v, Count: c}
		if buckets > 0 {
			for range buckets {
				g.Counts = append(g.Counts, c)
			}
		}
		res.Groups = append(res.Groups, g)
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

// query answers count by (<label>) (last_over_time(<selector>[<range>])).
func (f *fakeFrontend) query(w http.ResponseWriter, r *http.Request) {
	q := r.FormValue("query")
	f.queries = append(f.queries, q)
	expr, err := promParser.ParseExpr(q)
	require.NoError(f.t, err)
	agg, ok := expr.(*parser.AggregateExpr)
	require.True(f.t, ok, q)
	require.Equal(f.t, parser.ItemType(parser.COUNT), agg.Op, q)
	call := agg.Expr.(*parser.Call)
	require.Equal(f.t, "last_over_time", call.Func.Name, q)
	matrix := call.Args[0].(*parser.MatrixSelector)
	vs := matrix.VectorSelector.(*parser.VectorSelector)
	label := ""
	if len(agg.Grouping) == 1 {
		label = agg.Grouping[0]
	}
	counts := map[string]int64{}
	if label == "" {
		if n := len(f.matching(vs.LabelMatchers)); n > 0 {
			counts[""] = int64(n)
		}
	} else {
		counts = f.countBy(vs.LabelMatchers, label)
	}
	if f.truthHook != nil {
		if status := f.truthHook(q, matrix.Range, counts); status != 0 {
			w.WriteHeader(status)
			_, _ = fmt.Fprint(w, `{"status":"error","errorType":"execution","error":"the query exceeded the maximum number of series (limit: 10)"}`)
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

// testSeries is a small tenant: two jobs, three metrics, one high-cardinality label.
func testSeries() []labels.Labels {
	var out []labels.Labels
	for i := range 6 {
		out = append(out, labels.FromStrings("__name__", "http_requests_total", "job", "api", "pod", fmt.Sprintf("api-%d", i)))
	}
	for i := range 3 {
		out = append(out, labels.FromStrings("__name__", "up", "job", "api", "pod", fmt.Sprintf("api-%d", i)))
		out = append(out, labels.FromStrings("__name__", "up", "job", "db", "pod", fmt.Sprintf("db-%d", i)))
	}
	out = append(out, labels.FromStrings("__name__", "build_info", "version", "1"))
	return out
}

func newTestRunner(srv *httptest.Server) (*runner, *metrics) {
	m := newMetrics(prometheus.NewRegistry())
	return &runner{
		client:        &client{baseURL: srv.URL, tenant: "test-tenant", http: srv.Client()},
		cfg:           classifyConfig{sketchSigmas: 3},
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
