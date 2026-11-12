// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSketchStdError(t *testing.T) {
	// p=11: 1.04/sqrt(2048).
	assert.InDelta(t, 0.02298, sketchStdError, 0.00001)
}

func TestCompareAnswers(t *testing.T) {
	cfg := classifyConfig{sketchSigmas: 3}
	truth := answer{counts: map[string]int64{"a": 1000, "b": 10, "c": 5}}
	tests := map[string]struct {
		resp      estimateResponse
		est       map[string]int64
		estimated map[string]bool
		want      []groupDiff
	}{
		"exact": {
			est: map[string]int64{"a": 1000, "b": 10, "c": 5},
		},
		"sketch within the bound, either sign": {
			est:       map[string]int64{"a": 1060, "b": 10, "c": 5},
			estimated: map[string]bool{"a": true},
			want:      []groupDiff{{Value: "a", Estimate: 1060, Truth: 1000, Estimated: true, Outcome: outcomeTolerated, Class: classSketch}},
		},
		"sketch outside the bound": {
			est:       map[string]int64{"a": 1100, "b": 10, "c": 5},
			estimated: map[string]bool{"a": true},
			want:      []groupDiff{{Value: "a", Estimate: 1100, Truth: 1000, Estimated: true, Outcome: outcomeMismatch, Class: classUnclassified}},
		},
		"an exact group off by one": {
			est:  map[string]int64{"a": 1000, "b": 11, "c": 5},
			want: []groupDiff{{Value: "b", Estimate: 11, Truth: 10, Outcome: outcomeMismatch, Class: classUnclassified}},
		},
		"lower bound below the truth, and a group missing": {
			resp: estimateResponse{LowerBound: true},
			est:  map[string]int64{"a": 900, "b": 10},
			want: []groupDiff{
				{Value: "a", Estimate: 900, Truth: 1000, Outcome: outcomeTolerated, Class: classIncomplete},
				{Value: "c", Estimate: 0, Truth: 5, Outcome: outcomeTolerated, Class: classIncomplete},
			},
		},
		"lower bound above the truth": {
			resp: estimateResponse{LowerBound: true},
			est:  map[string]int64{"a": 1001, "b": 10, "c": 5},
			want: []groupDiff{{Value: "a", Estimate: 1001, Truth: 1000, Outcome: outcomeMismatch, Class: classUnclassified}},
		},
		"error trailer is incomplete": {
			resp: estimateResponse{Error: "store-gateway lost"},
			est:  map[string]int64{"a": 1000},
			want: []groupDiff{
				{Value: "b", Truth: 10, Outcome: outcomeTolerated, Class: classIncomplete},
				{Value: "c", Truth: 5, Outcome: outcomeTolerated, Class: classIncomplete},
			},
		},
		"head whole above the truth": {
			resp: estimateResponse{HeadWhole: true},
			est:  map[string]int64{"a": 1200, "b": 10, "c": 5},
			want: []groupDiff{{Value: "a", Estimate: 1200, Truth: 1000, Outcome: outcomeTolerated, Class: classHeadWhole}},
		},
		"head whole below the truth": {
			resp: estimateResponse{HeadWhole: true},
			est:  map[string]int64{"a": 999, "b": 10, "c": 5},
			want: []groupDiff{{Value: "a", Estimate: 999, Truth: 1000, Outcome: outcomeMismatch, Class: classUnclassified}},
		},
		"has_more ignores groups left out of the table": {
			resp: estimateResponse{HasMore: true, GroupCount: 3, Total: 1015},
			est:  map[string]int64{"a": 1000},
		},
		"has_more compares group_count and total": {
			resp: estimateResponse{HasMore: true, GroupCount: 4, Total: 1015},
			est:  map[string]int64{"a": 1000},
			want: []groupDiff{{Value: groupCountKey, Estimate: 4, Truth: 3, Outcome: outcomeMismatch, Class: classUnclassified}},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			est := answer{counts: tc.est, estimated: tc.estimated}
			if est.estimated == nil {
				est.estimated = map[string]bool{}
			}
			assert.Equal(t, tc.want, compareAnswers(&tc.resp, est, truth, nil, cfg))
		})
	}
}

func TestStreamedResponseWithErrorTrailer(t *testing.T) {
	// The streamed name table: head, groups, then a trailer that sets lower_bound and error.
	body := `{"window":{"start":0,"end":3600000},"group_by":"__name__","read":"index_header","source":"store_gateways","lower_bound":false,"has_more":false,"dedup":true,"group_count":0,"total":0,"estimated_groups":0,"groups":[{"value":"a","count":5}],"lower_bound":true,"has_more":false,"group_count":1,"total":5,"error":"merging: store-gateway lost"}`
	var r estimateResponse
	require.NoError(t, json.Unmarshal([]byte(body), &r))
	assert.True(t, r.incomplete())
	diffs := compareAnswers(&r, estimateAnswer(&r), answer{counts: map[string]int64{"a": 5, "b": 2}}, nil, classifyConfig{sketchSigmas: 3})
	assert.Equal(t, []groupDiff{{Value: "b", Truth: 2, Outcome: outcomeTolerated, Class: classIncomplete}}, diffs)
}

func TestEstimateAnswerBuckets(t *testing.T) {
	r := estimateResponse{StepMS: 1, Groups: []estimateGroup{{Value: "a", Count: 3, Counts: []int64{1, 3}}}}
	assert.Equal(t, map[string]int64{"a@0": 1, "a@1": 3}, estimateAnswer(&r).counts)
}

func TestWorst(t *testing.T) {
	o, c := worst(nil)
	assert.Equal(t, []string{outcomePass, classExact}, []string{o, c})
	o, c = worst([]groupDiff{{Outcome: outcomeTolerated, Class: classSketch}, {Outcome: outcomeMismatch, Class: classUnclassified}, {Outcome: outcomeTolerated, Class: classEdge}})
	assert.Equal(t, []string{outcomeMismatch, classUnclassified}, []string{o, c})
	o, c = worst([]groupDiff{{Outcome: outcomeTolerated, Class: classSketch}, {Outcome: outcomeTolerated, Class: classEdge}})
	assert.Equal(t, []string{outcomeTolerated, classEdge}, []string{o, c})
}

func TestTruthQuery(t *testing.T) {
	ms := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"), labels.MustNewMatcher(labels.MatchEqual, "job", "api")}
	assert.Equal(t, `count by (pod) (last_over_time({__name__="up", job="api"}[1h]))`, truthQuery(ms, "pod", time.Hour))
	assert.Equal(t, `count by ("k8s.pod") (last_over_time({__name__="up", job="api"}[1h1m]))`, truthQuery(ms, "k8s.pod", time.Hour+time.Minute))
}

func TestWindows(t *testing.T) {
	now := time.Date(2026, 1, 2, 12, 0, 30, 0, time.UTC)
	cfg := windowConfig{lengths: []time.Duration{time.Hour, 12 * time.Hour}, queryIngestersWithin: 6 * time.Hour, lag: 5 * time.Minute, blocksMargin: time.Hour}
	at := func(h, m int) int64 { return time.Date(2026, 1, 2, h, m, 0, 0, time.UTC).UnixMilli() }
	assert.Equal(t, []window{
		{windowBlocks, at(4, 0), at(5, 0)},
		{windowIngesters, at(10, 55), at(11, 55)},
		{windowBoundary, at(5, 30), at(6, 30)},
		// 12h fits neither in the ingesters' 6h nor across the boundary before now.
		{windowBlocks, at(5, 0) - 12*time.Hour.Milliseconds(), at(5, 0)},
	}, cfg.windows(now))
}

func pairsCount(m *metrics, query, outcome, class string) float64 {
	total := 0.0
	for _, w := range []string{windowBlocks, windowIngesters, windowBoundary} {
		for _, o := range []string{"estimate_first", "promql_first"} {
			total += testutil.ToFloat64(m.pairs.WithLabelValues(query, w, o, outcome, class))
		}
	}
	return total
}

func TestRunnerExactPair(t *testing.T) {
	_, srv := newFakeFrontend(t, testSeries()...)
	r, m := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomePass, res.Outcome)
	assert.Equal(t, "estimate_first", res.Order)
	// The order alternates.
	assert.Equal(t, "promql_first", r.run(context.Background(), metricPair("pod")).Order)
	assert.Equal(t, 2.0, pairsCount(m, queryMetricByLabel, outcomePass, classExact))
	// Each side was once cold and once warm.
	assert.Equal(t, 4, testutil.CollectAndCount(m.duration, "cardinality_estimate_compare_request_duration_seconds"))
}

func TestRunnerTruthAtWindowEndMinusOneMillisecond(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	var times []string
	f.truthHook = func(string, time.Duration, map[string]int64) int { return 0 }
	r, _ := newTestRunner(srv)
	orig := r.client.http.Transport
	r.client.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/prometheus/api/v1/query" {
			times = append(times, req.URL.Query().Get("time"))
		}
		return orig.RoundTrip(req)
	})
	r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, []string{formatTime(testWindow.end - 1)}, times)
}

func TestRunnerAnswerChanged(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.estimateHook = func(n int, r *estimateResponse) {
		if n == 1 {
			r.Groups[0].Count++
		}
	}
	r, _ := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeTolerated, res.Outcome)
	assert.Equal(t, classAnswerChanged, res.Class)
	assert.Equal(t, 2, f.estimates, "a mismatching pair is rerun once")
}

func TestRunnerStillMismatchingOnRerun(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	// Wrong on both runs, by a different amount each time.
	f.estimateHook = func(n int, r *estimateResponse) { r.Groups[0].Count += int64(n) }
	r, _ := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeMismatch, res.Outcome)
	assert.Equal(t, classUnclassified, res.Class)
	require.Len(t, res.Diffs, 1)
	assert.Equal(t, int64(2), res.Diffs[0].Estimate-res.Diffs[0].Truth, "the rerun's counts are reported: the first run was off by 1, the rerun by 2")
}

func TestRunnerEmpty(t *testing.T) {
	_, srv := newFakeFrontend(t, testSeries()...)
	r, _ := newTestRunner(srv)
	p := metricPair("pod")
	p.matchers = []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "absent")}
	res := r.run(context.Background(), p)
	assert.Equal(t, outcomePass, res.Outcome)
	assert.Equal(t, classEmpty, res.Class)
}

func TestRunnerEdgeProbe(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	// One db pod's samples are just outside the window: the exact window misses it, the widened one sees it.
	f.truthHook = func(_ string, rng time.Duration, counts map[string]int64) int {
		if rng == time.Hour {
			delete(counts, "db-0")
		}
		return 0
	}
	r, _ := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeTolerated, res.Outcome)
	assert.Equal(t, classEdge, res.Class)
	require.Len(t, res.Diffs, 1)
	assert.Equal(t, groupDiff{Value: "db-0", Estimate: 1, Truth: 0, Outcome: outcomeTolerated, Class: classEdge}, res.Diffs[0])
}

func TestRunnerUnexplainedMismatch(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.estimateHook = func(_ int, r *estimateResponse) { r.Groups[0].Count += 5 }
	r, m := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeMismatch, res.Outcome)
	assert.Equal(t, classUnclassified, res.Class)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.groups.WithLabelValues(queryMetricByLabel, outcomeMismatch, classUnclassified)))
}

func TestRunnerNoCountRPC(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.estimateError = func() (int, string) {
		return http.StatusUnprocessableEntity, "a store-gateway does not support cardinality estimates yet, it may run an older version"
	}
	r, m := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeRefusal, res.Outcome)
	assert.Equal(t, classNoCountRPC, res.Class)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.refusals.WithLabelValues(queryMetricByLabel, sideEstimate, classNoCountRPC)))
}

func TestRunnerEstimateRefused(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.estimateError = func() (int, string) {
		return http.StatusUnprocessableEntity, "the estimate would read more than max_index_bytes"
	}
	r, _ := newTestRunner(srv)
	assert.Equal(t, classEstimateRefused, r.run(context.Background(), metricPair("pod")).Class)
}

func TestRunnerPromQLLimitFallsBackToOneNameAtATime(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.truthHook = func(q string, _ time.Duration, _ map[string]int64) int {
		if strings.Contains(q, `=~".+"`) {
			return http.StatusUnprocessableEntity
		}
		return 0
	}
	r, m := newTestRunner(srv)
	r.fallbackNames = 2
	p := pair{query: queryTenantByName, matchers: allSeriesMatchers(), groupBy: "__name__", window: testWindow}
	res := r.run(context.Background(), p)
	assert.Equal(t, outcomePass, res.Outcome)
	assert.Equal(t, []string{"promql:" + classPromQLLimit}, res.Refusals)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.refusals.WithLabelValues(queryTenantByName, sideTruth, classPromQLLimit)))
	// The two largest names, counted one at a time; the name replaces the selector's name matcher.
	assert.Equal(t, []string{
		`count by (__name__) (last_over_time({__name__=~".+"}[1h]))`,
		`count by (__name__) (last_over_time({__name__="http_requests_total"}[1h]))`,
		`count by (__name__) (last_over_time({__name__="up"}[1h]))`,
	}, f.queries)
}

func TestRunnerPromQLLimitWithoutFallback(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.truthHook = func(string, time.Duration, map[string]int64) int { return http.StatusUnprocessableEntity }
	r, _ := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeRefusal, res.Outcome)
	assert.Equal(t, classPromQLLimit, res.Class)
}

func TestRunnerWindowEcho(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.estimateHook = func(_ int, r *estimateResponse) { r.Window.Start -= time.Hour.Milliseconds() }
	r, _ := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeMismatch, res.Outcome)
	assert.Equal(t, classWindowEcho, res.Class)
}

func TestRunnerSteps(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	r, _ := newTestRunner(srv)
	p := pair{query: queryMetricSteps, matchers: metricPair("").matchers, groupBy: "__name__", window: testWindow, steps: 4}
	res := r.run(context.Background(), p)
	assert.Equal(t, outcomePass, res.Outcome, "%+v", res.Diffs)
	assert.Len(t, f.queries, 4)
	assert.Equal(t, `count by (__name__) (last_over_time({__name__="up"}[15m]))`, f.queries[0])

	// A bucket off by one is a mismatch on that bucket only.
	f.estimateHook = func(_ int, r *estimateResponse) { r.Groups[0].Counts[2]++ }
	res = r.run(context.Background(), p)
	assert.Equal(t, outcomeMismatch, res.Outcome)
	require.Len(t, res.Diffs, 1)
	assert.Equal(t, "up@2", res.Diffs[0].Value)
}

// TestRunOneRound runs a whole round against an honest fake: every query shape on every window agrees.
func TestRunOneRound(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	cfg := config{
		address: srv.URL, tenant: "test-tenant", pairsPerRound: 1000, timeout: time.Minute,
		queries:  allQueries,
		windows:  windowConfig{lengths: []time.Duration{time.Hour}, queryIngestersWithin: 6 * time.Hour, lag: 5 * time.Minute, blocksMargin: time.Hour},
		targets:  targetConfig{names: 3, labels: 3, matcherLabel: "job"},
		pairs:    pairConfig{steps: 4, tableLimit: 2},
		classify: classifyConfig{sketchSigmas: 3}, edgeSlack: time.Minute, fallbackNames: 2, seed: 1,
	}
	now := func() time.Time { return time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC) }
	mismatched, err := run(context.Background(), cfg, m, log.NewNopLogger(), now)
	require.NoError(t, err)
	assert.False(t, mismatched)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.rounds.WithLabelValues("ok")))
	for _, q := range allQueries {
		assert.Equal(t, 3.0, pairsCount(m, q, outcomePass, classExact), q)
	}
	assert.NotEmpty(t, f.queries)

	// A broken estimate makes the round report a mismatch.
	f.estimateHook = func(n int, r *estimateResponse) {
		if len(r.Groups) > 0 && n > 2 { // after the two target-picking calls
			r.Groups[0].Count += 100
		}
	}
	cfg.queries = []string{queryMetricTotal}
	mismatched, err = run(context.Background(), cfg, newMetrics(prometheus.NewRegistry()), log.NewNopLogger(), now)
	require.NoError(t, err)
	assert.True(t, mismatched)
}

func TestConfigValidate(t *testing.T) {
	cfg := config{address: "http://x", tenant: "t", pairsPerRound: 1, queries: []string{"nope"}, windowLengths: []string{"1h"}}
	require.ErrorContains(t, cfg.validate(), "unknown query")
	cfg.queries = allQueries
	require.NoError(t, cfg.validate())
	assert.Equal(t, []time.Duration{time.Hour}, cfg.windows.lengths)
	cfg.tenant = ""
	require.ErrorContains(t, cfg.validate(), "required")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
