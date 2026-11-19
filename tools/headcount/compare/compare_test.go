// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompareAnswers(t *testing.T) {
	truth := answer{counts: map[string]int64{"a": 100000, "b": 10, "c": 5}}
	tests := map[string]struct {
		resp estimateResponse
		est  map[string]int64
		want []groupDiff
	}{
		"exact": {
			est: map[string]int64{"a": 100000, "b": 10, "c": 5},
		},
		"an exact group off by one": {
			est:  map[string]int64{"a": 100000, "b": 11, "c": 5},
			want: []groupDiff{{Value: "b", Estimate: 11, Truth: 10, Outcome: outcomeMismatch, Class: classUnclassified}},
		},
		"lower bound below the truth, and a group missing": {
			resp: estimateResponse{LowerBound: true},
			est:  map[string]int64{"a": 90000, "b": 10},
			want: []groupDiff{
				{Value: "a", Estimate: 90000, Truth: 100000, Outcome: outcomeFlagged, Class: classIncomplete},
				{Value: "c", Estimate: 0, Truth: 5, Outcome: outcomeFlagged, Class: classIncomplete},
			},
		},
		"lower bound above the truth": {
			resp: estimateResponse{LowerBound: true},
			est:  map[string]int64{"a": 100001, "b": 10, "c": 5},
			want: []groupDiff{{Value: "a", Estimate: 100001, Truth: 100000, Outcome: outcomeMismatch, Class: classUnclassified}},
		},
		"upper bound above the truth": {
			resp: estimateResponse{UpperBound: true},
			est:  map[string]int64{"a": 100003, "b": 10, "c": 5},
			want: []groupDiff{{Value: "a", Estimate: 100003, Truth: 100000, Outcome: outcomeFlagged, Class: classUpperBound}},
		},
		"upper bound below the truth": {
			resp: estimateResponse{UpperBound: true},
			est:  map[string]int64{"a": 99999, "b": 10, "c": 5},
			want: []groupDiff{{Value: "a", Estimate: 99999, Truth: 100000, Outcome: outcomeMismatch, Class: classUnclassified}},
		},
		"has_more ignores groups left out of the table": {
			resp: estimateResponse{HasMore: true, GroupCount: 3, Total: 100015},
			est:  map[string]int64{"a": 100000},
		},
		"has_more compares group_count and total": {
			resp: estimateResponse{HasMore: true, GroupCount: 4, Total: 100015},
			est:  map[string]int64{"a": 100000},
			want: []groupDiff{{Value: groupCountKey, Estimate: 4, Truth: 3, Outcome: outcomeMismatch, Class: classUnclassified}},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, compareAnswers(&tc.resp, answer{counts: tc.est}, truth, nil))
		})
	}
}

// TestRunnerIncompleteBound: an incomplete answer below the truth is flagged while it is not below a truth over the part of the window it says it covered, and a mismatch below that.
func TestRunnerIncompleteBound(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.estimateHook = func(_ int, r *estimateResponse) {
		// The ingesters hold the last half hour of the window.
		r.LowerBound, r.HeadMinTime = true, testWindow.start+30*time.Minute.Milliseconds()
		for i := range r.Groups {
			if r.Groups[i].Value == "db-0" {
				r.Groups[i].Count--
			}
		}
	}
	// Over the covered half hour, db-0 has one series fewer than over the window.
	var ranges []time.Duration
	f.truthHook = func(_ string, rng time.Duration, counts map[string]int64) int {
		ranges = append(ranges, rng)
		if rng == 30*time.Minute {
			counts["db-0"]--
		}
		return 0
	}
	r, _ := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeFlagged, res.Outcome)
	assert.Equal(t, classIncomplete, res.Class)
	assert.Contains(t, ranges, 30*time.Minute)

	f.truthHook = func(string, time.Duration, map[string]int64) int { return 0 }
	res = r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeMismatch, res.Outcome)
	assert.Equal(t, classIncompleteUnder, res.Class)
}

func TestCovered(t *testing.T) {
	const start, end = 0, 100
	tests := map[string]struct {
		resp     estimateResponse
		from, to int64
		ok       bool
	}{
		"head_min_time":                    {resp: estimateResponse{LowerBound: true, HeadMinTime: 40}, from: 40, to: 100, ok: true},
		"head_min_time with a gap":         {resp: estimateResponse{LowerBound: true, CoverageGap: true, HeadMinTime: 40, MissingBlocks: []string{"b"}}, from: 40, to: 100, ok: true},
		"head_min_time at the end":         {resp: estimateResponse{LowerBound: true, HeadMinTime: 100}},
		"missing block":                    {resp: estimateResponse{LowerBound: true, MissingBlocks: []string{"b"}}},
		"coverage gap":                     {resp: estimateResponse{LowerBound: true, CoverageGap: true}},
		"missing shards, longest part":     {resp: estimateResponse{LowerBound: true, MissingShards: []missingShard{{Start: 10, End: 30}, {Start: 60, End: 70}}}, from: 30, to: 60, ok: true},
		"missing shard over every part":    {resp: estimateResponse{LowerBound: true, MissingShards: []missingShard{{Start: -10, End: 200}}}},
		"missing shard outside the window": {resp: estimateResponse{LowerBound: true, MissingShards: []missingShard{{Start: 100, End: 200}}}, from: 0, to: 100, ok: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			from, to, ok := tc.resp.covered(start, end)
			require.Equal(t, tc.ok, ok)
			if ok {
				assert.Equal(t, [2]int64{tc.from, tc.to}, [2]int64{from, to})
			}
		})
	}
}

func TestWorst(t *testing.T) {
	o, c := worst(nil)
	assert.Equal(t, []string{outcomePass, classExact}, []string{o, c})
	o, c = worst([]groupDiff{{Outcome: outcomeTolerated, Class: classAnswerChanged}, {Outcome: outcomeMismatch, Class: classUnclassified}, {Outcome: outcomeTolerated, Class: classEdge}})
	assert.Equal(t, []string{outcomeMismatch, classUnclassified}, []string{o, c})
	o, c = worst([]groupDiff{{Outcome: outcomeTolerated, Class: classAnswerChanged}, {Outcome: outcomeTolerated, Class: classEdge}})
	assert.Equal(t, []string{outcomeTolerated, classAnswerChanged}, []string{o, c}, "the first class by name")
}

func TestTruthQuery(t *testing.T) {
	ms := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"), labels.MustNewMatcher(labels.MatchEqual, "job", "api")}
	assert.Equal(t, `count by (pod) (last_over_time({__name__="up", job="api"}[1h]))`, truthQuery(ms, "pod", time.Hour))
	assert.Equal(t, `count by ("k8s.pod") (last_over_time({__name__="up", job="api"}[1h1m]))`, truthQuery(ms, "k8s.pod", time.Hour+time.Minute))
}

func TestWindows(t *testing.T) {
	now := time.Date(2026, 1, 2, 12, 0, 30, 0, time.UTC)
	cfg := windowConfig{lengths: []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour}, queryIngestersWithin: 6 * time.Hour, lag: 5 * time.Minute, blocksMargin: time.Hour}
	at := func(h, m int) int64 { return time.Date(2026, 1, 2, h, m, 0, 0, time.UTC).UnixMilli() }
	assert.Equal(t, []window{
		{windowBlocks, at(4, 0), at(5, 0)},
		{windowIngesters, at(10, 55), at(11, 55)},
		{windowBoundary, at(5, 30), at(6, 30)},
		// 6h starts at 05:55, before the boundary plus the lag: a recent window, not an ingester one.
		{windowBlocks, at(5, 0) - 6*time.Hour.Milliseconds(), at(5, 0)},
		{windowRecent, at(5, 55), at(11, 55)},
		{windowBoundary, at(3, 0), at(9, 0)},
		// 24h: the last day, 6h from the ingesters and 18h from the store-gateways. Across the boundary it would end in the future.
		{windowBlocks, at(5, 0) - 24*time.Hour.Milliseconds(), at(5, 0)},
		{windowRecent, at(11, 55) - 24*time.Hour.Milliseconds(), at(11, 55)},
	}, cfg.windows(now))
}

func pairsCount(m *metrics, query, outcome, class string) float64 {
	return counterSum(m.pairs, map[string]string{"query": query, "outcome": outcome, "class": class})
}

// counterSum sums the series of c whose labels have the given values: a counter's value, or a histogram's sample count.
func counterSum(c prometheus.Collector, want map[string]string) float64 {
	ch := make(chan prometheus.Metric)
	go func() { c.Collect(ch); close(ch) }()
	total := 0.0
	for m := range ch {
		var pb dto.Metric
		if err := m.Write(&pb); err != nil {
			panic(err)
		}
		matched := 0
		for _, l := range pb.GetLabel() {
			if v, ok := want[l.GetName()]; ok && v == l.GetValue() {
				matched++
			}
		}
		if matched == len(want) {
			total += pb.GetCounter().GetValue() + float64(pb.GetHistogram().GetSampleCount())
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

func TestLatencyBuckets(t *testing.T) {
	b := latencyBuckets(2 * time.Minute)
	assert.Greater(t, b[len(b)-1], 120.0)
	assert.LessOrEqual(t, b[len(b)-2], 120.0)
}

// TestRunnerLatencyOncePerSide: each side that answered is observed once per pair, its queries summed, and a side that failed is not observed.
func TestRunnerLatencyOncePerSide(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.estimateHook = func(_ int, r *estimateResponse) { r.Read, r.Source = "index_header", "blocks" }
	r, m := newTestRunner(srv)
	p := pair{query: queryTenantByName, matchers: allSeriesMatchers(), groupBy: "__name__", window: testWindow, perValue: true}
	r.run(context.Background(), p)
	assert.Equal(t, 2, testutil.CollectAndCount(m.duration))
	assert.Equal(t, 1.0, counterSum(m.pairs, map[string]string{"read": "index_header", "source": "blocks", "outcome": outcomePass}))
	ch := make(chan prometheus.Metric, 10)
	m.duration.WithLabelValues(queryTenantByName, windowBlocks, "1h", sideTruth, positionSecond, referencePerValue, "index_header", "blocks").(prometheus.Histogram).Collect(ch)
	var pb dto.Metric
	require.NoError(t, (<-ch).Write(&pb))
	assert.Equal(t, uint64(1), pb.GetHistogram().GetSampleCount(), "one query per name, one observation")

	m = newMetrics(prometheus.NewRegistry(), time.Minute)
	r.m = m
	f.truthHook = func(string, time.Duration, map[string]int64) int { return http.StatusTooManyRequests }
	r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, 1, testutil.CollectAndCount(m.duration), "only the estimate answered")
}

func TestRunnerFlags(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.estimateHook = func(_ int, r *estimateResponse) { r.LowerBound, r.Dedup = true, true }
	r, m := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomePass, res.Outcome)
	assert.Equal(t, []string{"lower_bound", "dedup"}, res.Flags)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.flags.WithLabelValues(queryMetricByLabel, "lower_bound")))
}

// TestRunnerCanceledPairIsNotRecorded: a pair cut short by shutdown records nothing.
func TestRunnerCanceledPairIsNotRecorded(t *testing.T) {
	_, srv := newFakeFrontend(t, testSeries()...)
	r, m := newTestRunner(srv)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res := r.run(ctx, metricPair("pod"))
	assert.Equal(t, outcomeRefusal, res.Outcome)
	assert.Zero(t, testutil.CollectAndCount(m.pairs))
	assert.Zero(t, testutil.CollectAndCount(m.refusals))
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
	// The data changes between the runs: the first truth misses a series the estimate and the rerun both see.
	truths := 0
	f.truthHook = func(_ string, rng time.Duration, counts map[string]int64) int {
		if rng == time.Hour {
			if truths++; truths == 1 {
				counts["db-0"]--
			}
		}
		return 0
	}
	r, _ := newTestRunner(srv)
	r.edgeSlack = 0
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeTolerated, res.Outcome)
	assert.Equal(t, classAnswerChanged, res.Class)
	assert.Equal(t, 2, f.estimates, "a mismatching pair is rerun once")
}

// TestRunnerEstimateUnstable: when only the estimate changed between the runs, the estimate isn't deterministic.
func TestRunnerEstimateUnstable(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.estimateHook = func(n int, r *estimateResponse) {
		if n == 1 {
			r.Groups[0].Count++
		}
	}
	r, _ := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeMismatch, res.Outcome)
	assert.Equal(t, classUnstable, res.Class)
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
	// One db pod's last sample is just before the window, and the estimate counts it: the window's truth misses it, the edge probe sees it.
	edgeSeen := int64(1)
	f.truthHook = func(q string, rng time.Duration, counts map[string]int64) int {
		switch {
		case strings.Contains(q, "unless"):
			counts["db-0"] = edgeSeen
		case rng == time.Hour:
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
	assert.Equal(t, `count by (pod) (last_over_time({__name__="up"}[1m] offset 1h) unless last_over_time({__name__="up"}[1h]))`, f.queries[len(f.queries)-1])

	// No series before the window explains it.
	edgeSeen = 0
	res = r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeMismatch, res.Outcome)
}

// TestRunnerEdgeFromSampleTimes runs the edge probe against series with sample times, which the estimate counts by chunk overlap and PromQL by samples.
func TestRunnerEdgeFromSampleTimes(t *testing.T) {
	start, end := testWindow.start, testWindow.end
	sec := time.Second.Milliseconds()
	stopped := labels.FromStrings("__name__", "up", "job", "db", "pod", "db-stopped")
	gap := labels.FromStrings("__name__", "up", "job", "db", "pod", "db-gap")
	late := labels.FromStrings("__name__", "up", "job", "db", "pod", "db-late")
	f, srv := newFakeFrontend(t, append(testSeries(), stopped, gap, late)...)
	// Last sample 30s before the window, staleness marker 30s into it: the chunk overlaps the window, PromQL sees no sample in it.
	f.spans[stopped.String()] = seriesSpan{samples: []int64{start - 90*sec, start - 30*sec}, stale: start + 30*sec}
	// Samples on both sides of the window and none in it.
	f.spans[gap.String()] = seriesSpan{samples: []int64{start - 30*sec, end + 30*sec}}
	// Starts 10s after the window: neither side counts it.
	f.spans[late.String()] = seriesSpan{samples: []int64{end + 10*sec, end + 70*sec}}

	r, _ := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeTolerated, res.Outcome, "%+v", res.Diffs)
	assert.Equal(t, classEdge, res.Class)
	assert.Equal(t, []groupDiff{
		{Value: "db-gap", Estimate: 1, Outcome: outcomeTolerated, Class: classEdge},
		{Value: "db-stopped", Estimate: 1, Outcome: outcomeTolerated, Class: classEdge},
	}, res.Diffs)

	// An overcount on a group whose only extra series starts after the window is not an edge: the probe doesn't look after the window.
	f.estimateHook = func(_ int, r *estimateResponse) {
		r.Groups = append(r.Groups, estimateGroup{Value: "db-late", Count: 1})
	}
	res = r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeMismatch, res.Outcome)
	assert.Contains(t, res.Diffs, groupDiff{Value: "db-late", Estimate: 1, Outcome: outcomeMismatch, Class: classUnclassified})
}

// TestRunnerEdgeGroupCount: an extra group in a cut table is an edge only when an edge series is in a group the window's truth lacks.
func TestRunnerEdgeGroupCount(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	edgePod := "api-0"
	f.truthHook = func(q string, _ time.Duration, counts map[string]int64) int {
		if strings.Contains(q, "unless") {
			counts[edgePod] = 1
		}
		return 0
	}
	f.estimateHook = func(_ int, r *estimateResponse) { r.GroupCount++ }
	r, _ := newTestRunner(srv)
	p := metricPair("pod")
	p.limit = 1
	res := r.run(context.Background(), p)
	assert.Equal(t, outcomeMismatch, res.Outcome, "api-0 is already in the window's truth: %+v", res.Diffs)

	edgePod = "db-9"
	res = r.run(context.Background(), p)
	assert.Equal(t, outcomeTolerated, res.Outcome, "%+v", res.Diffs)
	assert.Equal(t, classEdge, res.Class)
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
		return http.StatusUnprocessableEntity, "the request would hold more than 5 bytes in the store-gateway while counting, the tenant's cardinality estimate memory limit"
	}
	r, m := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeRefusal, res.Outcome)
	assert.Equal(t, classMemoryLimit, res.Class)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.refusals.WithLabelValues(queryMetricByLabel, sideEstimate, classMemoryLimit)))
}

func TestClassifyRefusal(t *testing.T) {
	apiError := func(errorType, msg string) string {
		return `{"status":"error","errorType":"` + errorType + `","error":"` + msg + `"}`
	}
	tests := map[string]struct {
		side   string
		status int
		body   string
		expect string
	}{
		"no count RPC":         {sideEstimate, 422, apiError("execution", "a store-gateway does not support cardinality estimates yet, it may run an older version"), classNoCountRPC},
		"memory, source":       {sideEstimate, 422, apiError("execution", "the request would hold more than 5 bytes in the ingester while counting, the tenant's cardinality estimate memory limit"), classMemoryLimit},
		"memory, querier":      {sideEstimate, 422, apiError("execution", "the groups and hashes this estimate holds in the querier: the query exceeded the maximum allowed estimated amount of memory consumed by a single query (limit: 5 bytes)"), classMemoryLimit},
		"memory, flag name":    {sideEstimate, 422, "over -querier.cardinality-estimate-max-memory-bytes", classMemoryLimit},
		"max groups":           {sideEstimate, 422, apiError("execution", "the blocks of one range hold more than 4 distinct values of pod (the -querier.cardinality-estimate-max-groups limit)"), classGroupsLimit},
		"max groups, source":   {sideEstimate, 422, apiError("execution", "the blocks hold 5 distinct values of pod, more than max_groups 4"), classGroupsLimit},
		"other estimate limit": {sideEstimate, 422, apiError("execution", "something else was refused"), classEstimateRefused},
		"estimate bad request": {sideEstimate, 400, apiError("bad_data", "group_by: invalid label name"), classBadRequest},
		"estimate throttled":   {sideEstimate, 429, "too many outstanding requests", classThrottled},
		"estimate timeout":     {sideEstimate, 503, apiError("timeout", "cardinality estimate timed out after 2m0s (-querier.timeout)"), classError},
		"estimate busy":        {sideEstimate, 503, apiError("unavailable", "ingester: too many cardinality estimates running"), classBusy},
		"promql unavailable":   {sideTruth, 503, apiError("unavailable", "no healthy querier"), classError},
		"promql limit":         {sideTruth, 422, apiError("execution", "the query exceeded the maximum number of series (limit: 10) (err-mimir-max-series-per-query)"), classPromQLLimit},
		"promql 422 no limit":  {sideTruth, 422, apiError("execution", "found duplicate series for the match group"), classError},
		"promql throttled":     {sideTruth, 429, "the query exceeded a limit: too many outstanding requests", classThrottled},
		"promql bad request":   {sideTruth, 400, apiError("bad_data", "parse error"), classBadRequest},
		"promql 5xx":           {sideTruth, 500, "limit reached somewhere", classError},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expect, classifyRefusal(tc.side, newHTTPError(tc.status, []byte(tc.body))))
		})
	}
	assert.Equal(t, classError, classifyRefusal(sideTruth, errors.New("connection refused")), "not HTTP")
}

func TestNewHTTPError(t *testing.T) {
	e := newHTTPError(422, []byte(`{"status":"error","errorType":"execution","error":"more than max_groups 4"}`))
	assert.Equal(t, &httpError{status: 422, errorType: "execution", body: "more than max_groups 4"}, e)
	assert.Equal(t, "HTTP 422 (execution): more than max_groups 4", e.Error())

	e = newHTTPError(502, []byte("bad gateway\n"))
	assert.Equal(t, &httpError{status: 502, body: "bad gateway"}, e)
	assert.Equal(t, "HTTP 502: bad gateway", e.Error())
}

// TestRunnerThrottledTruthDoesNotFallBack: only a query limit starts the per-name fallback, not a 429.
func TestRunnerThrottledTruthDoesNotFallBack(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.truthHook = func(string, time.Duration, map[string]int64) int { return http.StatusTooManyRequests }
	r, _ := newTestRunner(srv)
	p := pair{query: queryTenantByName, matchers: allSeriesMatchers(), groupBy: "__name__", window: testWindow}
	res := r.run(context.Background(), p)
	assert.Equal(t, outcomeRefusal, res.Outcome)
	assert.Equal(t, classThrottled, res.Class)
	assert.Len(t, f.queries, 1)
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

func TestRunnerPromQLLimitFallsBackToOneValueAtATime(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.truthHook = func(q string, _ time.Duration, _ map[string]int64) int {
		if strings.HasPrefix(q, "count by (job) (last_over_time({__name__") {
			return http.StatusUnprocessableEntity
		}
		return 0
	}
	r, _ := newTestRunner(srv)
	p := pair{query: queryTenantByLabel, matchers: allSeriesMatchers(), groupBy: "job", window: testWindow}
	res := r.run(context.Background(), p)
	assert.Equal(t, outcomePass, res.Outcome, "%+v", res.Diffs)
	assert.Equal(t, []string{
		`count by (job) (last_over_time({__name__=~".+"}[1h]))`,
		`count by (job) (last_over_time({job="api", __name__=~".+"}[1h]))`,
		`count by (job) (last_over_time({job="db", __name__=~".+"}[1h]))`,
		`count by (job) (last_over_time({job="", __name__=~".+"}[1h]))`,
	}, f.queries)
}

// TestRunnerPerValueSkipsTheGroupedTruth: a tenant too large for one query sends the estimate first and never the grouped truth.
func TestRunnerPerValueSkipsTheGroupedTruth(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	r, m := newTestRunner(srv)
	r.fallbackNames = 2
	r.pairs = 1 // the order would be promql_first
	p := pair{query: queryTenantByName, matchers: allSeriesMatchers(), groupBy: "__name__", window: testWindow, perValue: true}
	res := r.run(context.Background(), p)
	assert.Equal(t, outcomePass, res.Outcome)
	assert.Equal(t, "estimate_first", res.Order)
	assert.Empty(t, res.Refusals)
	assert.Equal(t, []string{
		`count by (__name__) (last_over_time({__name__="http_requests_total"}[1h]))`,
		`count by (__name__) (last_over_time({__name__="up"}[1h]))`,
	}, f.queries)
	assert.Equal(t, 1.0, counterSum(m.pairs, map[string]string{"outcome": outcomePass}))
	assert.Equal(t, 2, testutil.CollectAndCount(m.duration), "both sides observed")
}

// TestRunnerPerValueGroupCountBound: a tenant-wide table truthed one group at a time is a mismatch when its group_count is above the label's values in the index.
func TestRunnerPerValueGroupCountBound(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	r, _ := newTestRunner(srv)
	for _, groupBy := range []string{"__name__", "pod"} {
		p := pair{query: queryTenantByName, matchers: allSeriesMatchers(), groupBy: groupBy, window: testWindow, limit: 2, perValue: true}
		f.estimateHook = nil
		res := r.run(context.Background(), p)
		assert.Equal(t, outcomePass, res.Outcome, groupBy)

		// A pod table has a "" group for the series without pod, which the index lists no value for.
		extra := 1
		if groupBy == "pod" {
			extra = 2
		}
		f.estimateHook = func(_ int, r *estimateResponse) { r.GroupCount += extra }
		res = r.run(context.Background(), p)
		assert.Equal(t, outcomeMismatch, res.Outcome, groupBy)
		assert.Equal(t, classGroupCountHigh, res.Class, groupBy)
	}
	assert.Equal(t, 4, f.labelValueCalls)

	// A selector narrower than every series isn't bounded by the label's values.
	f.estimateHook = func(_ int, r *estimateResponse) { r.GroupCount += 5 }
	p := metricPair("pod")
	p.perValue = true
	res := r.run(context.Background(), p)
	assert.Equal(t, outcomePass, res.Outcome)
	assert.Equal(t, 4, f.labelValueCalls)
}

func TestCandidatesPerValueAboveTheBound(t *testing.T) {
	tg := &targets{names: []string{"up"}, labels: map[string][]string{}, facets: map[string]facet{}, tenantSeries: 1000, tenantLabels: []string{"pod"}, matcherValues: []string{"api"}}
	queries := []string{queryTenantByName, queryTenantByLabel, queryTenantLabelVals, queryMetricTotal}
	perValue := func(bound int64) map[string]bool {
		out := map[string]bool{}
		for _, p := range candidates(queries, tg, []window{testWindow}, pairConfig{maxReferenceSeries: bound, matcherLabel: "job"}, rand.New(rand.NewPCG(1, 1))) {
			out[p.query] = p.perValue
		}
		return out
	}
	assert.Equal(t, map[string]bool{queryTenantByName: true, queryTenantByLabel: true, queryTenantLabelVals: true, queryMetricTotal: false}, perValue(999))
	assert.Equal(t, map[string]bool{queryTenantByName: false, queryTenantByLabel: false, queryTenantLabelVals: false, queryMetricTotal: false}, perValue(1000))
	assert.Equal(t, map[string]bool{queryTenantByName: false, queryTenantByLabel: false, queryTenantLabelVals: false, queryMetricTotal: false}, perValue(0))
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

// TestRunOneRound runs a whole round against an honest fake: every query shape on every window agrees.
func TestRunOneRound(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	reg := prometheus.NewRegistry()
	m := newMetrics(reg, time.Minute)
	cfg := config{
		address: srv.URL, tenant: "test-tenant", pairsPerRound: 1000, timeout: time.Minute,
		queries: allQueries,
		windows: windowConfig{lengths: []time.Duration{time.Hour, 24 * time.Hour}, queryIngestersWithin: 6 * time.Hour, lag: 5 * time.Minute, blocksMargin: time.Hour},
		targets: testTargets(), targetWindow: 6 * time.Hour,
		pairs:     pairConfig{tableLimit: 2},
		edgeSlack: time.Minute, fallbackNames: 2, seed: 1,
	}
	now := func() time.Time { return time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC) }
	sum, err := run(context.Background(), cfg, m, log.NewNopLogger(), now)
	require.NoError(t, err)
	assert.False(t, sum.mismatched)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.rounds.WithLabelValues("ok")))
	// 1h on blocks, ingester and boundary windows; 24h on blocks and recent windows. A negative matcher on build_info's only value selects nothing.
	for _, q := range allQueries {
		assert.Equal(t, 5.0, pairsCount(m, q, outcomePass, classExact)+pairsCount(m, q, outcomePass, classEmpty), q)
	}
	recent := counterSum(m.pairs, map[string]string{"window": windowRecent, "outcome": outcomePass, "class": classExact})
	assert.Equal(t, float64(len(allQueries)), recent, "one recent pair per query")
	assert.NotEmpty(t, f.queries)

	// A broken estimate makes the round report a mismatch.
	f.estimateHook = func(n int, r *estimateResponse) {
		if len(r.Groups) > 0 && n > 0 { // target picking sends no estimate
			r.Groups[0].Count += 100
		}
	}
	cfg.queries = []string{queryMetricTotal}
	sum, err = run(context.Background(), cfg, newMetrics(prometheus.NewRegistry(), time.Minute), log.NewNopLogger(), now)
	require.NoError(t, err)
	assert.True(t, sum.mismatched)
}

// TestRunNothingCompared: a round in which every pair was refused compares nothing, which one-shot mode exits on.
func TestRunNothingCompared(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.truthHook = func(string, time.Duration, map[string]int64) int { return http.StatusInternalServerError }
	cfg := config{
		address: srv.URL, tenant: "test-tenant", pairsPerRound: 1000, timeout: time.Minute,
		queries: []string{queryMetricTotal},
		windows: windowConfig{lengths: []time.Duration{time.Hour}, queryIngestersWithin: 6 * time.Hour, lag: 5 * time.Minute, blocksMargin: time.Hour},
		targets: testTargets(), targetWindow: 6 * time.Hour,
		seed: 1,
	}
	now := func() time.Time { return time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC) }
	sum, err := run(context.Background(), cfg, newMetrics(prometheus.NewRegistry(), time.Minute), log.NewNopLogger(), now)
	require.NoError(t, err)
	assert.Equal(t, summary{}, sum)
}

func TestPickTargetsFacets(t *testing.T) {
	_, srv := newFakeFrontend(t, testSeries()...)
	r, _ := newTestRunner(srv)
	cfg := testTargets()
	cfg.facets = true
	got, err := pickTargets(context.Background(), r.client, cfg, testTargetWindows, log.NewNopLogger(), func() {})
	require.NoError(t, err)
	assert.Equal(t, []string{"http_requests_total", "up", "build_info"}, got.names)
	// pod and job are on every http_requests_total series; status is not.
	assert.Equal(t, facet{label: "status", values: []string{"404", "500"}, partial: true}, got.facets["http_requests_total"])
	assert.Equal(t, facet{label: "version", values: []string{"1"}}, got.facets["build_info"])
}

// TestRunFacetProbeRefused: a refused facet probe skips that name's matcher queries; the round runs its other pairs.
func TestRunFacetProbeRefused(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	probes := 0
	f.estimateMatchHook = func(sel string) (int, string) {
		if strings.Contains(sel, `=""`) {
			probes++
			return http.StatusBadRequest, "unsupported matcher"
		}
		return 0, ""
	}
	m := newMetrics(prometheus.NewRegistry(), time.Minute)
	cfg := config{
		address: srv.URL, tenant: "test-tenant", pairsPerRound: 1000, timeout: time.Minute,
		queries: []string{queryMetricTotal, queryMetricNotEqual},
		windows: windowConfig{lengths: []time.Duration{time.Hour}, queryIngestersWithin: 6 * time.Hour, lag: 5 * time.Minute, blocksMargin: time.Hour},
		targets: testTargets(), targetWindow: 6 * time.Hour,
		seed: 1,
	}
	now := func() time.Time { return time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC) }
	sum, err := run(context.Background(), cfg, m, log.NewNopLogger(), now)
	require.NoError(t, err)
	assert.False(t, sum.mismatched)
	assert.Equal(t, 3, probes, "one refused probe per name")
	assert.Equal(t, 3.0, counterSum(m.refusals, map[string]string{"query": queryFacetProbe, "side": sideEstimate, "class": classBadRequest}), "each skipped name is a refusal")
	assert.Equal(t, 3.0, pairsCount(m, queryMetricTotal, outcomePass, classExact))
	assert.Equal(t, 3, sum.compared)
	assert.Equal(t, 0.0, pairsCount(m, queryMetricNotEqual, outcomePass, classExact)+pairsCount(m, queryMetricNotEqual, outcomePass, classEmpty))

	// Without a matcher query in -queries, no facet is probed.
	probes = 0
	cfg.queries = []string{queryMetricTotal}
	_, err = run(context.Background(), cfg, newMetrics(prometheus.NewRegistry(), time.Minute), log.NewNopLogger(), now)
	require.NoError(t, err)
	assert.Zero(t, probes)
}

func TestCandidatesMatcherQueries(t *testing.T) {
	tg := &targets{
		names:  []string{"http_requests_total"},
		labels: map[string][]string{"http_requests_total": {"pod", "status"}},
		facets: map[string]facet{"http_requests_total": {label: "status", values: []string{"500", "4.4"}, partial: true}},
	}
	queries := []string{queryNameRegex, queryMetricNotEqual, queryMetricRegex, queryTwoMatchers}
	got := map[string]string{}
	for _, p := range candidates(queries, tg, []window{testWindow}, pairConfig{tableLimit: 10}, rand.New(rand.NewPCG(1, 1))) {
		got[p.query] = p.selector() + " by " + p.groupBy
	}
	assert.Equal(t, map[string]string{
		queryNameRegex:      `{__name__=~"http_.*"} by __name__`,
		queryMetricNotEqual: `{__name__="http_requests_total", status!="500"} by status`,
		queryMetricRegex:    `{__name__="http_requests_total", status=~"500|4\\.4"} by status`,
	}, map[string]string{queryNameRegex: got[queryNameRegex], queryMetricNotEqual: got[queryMetricNotEqual], queryMetricRegex: got[queryMetricRegex]})
	// The equality matcher draws one of the facet's values.
	assert.Contains(t, []string{
		`{__name__="http_requests_total", status="500"} by pod`,
		`{__name__="http_requests_total", status="4.4"} by pod`,
	}, got[queryTwoMatchers])

	// Without a facet the matcher queries are skipped, and a name without an underscore is its own prefix.
	tg = &targets{names: []string{"up"}, labels: map[string][]string{}, facets: map[string]facet{}}
	ps := candidates(queries, tg, []window{testWindow}, pairConfig{}, rand.New(rand.NewPCG(1, 1)))
	require.Len(t, ps, 1)
	assert.Equal(t, `{__name__=~"up.*"}`, ps[0].selector())
}

// TestRunnerNegativeMatcherCountsSeriesWithoutTheLabel: PromQL's status!="500" keeps the series without status, so an estimate that drops them is a mismatch.
func TestRunnerNegativeMatcherCountsSeriesWithoutTheLabel(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	r, _ := newTestRunner(srv)
	p := pair{query: queryMetricNotEqual, matchers: []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"),
		labels.MustNewMatcher(labels.MatchNotEqual, "status", "500"),
	}, groupBy: "status", window: testWindow}
	res := r.run(context.Background(), p)
	assert.Equal(t, outcomePass, res.Outcome)
	assert.Equal(t, classExact, res.Class)

	f.estimateHook = func(_ int, r *estimateResponse) {
		r.Groups = slices.DeleteFunc(r.Groups, func(g estimateGroup) bool { return g.Value == "" })
	}
	res = r.run(context.Background(), p)
	assert.Equal(t, outcomeMismatch, res.Outcome)
	require.Len(t, res.Diffs, 1)
	assert.Equal(t, groupDiff{Value: "", Estimate: 0, Truth: 6, Outcome: outcomeMismatch, Class: classUnclassified}, res.Diffs[0])
}

func TestRunnerBurst(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	// The first three fit the limit; the rest are refused, as the route answers when every source is at its count limit.
	f.estimateError = func() (int, string) {
		if f.estimates > 3 {
			return http.StatusServiceUnavailable, `{"status":"error","errorType":"unavailable","error":"ingester: too many cardinality estimates running"}`
		}
		return 0, ""
	}
	r, m := newTestRunner(srv)
	cands := []pair{metricPair("pod"), {query: queryMetricTotal, matchers: metricPair("").matchers, groupBy: "__name__", window: testWindow}}
	r.burst(context.Background(), cands, 5)

	assert.Equal(t, 5, f.estimates)
	assert.Empty(t, f.queries, "a burst sends no truth")
	assert.Equal(t, 1.0, testutil.ToFloat64(m.bursts))
	ok, refused := 0.0, 0.0
	for _, q := range []string{queryMetricByLabel, queryMetricTotal} {
		ok += testutil.ToFloat64(m.burstRequests.WithLabelValues(q, "200"))
		refused += testutil.ToFloat64(m.burstRequests.WithLabelValues(q, classBusy))
	}
	assert.Equal(t, 3.0, ok)
	assert.Equal(t, 2.0, refused)
}

// TestRunnerPerValueSkipsRefusedName: a tenant-wide table truthed one name at a time compares the names PromQL answers and records the refused one.
func TestRunnerPerValueSkipsRefusedName(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.truthHook = func(q string, _ time.Duration, _ map[string]int64) int {
		if strings.Contains(q, `"http_requests_total"`) {
			return http.StatusUnprocessableEntity
		}
		return 0
	}
	r, m := newTestRunner(srv)
	p := pair{query: queryTenantByName, matchers: allSeriesMatchers(), groupBy: "__name__", window: testWindow, perValue: true}
	res := r.run(context.Background(), p)
	assert.Equal(t, outcomePass, res.Outcome)
	assert.Contains(t, res.Refusals, sideTruth+"_per_value http_requests_total:"+classPromQLLimit)
	assert.Equal(t, 1.0, counterSum(m.refusals, map[string]string{"query": queryTenantByName, "side": sideTruth, "class": classPromQLLimit}))
}

func TestBurstStatus(t *testing.T) {
	assert.Equal(t, "200", burstStatus(&estimateResponse{}, nil))
	assert.Equal(t, "422", burstStatus(nil, &httpError{status: 422}))
	assert.Equal(t, "busy", burstStatus(nil, &httpError{status: 503, errorType: "unavailable"}))
	assert.Equal(t, "503", burstStatus(nil, &httpError{status: 503, errorType: "timeout"}))
	assert.Equal(t, "timeout", burstStatus(nil, context.DeadlineExceeded))
	assert.Equal(t, "error", burstStatus(nil, errors.New("connection refused")))
}

// TestRunBurstOncePerInterval: the first round sends a burst, and the next one only after -burst-interval.
func TestRunBurstOncePerInterval(t *testing.T) {
	_, srv := newFakeFrontend(t, testSeries()...)
	m := newMetrics(prometheus.NewRegistry(), time.Minute)
	cfg := config{
		address: srv.URL, tenant: "test-tenant", pairsPerRound: 1, timeout: time.Minute,
		queries: []string{queryMetricTotal},
		windows: windowConfig{lengths: []time.Duration{time.Hour}, queryIngestersWithin: 6 * time.Hour, lag: 5 * time.Minute, blocksMargin: time.Hour},
		targets: testTargets(), targetWindow: 6 * time.Hour,
		seed:  1,
		burst: 4, burstInterval: time.Hour, interval: time.Millisecond,
	}
	// Each round reads the clock at its start and at its end. Three rounds, 40 minutes apart; the run stops after the third.
	at := func(m int) time.Time { return time.Date(2026, 1, 2, 12, m, 0, 0, time.UTC) }
	clock := []time.Time{at(0), at(0), at(40), at(40), at(80), at(80)}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	now := func() time.Time {
		c := clock[min(calls, len(clock)-1)]
		calls++
		if calls == len(clock) {
			cancel()
		}
		return c
	}
	_, err := run(ctx, cfg, m, log.NewNopLogger(), now)
	require.NoError(t, err)
	// Rounds at 12:00, 12:40 and 13:20: bursts at 12:00 and 13:20.
	assert.Equal(t, 2.0, testutil.ToFloat64(m.bursts))
	assert.Equal(t, 8.0, testutil.ToFloat64(m.burstRequests.WithLabelValues(queryMetricTotal, "200")))
}

func TestConfigValidate(t *testing.T) {
	parse := func(args ...string) error {
		var cfg config
		fs := flag.NewFlagSet("compare", flag.ContinueOnError)
		cfg.register(fs)
		require.NoError(t, fs.Parse(append([]string{"-address", "http://x", "-tenant", "t"}, args...)))
		return cfg.validate()
	}
	require.NoError(t, parse())
	require.NoError(t, parse("-windows", "1h"))
	require.ErrorContains(t, parse("-queries", "nope"), "unknown query")
	require.ErrorContains(t, parse("-tenant", ""), "required")
	var cfg config
	fs := flag.NewFlagSet("compare", flag.ContinueOnError)
	cfg.register(fs)
	require.NoError(t, fs.Parse([]string{"-address", "http://x", "-tenant", "t", "-name-ranks", "500,5,5,50"}))
	require.NoError(t, cfg.validate())
	assert.Equal(t, []int{5, 50, 500}, cfg.targets.ranks, "sorted, without duplicates")
	for _, args := range [][]string{
		{"-name-ranks", "0"}, {"-name-ranks", "a"}, {"-name-ranks", ""}, {"-target-source", "nope"}, {"-target-window", "0"}, {"-label-values-limit", "0"}, {"-top-labels", "0"}, {"-fallback-names", "0"}, {"-pairs-per-round", "0"},
		{"-timeout", "0"}, {"-interval", "-1s"}, {"-request-gap", "-1s"}, {"-table-limit", "-1"},
		{"-burst", "-1"}, {"-burst", "65"},
	} {
		assert.Error(t, parse(args...), "%v", args)
	}
}

// TestPickTargetsPaced: target picking pauses between its requests.
func TestPickTargetsPaced(t *testing.T) {
	_, srv := newFakeFrontend(t, testSeries()...)
	r, _ := newTestRunner(srv)
	requests := 0
	orig := r.client.http.Transport
	r.client.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		return orig.RoundTrip(req)
	})
	paces := 0
	cfg := testTargets()
	cfg.facets = true
	_, err := pickTargets(context.Background(), r.client, cfg, testTargetWindows, log.NewNopLogger(), func() { paces++ })
	require.NoError(t, err)
	assert.Equal(t, requests-1, paces)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestRunEstimateOnly: with -estimate-only, drawn and scenario pairs send their estimate and no truth, every estimate goes without strong read consistency, and an answered estimate counts as compared.
func TestRunEstimateOnly(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.eventualEstimates = true
	truths := 0
	f.truthHook = func(string, time.Duration, map[string]int64) int {
		truths++
		return 0
	}
	m := newMetrics(prometheus.NewRegistry(), time.Minute)
	cfg := config{
		address: srv.URL, tenant: "test-tenant", pairsPerRound: 2, timeout: time.Minute,
		queries:       []string{queryMetricTotal, queryMetricByLabel},
		windowLengths: []string{"1h"},
		windows:       windowConfig{queryIngestersWithin: 6 * time.Hour, lag: 5 * time.Minute, blocksMargin: time.Hour},
		targets:       testTargets(), targetWindow: 6 * time.Hour,
		seed: 1, fallbackNames: 5,
		nameRanks: []string{"1", "2", "3"}, estimateOnly: true,
		scenarioPath: writeScenarios(t, `
- {name: api_by_pod, selector: '{__name__="up", job="api"}', group_by: pod}
`),
	}
	require.NoError(t, cfg.validate())
	now := func() time.Time { return time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC) }
	sum, err := run(context.Background(), cfg, m, log.NewNopLogger(), now)
	require.NoError(t, err)
	assert.Zero(t, truths, "no PromQL truth is sent")
	assert.False(t, sum.mismatched)
	assert.Equal(t, 3, sum.compared)
	assert.Equal(t, 3.0, counterSum(m.pairs, map[string]string{"order": orderEstimateOnly, "outcome": outcomeAnswered}))
	assert.Equal(t, 1.0, counterSum(m.pairs, map[string]string{"query": "scenario:api_by_pod", "outcome": outcomeAnswered}))
	assert.Equal(t, 3, testutil.CollectAndCount(m.duration), "one latency per estimate")
	assert.Equal(t, 3.0, counterSum(m.duration, map[string]string{"side": sideEstimate, "position": positionAlone, "reference": referenceNone}))

	// A refused estimate is a refusal, and a round of refusals compares nothing.
	m = newMetrics(prometheus.NewRegistry(), time.Minute)
	f.estimateMatchHook = func(sel string) (int, string) {
		if strings.Contains(sel, `job="api"`) {
			return http.StatusUnprocessableEntity, "the query exceeded the index bytes limit"
		}
		return 0, ""
	}
	cfg.queries, cfg.pairsPerRound = []string{queryMetricTotal}, 1
	require.NoError(t, cfg.validate())
	sum, err = run(context.Background(), cfg, m, log.NewNopLogger(), now)
	require.NoError(t, err)
	assert.Equal(t, 1, sum.compared, "the drawn pair answered")
	assert.Equal(t, 1.0, counterSum(m.refusals, map[string]string{"query": "scenario:api_by_pod", "side": sideEstimate}))
	assert.Equal(t, 1.0, counterSum(m.pairs, map[string]string{"query": "scenario:api_by_pod", "order": orderEstimateOnly, "outcome": outcomeRefusal}))
}

// TestRunnerTruthTruncated: a PromQL answer that says a limit cut it is not a table to compare with, so it is a refusal and not a mismatch.
func TestRunnerTruthTruncated(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.queryWarnings = []string{"results truncated due to limit"}
	r, m := newTestRunner(srv)
	res := r.run(context.Background(), metricPair("pod"))
	assert.Equal(t, outcomeRefusal, res.Outcome)
	assert.Equal(t, classTruncated, res.Class)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.refusals.WithLabelValues(queryMetricByLabel, sideTruth, classTruncated)))

	// Other warnings don't cut the table.
	f.queryWarnings = []string{"PromQL info: something about the query"}
	assert.Equal(t, outcomePass, r.run(context.Background(), metricPair("pod")).Outcome)
}

// TestRunnerGroupCountBoundTruncated: a label values answer with a warning, such as the querier lowering its limit, is no upper bound on group_count, so it must not report group_count_excess.
func TestRunnerGroupCountBoundTruncated(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.labelValuesWarnings = []string{"the limit was lowered to the tenant's max label values limit"}
	f.estimateHook = func(_ int, r *estimateResponse) { r.GroupCount += 5 }
	r, m := newTestRunner(srv)
	p := pair{query: queryTenantByName, matchers: allSeriesMatchers(), groupBy: "__name__", window: testWindow, limit: 2, perValue: true}
	res := r.run(context.Background(), p)
	assert.NotEqual(t, classGroupCountHigh, res.Class)
	assert.Contains(t, res.Refusals, "label values probe:"+classTruncated)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.refusals.WithLabelValues(queryTenantByName, sideTruth, classTruncated)))
}

// TestRunnerLargeGroupedTruth: a tenant-wide table of tens of thousands of groups is counted whole: the grouped truth has as many groups as the estimate's group_count.
func TestRunnerLargeGroupedTruth(t *testing.T) {
	const groups = 25_000
	series := testSeries()
	for i := range groups {
		series = append(series, labels.FromStrings("__name__", "spans", "job", "tempo", "span_name", "op-"+strconv.Itoa(i)))
	}
	_, srv := newFakeFrontend(t, series...)
	r, _ := newTestRunner(srv)
	p := pair{query: queryTenantByLabel, matchers: allSeriesMatchers(), groupBy: "span_name", window: testWindow, limit: 50}
	res := r.run(context.Background(), p)
	assert.Equal(t, outcomePass, res.Outcome, "%+v", res.Diffs)
	truth, _, err := r.truth(context.Background(), p, truthSet{}, 0)
	require.NoError(t, err)
	assert.Len(t, truth.counts, groups+1, "every span_name, and the series without one as the empty value")
}

// TestRunnerMismatchLogsTheTruthQuery: the log line of a mismatch has the grouped truth's query and its evaluation time, to run it by hand.
func TestRunnerMismatchLogsTheTruthQuery(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.estimateHook = func(_ int, r *estimateResponse) { r.Groups[0].Count += 5 }
	r, _ := newTestRunner(srv)
	var buf bytes.Buffer
	r.logger = log.NewLogfmtLogger(&buf)
	r.run(context.Background(), metricPair("pod"))
	assert.Contains(t, buf.String(), `truth_query="count by (pod) (last_over_time({__name__=\"up\"}[1h]))"`)
	assert.Contains(t, buf.String(), "truth_at="+formatTime(testWindow.end-1))
}
