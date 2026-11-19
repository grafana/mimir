// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

// The fixture has 8 http_requests_total, 6 up and 1 build_info series; by job, 11 api, 3 db and 1 without the label.

// TestRunnerMaxTruthSeriesSkipsLargeGroups: a table truthed one group at a time leaves out the groups over the limit.
func TestRunnerMaxTruthSeriesSkipsLargeGroups(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	r, _ := newTestRunner(srv)
	r.fallbackNames, r.maxTruthSeries = 2, 6
	p := pair{query: queryTenantByName, matchers: allSeriesMatchers(), groupBy: "__name__", window: testWindow, perValue: true}
	res := r.run(context.Background(), p)
	assert.Equal(t, outcomePass, res.Outcome, "%+v", res.Diffs)
	assert.Equal(t, []string{
		`count by (__name__) (last_over_time({__name__="up"}[1h]))`,
		`count by (__name__) (last_over_time({__name__="build_info"}[1h]))`,
	}, f.queries)
}

// TestRunnerMaxTruthSeriesReplacesTheGroupedTruth: a grouped truth over the limit is never sent; the groups that fit are counted one at a time, estimate first.
func TestRunnerMaxTruthSeriesReplacesTheGroupedTruth(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	r, m := newTestRunner(srv)
	r.maxTruthSeries = 5
	r.pairs = 1 // the order would be promql_first
	p := pair{query: queryTenantByLabel, matchers: allSeriesMatchers(), groupBy: "job", window: testWindow}
	res := r.run(context.Background(), p)
	assert.Equal(t, outcomePass, res.Outcome, "%+v", res.Diffs)
	assert.Equal(t, "estimate_first", res.Order)
	assert.Equal(t, []string{"promql:" + classTruthTooLarge}, res.Refusals)
	assert.Equal(t, []string{
		`count by (job) (last_over_time({job="db", __name__=~".+"}[1h]))`,
		`count by (job) (last_over_time({job="", __name__=~".+"}[1h]))`,
	}, f.queries)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.refusals.WithLabelValues(queryTenantByLabel, sideTruth, classTruthTooLarge)))
}

// TestRunnerMaxTruthSeriesRefusesWhenNothingFits: with every group over the limit, no PromQL query is sent.
func TestRunnerMaxTruthSeriesRefusesWhenNothingFits(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	r, _ := newTestRunner(srv)
	r.maxTruthSeries = 5
	name := labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total")
	for _, perValue := range []bool{false, true} {
		p := pair{query: queryMetricTotal, matchers: []*labels.Matcher{name}, groupBy: "__name__", window: testWindow, perValue: perValue}
		res := r.run(context.Background(), p)
		assert.Equal(t, outcomeRefusal, res.Outcome)
		assert.Equal(t, classTruthTooLarge, res.Class)
	}
	assert.Empty(t, f.queries)
}

// TestBoundsCheckMaxTruthSeries: a name whose upper is over the limit is not counted with PromQL, and the other names are.
func TestBoundsCheckMaxTruthSeries(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	r, m := newTestRunner(srv)
	r.maxTruthSeries = 6
	cfg := testBoundsConfig
	cfg.ranks, cfg.perValue = []int{1, 2}, true
	outcomes := r.boundsCheck(context.Background(), testWindow, cfg)
	assert.Equal(t, []string{outcomePass, outcomePass}, outcomes)
	assert.Equal(t, []string{`count by (__name__) (last_over_time({__name__="up"}[2h]))`}, f.queries)
	assert.Equal(t, 1.0, counterSum(m.refusals, map[string]string{"query": queryBoundsCheck, "side": sideTruth, "class": classTruthTooLarge}))
}

func TestLargestIsNeverNil(t *testing.T) {
	est := &estimateResponse{Groups: []estimateGroup{{Value: "a", Count: 9}, {Value: "b", Count: 3}}}
	assert.Equal(t, []string{"a", "b"}, largest(est, 5, 0))
	assert.Equal(t, []string{"b"}, largest(est, 5, 3))
	got := largest(est, 5, 1)
	assert.NotNil(t, got, "a nil set would ask for the grouped truth")
	assert.Empty(t, got)
}

func TestBurstCandidates(t *testing.T) {
	cands := []pair{{query: queryTenantByName}, {query: queryMetricTotal}, {query: queryTenantByLabel}}
	assert.Equal(t, cands, burstCandidates(cands, nil))
	assert.Equal(t, []pair{{query: queryMetricTotal}}, burstCandidates(cands, []string{queryMetricTotal}))
	assert.Empty(t, burstCandidates(cands, []string{queryNameRegex}))
}

// TestRunnerAlternatesSmallPairs: a pair whose name is known to be small alternates its order; one of unknown or large size sends the estimate first.
func TestRunnerAlternatesSmallPairs(t *testing.T) {
	_, srv := newFakeFrontend(t, testSeries()...)
	r, _ := newTestRunner(srv)
	r.maxTruthSeries = 100
	small := metricPair("pod")
	small.knownSeries = 25
	assert.Equal(t, "estimate_first", r.run(context.Background(), small).Order)
	assert.Equal(t, "promql_first", r.run(context.Background(), small).Order)
	large := metricPair("pod")
	large.knownSeries = 26
	r.pairs = 1
	assert.Equal(t, "estimate_first", r.run(context.Background(), large).Order, "over a quarter of the limit")
	unknown := metricPair("pod")
	r.pairs = 1
	assert.Equal(t, "estimate_first", r.run(context.Background(), unknown).Order, "size unknown")
	r.maxTruthSeries = 0
	r.pairs = 1
	assert.Equal(t, "promql_first", r.run(context.Background(), unknown).Order, "no limit")
}

func TestCandidatesKnownSeries(t *testing.T) {
	tg := &targets{names: []string{"up"}, labels: map[string][]string{"up": {"pod"}}, facets: map[string]facet{}, tenantLabels: []string{"pod"}, nameSeries: map[string]int64{"up": 6}}
	cands := candidates([]string{queryMetricTotal, queryMetricByLabel, queryTenantByName}, tg, []window{testWindow}, pairConfig{}, rand.New(rand.NewPCG(1, 1)))
	got := map[string]int64{}
	for _, p := range cands {
		got[p.query] = p.knownSeries
	}
	assert.Equal(t, map[string]int64{queryMetricTotal: 6, queryMetricByLabel: 6, queryTenantByName: 0}, got)
}
