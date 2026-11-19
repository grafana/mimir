// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testBoundsConfig = boundsConfig{ranks: []int{1, 2, 3}, maxRanges: 4}

// The fake answers one range, [00:00, 02:00), for testWindow.
const testRangeQuery = `count by (__name__) (last_over_time({__name__=~".+"}[2h]))`

func boundsGroups(m *metrics, query, outcome, class string) float64 {
	return counterSum(m.groups, map[string]string{"query": query, "outcome": outcome, "class": class})
}

func histogramOf(t *testing.T, h prometheus.Histogram) *dto.Histogram {
	var pb dto.Metric
	require.NoError(t, h.Write(&pb))
	return pb.GetHistogram()
}

// TestBoundsCheckPass: every group of the range is within its bounds, whose truth is PromQL on the range's window at its end minus 1 ms, and the relative widths are observed.
func TestBoundsCheckPass(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.boundsSlack = 1
	r, m := newTestRunner(srv)
	var times []string
	orig := r.client.http.Transport
	r.client.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/prometheus/api/v1/query" {
			times = append(times, req.URL.Query().Get("time"))
		}
		return orig.RoundTrip(req)
	})
	outcomes := r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
	assert.Equal(t, []string{outcomePass, outcomePass}, outcomes)
	assert.Equal(t, []string{testRangeQuery}, f.queries)
	assert.Equal(t, []string{formatTime(testWindow.start + 2*time.Hour.Milliseconds() - 1)}, times, "the range's end, not the window's")
	assert.Equal(t, 3.0, boundsGroups(m, queryBoundsCheck, outcomePass, classWithinBounds))
	assert.Equal(t, 1.0, pairsCount(m, queryBoundsCheck, outcomePass, classWithinBounds))
	assert.Equal(t, 1.0, counterSum(m.pairs, map[string]string{"query": queryBoundsCheck, "read": "bounds", "source": "blocks", "order": orderBounds}))

	// build_info has lower 0, which has no relative width: http_requests_total is 2/7 wide and up 2/5.
	h := histogramOf(t, m.boundsWidth)
	assert.Equal(t, uint64(2), h.GetSampleCount())
	assert.InDelta(t, 2.0/7+2.0/5, h.GetSampleSum(), 1e-9)
}

func TestBoundsCheckExactBounds(t *testing.T) {
	_, srv := newFakeFrontend(t, testSeries()...)
	r, m := newTestRunner(srv)
	res := r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
	assert.Equal(t, []string{outcomePass, outcomePass}, res)
	h := histogramOf(t, m.boundsWidth)
	assert.Equal(t, uint64(3), h.GetSampleCount())
	assert.Zero(t, h.GetSampleSum(), "lower == upper")
}

// TestBoundsCheckViolation: a truth above upper, below lower, or in a group the answer lacks is a violation, and the check counts as a mismatch.
func TestBoundsCheckViolation(t *testing.T) {
	tests := map[string]struct {
		hook func(r *boundsResponse)
		want groupDiff
	}{
		"truth above upper": {
			hook: func(r *boundsResponse) {
				for i, g := range r.Ranges[0].Groups {
					if g.Value == "up" {
						r.Ranges[0].Groups[i].Upper = 5
					}
				}
			},
			want: groupDiff{Value: "up", Truth: 6, Lower: 6, Upper: 5, Outcome: outcomeMismatch, Class: classBoundsViolation},
		},
		"truth below lower": {
			hook: func(r *boundsResponse) {
				for i, g := range r.Ranges[0].Groups {
					if g.Value == "up" {
						r.Ranges[0].Groups[i].Lower, r.Ranges[0].Groups[i].Upper = 7, 9
					}
				}
			},
			want: groupDiff{Value: "up", Truth: 6, Lower: 7, Upper: 9, Outcome: outcomeMismatch, Class: classBoundsViolation},
		},
		"group missing from the answer": {
			hook: func(r *boundsResponse) {
				r.Ranges[0].Groups = r.Ranges[0].Groups[:len(r.Ranges[0].Groups)-1]
			},
			want: groupDiff{Value: "build_info", Truth: 1, Outcome: outcomeMismatch, Class: classBoundsViolation},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			f, srv := newFakeFrontend(t, testSeries()...)
			f.boundsHook = func(_ int, r *boundsResponse) { tc.hook(r) }
			r, m := newTestRunner(srv)

			var a boundsAnswer
			var res pairResult
			ans, _, err := r.client.bounds(context.Background(), "__name__", testWindow.start, testWindow.end, 0)
			require.NoError(t, err)
			_, ok := r.checkRangeAgainstPromQL(context.Background(), testWindow, ans.Ranges[0], testBoundsConfig, &a, &res)
			require.True(t, ok)
			tc.want.Range = rangeKey(ans.Ranges[0])
			assert.Equal(t, []groupDiff{tc.want}, a.diffs)
			assert.Equal(t, 2, a.passed)

			outcomes := r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
			assert.Equal(t, outcomeMismatch, outcomes[0])
			assert.Equal(t, 1.0, boundsGroups(m, queryBoundsCheck, outcomeMismatch, classBoundsViolation))
			assert.Equal(t, 1.0, pairsCount(m, queryBoundsCheck, outcomeMismatch, classBoundsViolation))
		})
	}
}

// TestBoundsCheckEdge: a series whose last sample is just before the range and whose staleness marker is in it is in the block's index, so lower is above the truth by it, and the edge probe explains that.
func TestBoundsCheckEdge(t *testing.T) {
	rangeStart := testWindow.start
	stopped := labels.FromStrings("__name__", "up", "job", "db", "pod", "db-stopped")
	f, srv := newFakeFrontend(t, append(testSeries(), stopped)...)
	f.spans[stopped.String()] = seriesSpan{samples: []int64{rangeStart - 30_000}, stale: rangeStart + 30_000}
	r, m := newTestRunner(srv)
	outcomes := r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
	assert.Equal(t, outcomeTolerated, outcomes[0])
	assert.Equal(t, 1.0, boundsGroups(m, queryBoundsCheck, outcomeTolerated, classEdge))
	assert.Equal(t, 1.0, pairsCount(m, queryBoundsCheck, outcomeTolerated, classEdge))
	assert.Contains(t, f.queries, `count by (__name__) (last_over_time({__name__=~".+"}[1m] offset 2h) unless last_over_time({__name__=~".+"}[2h]))`)

	// Without the probe, or when no series before the range explains it, it is a violation.
	r.edgeSlack = 0
	outcomes = r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
	assert.Equal(t, outcomeMismatch, outcomes[0])
	r.edgeSlack = time.Minute
	f.spans[stopped.String()] = seriesSpan{samples: []int64{rangeStart - 10*60_000}, stale: rangeStart + 30_000}
	outcomes = r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
	assert.Equal(t, outcomeMismatch, outcomes[0])
}

// TestBoundsCheckLowerBoundRange: a range that misses a block can have a short upper too, so a truth above upper is flagged, not a violation.
func TestBoundsCheckLowerBoundRange(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.boundsHook = func(_ int, r *boundsResponse) {
		r.Ranges[0].LowerBound, r.Ranges[0].MissingBlocks = true, []string{"01ABC"}
		for i, g := range r.Ranges[0].Groups {
			if g.Value == "up" {
				r.Ranges[0].Groups[i].Upper = 5
			}
		}
	}
	r, m := newTestRunner(srv)
	outcomes := r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
	assert.Equal(t, outcomeFlagged, outcomes[0])
	assert.Equal(t, 1.0, boundsGroups(m, queryBoundsCheck, outcomeFlagged, classIncomplete))
	assert.Equal(t, 1.0, counterSum(m.flags, map[string]string{"query": queryBoundsCheck, "flag": "lower_bound"}))
	assert.Equal(t, 1.0, counterSum(m.flags, map[string]string{"query": queryBoundsCheck, "flag": "missing_blocks"}))
}

// TestBoundsCheckPerValue: a large tenant gets one PromQL query per name at the ranks, on the range's window, and the other names are not compared.
func TestBoundsCheckPerValue(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	var limits []string
	f.boundsHook = func(_ int, r *boundsResponse) {
		// build_info is far off, and is not at a rank asked for.
		for i := range r.Ranges[0].Groups {
			if r.Ranges[0].Groups[i].Value == "build_info" {
				r.Ranges[0].Groups[i].Upper = 0
			}
		}
	}
	r, m := newTestRunner(srv)
	rt := r.client.http.Transport
	r.client.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Query().Get("mode") == "bounds" {
			limits = append(limits, req.URL.Query().Get("limit"))
		}
		return rt.RoundTrip(req)
	})
	cfg := testBoundsConfig
	cfg.ranks, cfg.perValue = []int{1, 2}, true
	outcomes := r.boundsCheck(context.Background(), testWindow, cfg)
	assert.Equal(t, []string{outcomePass, outcomePass}, outcomes)
	assert.Equal(t, []string{"2"}, limits, "the table is cut at the largest rank")
	assert.Equal(t, []string{
		`count by (__name__) (last_over_time({__name__="http_requests_total"}[2h]))`,
		`count by (__name__) (last_over_time({__name__="up"}[2h]))`,
	}, f.queries)
	assert.Equal(t, 2.0, boundsGroups(m, queryBoundsCheck, outcomePass, classWithinBounds))
	assert.Equal(t, 1.0, counterSum(m.duration, map[string]string{"query": queryBoundsCheck, "side": sideTruth, "reference": referencePerValue}))
}

// TestBoundsCheckPromQLLimitFallsBack: a grouped truth that hits a query limit is asked for again one name at a time.
func TestBoundsCheckPromQLLimitFallsBack(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.truthHook = func(q string, _ time.Duration, _ map[string]int64) int {
		if strings.Contains(q, `=~".+"`) {
			return http.StatusUnprocessableEntity
		}
		return 0
	}
	r, m := newTestRunner(srv)
	cfg := testBoundsConfig
	cfg.ranks = []int{1, 2}
	outcomes := r.boundsCheck(context.Background(), testWindow, cfg)
	assert.Equal(t, []string{outcomePass, outcomePass}, outcomes)
	assert.Equal(t, 1.0, counterSum(m.refusals, map[string]string{"query": queryBoundsCheck, "side": sideTruth, "class": classPromQLLimit}))
	assert.Equal(t, 2.0, boundsGroups(m, queryBoundsCheck, outcomePass, classWithinBounds))
}

// TestBoundsCheckNewestRanges: only the newest -bounds-max-ranges ranges of an answer are checked, each on its own window.
func TestBoundsCheckNewestRanges(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.rangeLen = time.Hour
	r, m := newTestRunner(srv)
	cfg := testBoundsConfig
	cfg.maxRanges = 2
	w := window{kind: windowBlocks, start: testWindow.start, end: testWindow.start + 3*time.Hour.Milliseconds()}
	var times []string
	orig := r.client.http.Transport
	r.client.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/prometheus/api/v1/query" {
			times = append(times, req.URL.Query().Get("time"))
		}
		return orig.RoundTrip(req)
	})
	outcomes := r.boundsCheck(context.Background(), w, cfg)
	assert.Equal(t, []string{outcomePass, outcomePass}, outcomes)
	assert.Equal(t, []string{
		formatTime(testWindow.start + 2*time.Hour.Milliseconds() - 1),
		formatTime(testWindow.start + 3*time.Hour.Milliseconds() - 1),
	}, times)
	assert.Equal(t, 6.0, boundsGroups(m, queryBoundsCheck, outcomePass, classWithinBounds))
}

// TestBoundsCheckHead: the heads' row set is not checked, and a window that reaches them still has its block ranges checked.
func TestBoundsCheckHead(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.headFrom = testWindow.start + time.Hour.Milliseconds()
	f.rangeLen = time.Hour
	r, m := newTestRunner(srv)
	w := window{kind: windowBoundary, start: testWindow.start, end: testWindow.start + 2*time.Hour.Milliseconds()}
	outcomes := r.boundsCheck(context.Background(), w, testBoundsConfig)
	assert.Equal(t, []string{outcomePass, outcomePass}, outcomes)
	assert.Equal(t, 1.0, counterSum(m.pairs, map[string]string{"query": queryBoundsCheck, "source": "blocks+head"}))
	assert.Equal(t, 3.0, boundsGroups(m, queryBoundsCheck, outcomePass, classWithinBounds))
}

func TestBoundsCheckNoRange(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.headFrom = testWindow.start
	r, m := newTestRunner(srv)
	outcomes := r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
	assert.Equal(t, []string{outcomePass}, outcomes)
	assert.Equal(t, 1.0, pairsCount(m, queryBoundsCheck, outcomePass, classEmpty))
	assert.Empty(t, f.queries)
}

// TestBoundsCheckRefused: a refused bounds request is a refusal of the estimate, classed by its text.
func TestBoundsCheckRefused(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.estimateError = func() (int, string) {
		return http.StatusUnprocessableEntity, "the blocks of one range hold more than 4 distinct values of __name__ (the -querier.cardinality-estimate-max-groups limit)"
	}
	r, m := newTestRunner(srv)
	outcomes := r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
	assert.Equal(t, []string{outcomeRefusal}, outcomes)
	assert.Equal(t, 1.0, counterSum(m.refusals, map[string]string{"query": queryBoundsCheck, "side": sideEstimate, "class": classGroupsLimit}))
	assert.Empty(t, f.queries)
}

// TestBoundsCheckRefusedTruth: a truth that fails leaves the check with nothing compared, and a refusal.
func TestBoundsCheckRefusedTruth(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.truthHook = func(string, time.Duration, map[string]int64) int { return http.StatusTooManyRequests }
	r, m := newTestRunner(srv)
	outcomes := r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
	assert.Equal(t, outcomeRefusal, outcomes[0])
	assert.Equal(t, 1.0, counterSum(m.refusals, map[string]string{"query": queryBoundsCheck, "side": sideTruth, "class": classThrottled}))
}

// TestBoundsExactCheck: the names at the ranks are counted with the exact route over the range's window and compared with their bounds, with no PromQL.
func TestBoundsExactCheck(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.boundsSlack = 1
	var selectors []string
	f.estimateMatchHook = func(sel string) (int, string) {
		selectors = append(selectors, sel)
		return 0, ""
	}
	r, m := newTestRunner(srv)
	outcomes := r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
	assert.Equal(t, []string{outcomePass, outcomePass}, outcomes)
	assert.ElementsMatch(t, []string{`{__name__="http_requests_total"}`, `{__name__="up"}`, `{__name__="build_info"}`}, selectors)
	assert.Equal(t, 3.0, boundsGroups(m, queryBoundsExact, outcomePass, classWithinBounds))
	assert.Equal(t, 1.0, pairsCount(m, queryBoundsExact, outcomePass, classWithinBounds))
	// The relative widths are observed once, by the PromQL check.
	assert.Equal(t, uint64(2), histogramOf(t, m.boundsWidth).GetSampleCount())

	// An exact count outside the bounds is a violation, whether or not PromQL agrees with the bounds.
	f.boundsSlack = 0
	f.estimateHook = func(_ int, r *estimateResponse) {
		for i := range r.Groups {
			if r.Groups[i].Value == "up" {
				r.Groups[i].Count += 3
			}
		}
	}
	outcomes = r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
	assert.Equal(t, []string{outcomePass, outcomeMismatch}, outcomes)
	assert.Equal(t, 1.0, boundsGroups(m, queryBoundsExact, outcomeMismatch, classBoundsViolation))

	// The route's own flags explain a difference in their direction.
	f.estimateHook = func(_ int, r *estimateResponse) {
		r.UpperBound = true
		for i := range r.Groups {
			if r.Groups[i].Value == "up" {
				r.Groups[i].Count += 3
			}
		}
	}
	outcomes = r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
	assert.Equal(t, []string{outcomePass, outcomeFlagged}, outcomes)
	assert.Equal(t, 1.0, boundsGroups(m, queryBoundsExact, outcomeFlagged, classUpperBound))
	f.estimateHook = func(_ int, r *estimateResponse) {
		r.LowerBound = true
		for i := range r.Groups {
			r.Groups[i].Count--
		}
	}
	outcomes = r.boundsCheck(context.Background(), testWindow, testBoundsConfig)
	assert.Equal(t, []string{outcomePass, outcomeFlagged}, outcomes)
	assert.Equal(t, 3.0, boundsGroups(m, queryBoundsExact, outcomeFlagged, classIncomplete))
}

// TestBoundsRefusesMatchers: the fake answers bounds mode as the route does, with no matchers except one every series passes.
func TestBoundsRefusesMatchers(t *testing.T) {
	_, srv := newFakeFrontend(t, testSeries()...)
	r, _ := newTestRunner(srv)
	ask := func(selector string) error {
		params := url.Values{"mode": {"bounds"}, "start": {formatTime(testWindow.start)}, "end": {formatTime(testWindow.end)}, "match[]": {selector}}
		_, err := r.client.get(context.Background(), "/prometheus/api/v1/cardinality/estimate", params, true, &boundsResponse{})
		return err
	}
	require.NoError(t, ask(`{__name__=~".+"}`))
	var he *httpError
	require.ErrorAs(t, ask(`{job="api"}`), &he)
	assert.Equal(t, http.StatusBadRequest, he.status)
	assert.Equal(t, classBadRequest, classifyRefusal(sideEstimate, he))
}

// TestRunBoundsChecks: a round sends -bounds-checks checks on block or boundary windows, and a violation is a mismatch of the round.
func TestRunBoundsChecks(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	m := newMetrics(prometheus.NewRegistry(), time.Minute)
	cfg := config{
		address: srv.URL, tenant: "test-tenant", pairsPerRound: 1, timeout: time.Minute,
		queries: []string{queryMetricTotal},
		windows: windowConfig{lengths: []time.Duration{time.Hour}, queryIngestersWithin: 6 * time.Hour, lag: 5 * time.Minute, blocksMargin: time.Hour},
		targets: testTargets(), targetWindow: 6 * time.Hour,
		boundsChecks: 2, boundsMaxRanges: 4, seed: 1,
	}
	now := func() time.Time { return time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC) }
	sum, err := run(context.Background(), cfg, m, log.NewNopLogger(), now)
	require.NoError(t, err)
	assert.False(t, sum.mismatched)
	assert.Equal(t, 2.0, counterSum(m.pairs, map[string]string{"query": queryBoundsCheck, "outcome": outcomePass}))
	assert.Equal(t, 2.0, counterSum(m.pairs, map[string]string{"query": queryBoundsExact, "outcome": outcomePass}))
	assert.Equal(t, 1+4, sum.compared, "the drawn pair, and a pass of each check")

	// The checks don't run with -bounds-checks 0.
	m = newMetrics(prometheus.NewRegistry(), time.Minute)
	cfg.boundsChecks = 0
	_, err = run(context.Background(), cfg, m, log.NewNopLogger(), now)
	require.NoError(t, err)
	assert.Zero(t, counterSum(m.pairs, map[string]string{"query": queryBoundsCheck}))

	// A bounds answer that excludes a group makes the round a mismatch.
	f.boundsHook = func(n int, r *boundsResponse) {
		if n > 2 && r.GroupBy == "__name__" && len(r.Ranges) > 0 {
			for i := range r.Ranges {
				r.Ranges[i].Groups = r.Ranges[i].Groups[1:]
			}
		}
	}
	cfg.boundsChecks = 1
	sum, err = run(context.Background(), cfg, newMetrics(prometheus.NewRegistry(), time.Minute), log.NewNopLogger(), now)
	require.NoError(t, err)
	assert.True(t, sum.mismatched)
}

// TestBoundsCheckPerValueSkipsRefusedName: a name whose PromQL truth a query limit refuses is skipped and recorded, and the other names are still checked.
func TestBoundsCheckPerValueSkipsRefusedName(t *testing.T) {
	f, srv := newFakeFrontend(t, testSeries()...)
	f.truthHook = func(q string, _ time.Duration, _ map[string]int64) int {
		if strings.Contains(q, `"http_requests_total"`) {
			return http.StatusUnprocessableEntity
		}
		return 0
	}
	r, m := newTestRunner(srv)
	cfg := testBoundsConfig
	cfg.ranks, cfg.perValue = []int{1, 2}, true
	outcomes := r.boundsCheck(context.Background(), testWindow, cfg)
	assert.Equal(t, []string{outcomePass, outcomePass}, outcomes)
	assert.Equal(t, 1.0, boundsGroups(m, queryBoundsCheck, outcomePass, classWithinBounds), "up is checked")
	assert.Equal(t, 1.0, counterSum(m.refusals, map[string]string{"query": queryBoundsCheck, "side": sideTruth, "class": classPromQLLimit}))
	assert.Equal(t, 2.0, boundsGroups(m, queryBoundsExact, outcomePass, classWithinBounds), "the exact route needs no PromQL")

	f.truthHook = func(string, time.Duration, map[string]int64) int { return http.StatusUnprocessableEntity }
	outcomes = r.boundsCheck(context.Background(), testWindow, cfg)
	assert.Equal(t, outcomeRefusal, outcomes[0], "every name refused refuses the check")
}

// TestLogBoundsAnswer: one line per range with its blocks, totals and width, and the run's first bounds request marked as cold.
func TestLogBoundsAnswer(t *testing.T) {
	var buf strings.Builder
	logger := log.NewLogfmtLogger(&buf)
	ans := &boundsResponse{Ranges: []boundsRange{{Start: 0, End: 7200000, MainSet: true, MainBlocks: 4, OtherBlocks: 2, GroupCount: 3, LowerTotal: 100, UpperTotal: 110}}, first: true}
	logBoundsAnswer(logger, queryBoundsCheck, testWindow, ans, time.Second)
	ans.first = false
	logBoundsAnswer(logger, queryBoundsCheck, testWindow, ans, time.Second)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 2)
	for _, want := range []string{`msg="bounds range"`, "purpose=bounds_check", "range=[0,7200000)", "main_blocks=4", "other_blocks=2", "lower_total=100", "upper_total=110", "total_width=0.1", "latency=1s", "first=true"} {
		assert.Contains(t, lines[0], want)
	}
	assert.Contains(t, lines[1], "first=false")
}

// TestBoundsFirstRequest: only the run's first bounds request, whichever sent it, is marked first.
func TestBoundsFirstRequest(t *testing.T) {
	_, srv := newFakeFrontend(t, testSeries()...)
	r, _ := newTestRunner(srv)
	a, _, err := r.client.bounds(context.Background(), "__name__", testWindow.start, testWindow.end, 0)
	require.NoError(t, err)
	b, _, err := r.client.bounds(context.Background(), "__name__", testWindow.start, testWindow.end, 0)
	require.NoError(t, err)
	assert.True(t, a.first)
	assert.False(t, b.first)
}
