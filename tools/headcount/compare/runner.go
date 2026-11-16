// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

// Sides of a pair, and which of them went first.
const (
	sideEstimate = "estimate"
	sideTruth    = "promql"

	cacheCold = "cold" // the request went first, so it warmed the index-headers and postings cache for the other
	cacheWarm = "warm"
)

type runner struct {
	client *client
	cfg    classifyConfig
	m      *metrics
	logger log.Logger
	// gap separates consecutive requests, the rate agreed with the cell's owners.
	gap time.Duration
	// edgeSlack widens the window on both sides when probing whether a group is above the truth because of its edges.
	edgeSlack time.Duration
	// fallbackNames is how many of the estimate's largest names the truth counts one at a time when the grouped truth is refused.
	fallbackNames int
	// headSpan is how far back from now every series in an ingester head has a sample; it bounds a head_whole answer.
	headSpan time.Duration
	now      func() time.Time
	pairs    int
}

type pairResult struct {
	Pair     string      `json:"pair"`
	Order    string      `json:"order"`
	Outcome  string      `json:"outcome"`
	Class    string      `json:"class"`
	Refusals []string    `json:"refusals,omitempty"`
	Diffs    []groupDiff `json:"diffs,omitempty"`
	// Estimated holds every estimated group, equal to its truth or not.
	Estimated []groupDiff `json:"estimated,omitempty"`
}

// truthSet is how the truth was asked for: grouped, or per name when perName is set.
type truthSet struct {
	perName []string
}

func (s truthSet) restrict() map[string]bool {
	if s.perName == nil {
		return nil
	}
	out := make(map[string]bool, len(s.perName))
	for _, n := range s.perName {
		out[n] = true
	}
	return out
}

// run compares one pair, reruns it once if it mismatches, and records the result.
func (r *runner) run(ctx context.Context, p pair) pairResult {
	estimateFirst := r.pairs%2 == 0
	r.pairs++
	res := pairResult{Pair: p.String(), Order: "estimate_first"}
	if !estimateFirst {
		res.Order = "promql_first"
	}
	defer r.record(p, &res)

	est, truth, set, refusal := r.both(ctx, p, estimateFirst, truthSet{}, true)
	if refusal != nil {
		res.Refusals = append(res.Refusals, refusal.side+":"+refusal.class)
		r.m.refusals.WithLabelValues(p.query, refusal.side, refusal.class).Inc()
		// The grouped truth was refused: count the largest names one at a time, on the same window.
		if refusal.side == sideTruth && refusal.class == classPromQLLimit && p.groupBy == model.MetricNameLabel && p.steps == 0 && est != nil {
			set = truthSet{perName: largest(est, r.fallbackNames)}
			r.sleep(ctx)
			var err error
			if truth, err = r.truth(ctx, p, set, 0, cacheWarm, true); err != nil {
				class := classifyRefusal(sideTruth, err)
				res.Refusals = append(res.Refusals, sideTruth+"_per_name:"+class)
				r.m.refusals.WithLabelValues(p.query, sideTruth, class).Inc()
				res.Outcome, res.Class = outcomeRefusal, class
				level.Warn(r.logger).Log("msg", "refused", "pair", res.Pair, "side", "promql per name", "class", class, "err", err)
				return res
			}
		} else {
			res.Outcome, res.Class = outcomeRefusal, refusal.class
			level.Warn(r.logger).Log("msg", "refused", "pair", res.Pair, "side", refusal.side, "class", refusal.class, "err", refusal.err)
			return res
		}
	}
	if est.Window.Start != p.window.start || est.Window.End != p.window.end {
		res.Diffs = []groupDiff{{Value: fmt.Sprintf("window [%d, %d)", est.Window.Start, est.Window.End), Outcome: outcomeMismatch, Class: classWindowEcho}}
		res.Outcome, res.Class = worst(res.Diffs)
		return res
	}

	estA := estimateAnswer(est)
	diffs := compareAnswers(est, estA, truth, set.restrict(), r.cfg)
	if hasMismatch(diffs) {
		est, truth, diffs = r.explain(ctx, p, estimateFirst, set, est, truth, diffs, &res)
		estA = estimateAnswer(est)
	}
	res.Estimated = sketchGroups(est, estA, truth, set.restrict())
	// The head holds no series without a sample in the last headSpan, so a whole-head count above a truth over it is wrong.
	if est.HeadWhole && r.headSpan > 0 && slices.ContainsFunc(diffs, func(d groupDiff) bool { return d.Class == classHeadWhole }) {
		r.sleep(ctx)
		now := r.now().UnixMilli()
		held := p
		held.window.start, held.window.end = min(p.window.start, now-r.headSpan.Milliseconds()), max(p.window.end, now)
		bound, err := r.truth(ctx, held, set, 0, cacheWarm, false)
		if err != nil {
			res.Refusals = append(res.Refusals, "head span probe:"+classifyRefusal(sideTruth, err))
		} else {
			for i, d := range diffs {
				if d.Class == classHeadWhole && d.Estimate > truthValue(bound, d.Value) {
					diffs[i].Outcome, diffs[i].Class = outcomeMismatch, classHeadExcess
				}
			}
		}
	}
	sort.Slice(diffs, func(i, j int) bool { return diffs[i].Value < diffs[j].Value })
	res.Diffs = diffs
	res.Outcome, res.Class = worst(diffs)
	if len(estA.counts) == 0 && len(truth.counts) == 0 {
		res.Class = classEmpty
	}
	return res
}

// explain reruns a pair with mismatching groups once, and probes the edges of the groups still above the truth. It returns the estimate the diffs are of.
func (r *runner) explain(ctx context.Context, p pair, estimateFirst bool, set truthSet, est *estimateResponse, truth answer, diffs []groupDiff, res *pairResult) (*estimateResponse, answer, []groupDiff) {
	// Rerun once: compaction or a late block can change the answer between the two calls.
	r.sleep(ctx)
	est2, truth2, _, refusal := r.both(ctx, p, estimateFirst, set, false)
	if refusal != nil {
		res.Refusals = append(res.Refusals, "rerun "+refusal.side+":"+refusal.class)
	} else {
		est, truth = est2, truth2
		// The rerun is what counts. A group that mismatched and no longer does changed between the calls; one that still mismatches stays a mismatch, with the rerun's counts.
		rerun := compareAnswers(est2, estimateAnswer(est2), truth2, set.restrict(), r.cfg)
		still := map[string]bool{}
		for _, d := range rerun {
			still[d.Value] = true
		}
		for _, d := range diffs {
			if d.Outcome == outcomeMismatch && !still[d.Value] {
				d.Outcome, d.Class = outcomeTolerated, classAnswerChanged
				rerun = append(rerun, d)
			}
		}
		diffs = rerun
	}

	// A group above the truth may count series whose samples are just outside the window, or whose only samples in it are staleness markers, which PromQL drops.
	if p.steps == 0 && r.edgeSlack > 0 && hasOvercount(diffs) {
		r.sleep(ctx)
		wide, err := r.truth(ctx, p, set, r.edgeSlack, cacheWarm, false)
		if err != nil {
			res.Refusals = append(res.Refusals, "edge probe:"+classifyRefusal(sideTruth, err))
		} else {
			for i, d := range diffs {
				if d.Outcome == outcomeMismatch && d.Estimate > d.Truth && d.Estimate <= wide.counts[d.Value] {
					diffs[i].Outcome, diffs[i].Class = outcomeTolerated, classEdge
				}
			}
		}
	}
	return est, truth, diffs
}

type refusal struct {
	side, class string
	err         error
}

// both sends the two requests of a pair in the given order. With observe, their latencies are recorded, labelled cold for the first and warm for the second.
func (r *runner) both(ctx context.Context, p pair, estimateFirst bool, set truthSet, observe bool) (*estimateResponse, answer, truthSet, *refusal) {
	var (
		est      *estimateResponse
		truth    answer
		estErr   error
		truthErr error
	)
	doEstimate := func(cache string) {
		var latency time.Duration
		est, latency, estErr = r.client.estimate(ctx, p.selector(), p.groupBy, p.window.start, p.window.end, p.step(), p.limit)
		if observe {
			r.m.duration.WithLabelValues(p.query, sideEstimate, cache).Observe(latency.Seconds())
		}
	}
	doTruth := func(cache string) {
		truth, truthErr = r.truth(ctx, p, set, 0, cache, observe)
	}
	if estimateFirst {
		doEstimate(cacheCold)
		r.sleep(ctx)
		doTruth(cacheWarm)
	} else {
		doTruth(cacheCold)
		r.sleep(ctx)
		doEstimate(cacheWarm)
	}
	switch {
	case estErr != nil:
		return nil, truth, set, &refusal{sideEstimate, classifyRefusal(sideEstimate, estErr), estErr}
	case truthErr != nil:
		return est, truth, set, &refusal{sideTruth, classifyRefusal(sideTruth, truthErr), truthErr}
	}
	return est, truth, set, nil
}

// truth counts with PromQL what the estimate counts. The estimate counts [start, end); an instant query at end-1ms with a range of the window's length selects (start-1ms, end-1ms], the same milliseconds. slack widens the window by that much on both sides.
func (r *runner) truth(ctx context.Context, p pair, set truthSet, slack time.Duration, cache string, observe bool) (answer, error) {
	out := answer{counts: map[string]int64{}}
	query := func(q, label string, atMS int64) (map[string]int64, error) {
		counts, latency, err := r.client.instantQuery(ctx, q, label, atMS)
		if observe {
			r.m.duration.WithLabelValues(p.query, sideTruth, cache).Observe(latency.Seconds())
		}
		return counts, err
	}
	if set.perName != nil {
		for i, name := range set.perName {
			if i > 0 {
				r.sleep(ctx)
			}
			ms := append([]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, name)}, withoutNameMatchers(p.matchers)...)
			counts, err := query(truthQuery(ms, model.MetricNameLabel, p.window.length()+2*slack), model.MetricNameLabel, p.window.end+slack.Milliseconds()-1)
			if err != nil {
				return out, err
			}
			out.counts[name] = counts[name]
		}
		return out, nil
	}
	if p.steps == 0 {
		counts, err := query(truthQuery(p.matchers, p.groupBy, p.window.length()+2*slack), p.groupBy, p.window.end+slack.Milliseconds()-1)
		out.counts = counts
		return out, err
	}
	step := p.step()
	for i := 0; i < p.steps; i++ {
		if i > 0 {
			r.sleep(ctx)
		}
		end := p.window.start + int64(i+1)*step.Milliseconds()
		counts, err := query(truthQuery(p.matchers, p.groupBy, step), p.groupBy, end-1)
		if err != nil {
			return out, err
		}
		for v, c := range counts {
			out.counts[bucketKey(v, i)] = c
		}
	}
	return out, nil
}

// burst sends n estimates at once, drawn in turn from cands, without their truth, so the count limits fill. It records how each was answered and how long it took.
func (r *runner) burst(ctx context.Context, cands []pair, n int) {
	if len(cands) == 0 || n <= 0 {
		return
	}
	type answered struct {
		query, status string
		latency       time.Duration
	}
	results := make([]answered, n)
	var wg sync.WaitGroup
	for i := range n {
		p := cands[i%len(cands)]
		wg.Add(1)
		go func() {
			defer wg.Done()
			est, latency, err := r.client.estimate(ctx, p.selector(), p.groupBy, p.window.start, p.window.end, p.step(), p.limit)
			results[i] = answered{p.query, burstStatus(est, err), latency}
		}()
	}
	wg.Wait()

	statuses := map[string]int{}
	var slowest time.Duration
	for _, a := range results {
		r.m.burstRequests.WithLabelValues(a.query, a.status).Inc()
		r.m.burstDuration.WithLabelValues(a.status).Observe(a.latency.Seconds())
		statuses[a.status]++
		slowest = max(slowest, a.latency)
	}
	r.m.bursts.Inc()
	level.Info(r.logger).Log("msg", "burst", "requests", n, "statuses", fmt.Sprint(statuses), "slowest", slowest)
}

// burstStatus is how one estimate of a burst was answered.
func burstStatus(est *estimateResponse, err error) string {
	var he *httpError
	var ne net.Error
	switch {
	case errors.As(err, new(*trailerError)):
		return "error_trailer"
	case err == nil:
		return strconv.Itoa(200)
	case errors.As(err, &he):
		return strconv.Itoa(he.status)
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	}
	return "error"
}

func (r *runner) sleep(ctx context.Context) {
	if r.gap <= 0 {
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(r.gap):
	}
}

func (r *runner) record(p pair, res *pairResult) {
	r.m.pairs.WithLabelValues(p.query, p.window.kind, res.Order, res.Outcome, res.Class).Inc()
	for _, d := range res.Diffs {
		r.m.groups.WithLabelValues(p.query, d.Outcome, d.Class).Inc()
	}
	groupBy := "label_value"
	if p.groupBy == model.MetricNameLabel {
		groupBy = "metric"
	}
	for _, d := range res.Estimated {
		if d.Truth > 0 {
			r.m.sketchError.WithLabelValues(groupBy).Observe(math.Abs(float64(d.Estimate-d.Truth)) / float64(d.Truth))
		}
	}
	const maxLogged = 20
	logger := log.With(r.logger, "pair", res.Pair, "order", res.Order, "outcome", res.Outcome, "class", res.Class)
	if len(res.Estimated) > 0 {
		logger = log.With(logger, "estimated_groups", len(res.Estimated), "estimated", fmt.Sprintf("%+v", res.Estimated[:min(len(res.Estimated), maxLogged)]))
	}
	if res.Outcome != outcomeMismatch {
		level.Info(logger).Log("msg", "compared", "differing_groups", len(res.Diffs))
		return
	}
	diffs := res.Diffs
	if len(diffs) > maxLogged {
		diffs = diffs[:maxLogged]
	}
	level.Warn(logger).Log("msg", "mismatch no class explains; file a Bug issue", "differing_groups", len(res.Diffs), "diffs", fmt.Sprintf("%+v", diffs), "refusals", fmt.Sprint(res.Refusals))
}

func hasMismatch(diffs []groupDiff) bool {
	for _, d := range diffs {
		if d.Outcome == outcomeMismatch {
			return true
		}
	}
	return false
}

func hasOvercount(diffs []groupDiff) bool {
	for _, d := range diffs {
		if d.Outcome == outcomeMismatch && d.Estimate > d.Truth {
			return true
		}
	}
	return false
}

// largest returns up to n of the table's values, largest first, without the pseudo-groups.
func largest(r *estimateResponse, n int) []string {
	var out []string
	for _, g := range r.Groups {
		if len(out) == n {
			break
		}
		out = append(out, g.Value)
	}
	return out
}

func withoutNameMatchers(ms []*labels.Matcher) []*labels.Matcher {
	var out []*labels.Matcher
	for _, m := range ms {
		if m.Name != model.MetricNameLabel {
			out = append(out, m)
		}
	}
	return out
}
