// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
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

	// Which of the pair's requests went first. The first may warm caches for the second, but neither starts cold: target picking and earlier pairs read the same indexes.
	positionFirst  = "first"
	positionSecond = "second"
	// positionAlone is an estimate sent without its truth, with -estimate-only.
	positionAlone = "alone"
	// orderEstimateOnly is the order of a pair whose truth isn't sent.
	orderEstimateOnly = "estimate_only"

	// How the truth was asked for.
	referenceGrouped  = "grouped"   // one query for the whole table
	referencePerValue = "per_value" // one query per group, for a table whose grouped truth was refused
	referenceNone     = "none"      // no truth was sent

	// queryFacetProbe labels the refusals of facet probes during target picking.
	queryFacetProbe = "facet_probe"
	queryTargets    = "targets" // the bounds requests target picking reads names and values from
)

type runner struct {
	client *client
	m      *metrics
	logger log.Logger
	// gap separates consecutive requests, the rate agreed with the cell's owners.
	gap time.Duration
	// edgeSlack is how far before the window the edge probe looks for series that may be counted without a sample in it.
	edgeSlack time.Duration
	// fallbackNames is how many of the estimate's largest names the truth counts one at a time when the grouped truth is refused.
	fallbackNames int
	// maxTruthSeries, when > 0, is the most series a PromQL truth may count, going by the estimate. A truth over it is not sent: query sharding keeps each shard under the per-query limits, so nothing else stops a count of millions of series from filling the queriers.
	maxTruthSeries int64
	pairs          int
}

type pairResult struct {
	Pair     string      `json:"pair"`
	Order    string      `json:"order"`
	Outcome  string      `json:"outcome"`
	Class    string      `json:"class"`
	Refusals []string    `json:"refusals,omitempty"`
	Diffs    []groupDiff `json:"diffs,omitempty"`
	// Read, Source and Flags are the estimate's, when it answered.
	Read   string   `json:"read,omitempty"`
	Source string   `json:"source,omitempty"`
	Flags  []string `json:"flags,omitempty"`

	latency latencies
	// refused holds the side and class of each refusal the refusals counter counts.
	refused   [][2]string
	reference string
	// passed counts the groups of a bounds check within their bounds, which Diffs leaves out, and widths holds their relative widths.
	passed int
	widths []float64
}

// latencies are the end-to-end latencies of a pair's two sides, each summed over its queries. A side that failed has none.
type latencies struct {
	estimate, truth       time.Duration
	estimateOK, truthOK   bool
	estimatePos, truthPos string
}

// truthSet is how the truth is asked for: grouped, or one query per value of label when values is set.
type truthSet struct {
	label  string
	values []string
	// fromEstimate asks for the estimate's largest groups one at a time, picked once the estimate answered.
	fromEstimate bool
}

func (s truthSet) restrict() map[string]bool {
	if s.values == nil {
		return nil
	}
	out := make(map[string]bool, len(s.values))
	for _, n := range s.values {
		out[n] = true
	}
	return out
}

// restrictFor is the set's values that truth answered: a value it skipped has no truth to compare with.
func (s truthSet) restrictFor(truth answer) map[string]bool {
	out := s.restrict()
	for v := range truth.skipped {
		delete(out, v)
	}
	return out
}

// skippedRefusals names the values truth skipped, each with its refusal class, sorted.
func skippedRefusals(truth answer) [][2]string {
	out := make([][2]string, 0, len(truth.skipped))
	for v, class := range truth.skipped {
		out = append(out, [2]string{v, class})
	}
	slices.SortFunc(out, func(x, y [2]string) int { return strings.Compare(x[0], y[0]) })
	return out
}

// largestSet asks for the estimate's -fallback-names largest groups one at a time, leaving out the groups over -max-truth-series.
func (r *runner) largestSet(p pair, est *estimateResponse) truthSet {
	label := p.groupBy
	if label == "" {
		label = model.MetricNameLabel
	}
	return truthSet{label: label, values: largest(est, r.fallbackNames, r.maxTruthSeries)}
}

// run compares one pair, reruns it once if it mismatches, and records the result.
func (r *runner) run(ctx context.Context, p pair) pairResult {
	// A table asked for one group at a time needs the estimate's groups first, and so does the check against -max-truth-series.
	estimateFirst := r.pairs%2 == 0 || p.perValue || r.maxTruthSeries > 0
	r.pairs++
	res := pairResult{Pair: p.String(), Order: "estimate_first"}
	if !estimateFirst {
		res.Order = "promql_first"
	}
	// A pair cut short by shutdown would record its cancelled requests as refusals.
	defer func() {
		if ctx.Err() == nil {
			r.record(p, &res)
		}
	}()
	res.reference = referenceGrouped
	first := truthSet{fromEstimate: p.perValue}
	if p.perValue {
		res.reference = referencePerValue
	}

	est, truth, lat, set, refusal := r.both(ctx, p, estimateFirst, first)
	res.latency = lat
	if est != nil {
		res.Read, res.Source, res.Flags = est.Read, est.Source, est.flags()
	}
	if refusal != nil {
		res.Refusals = append(res.Refusals, refusal.side+":"+refusal.class)
		res.refused = append(res.refused, [2]string{refusal.side, refusal.class})
		// The grouped truth hit a per-query limit or is over -max-truth-series: count the largest groups that fit one at a time, on the same window.
		if refusal.side == sideTruth && (refusal.class == classPromQLLimit || refusal.class == classTruthTooLarge) && set.values == nil && est != nil && len(r.largestSet(p, est).values) > 0 {
			set = r.largestSet(p, est)
			res.reference = referencePerValue
			r.sleep(ctx)
			var err error
			truth, res.latency.truth, err = r.truth(ctx, p, set, 0)
			res.latency.truthOK, res.latency.truthPos = err == nil, positionSecond
			if err != nil {
				class := classifyRefusal(sideTruth, err)
				res.Refusals = append(res.Refusals, sideTruth+"_per_value:"+class)
				res.refused = append(res.refused, [2]string{sideTruth, class})
				res.Outcome, res.Class = outcomeRefusal, class
				level.Warn(r.logger).Log("msg", "refused", "pair", res.Pair, "side", "promql per value", "class", class, "err", err)
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
	for _, s := range skippedRefusals(truth) {
		res.Refusals = append(res.Refusals, sideTruth+"_per_value "+s[0]+":"+s[1])
		res.refused = append(res.refused, [2]string{sideTruth, s[1]})
	}
	diffs := compareAnswers(est, estA, truth, set.restrictFor(truth))
	if hasMismatch(diffs) {
		est, truth, diffs = r.explain(ctx, p, estimateFirst, set, est, truth, diffs, &res)
	}
	// A tenant-wide table truthed one group at a time checks neither group_count nor total against a truth. Every group it counts has its value in the index of a block or head over the window, so group_count can't be above the label's values there, plus the "" group of series without the label. total has no such cheap reference: any count of it reads every series.
	if set.values != nil && p.selector() == allSeries {
		allowed := 0
		if set.label != model.MetricNameLabel {
			allowed = 1
		}
		r.sleep(ctx)
		// A page longer than group_count already bounds it, so the page stops there.
		values, err := r.client.labelValues(ctx, set.label, p.window.start, p.window.end, est.GroupCount+1)
		if err != nil {
			class := classifyRefusal(sideTruth, err)
			res.Refusals = append(res.Refusals, "label values probe:"+class)
			res.refused = append(res.refused, [2]string{sideTruth, class})
		} else if bound := len(values) + allowed; est.GroupCount > bound {
			diffs = append(diffs, groupDiff{Value: groupCountKey, Estimate: int64(est.GroupCount), Truth: int64(bound), Outcome: outcomeMismatch, Class: classGroupCountHigh})
		}
	}
	// An incomplete answer still counts every series of the part of the window it says it covered, so it can't be below a truth over that part.
	if est.incomplete() && slices.ContainsFunc(diffs, func(d groupDiff) bool { return d.Class == classIncomplete }) {
		if from, to, ok := est.covered(p.window.start, p.window.end); ok {
			r.sleep(ctx)
			part := p
			part.window.start, part.window.end = from, to
			bound, _, err := r.truth(ctx, part, set, 0)
			if err != nil {
				res.Refusals = append(res.Refusals, "covered part probe:"+classifyRefusal(sideTruth, err))
			} else {
				for i, d := range diffs {
					if d.Class == classIncomplete && d.Estimate < truthValue(bound, d.Value) {
						diffs[i].Outcome, diffs[i].Class = outcomeMismatch, classIncompleteUnder
					}
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
	est2, truth2, _, _, refusal := r.both(ctx, p, estimateFirst, set)
	if refusal != nil {
		res.Refusals = append(res.Refusals, "rerun "+refusal.side+":"+refusal.class)
	} else {
		est, truth = est2, truth2
		// The rerun is what counts. A group that mismatched and no longer does changed between the calls: the data did if its truth changed, and the estimate isn't deterministic if only the estimate did. One that still mismatches stays a mismatch, with the rerun's counts.
		// A value either truth skipped has nothing to compare.
		restrict := set.restrictFor(truth2)
		for v := range truth.skipped {
			delete(restrict, v)
		}
		rerun := compareAnswers(est2, estimateAnswer(est2), truth2, restrict)
		still := map[string]bool{}
		for _, d := range rerun {
			still[d.Value] = true
		}
		for _, d := range diffs {
			if d.Outcome == outcomeMismatch && !still[d.Value] {
				d.Outcome, d.Class = outcomeTolerated, classAnswerChanged
				if truthValue(truth2, d.Value) == d.Truth {
					d.Outcome, d.Class = outcomeMismatch, classUnstable
				}
				rerun = append(rerun, d)
			}
		}
		diffs = rerun
	}

	// A group above the truth may count series with a chunk that overlaps the window but no sample PromQL sees in it: their last samples are just before it and their staleness marker in it, or they have a gap across it. Each such series has a sample just before the window, and series that start after it can't be counted, so the probe counts only the series seen before the window and not in it.
	if r.edgeSlack > 0 && needsEdgeProbe(diffs) {
		r.sleep(ctx)
		edge, _, err := r.truth(ctx, p, set, r.edgeSlack)
		if err != nil {
			res.Refusals = append(res.Refusals, "edge probe:"+classifyRefusal(sideTruth, err))
		} else {
			kept := diffs[:0]
			for _, d := range diffs {
				allowed := truthValue(edge, d.Value)
				if d.Value == groupCountKey {
					// Only an edge group the window's truth lacks adds a group.
					allowed = 0
					for k, c := range edge.counts {
						if c > 0 && truth.counts[k] == 0 {
							allowed++
						}
					}
				}
				if d.Class == classUnclassified && d.Estimate > d.Truth && d.Estimate-d.Truth <= allowed {
					d.Outcome, d.Class = outcomeTolerated, classEdge
				}
				kept = append(kept, d)
			}
			diffs = kept
		}
	}
	return est, truth, diffs
}

type refusal struct {
	side, class string
	err         error
}

// both sends the two requests of a pair in the given order. It returns the set the truth was asked for with, which a set fromEstimate resolves to; when the estimate failed, such a truth isn't sent.
func (r *runner) both(ctx context.Context, p pair, estimateFirst bool, set truthSet) (*estimateResponse, answer, latencies, truthSet, *refusal) {
	var (
		est      *estimateResponse
		truth    answer
		lat      latencies
		estErr   error
		truthErr error
	)
	doEstimate := func(pos string) {
		est, lat.estimate, estErr = r.client.estimate(ctx, p.selector(), p.groupBy, p.window.start, p.window.end, p.limit)
		lat.estimateOK, lat.estimatePos = estErr == nil, pos
	}
	doTruth := func(pos string) {
		if set.fromEstimate {
			if est == nil {
				return
			}
			set = r.largestSet(p, est)
			if len(set.values) == 0 {
				truthErr = errTruthTooLarge
				return
			}
		} else if set.values == nil && r.maxTruthSeries > 0 && est != nil && est.Total > r.maxTruthSeries {
			truthErr = errTruthTooLarge
			return
		}
		truth, lat.truth, truthErr = r.truth(ctx, p, set, 0)
		lat.truthOK, lat.truthPos = truthErr == nil, pos
	}
	if estimateFirst {
		doEstimate(positionFirst)
		r.sleep(ctx)
		doTruth(positionSecond)
	} else {
		doTruth(positionFirst)
		r.sleep(ctx)
		doEstimate(positionSecond)
	}
	switch {
	case estErr != nil:
		return nil, truth, lat, set, &refusal{sideEstimate, classifyRefusal(sideEstimate, estErr), estErr}
	case truthErr != nil:
		return est, truth, lat, set, &refusal{sideTruth, classifyRefusal(sideTruth, truthErr), truthErr}
	}
	return est, truth, lat, set, nil
}

// truth counts with PromQL what the estimate counts. The estimate counts [start, end); an instant query at end-1ms with a range of the window's length selects (start-1ms, end-1ms], the same milliseconds. With edgeSlack > 0 it counts instead the series with a sample in the edgeSlack before the window and none in it (edgeQuery).
// It returns the latency summed over its queries.
func (r *runner) truth(ctx context.Context, p pair, set truthSet, edgeSlack time.Duration) (answer, time.Duration, error) {
	build := truthQuery
	if edgeSlack > 0 {
		build = func(ms []*labels.Matcher, label string, w time.Duration) string {
			return edgeQuery(ms, label, w, edgeSlack)
		}
	}
	out := answer{counts: map[string]int64{}}
	var total time.Duration
	query := func(q, label string, atMS int64) (map[string]int64, error) {
		counts, latency, err := r.client.instantQuery(ctx, q, label, atMS)
		total += latency
		return counts, err
	}
	if set.values != nil {
		// A value over a PromQL limit is skipped, so the other values are still compared; the truth fails only when every value is.
		var lastLimit error
		for i, v := range set.values {
			if i > 0 {
				r.sleep(ctx)
			}
			// The value's equality matcher replaces the selector's matchers on its label.
			ms := append([]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, set.label, v)}, withoutMatchersOn(p.matchers, set.label)...)
			counts, err := query(build(ms, set.label, p.window.length()), set.label, p.window.end-1)
			if err != nil {
				if class := classifyRefusal(sideTruth, err); class == classPromQLLimit {
					if out.skipped == nil {
						out.skipped = map[string]string{}
					}
					out.skipped[v] = class
					lastLimit = err
					continue
				}
				return out, total, err
			}
			out.counts[v] = counts[v]
		}
		if len(set.values) > 0 && len(out.skipped) == len(set.values) {
			return out, total, lastLimit
		}
		return out, total, nil
	}
	counts, err := query(build(p.matchers, p.groupBy, p.window.length()), p.groupBy, p.window.end-1)
	out.counts = counts
	return out, total, err
}

// estimateOnly sends only the pair's estimate, without its truth, and records its latency, flags or refusal. It returns the outcome.
func (r *runner) estimateOnly(ctx context.Context, p pair) string {
	est, latency, err := r.client.estimate(ctx, p.selector(), p.groupBy, p.window.start, p.window.end, p.limit)
	if ctx.Err() != nil {
		return ""
	}
	res := pairResult{Pair: p.String(), Order: orderEstimateOnly, reference: referenceNone}
	if err != nil {
		class := classifyRefusal(sideEstimate, err)
		res.Outcome, res.Class = outcomeRefusal, class
		res.Refusals = []string{sideEstimate + ":" + class}
		res.refused = [][2]string{{sideEstimate, class}}
		level.Warn(r.logger).Log("msg", "refused", "pair", res.Pair, "side", sideEstimate, "class", class, "err", err)
	} else {
		res.Outcome = outcomeAnswered
		res.Read, res.Source, res.Flags = est.Read, est.Source, est.flags()
		res.latency = latencies{estimate: latency, estimateOK: true, estimatePos: positionAlone}
	}
	r.record(p, &res)
	return res.Outcome
}

// burst sends n estimates at once, drawn in turn from cands, without their truth, so the count limits fill. It records how each was answered and how long it took.
func (r *runner) burst(ctx context.Context, cands []pair, n int) {
	if len(cands) == 0 {
		return
	}
	r.sendBurst(n, func(i int) (string, error, time.Duration) {
		p := cands[i%len(cands)]
		_, latency, err := r.client.estimate(ctx, p.selector(), p.groupBy, p.window.start, p.window.end, p.limit)
		return p.query, err, latency
	})
}

// burstBounds sends n bounds requests over w at once, without their truth. Their head rows go through the ingesters' label values cardinality RPC, so they fill the store-gateways' count limit and not the ingesters'.
func (r *runner) burstBounds(ctx context.Context, w window, n int) {
	r.sendBurst(n, func(int) (string, error, time.Duration) {
		_, latency, err := r.client.bounds(ctx, model.MetricNameLabel, w.start, w.end, 0)
		return queryBoundsBurst, err, latency
	})
}

// sendBurst runs send for 0 to n-1 at once and records how each request was answered and how long it took.
func (r *runner) sendBurst(n int, send func(i int) (query string, err error, latency time.Duration)) {
	if n <= 0 {
		return
	}
	type answered struct {
		query, status string
		latency       time.Duration
		err           error
	}
	results := make([]answered, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			query, err, latency := send(i)
			results[i] = answered{query, burstStatus(err), latency, err}
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
	level.Info(r.logger).Log("msg", "burst", "query", results[0].query, "requests", n, "statuses", fmt.Sprint(statuses), "slowest", slowest)
	// One line per status that isn't 200, with the first error of it, so a refusal's reason is in the log.
	logged := map[string]bool{}
	for _, a := range results {
		if a.err != nil && !logged[a.status] {
			logged[a.status] = true
			level.Warn(r.logger).Log("msg", "burst request failed", "query", a.query, "status", a.status, "requests", statuses[a.status], "err", truncate(a.err.Error(), 400))
		}
	}
}

// burstStatus is how one request of a burst was answered.
func burstStatus(err error) string {
	var he *httpError
	var ne net.Error
	switch {
	case err == nil:
		return strconv.Itoa(200)
	case errors.As(err, &he) && isBusy(he):
		return classBusy
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
	r.m.pairs.WithLabelValues(p.query, p.window.kind, res.Order, res.Outcome, res.Class, res.Read, res.Source).Inc()
	length := model.Duration(p.window.length()).String()
	if l := res.latency; l.estimateOK {
		r.m.duration.WithLabelValues(p.query, p.window.kind, length, sideEstimate, l.estimatePos, res.reference, res.Read, res.Source).Observe(l.estimate.Seconds())
	}
	if l := res.latency; l.truthOK {
		r.m.duration.WithLabelValues(p.query, p.window.kind, length, sideTruth, l.truthPos, res.reference, res.Read, res.Source).Observe(l.truth.Seconds())
	}
	for _, f := range res.Flags {
		r.m.flags.WithLabelValues(p.query, f).Inc()
	}
	for _, rf := range res.refused {
		r.m.refusals.WithLabelValues(p.query, rf[0], rf[1]).Inc()
	}
	for _, d := range res.Diffs {
		r.m.groups.WithLabelValues(p.query, d.Outcome, d.Class).Inc()
	}
	if res.passed > 0 {
		r.m.groups.WithLabelValues(p.query, outcomePass, classWithinBounds).Add(float64(res.passed))
	}
	for _, w := range res.widths {
		r.m.boundsWidth.Observe(w)
	}
	const maxLogged = 20
	logger := log.With(r.logger, "pair", res.Pair, "order", res.Order, "outcome", res.Outcome, "class", res.Class, "read", res.Read, "source", res.Source, "flags", strings.Join(res.Flags, ","))
	switch res.Outcome {
	case outcomeAnswered:
		level.Info(logger).Log("msg", "estimated")
		return
	case outcomeMismatch:
	default:
		level.Info(logger).Log("msg", "compared", "differing_groups", len(res.Diffs))
		return
	}
	diffs := res.Diffs
	if len(diffs) > maxLogged {
		diffs = diffs[:maxLogged]
	}
	// The grouped truth is one query, which can be run by hand at truth_at to compare.
	if res.reference == referenceGrouped {
		logger = log.With(logger, "truth_query", truthQuery(p.matchers, p.groupBy, p.window.length()), "truth_at", formatTime(p.window.end-1))
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

// needsEdgeProbe reports whether a group is above the truth unexplained, which edge series may explain.
func needsEdgeProbe(diffs []groupDiff) bool {
	for _, d := range diffs {
		if d.Class == classUnclassified && d.Estimate > d.Truth {
			return true
		}
	}
	return false
}

// largest returns up to n of the table's values, largest first, without the pseudo-groups.
// largest returns the first n groups of r with at most maxSeries series, or of any size when maxSeries is 0. It is never nil, since a nil set asks for the grouped truth.
func largest(r *estimateResponse, n int, maxSeries int64) []string {
	out := []string{}
	for _, g := range r.Groups {
		if len(out) == n {
			break
		}
		if maxSeries > 0 && g.Count > maxSeries {
			continue
		}
		out = append(out, g.Value)
	}
	return out
}

func withoutMatchersOn(ms []*labels.Matcher, label string) []*labels.Matcher {
	var out []*labels.Matcher
	for _, m := range ms {
		if m.Name != label {
			out = append(out, m)
		}
	}
	return out
}

// truncate cuts s to at most n bytes.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
