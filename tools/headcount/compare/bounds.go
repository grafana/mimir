// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
)

// Names of the bounds checks, in the query label.
const (
	queryBoundsCheck = "bounds_check"       // the bounds answer against PromQL, on the window of each range answered
	queryBoundsExact = "bounds_exact_check" // the bounds answer against the exact route, for sampled names
	queryBoundsBurst = "bounds_burst"       // a bounds request of a burst, sent without its truth
)

// Classes of a group of a bounds check. A group within the bounds of its range is a pass, and one outside them a mismatch unless the range or the edge explains it.
const (
	classWithinBounds    = "within_bounds"    // lower <= truth <= upper
	classBoundsViolation = "bounds_violation" // the truth is outside the bounds, and no flag or edge series explains it
)

// orderBounds is the order of a bounds check: the bounds request goes first, then the truth of each range.
const orderBounds = "bounds"

// boundsConfig shapes a bounds check.
type boundsConfig struct {
	// ranks pick the names compared when the tenant is too large for the grouped truth, and for the exact check.
	ranks []int
	// maxRanges is how many of the answer's newest block ranges are checked.
	maxRanges int
	// perValue counts the names at ranks one at a time instead of the whole table, for a tenant above -max-reference-series.
	perValue bool
}

// boundsAnswer holds what a check of one pair found, to be recorded as the pair's result.
type boundsAnswer struct {
	diffs   []groupDiff
	passed  int
	widths  []float64
	refused [][2]string
	notes   []string
}

func (a *boundsAnswer) refuse(side, class, note string) {
	a.refused = append(a.refused, [2]string{side, class})
	a.notes = append(a.notes, note+":"+class)
}

// boundsCheck sends a bounds request over w and checks each range it answers two ways: against PromQL on the range's window, and, for names at the configured ranks, against the exact route. It returns the outcome of each check.
func (r *runner) boundsCheck(ctx context.Context, w window, cfg boundsConfig) []string {
	p := pair{query: queryBoundsCheck, matchers: allSeriesMatchers(), groupBy: model.MetricNameLabel, window: w}
	res := pairResult{Pair: p.String(), Order: orderBounds, reference: referenceGrouped}
	if cfg.perValue {
		res.reference = referencePerValue
	}
	exactRes := pairResult{Pair: fmt.Sprintf("%s by %s over %s %s", queryBoundsExact, model.MetricNameLabel, w.kind, model.Duration(w.length())), Order: orderBounds, reference: referenceNone}
	exactPair := pair{query: queryBoundsExact, matchers: allSeriesMatchers(), groupBy: model.MetricNameLabel, window: w}
	// A check cut short by shutdown would record its cancelled requests as refusals.
	defer func() {
		if ctx.Err() == nil {
			r.record(p, &res)
			if exactRes.Outcome != "" {
				r.record(exactPair, &exactRes)
			}
		}
	}()

	limit := 0
	if cfg.perValue {
		limit = slices.Max(cfg.ranks)
	}
	ans, latency, err := r.client.bounds(ctx, model.MetricNameLabel, w.start, w.end, limit)
	if err != nil {
		class := classifyRefusal(sideEstimate, err)
		res.Outcome, res.Class = outcomeRefusal, class
		res.Refusals = []string{sideEstimate + ":" + class}
		res.refused = [][2]string{{sideEstimate, class}}
		level.Warn(r.logger).Log("msg", "refused", "pair", res.Pair, "side", sideEstimate, "class", class, "err", err)
		return []string{res.Outcome}
	}
	res.Read, res.Source, res.Flags = readBounds, "blocks", boundsFlags(ans)
	if ans.Head != nil {
		res.Source = "blocks+head"
	}
	res.latency = latencies{estimate: latency, estimateOK: true, estimatePos: positionFirst, truthOK: true, truthPos: positionSecond}
	logBoundsAnswer(r.logger, queryBoundsCheck, w, ans, latency)

	var ranges []boundsRange
	for _, rng := range ans.Ranges {
		if len(rng.Groups) > 0 {
			ranges = append(ranges, rng)
		}
	}
	if len(ranges) > cfg.maxRanges {
		ranges = ranges[len(ranges)-cfg.maxRanges:]
	}
	if len(ranges) == 0 {
		res.Outcome, res.Class = outcomePass, classEmpty
		return []string{res.Outcome}
	}

	var promql, exact boundsAnswer
	for _, rng := range ranges {
		r.sleep(ctx)
		truthLatency, ok := r.checkRangeAgainstPromQL(ctx, w, rng, cfg, &promql, &res)
		res.latency.truth += truthLatency
		res.latency.truthOK = res.latency.truthOK && ok
	}
	for _, rng := range ranges {
		r.checkRangeAgainstExact(ctx, rng, cfg, &exact, &exactRes)
	}
	r.finishBounds(&res, promql, true)
	outcomes := []string{res.Outcome}
	if len(exact.diffs)+exact.passed+len(exact.refused) > 0 {
		r.finishBounds(&exactRes, exact, false)
		exactRes.Read, exactRes.Source = res.Read, res.Source
		exactRes.Flags = res.Flags
		outcomes = append(outcomes, exactRes.Outcome)
	}
	return outcomes
}

// logBoundsAnswer logs one line per block range of a bounds answer, and one for its head rows: the blocks behind each range, its totals and their relative width, and the request's latency. purpose is what the request was sent for, and first marks the run's first bounds request, read with cold caches.
func logBoundsAnswer(logger log.Logger, purpose string, w window, ans *boundsResponse, latency time.Duration) {
	for _, rng := range ans.Ranges {
		width := 0.0
		if rng.LowerTotal > 0 {
			width = float64(rng.UpperTotal-rng.LowerTotal) / float64(rng.LowerTotal)
		}
		level.Info(logger).Log("msg", "bounds range", "purpose", purpose, "window", w.kind, "window_length", model.Duration(w.length()), "range", rangeKey(rng),
			"main_set", rng.MainSet, "main_blocks", rng.MainBlocks, "other_blocks", rng.OtherBlocks, "lower_bound", rng.LowerBound,
			"group_count", rng.GroupCount, "lower_total", rng.LowerTotal, "upper_total", rng.UpperTotal, "total_width", width,
			"has_more", rng.HasMore, "latency", latency, "first", ans.first)
	}
	if h := ans.Head; h != nil {
		level.Info(logger).Log("msg", "bounds head", "purpose", purpose, "window", w.kind, "window_length", model.Duration(w.length()), "approximate", h.Approximate,
			"group_count", h.GroupCount, "in_memory_total", h.InMemoryTotal, "active_total", h.ActiveTotal, "latency", latency, "first", ans.first)
	}
	if len(ans.NotCovered) > 0 {
		level.Info(logger).Log("msg", "bounds not covered", "purpose", purpose, "window", w.kind, "parts", fmt.Sprint(ans.NotCovered))
	}
}

// readBounds is the read value of a bounds answer, as the route labels it.
const readBounds = "bounds"

// boundsFlags names the flags set on any range of the answer.
func boundsFlags(ans *boundsResponse) []string {
	var lower, blocks, shards, more bool
	for _, rng := range ans.Ranges {
		lower = lower || rng.LowerBound
		blocks = blocks || len(rng.MissingBlocks) > 0
		shards = shards || len(rng.MissingShards) > 0
		more = more || rng.HasMore
	}
	var out []string
	for _, f := range []struct {
		name string
		set  bool
	}{{"lower_bound", lower}, {"missing_blocks", blocks}, {"missing_shards", shards}, {"has_more", more}} {
		if f.set {
			out = append(out, f.name)
		}
	}
	return out
}

// finishBounds turns what the ranges found into the pair's result. withWidths records the relative widths of the bounds, which only the PromQL check does, so a group isn't counted twice.
func (r *runner) finishBounds(res *pairResult, a boundsAnswer, withWidths bool) {
	res.Diffs, res.passed = a.diffs, a.passed
	if withWidths {
		res.widths = a.widths
	}
	res.Refusals = append(res.Refusals, a.notes...)
	res.refused = append(res.refused, a.refused...)
	slices.SortFunc(res.Diffs, func(x, y groupDiff) int {
		if c := strings.Compare(x.Range, y.Range); c != 0 {
			return c
		}
		return strings.Compare(x.Value, y.Value)
	})
	switch {
	case len(a.diffs) > 0:
		res.Outcome, res.Class = worst(a.diffs)
		if res.Outcome == outcomePass {
			res.Class = classWithinBounds
		}
	case a.passed > 0:
		res.Outcome, res.Class = outcomePass, classWithinBounds
	case len(a.refused) > 0:
		res.Outcome, res.Class = outcomeRefusal, a.refused[0][1]
	default:
		res.Outcome, res.Class = outcomePass, classEmpty
	}
	if res.Outcome == outcomeRefusal {
		level.Warn(r.logger).Log("msg", "refused", "pair", res.Pair, "class", res.Class, "refusals", fmt.Sprint(res.Refusals))
	}
}

func rangeKey(rng boundsRange) string { return fmt.Sprintf("[%d,%d)", rng.Start, rng.End) }

func rangeValues(rng boundsRange) []string {
	values := make([]string, len(rng.Groups))
	for i, g := range rng.Groups {
		values[i] = g.Value
	}
	return values
}

// rangeProbeSeries is how many series an edge probe over set reads in the range, going by upper: the set's names, or every name of the range for a grouped truth.
func rangeProbeSeries(rng boundsRange, set truthSet) int64 {
	if set.values == nil {
		return rng.UpperTotal
	}
	in := set.restrict()
	var n int64
	for _, g := range rng.Groups {
		if in[g.Value] {
			n += g.Upper
		}
	}
	return n
}

// rankedNames returns the range's names at ranks that PromQL may count: a name whose upper is over -max-truth-series is left out and recorded as a refusal of the truth. It is never nil, since a nil set asks for the grouped truth.
func (r *runner) rankedNames(rng boundsRange, ranks []int, a *boundsAnswer) []string {
	upper := map[string]int64{}
	for _, g := range rng.Groups {
		upper[g.Value] = g.Upper
	}
	out := []string{}
	for _, n := range namesByRank(rangeValues(rng), ranks) {
		if r.maxTruthSeries > 0 && upper[n] > r.maxTruthSeries {
			a.refuse(sideTruth, classTruthTooLarge, "truth "+n+" "+rangeKey(rng))
			continue
		}
		out = append(out, n)
	}
	return out
}

// checkRangeAgainstPromQL counts with PromQL the series of every group of the range over the range's own window and checks lower <= truth <= upper. It returns the truth's latency, and whether it answered.
func (r *runner) checkRangeAgainstPromQL(ctx context.Context, w window, rng boundsRange, cfg boundsConfig, a *boundsAnswer, res *pairResult) (latency time.Duration, ok bool) {
	rp := pair{query: queryBoundsCheck, matchers: allSeriesMatchers(), groupBy: model.MetricNameLabel, window: window{kind: w.kind, start: rng.Start, end: rng.End}}
	set := truthSet{}
	if cfg.perValue {
		set = truthSet{label: model.MetricNameLabel, values: r.rankedNames(rng, cfg.ranks, a)}
	}
	truth, latency, err := r.truth(ctx, rp, set, 0)
	if err != nil && set.values == nil && classifyRefusal(sideTruth, err) == classPromQLLimit {
		// As a pair does, count the names at the ranks one at a time on the same window.
		a.refuse(sideTruth, classPromQLLimit, "grouped truth "+rangeKey(rng))
		set = truthSet{label: model.MetricNameLabel, values: r.rankedNames(rng, cfg.ranks, a)}
		res.reference = referencePerValue
		r.sleep(ctx)
		var more time.Duration
		truth, more, err = r.truth(ctx, rp, set, 0)
		latency += more
	}
	if err != nil {
		a.refuse(sideTruth, classifyRefusal(sideTruth, err), "truth "+rangeKey(rng))
		return latency, false
	}
	for _, s := range skippedRefusals(truth) {
		a.refuse(sideTruth, s[1], "truth "+s[0]+" "+rangeKey(rng))
	}

	restrict := set.restrictFor(truth)
	groups := map[string]boundsGroup{}
	for _, g := range rng.Groups {
		groups[g.Value] = g
	}
	keys := map[string]bool{}
	for v := range groups {
		keys[v] = true
	}
	// A group the answer lacks has no series in the blocks, so any series PromQL counts there is outside the bounds. A cut table leaves groups out on purpose.
	if restrict == nil && !rng.HasMore {
		for v := range truth.counts {
			keys[v] = true
		}
	}
	var below []groupDiff
	for v := range keys {
		if restrict != nil && !restrict[v] {
			continue
		}
		g, t := groups[v], truth.counts[v]
		d := groupDiff{Value: v, Truth: t, Lower: g.Lower, Upper: g.Upper, Range: rangeKey(rng)}
		if g.Lower > 0 {
			a.widths = append(a.widths, float64(g.Upper-g.Lower)/float64(g.Lower))
		}
		switch {
		case t >= g.Lower && t <= g.Upper:
			a.passed++
		case t > g.Upper && rng.LowerBound:
			// A missing block or shard can leave upper short too.
			d.Outcome, d.Class = outcomeFlagged, classIncomplete
			a.diffs = append(a.diffs, d)
		case t > g.Upper:
			d.Outcome, d.Class = outcomeMismatch, classBoundsViolation
			a.diffs = append(a.diffs, d)
		default:
			below = append(below, d)
		}
	}
	if len(below) == 0 {
		return latency, true
	}
	// A block's index lists a series whose only sample in the range is a staleness marker, which PromQL drops, so lower can be above the truth by the series seen just before the range and not in it: the edge probe counts those.
	edgeOK := false
	var edgeAnswer answer
	if r.edgeSlack > 0 && !r.edgeProbeFits(rangeProbeSeries(rng, set)) {
		a.refuse(sideTruth, classTruthTooLarge, "edge probe "+rangeKey(rng))
	} else if r.edgeSlack > 0 {
		r.sleep(ctx)
		var err error
		edgeAnswer, _, err = r.truth(ctx, rp, set, r.edgeSlack)
		if err != nil {
			a.refuse(sideTruth, classifyRefusal(sideTruth, err), "edge probe "+rangeKey(rng))
		} else {
			edgeOK = true
		}
	}
	for _, d := range below {
		d.Outcome, d.Class = outcomeMismatch, classBoundsViolation
		if edgeOK && d.Lower-d.Truth <= edgeAnswer.counts[d.Value] {
			d.Outcome, d.Class = outcomeTolerated, classEdge
		}
		a.diffs = append(a.diffs, d)
	}
	return latency, true
}

// checkRangeAgainstExact compares, for the names at the configured ranks, the bounds with the exact route over the range's window and the name's selector. It needs no PromQL.
func (r *runner) checkRangeAgainstExact(ctx context.Context, rng boundsRange, cfg boundsConfig, a *boundsAnswer, res *pairResult) {
	groups := map[string]boundsGroup{}
	for _, g := range rng.Groups {
		groups[g.Value] = g
	}
	for _, name := range namesByRank(rangeValues(rng), cfg.ranks) {
		r.sleep(ctx)
		est, latency, err := r.client.estimate(ctx, metricSelector(name), model.MetricNameLabel, rng.Start, rng.End, 0)
		if err != nil {
			a.refuse(sideEstimate, classifyRefusal(sideEstimate, err), "exact "+name+" "+rangeKey(rng))
			continue
		}
		res.latency.estimate += latency
		res.latency.estimateOK, res.latency.estimatePos = true, positionAlone
		var exact int64
		for _, g := range est.Groups {
			if g.Value == name {
				exact = g.Count
			}
		}
		g := groups[name]
		d := groupDiff{Value: name, Estimate: exact, Lower: g.Lower, Upper: g.Upper, Range: rangeKey(rng)}
		switch {
		case exact >= g.Lower && exact <= g.Upper:
			a.passed++
			continue
		case exact < g.Lower && est.incomplete():
			d.Outcome, d.Class = outcomeFlagged, classIncomplete
		case exact > g.Upper && est.UpperBound:
			d.Outcome, d.Class = outcomeFlagged, classUpperBound
		case exact > g.Upper && (est.incomplete() || rng.LowerBound):
			d.Outcome, d.Class = outcomeFlagged, classIncomplete
		default:
			d.Outcome, d.Class = outcomeMismatch, classBoundsViolation
		}
		a.diffs = append(a.diffs, d)
	}
}
