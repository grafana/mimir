// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"

	"github.com/grafana/mimir/pkg/cardinality"
)

// Outcomes of a pair or a group.
const (
	outcomePass      = "pass"      // the estimate equals the truth
	outcomeTolerated = "tolerated" // a mismatch inside a named class's tolerance
	outcomeMismatch  = "mismatch"  // a mismatch no class explains: file a Bug issue
	outcomeRefusal   = "refusal"   // one side refused or failed
)

// Classes. A tolerated mismatch names the class that explains it; a refusal names why.
const (
	classExact         = "exact"
	classEmpty         = "empty"          // both sides have no series in the window
	classSketch        = "sketch"         // an HLL group within the sketch's error bound
	classIncomplete    = "incomplete"     // the answer is flagged as leaving out series, and is below the truth
	classHeadWhole     = "head_whole"     // the answer counts the whole head whatever the window, and is above the truth
	classAnswerChanged = "answer_changed" // the group mismatched and the rerun agrees: compaction or late out-of-order blocks between the calls
	classEdge          = "edge"           // above the truth, explained by samples just outside the window or staleness markers inside it
	classWindowEcho    = "window_echo"    // the route counted another window than the one asked for
	classUnclassified  = "unclassified"

	classNoCountRPC      = "no_count_rpc"     // a component without the count RPCs answered
	classEstimateRefused = "estimate_refused" // the estimate refused the request (4xx)
	classPromQLLimit     = "promql_limit"     // the truth hit a fetched-series, chunk or other query limit
	classError           = "error"            // any other failure
)

// sketchStdError is the relative standard error of a HyperLogLog at the route's precision, 1.04/sqrt(2^p).
var sketchStdError = 1.04 / math.Sqrt(float64(uint64(1)<<cardinality.SketchPrecision))

// classifyConfig holds the tolerances.
type classifyConfig struct {
	// sketchSigmas is how many standard errors a sketched group may be off by.
	sketchSigmas float64
}

func (c classifyConfig) sketchBound() float64 { return c.sketchSigmas * sketchStdError }

// groupDiff is one group whose estimate and truth differ.
type groupDiff struct {
	Value     string `json:"value"`
	Estimate  int64  `json:"estimate"`
	Truth     int64  `json:"truth"`
	Estimated bool   `json:"estimated,omitempty"`
	Outcome   string `json:"outcome"`
	Class     string `json:"class"`
}

// answer is one side's counts per group. Bucketed answers key each group as value@bucket.
type answer struct {
	counts    map[string]int64
	estimated map[string]bool
}

// estimateAnswer flattens the route's table, with buckets keyed as value@i.
func estimateAnswer(r *estimateResponse) answer {
	a := answer{counts: map[string]int64{}, estimated: map[string]bool{}}
	for _, g := range r.Groups {
		if g.Counts == nil {
			a.counts[g.Value] = g.Count
			a.estimated[g.Value] = g.Estimated
			continue
		}
		for i, c := range g.Counts {
			k := bucketKey(g.Value, i)
			a.counts[k] = c
			a.estimated[k] = g.Estimated
		}
	}
	return a
}

func bucketKey(value string, i int) string { return fmt.Sprintf("%s@%d", value, i) }

// Pseudo-groups compare the table's summary fields when the table was cut.
const (
	groupCountKey = "<group_count>"
	totalKey      = "<total>"
)

// compareAnswers diffs the estimate against the truth group by group and classifies each difference. With has_more, only the groups in the table are compared, plus group_count and total against the whole truth. restrict, when set, limits the comparison to those groups, for a truth answered one group at a time.
func compareAnswers(r *estimateResponse, est, truth answer, restrict map[string]bool, cfg classifyConfig) []groupDiff {
	keys := map[string]bool{}
	for k := range est.counts {
		keys[k] = true
	}
	if !r.HasMore && restrict == nil {
		for k := range truth.counts {
			keys[k] = true
		}
	}
	var diffs []groupDiff
	for k := range keys {
		if restrict != nil && !restrict[k] {
			continue
		}
		e, t := est.counts[k], truth.counts[k]
		if e == t {
			continue
		}
		d := groupDiff{Value: k, Estimate: e, Truth: t, Estimated: est.estimated[k]}
		d.Outcome, d.Class = classify(r, d, cfg)
		diffs = append(diffs, d)
	}
	if r.HasMore && restrict == nil && r.StepMS == 0 {
		var sum int64
		for _, v := range truth.counts {
			sum += v
		}
		for _, d := range []groupDiff{
			{Value: groupCountKey, Estimate: int64(r.GroupCount), Truth: int64(len(truth.counts))},
			{Value: totalKey, Estimate: r.Total, Truth: sum, Estimated: r.EstimatedGroups > 0},
		} {
			if d.Estimate != d.Truth {
				d.Outcome, d.Class = classify(r, d, cfg)
				diffs = append(diffs, d)
			}
		}
	}
	sort.Slice(diffs, func(i, j int) bool { return diffs[i].Value < diffs[j].Value })
	return diffs
}

// classify names the class that explains one group's difference, checking the sign each class allows.
func classify(r *estimateResponse, d groupDiff, cfg classifyConfig) (outcome, class string) {
	switch {
	case d.Estimated && d.Truth > 0 && math.Abs(float64(d.Estimate-d.Truth)) <= cfg.sketchBound()*float64(d.Truth):
		return outcomeTolerated, classSketch
	case r.incomplete() && d.Estimate < d.Truth:
		return outcomeTolerated, classIncomplete
	case r.HeadWhole && d.Estimate > d.Truth:
		return outcomeTolerated, classHeadWhole
	}
	return outcomeMismatch, classUnclassified
}

// classifyRefusal names why a request failed.
func classifyRefusal(side string, err error) string {
	var he *httpError
	if !errors.As(err, &he) {
		return classError
	}
	body := strings.ToLower(he.body)
	// The querier refuses with 422 when a store-gateway or an ingester answers Unimplemented (classifyStoreGatewayStatus, classifyIngesterError).
	if strings.Contains(body, "does not support cardinality estimates") || strings.Contains(body, "may run an older version") {
		return classNoCountRPC
	}
	if side == sideEstimate && he.status >= 400 && he.status < 500 {
		return classEstimateRefused
	}
	if side == sideTruth && (he.status == http.StatusUnprocessableEntity || strings.Contains(body, "limit")) {
		return classPromQLLimit
	}
	return classError
}

// worst is the pair's outcome and class: its most severe group.
func worst(diffs []groupDiff) (outcome, class string) {
	rank := map[string]int{outcomePass: 0, outcomeTolerated: 1, outcomeMismatch: 2}
	outcome, class = outcomePass, classExact
	for _, d := range diffs {
		if rank[d.Outcome] > rank[outcome] || (rank[d.Outcome] == rank[outcome] && d.Class < class && outcome != outcomePass) {
			outcome, class = d.Outcome, d.Class
		}
	}
	return outcome, class
}
