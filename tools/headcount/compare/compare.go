// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/cardinality"
)

// Outcomes of a pair or a group.
const (
	outcomePass      = "pass"      // the estimate equals the truth
	outcomeTolerated = "tolerated" // a mismatch inside a named class's tolerance
	outcomeFlagged   = "flagged"   // a mismatch in the direction the answer's own flags allow, of a size they don't bound
	outcomeMismatch  = "mismatch"  // a mismatch no class explains: file a Bug issue
	outcomeRefusal   = "refusal"   // one side refused or failed
	outcomeAnswered  = "answered"  // with -estimate-only, the estimate answered; nothing was compared
)

// Classes. A tolerated mismatch names the class that explains it; a refusal names why.
const (
	classExact           = "exact"
	classEmpty           = "empty"                 // both sides have no series in the window
	classSketch          = "sketch"                // an HLL group within the sketch's error bound
	classSketchSmall     = "sketch_small"          // a group marked estimated whose truth is at or below sketchAbove, which the route counts exactly
	classIncomplete      = "incomplete"            // the answer is flagged as leaving out series, and is below the truth but not below a truth over the part of the window it says it covered
	classIncompleteUnder = "incomplete_undercount" // an incomplete answer below a truth over the part of the window it says it covered
	classGroupCountHigh  = "group_count_excess"    // a tenant-wide table truthed one group at a time whose group_count is above the label's values in the index over the window
	classAnswerChanged   = "answer_changed"        // the group mismatched, the rerun agrees, and the truth changed between the runs: compaction or late out-of-order blocks between the calls
	classUnstable        = "estimate_unstable"     // the group mismatched and the rerun agrees, but only the estimate changed between the runs
	classEdge            = "edge"                  // above the truth, explained by samples just outside the window or staleness markers inside it
	classWindowEcho      = "window_echo"           // the route counted another window than the one asked for
	classUnclassified    = "unclassified"

	classNoCountRPC      = "no_count_rpc"      // a component without the count RPCs answered
	classIndexBytesLimit = "index_bytes_limit" // the estimate refused the request on max_index_bytes (422)
	classEstimateRefused = "estimate_refused"  // the estimate refused the request on another limit (422) or another 4xx
	classPromQLLimit     = "promql_limit"      // the truth hit a fetched-series, chunk or other query limit (422 with a limit's error ID)
	classBadRequest      = "bad_request"       // the request is invalid (400): it fails the same way every round
	classThrottled       = "throttled"         // the query-frontend is throttling the tenant (429)
	classBusy            = "busy"              // every store-gateway or ingester asked was at its count limit (503 unavailable); safe to retry
	classError           = "error"             // any other failure
)

// promQLLimitIDPrefix starts the error ID of every per-query limit, such as err-mimir-max-series-per-query.
const promQLLimitIDPrefix = "err-mimir-max-"

// sketchStdError is the relative standard error of a HyperLogLog at the route's precision, 1.04/sqrt(2^p).
var sketchStdError = 1.04 / math.Sqrt(float64(uint64(1)<<cardinality.SketchPrecision))

// sketchAbove mirrors the route's sketch threshold (defaultSketchAbove in pkg/querier): a group with at most this many series is counted exactly.
const sketchAbove = 10_000

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
		if e == t && !sketchedSmall(r, est.estimated[k], t) {
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
	case sketchedSmall(r, d.Estimated, d.Truth):
		return outcomeMismatch, classSketchSmall
	case withinSketch(d, cfg):
		return outcomeTolerated, classSketch
	case r.incomplete() && d.Estimate < d.Truth:
		return outcomeFlagged, classIncomplete
	}
	return outcomeMismatch, classUnclassified
}

// withinSketch reports whether an estimated group is within the sketch's error bound of its truth.
func withinSketch(d groupDiff, cfg classifyConfig) bool {
	return d.Estimated && d.Truth > 0 && math.Abs(float64(d.Estimate-d.Truth)) <= cfg.sketchBound()*float64(d.Truth)
}

// sketchedSmall reports whether a group is marked estimated although its truth is small enough to be counted exactly. A step's buckets share their group's mark, so they are not checked.
func sketchedSmall(r *estimateResponse, estimated bool, truth int64) bool {
	return estimated && r.StepMS == 0 && truth <= sketchAbove
}

// sketchGroups are the estimated groups of an answer with their truth, equal or not, so their relative errors can be observed. A step's buckets are left out, as in sketchedSmall.
func sketchGroups(r *estimateResponse, est, truth answer, restrict map[string]bool) []groupDiff {
	if r.StepMS != 0 {
		return nil
	}
	var out []groupDiff
	for k, ok := range est.estimated {
		if ok && (restrict == nil || restrict[k]) {
			out = append(out, groupDiff{Value: k, Estimate: est.counts[k], Truth: truth.counts[k], Estimated: true})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Value < out[j].Value })
	return out
}

// classifyRefusal names why a request failed, from its status and its error text.
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
	switch he.status {
	case http.StatusBadRequest:
		return classBadRequest
	case http.StatusTooManyRequests:
		return classThrottled
	}
	if side == sideEstimate {
		if isBusy(he) {
			return classBusy
		}
		if he.status == http.StatusUnprocessableEntity && strings.Contains(body, "max_index_bytes") {
			return classIndexBytesLimit
		}
		if he.status >= 400 && he.status < 500 {
			return classEstimateRefused
		}
		return classError
	}
	if he.status == http.StatusUnprocessableEntity && strings.Contains(body, promQLLimitIDPrefix) {
		return classPromQLLimit
	}
	return classError
}

// isBusy reports whether an estimate was answered 503 because every source it asked was at its count limit, which the route tells apart from a timeout by the error type.
func isBusy(he *httpError) bool {
	return he.status == http.StatusServiceUnavailable && he.errorType == string(apierror.TypeUnavailable)
}

// truthValue is the truth of one group of a diff, pseudo-groups included.
func truthValue(truth answer, k string) int64 {
	switch k {
	case groupCountKey:
		return int64(len(truth.counts))
	case totalKey:
		var sum int64
		for _, v := range truth.counts {
			sum += v
		}
		return sum
	}
	return truth.counts[k]
}

// worst is the pair's outcome and class: its most severe group.
func worst(diffs []groupDiff) (outcome, class string) {
	rank := map[string]int{outcomePass: 0, outcomeTolerated: 1, outcomeFlagged: 2, outcomeMismatch: 3}
	outcome, class = outcomePass, classExact
	for _, d := range diffs {
		if rank[d.Outcome] > rank[outcome] || (rank[d.Outcome] == rank[outcome] && d.Class < class && outcome != outcomePass) {
			outcome, class = d.Outcome, d.Class
		}
	}
	return outcome, class
}
