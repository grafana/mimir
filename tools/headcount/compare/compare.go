// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"net/http"
	"sort"
	"strings"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

// Outcomes of a pair or a group.
const (
	outcomePass      = "pass"      // the estimate equals the truth
	outcomeTolerated = "tolerated" // a mismatch inside a named class's tolerance
	outcomeFlagged   = "flagged"   // a mismatch in the direction the answer's own flags allow (lower_bound, coverage_gap, upper_bound), of a size they don't bound
	outcomeMismatch  = "mismatch"  // a mismatch no class explains: file a Bug issue
	outcomeRefusal   = "refusal"   // one side refused or failed
	outcomeAnswered  = "answered"  // with -estimate-only, the estimate answered; nothing was compared
)

// Classes. A tolerated mismatch names the class that explains it; a refusal names why.
const (
	classExact           = "exact"
	classEmpty           = "empty"                 // both sides have no series in the window
	classIncomplete      = "incomplete"            // the answer is flagged as leaving out series, and is below the truth but not below a truth over the part of the window it says it covered
	classIncompleteUnder = "incomplete_undercount" // an incomplete answer below a truth over the part of the window it says it covered
	classUpperBound      = "upper_bound"           // the answer is flagged upper_bound, and is above the truth
	classGroupCountHigh  = "group_count_excess"    // a tenant-wide table truthed one group at a time whose group_count is above the label's values in the index over the window
	classAnswerChanged   = "answer_changed"        // the group mismatched, the rerun agrees, and the truth changed between the runs: compaction or late out-of-order blocks between the calls
	classUnstable        = "estimate_unstable"     // the group mismatched and the rerun agrees, but only the estimate changed between the runs
	classEdge            = "edge"                  // above the truth, explained by samples just outside the window or staleness markers inside it
	classWindowEcho      = "window_echo"           // the route counted another window than the one asked for
	classUnclassified    = "unclassified"

	classNoCountRPC      = "no_count_rpc"     // a component without the count RPCs answered
	classMemoryLimit     = "memory_limit"     // the estimate refused the request on the cardinality estimate memory limit (422)
	classGroupsLimit     = "groups_limit"     // the estimate refused the request on max_groups (422)
	classEstimateRefused = "estimate_refused" // the estimate refused the request on another limit (422) or another 4xx
	classPromQLLimit     = "promql_limit"     // the truth hit a fetched-series, chunk or other query limit (422 with a limit's error ID)
	classBadRequest      = "bad_request"      // the request is invalid (400): it fails the same way every round
	classThrottled       = "throttled"        // the query-frontend is throttling the tenant (429)
	classBusy            = "busy"             // every store-gateway or ingester asked was at its count limit (503 unavailable); safe to retry
	classError           = "error"            // any other failure
)

// promQLLimitIDPrefix starts the error ID of every per-query limit, such as err-mimir-max-series-per-query.
const promQLLimitIDPrefix = "err-mimir-max-"

// groupDiff is one group whose estimate and truth differ.
type groupDiff struct {
	Value    string `json:"value"`
	Estimate int64  `json:"estimate"`
	Truth    int64  `json:"truth"`
	Outcome  string `json:"outcome"`
	Class    string `json:"class"`
}

// answer is one side's counts per group.
type answer struct {
	counts map[string]int64
}

// estimateAnswer flattens the route's table.
func estimateAnswer(r *estimateResponse) answer {
	a := answer{counts: make(map[string]int64, len(r.Groups))}
	for _, g := range r.Groups {
		a.counts[g.Value] = g.Count
	}
	return a
}

// Pseudo-groups compare the table's summary fields when the table was cut.
const (
	groupCountKey = "<group_count>"
	totalKey      = "<total>"
)

// compareAnswers diffs the estimate against the truth group by group and classifies each difference. With has_more, only the groups in the table are compared, plus group_count and total against the whole truth. restrict, when set, limits the comparison to those groups, for a truth answered one group at a time.
func compareAnswers(r *estimateResponse, est, truth answer, restrict map[string]bool) []groupDiff {
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
		d := groupDiff{Value: k, Estimate: e, Truth: t}
		d.Outcome, d.Class = classify(r, d)
		diffs = append(diffs, d)
	}
	if r.HasMore && restrict == nil {
		var sum int64
		for _, v := range truth.counts {
			sum += v
		}
		for _, d := range []groupDiff{
			{Value: groupCountKey, Estimate: int64(r.GroupCount), Truth: int64(len(truth.counts))},
			{Value: totalKey, Estimate: r.Total, Truth: sum},
		} {
			if d.Estimate != d.Truth {
				d.Outcome, d.Class = classify(r, d)
				diffs = append(diffs, d)
			}
		}
	}
	sort.Slice(diffs, func(i, j int) bool { return diffs[i].Value < diffs[j].Value })
	return diffs
}

// classify names the class that explains one group's difference, checking the sign each class allows.
func classify(r *estimateResponse, d groupDiff) (outcome, class string) {
	switch {
	case r.incomplete() && d.Estimate < d.Truth:
		return outcomeFlagged, classIncomplete
	case r.UpperBound && d.Estimate > d.Truth:
		return outcomeFlagged, classUpperBound
	}
	return outcomeMismatch, classUnclassified
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
		if he.status == http.StatusUnprocessableEntity {
			// The limit's flag or its message names it: the source's message ends "the tenant's cardinality estimate memory limit", the querier's tracker error "estimated amount of memory consumed by a single query".
			switch {
			case strings.Contains(body, "cardinality-estimate-max-memory-bytes") || strings.Contains(body, "cardinality estimate memory limit") || strings.Contains(body, "amount of memory consumed by a single query"):
				return classMemoryLimit
			case strings.Contains(body, "max_groups") || strings.Contains(body, "cardinality-estimate-max-groups"):
				return classGroupsLimit
			}
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
