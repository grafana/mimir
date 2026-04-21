// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

type BlockedQuery struct {
	Pattern               string         `json:"pattern" yaml:"pattern" doc:"description=PromQL expression pattern to match. Rules without a pattern are a configuration error."`
	Regex                 bool           `json:"regex" yaml:"regex" doc:"description=If true, the pattern is treated as a regular expression; an invalid regular expression is a configuration error. If false, the pattern is treated as a literal match."`
	Reason                string         `json:"reason" yaml:"reason" doc:"description=Reason returned to clients when rejecting matching queries."`
	UnalignedRangeQueries bool           `json:"unaligned_range_queries,omitempty" yaml:"unaligned_range_queries,omitempty" doc:"description=If true, only block the query if the query time range is not aligned to the step, meaning the query is not eligible for range query result caching. If enabled, instant queries and remote read requests will not be blocked."`
	TimeRangeLongerThan   model.Duration `json:"time_range_longer_than,omitempty" yaml:"time_range_longer_than,omitempty" doc:"description=Block queries with time range longer than this duration. Set to 0 to disable."`
	StepSizeShorterThan   model.Duration `json:"step_size_shorter_than,omitempty" yaml:"step_size_shorter_than,omitempty" doc:"description=Block queries where the step is shorter than this duration. Instant queries and queries with no step are not blocked. Set to 0 to disable."`
}

// QueryType identifies the kind of query described by a BlockedQueryInput.
type QueryType uint8

const (
	QueryTypeUnknown    QueryType = iota // unknown query type
	QueryTypeInstant                     // instant query
	QueryTypeRange                       // range query
	QueryTypeRemoteRead                  // remote read
)

// BlockedQueryInput holds the query parameters needed to evaluate a blocked_query rule.
// It can be derived from a MetricsQueryRequest (in Mimir) or from parsed log fields.
type BlockedQueryInput struct {
	Query     string
	QueryType QueryType
	// QueryDuration is end - start; zero for instant queries.
	QueryDuration time.Duration
	// StepAligned is true when start and end are both aligned to step.
	StepAligned bool
	// StepKnown is false when step information is unavailable.
	StepKnown bool
	// Step is the step duration; only meaningful when StepKnown is true.
	Step time.Duration
}

// MatchesRule reports whether the query described by input matches this blocking rule.
// It returns false for rules with empty patterns or invalid regex; both are configuration errors enforced at config load.
func (q BlockedQuery) MatchesRule(input BlockedQueryInput) bool {
	pattern := strings.TrimSpace(q.Pattern)
	if pattern == "" {
		return false
	}

	// Check literal match regardless of regex setting (backwards compatibility).
	patternMatches := pattern == strings.TrimSpace(input.Query)

	if q.Regex {
		r, err := labels.NewFastRegexMatcher(q.Pattern)
		if err != nil {
			return false
		}
		patternMatches = patternMatches || r.MatchString(input.Query)
	}

	if !patternMatches {
		return false
	}

	if q.UnalignedRangeQueries && (input.QueryType != QueryTypeRange || input.StepAligned) {
		return false
	}

	if q.TimeRangeLongerThan > 0 && (input.QueryType == QueryTypeInstant || input.QueryDuration <= time.Duration(q.TimeRangeLongerThan)) {
		return false
	}

	if q.StepSizeShorterThan > 0 && (!input.StepKnown || input.Step >= time.Duration(q.StepSizeShorterThan)) {
		return false
	}

	return true
}

type BlockedQueriesConfig []BlockedQuery

func (lq BlockedQueriesConfig) Validate() error {
	for i, q := range lq {
		if strings.TrimSpace(q.Pattern) == "" {
			return fmt.Errorf("blocked_queries[%d]: pattern is required", i)
		}
		if q.Regex {
			if _, err := labels.NewFastRegexMatcher(q.Pattern); err != nil {
				return fmt.Errorf("blocked_queries[%d]: invalid regex pattern %q: %w", i, q.Pattern, err)
			}
		}
	}
	return nil
}

func (lq *BlockedQueriesConfig) ExampleDoc() (comment string, yaml any) {
	return `The following configuration shows various ways to block queries: by pattern, by time range, or by combining both. ` +
			`Rules are validated at configuration load; an error is returned if the pattern is missing or, when regex: true, the pattern is not a valid regular expression. ` +
			`Use pattern: ".*" with regex: true to match all queries. ` +
			`Time range filtering blocks queries with durations exceeding the specified threshold.`,
		[]BlockedQuery{
			{
				Pattern: "rate(metric_counter[5m])",
				Reason:  "because the query is misconfigured",
			},
			{
				Pattern:             ".*expensive.*",
				Regex:               true,
				TimeRangeLongerThan: model.Duration(7 * 24 * time.Hour), // 7 days
				Reason:              "expensive queries over 7 days are blocked",
			},
			{
				Pattern:             ".*",
				Regex:               true,
				TimeRangeLongerThan: model.Duration(21 * 24 * time.Hour), // 21 days
				Reason:              "queries longer than 21 days are blocked",
			},
		}
}
