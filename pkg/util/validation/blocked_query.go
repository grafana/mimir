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
	Pattern               string         `yaml:"pattern" doc:"description=PromQL expression pattern to match. Rules without a pattern are a configuration error."`
	Regex                 bool           `yaml:"regex" doc:"description=If true, the pattern is treated as a regular expression; an invalid regular expression is a configuration error. If false, the pattern is treated as a literal match."`
	Reason                string         `yaml:"reason" doc:"description=Reason returned to clients when rejecting matching queries."`
	UnalignedRangeQueries bool           `yaml:"unaligned_range_queries,omitempty" doc:"description=If true, only block the query if the query time range is not aligned to the step, meaning the query is not eligible for range query result caching. If enabled, instant queries and remote read requests will not be blocked."`
	TimeRangeLongerThan   model.Duration `yaml:"time_range_longer_than,omitempty" doc:"description=Block queries with time range longer than this duration. Set to 0 to disable."`
	StepSizeShorterThan   model.Duration `yaml:"step_size_shorter_than,omitempty" doc:"description=Block queries where the step is shorter than this duration. Instant queries and queries with no step are not blocked. Set to 0 to disable."`
	ID                    string         `yaml:"id,omitempty" doc:"description=Stable identifier for this rule. Optional; used by tooling to correlate edits and as a metric label for expiry export."`
	Note                  string         `yaml:"note,omitempty" doc:"description=Freeform operator note describing why this rule exists (e.g. an incident reference or chat link)."`
	CreatedBy             string         `yaml:"created_by,omitempty" doc:"description=Identity of whoever created this rule, if known."`
	CreatedAt             time.Time      `yaml:"created_at,omitempty" doc:"description=When this rule was created, if known."`
	ExpiresAt             time.Time      `yaml:"expires_at,omitempty" doc:"description=Optional expiry timestamp. Purely informational: exported as a metric for alerting on stale rules. Never enforced — an expired rule keeps blocking/limiting queries until explicitly removed."`
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

// IsExpired reports whether ExpiresAt is set and in the past, relative to now. Purely informational: an expired
// rule is still enforced.
func (q BlockedQuery) IsExpired(now time.Time) bool {
	return !q.ExpiresAt.IsZero() && now.After(q.ExpiresAt)
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
				ID:                  "block-expensive-queries",
				Note:                "added per incident INC-1234, see https://example.com/incident/1234",
				ExpiresAt:           time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
			},
			{
				Pattern:             ".*",
				Regex:               true,
				TimeRangeLongerThan: model.Duration(21 * 24 * time.Hour), // 21 days
				Reason:              "queries longer than 21 days are blocked",
			},
		}
}
