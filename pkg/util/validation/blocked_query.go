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
	MinimumStepSize       model.Duration `yaml:"minimum_step_size,omitempty" doc:"description=Block queries where the step is smaller than this duration. Instant queries and queries with no step are not blocked. Set to 0 to disable."`
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
