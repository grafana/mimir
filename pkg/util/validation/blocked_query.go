// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"time"

	"github.com/prometheus/common/model"
)

type BlockedQuery struct {
	Pattern               string         `yaml:"pattern" doc:"description=PromQL expression pattern to match."`
	Regex                 bool           `yaml:"regex" doc:"description=If true, the pattern is treated as a regular expression. If false, the pattern is treated as a literal match."`
	Reason                string         `yaml:"reason" doc:"description=Reason returned to clients when rejecting matching queries."`
	UnalignedRangeQueries bool           `yaml:"unaligned_range_queries" doc:"description=If true, only block the query if the query time range is not aligned to the step, meaning the query is not eligible for range query result caching. If enabled, instant queries and remote read requests will not be blocked."`
	TimeRangeLongerThan   model.Duration `yaml:"time_range_longer_than,omitempty" doc:"description=Block queries with time range longer than this duration. Set to 0 to disable."`
}

type BlockedQueriesConfig []BlockedQuery

func (lq *BlockedQueriesConfig) ExampleDoc() (comment string, yaml interface{}) {
	return `The following configuration shows various ways to block queries: by pattern, by time range, or by combining both. ` +
			`Setting the pattern to ".*" and regex to true blocks all queries. ` +
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
				TimeRangeLongerThan: model.Duration(21 * 24 * time.Hour), // 21 days
				Reason:              "queries longer than 21 days are blocked",
			},
		}
}
