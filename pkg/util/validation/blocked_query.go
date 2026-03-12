// SPDX-License-Identifier: AGPL-3.0-only

package validation

type BlockedQuery struct {
	Pattern               string `yaml:"pattern" doc:"description=PromQL expression pattern to match."`
	Regex                 bool   `yaml:"regex" doc:"description=If true, the pattern is treated as a regular expression. If false, the pattern is treated as a literal match."`
	Reason                string `yaml:"reason" doc:"description=Reason returned to clients when rejecting matching queries."`
	UnalignedRangeQueries bool   `yaml:"unaligned_range_queries" doc:"description=If true, only block the query if the query time range is not aligned to the step, meaning the query is not eligible for range query result caching. If enabled, instant queries and remote read requests will not be blocked."`
}

type BlockedQueriesConfig []BlockedQuery

func (lq *BlockedQueriesConfig) ExampleDoc() (comment string, yaml interface{}) {
	return `The following configuration blocks the query "rate(metric_counter[5m])". Setting the pattern to ".*" and regex to true blocks all queries.`,
		[]BlockedQuery{
			{
				Pattern: "rate(metric_counter[5m])",
				Reason:  "because the query is misconfigured",
			},
		}
}
