// SPDX-License-Identifier: AGPL-3.0-only

package validation

type BlockedQuery struct {
	Pattern string `yaml:"pattern"`
	Regex   bool   `yaml:"regex"`
	Reason  string `yaml:"reason"`
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
