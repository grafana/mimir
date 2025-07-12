// SPDX-License-Identifier: AGPL-3.0-only

package validation

import "time"

type LimitedQuery struct {
	Query            string        `yaml:"query"`
	AllowedFrequency time.Duration `yaml:"allowed_frequency"` // query may only be run once per this duration
}

type LimitedQueriesConfig []LimitedQuery

func (lq *LimitedQueriesConfig) ExampleDoc() (comment string, yaml interface{}) {
	return `The following configuration limits the query "rate(metric_counter[5m])" to running, at most, every minute.`,
		[]LimitedQuery{
			{
				Query:            "rate(metric_counter[5m])",
				AllowedFrequency: time.Minute,
			},
		}
}
