// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

type SubquerySpinOffDisabledQuery struct {
	Pattern string `yaml:"pattern" doc:"description=PromQL expression pattern to match. Rules without a pattern are a configuration error."`
	Regex   bool   `yaml:"regex" doc:"description=If true, the pattern is treated as a regular expression; an invalid regular expression is a configuration error. If false, the pattern is treated as a literal match."`
}

type SubquerySpinOffDisabledQueriesConfig []SubquerySpinOffDisabledQuery

func (c SubquerySpinOffDisabledQueriesConfig) Validate() error {
	for i, q := range c {
		if strings.TrimSpace(q.Pattern) == "" {
			return fmt.Errorf("subquery_spin_off_disabled_queries[%d]: pattern is required", i)
		}
		if q.Regex {
			if _, err := labels.NewFastRegexMatcher(q.Pattern); err != nil {
				return fmt.Errorf("subquery_spin_off_disabled_queries[%d]: invalid regex pattern %q: %w", i, q.Pattern, err)
			}
		}
	}
	return nil
}

func (c *SubquerySpinOffDisabledQueriesConfig) ExampleDoc() (comment string, yaml any) {
	return `Patterns are matched literally, or as regular expressions when regex: true. ` +
			`Rules are validated at configuration load; an error is returned if a pattern is missing or, with regex: true, is not a valid regular expression.`,
		[]SubquerySpinOffDisabledQuery{
			{
				Pattern: "max_over_time((sum(rate(metric_counter[5m])))[1h:1m])",
			},
			{
				Pattern: ".*expensive_metric.*",
				Regex:   true,
			},
		}
}
