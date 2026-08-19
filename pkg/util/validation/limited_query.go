// SPDX-License-Identifier: AGPL-3.0-only

package validation

import "time"

type LimitedQuery struct {
	Query            string        `yaml:"query" doc:"description=Literal PromQL expression to match."`
	AllowedFrequency time.Duration `yaml:"allowed_frequency" doc:"description=Minimum duration between matching queries. If a matching query arrives more often than this, it is rejected."`
	Reason           string        `yaml:"reason" doc:"description=Reason returned to clients when rejecting matching queries."`
	ID               string        `yaml:"id,omitempty" doc:"description=Stable identifier for this rule. Optional; used by tooling to correlate edits and as a metric label for expiry export."`
	Note             string        `yaml:"note,omitempty" doc:"description=Freeform operator note describing why this rule exists (e.g. an incident reference or chat link)."`
	CreatedBy        string        `yaml:"created_by,omitempty" doc:"description=Identity of whoever created this rule, if known."`
	CreatedAt        time.Time     `yaml:"created_at,omitempty" doc:"description=When this rule was created, if known."`
	ExpiresAt        time.Time     `yaml:"expires_at,omitempty" doc:"description=Optional expiry timestamp. Purely informational: exported as a metric for alerting on stale rules. Never enforced — an expired rule keeps blocking/limiting queries until explicitly removed."`
}

type LimitedQueriesConfig []LimitedQuery

// IsExpired reports whether ExpiresAt is set and in the past, relative to now. Purely informational: an expired
// rule is still enforced.
func (q LimitedQuery) IsExpired(now time.Time) bool {
	return !q.ExpiresAt.IsZero() && now.After(q.ExpiresAt)
}

func (lq *LimitedQueriesConfig) ExampleDoc() (comment string, yaml interface{}) {
	return `The following configuration limits the query "rate(metric_counter[5m])" to running, at most, every minute.`,
		[]LimitedQuery{
			{
				Query:            "rate(metric_counter[5m])",
				AllowedFrequency: time.Minute,
				Reason:           "the query is expensive and should not run more than once a minute",
				ID:               "limit-metric-counter-rate",
				Note:             "added per incident INC-1234, see https://example.com/incident/1234",
				ExpiresAt:        time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
			},
		}
}
