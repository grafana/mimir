// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"testing"

	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/require"
)

func TestAlertsCheckNativeVersionExists(t *testing.T) {
	testCases := []struct {
		name         string
		rules        []rulefmt.Rule
		wantFailures int
	}{
		{
			name: "rules without any histograms",
			rules: []rulefmt.Rule{
				{
					Alert: "HighErrorRate",
					Expr:  `rate(http_requests_total{status="500"}[5m]) > 0.1`,
				},
				{
					Alert: "HighMemoryUsage",
					Expr:  `node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes < 0.1`,
				},
			},
			wantFailures: 0,
		},
		{
			name: "with missing native version, different alert name follows",
			rules: []rulefmt.Rule{
				{
					Alert: "HighLatency",
					Expr:  `histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m])) > 1`,
				},
				{
					Alert: "DifferentAlert",
					Expr:  `up == 0`,
				},
			},
			wantFailures: 1,
		},
		{
			name: "missing native version, same name but no native metric in expression",
			rules: []rulefmt.Rule{
				{
					Alert: "HighLatency",
					Expr:  `histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m])) > 1`,
				},
				{
					Alert: "HighLatency",
					Expr:  `some_other_metric > 1`,
				},
			},
			wantFailures: 1,
		},
		{
			name: "rules with valid native version",
			rules: []rulefmt.Rule{
				{
					Alert: "HighLatency",
					Expr:  `histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket[5m]))) > 1`,
				},
				{
					Alert: "HighLatency",
					Expr:  `histogram_quantile(0.99, sum(rate(http_request_duration_seconds[5m]))) > 1`,
				},
			},
			wantFailures: 0,
		},
		{
			name: "rules with valid native version but different label matchers",
			rules: []rulefmt.Rule{
				{
					Alert: "HighLatency",
					Expr:  `histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket{job="a"}[5m]))) > 1`,
				},
				{
					Alert: "HighLatency",
					Expr:  `histogram_quantile(0.99, sum(rate(http_request_duration_seconds{job="b"}[5m]))) > 1`,
				},
			},
			wantFailures: 1,
		},
		{
			name: "multiple histogram metrics, all have native version",
			rules: []rulefmt.Rule{
				{
					Alert: "HighLatency",
					Expr:  `histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m])) / rate(http_request_duration_seconds_count[5m]) > 1`,
				},
				{
					Alert: "HighLatency",
					Expr:  `histogram_avg(rate(http_request_duration_seconds[5m])) > 1`,
				},
			},
			wantFailures: 0,
		},
		{
			name: "multiple histogram metrics, some missing native version",
			rules: []rulefmt.Rule{
				{
					Alert: "HighLatency",
					Expr:  `histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m])) / rate(other_metric_count[5m]) > 1`,
				},
				{
					Alert: "HighLatency",
					Expr:  `histogram_avg(rate(http_request_duration_seconds[5m])) > 1`,
				},
			},
			wantFailures: 1,
		},
		{
			name: "last rule is missing native version",
			rules: []rulefmt.Rule{
				{
					Alert: "HighErrorRate",
					Expr:  `rate(http_requests_total{status="500"}[5m]) > 0.1`,
				},
				{
					Alert: "HighLatency",
					Expr:  `histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket[5m]))) > 1`,
				},
			},
			wantFailures: 1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results := alertsCheckNativeVersionExists(tc.rules)
			var gotFailures int
			for _, r := range results {
				if r.failure {
					gotFailures++
				}
			}
			require.Equal(t, tc.wantFailures, gotFailures)
		})
	}
}
