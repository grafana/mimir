// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractLabelMatchers(t *testing.T) {
	tests := []struct {
		name                  string
		query                 string
		expectedSelectorCount int      // number of vector selectors
		expectedMetrics       []string // expected metric names across all selectors
	}{
		{
			name:                  "simple metric",
			query:                 "up",
			expectedSelectorCount: 1,
			expectedMetrics:       []string{"up"},
		},
		{
			name:                  "metric with label",
			query:                 `container_memory_working_set_bytes{namespace="default"}`,
			expectedSelectorCount: 1,
			expectedMetrics:       []string{"container_memory_working_set_bytes"},
		},
		{
			name:                  "metric with multiple labels",
			query:                 `node_cpu_seconds_total{mode="idle",cpu="0"}`,
			expectedSelectorCount: 1,
			expectedMetrics:       []string{"node_cpu_seconds_total"},
		},
		{
			name:                  "aggregation query",
			query:                 `sum by(pod) (container_memory_working_set_bytes{namespace="default"})`,
			expectedSelectorCount: 1,
			expectedMetrics:       []string{"container_memory_working_set_bytes"},
		},
		{
			name:                  "binary operation",
			query:                 `up + down`,
			expectedSelectorCount: 2, // Two separate vector selectors
			expectedMetrics:       []string{"up", "down"},
		},
		{
			name:                  "rate with label matchers",
			query:                 `rate(container_cpu_usage_seconds_total{namespace=~"kube.*"}[5m])`,
			expectedSelectorCount: 1,
			expectedMetrics:       []string{"container_cpu_usage_seconds_total"},
		},
		{
			name:                  "scalar query",
			query:                 "time()",
			expectedSelectorCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcherSets, err := extractLabelMatchers(tt.query)
			require.NoError(t, err)
			assert.Len(t, matcherSets, tt.expectedSelectorCount)

			// Check that expected metrics are present across all selectors
			foundMetrics := make(map[string]bool)
			for _, matchers := range matcherSets {
				for _, m := range matchers {
					if m.Name == labels.MetricName {
						foundMetrics[m.Value] = true
					}
				}
			}

			for _, expectedMetric := range tt.expectedMetrics {
				assert.True(t, foundMetrics[expectedMetric], "expected to find metric %s", expectedMetric)
			}
		})
	}
}

func TestExtractLabelMatchers_InvalidQuery(t *testing.T) {
	_, err := extractLabelMatchers("invalid query {{{")
	assert.Error(t, err)
}
