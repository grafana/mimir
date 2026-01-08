// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindSegmentLabelCandidates(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:     "single selector with equal matcher",
			query:    `metric{cluster="us-east-1"}`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "single selector with multiple equal matchers",
			query:    `metric{cluster="us-east-1", namespace="production"}`,
			expected: []string{"__name__", "cluster", "namespace"},
		},
		{
			name:     "single selector with regexp set matcher",
			query:    `metric{cluster=~"us-east-1|us-west-2"}`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "single selector with non-set regexp matcher",
			query:    `metric{cluster=~"us-.*"}`,
			expected: []string{"__name__"},
		},
		{
			name:     "single selector with not-equal matcher",
			query:    `metric{cluster!="us-east-1"}`,
			expected: []string{"__name__"},
		},
		{
			name:     "single selector with not-regexp matcher",
			query:    `metric{cluster!~"us-.*"}`,
			expected: []string{"__name__"},
		},
		{
			name:     "two aggregations with same label in both",
			query:    `sum(rate(metric1{cluster="us-east-1"}[5m])) + sum(rate(metric2{cluster="us-west-2"}[5m]))`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "two aggregations with label only in first",
			query:    `sum(rate(metric1{cluster="us-east-1"}[5m])) + sum(rate(metric2{namespace="production"}[5m]))`,
			expected: []string{"__name__"},
		},
		{
			name:     "two aggregations with common and different labels",
			query:    `sum(rate(metric1{cluster="us-east-1", foo="bar"}[5m])) + sum(rate(metric2{cluster="us-west-2", baz="qux"}[5m]))`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "aggregation with by clause - label in selector",
			query:    `sum by (cluster) (rate(metric{cluster="us-east-1"}[5m]))`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "binary operation with both sides having same label",
			query:    `sum(rate(metric1{cluster="us-east-1"}[5m])) / sum(rate(metric2{cluster="us-east-1"}[5m]))`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "subquery with label",
			query:    `max_over_time(rate(metric{cluster="us-east-1"}[5m])[1h:])`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "mixed equal and regexp set matchers",
			query:    `sum(rate(metric{cluster="us-east-1", env=~"prod|staging"}[5m]))`,
			expected: []string{"__name__", "cluster", "env"},
		},
		{
			name:     "three aggregations with common label",
			query:    `sum(rate(metric1{cluster="a"}[5m])) + sum(rate(metric2{cluster="b"}[5m])) + sum(rate(metric3{cluster="c"}[5m]))`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "no matchers besides metric name",
			query:    `metric`,
			expected: []string{"__name__"},
		},
		{
			name:     "empty query returns error",
			query:    ``,
			expected: nil,
		},
		{
			name:     "invalid query returns error",
			query:    `metric{invalid`,
			expected: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := FindSegmentLabelCandidates(tc.query)

			if tc.expected == nil {
				// Expect an error for invalid queries.
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, tc.expected, result)
		})
	}
}

func TestGetLabelStats_WeightedQueryCoverage(t *testing.T) {
	// Test that QueryCoverage extrapolates query counts to the longer observation window.
	//
	// Setup:
	// - 10 user queries over 1 hour, 5 with "cluster" label (50% user coverage)
	// - 10 rule queries over 5 minutes, 8 with "cluster" label (80% rule coverage)
	//
	// Expected extrapolated coverage:
	// - ruleScale = 60min/5min = 12 (extrapolate rule queries to 1h)
	// - scaledTotalQueries = 10 + 10*12 = 130
	// - scaledMatchingQueries = 5 + 8*12 = 101
	// - coverage = 101/130 * 100 = 77.69%

	analyzer := NewAnalyzer()

	// Process 10 user queries: 5 with cluster label.
	for i := 0; i < 5; i++ {
		analyzer.ProcessQuery(`metric{cluster="a"}`, UserQuery)
	}
	for i := 0; i < 5; i++ {
		analyzer.ProcessQuery(`metric`, UserQuery)
	}

	// Process 10 rule queries: 8 with cluster label.
	for i := 0; i < 8; i++ {
		analyzer.ProcessQuery(`metric{cluster="b"}`, RuleQuery)
	}
	for i := 0; i < 2; i++ {
		analyzer.ProcessQuery(`metric`, RuleQuery)
	}

	// Verify raw counts.
	require.Equal(t, 10, analyzer.TotalUserQueries())
	require.Equal(t, 10, analyzer.TotalRuleQueries())

	labelSeriesStats := map[string]struct {
		seriesCount uint64
		valuesCount uint64
	}{
		"cluster":  {seriesCount: 1000, valuesCount: 10},
		"__name__": {seriesCount: 1000, valuesCount: 100},
	}

	// User queries: 1 hour, Rule queries: 5 minutes.
	userDuration := 1 * time.Hour
	ruleDuration := 5 * time.Minute

	stats := analyzer.GetLabelStats(labelSeriesStats, 1000, userDuration, ruleDuration)

	// Find cluster stats.
	var clusterStats *LabelStats
	for i := range stats {
		if stats[i].Name == "cluster" {
			clusterStats = &stats[i]
			break
		}
	}
	require.NotNil(t, clusterStats, "cluster label not found in stats")

	// Verify individual coverages.
	assert.InDelta(t, 50.0, clusterStats.UserQueryCoverage, 0.01, "UserQueryCoverage")
	assert.InDelta(t, 80.0, clusterStats.RuleQueryCoverage, 0.01, "RuleQueryCoverage")

	// Verify extrapolated coverage.
	// ruleScale = 12, scaledTotal = 10 + 120 = 130, scaledMatching = 5 + 96 = 101
	// coverage = 101/130 * 100 = 77.69%
	assert.InDelta(t, 77.69, clusterStats.QueryCoverage, 0.1, "ExtrapolatedQueryCoverage")
}
