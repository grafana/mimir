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

	labelSeriesStats := map[string]LabelSeriesStats{
		"cluster": {
			SeriesCount:         1000,
			ValuesCount:         10,
			SeriesCountPerValue: []uint64{100, 100, 100, 100, 100, 100, 100, 100, 100, 100}, // uniform
		},
		"__name__": {
			SeriesCount:         1000,
			ValuesCount:         100,
			SeriesCountPerValue: []uint64{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}, // only top 10 values
		},
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

	// Verify LabelValuesDistribution for uniform distribution (10 values, all equal) = 1.0
	assert.InDelta(t, 1.0, clusterStats.LabelValuesDistribution, 0.01, "LabelValuesDistribution for uniform distribution")
}

func TestComputeNormalizedEntropy(t *testing.T) {
	tests := []struct {
		name         string
		seriesCounts []uint64
		expected     float64
	}{
		{
			name:         "empty slice",
			seriesCounts: []uint64{},
			expected:     0,
		},
		{
			name:         "single value",
			seriesCounts: []uint64{1000},
			expected:     0,
		},
		{
			name:         "two equal values",
			seriesCounts: []uint64{500, 500},
			expected:     1.0,
		},
		{
			name:         "uniform distribution (10 values)",
			seriesCounts: []uint64{100, 100, 100, 100, 100, 100, 100, 100, 100, 100},
			expected:     1.0,
		},
		{
			name:         "highly skewed (99% in one value)",
			seriesCounts: []uint64{990, 10},
			expected:     0.08, // approximately
		},
		{
			name:         "moderately skewed",
			seriesCounts: []uint64{500, 250, 125, 125},
			expected:     0.875, // H = 1.75 bits, maxH = 2 bits
		},
		{
			name:         "all zeros except one",
			seriesCounts: []uint64{0, 0, 1000, 0, 0},
			expected:     0,
		},
		{
			name:         "all zeros",
			seriesCounts: []uint64{0, 0, 0},
			expected:     0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := computeNormalizedEntropy(tc.seriesCounts)
			assert.InDelta(t, tc.expected, result, 0.02, "entropy mismatch")
		})
	}
}

func TestGetLabelStats_ScoreCalculation(t *testing.T) {
	tests := []struct {
		name          string
		seriesCount   uint64
		valuesCount   uint64
		distribution  []uint64
		queryMatches  int
		totalQueries  int
		expectedScore float64
	}{
		{
			name:         "perfect candidate with enough values",
			seriesCount:  1000,
			valuesCount:  20, // >= 10, no penalty
			distribution: []uint64{50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50}, // uniform
			queryMatches: 10,
			totalQueries: 10,
			// baseScore = 0.40*(100/100) + 0.40*(100/100) + 0.20*1.0 = 1.0
			// valueSufficiency = min(1.0, 20/10) = 1.0
			// score = 1.0 * 1.0 = 1.0
			expectedScore: 1.0,
		},
		{
			name:         "perfect candidate with few values - penalized",
			seriesCount:  1000,
			valuesCount:  5, // < 10, penalty applies
			distribution: []uint64{200, 200, 200, 200, 200}, // uniform
			queryMatches: 10,
			totalQueries: 10,
			// baseScore = 0.40*(100/100) + 0.40*(100/100) + 0.20*1.0 = 1.0
			// valueSufficiency = min(1.0, 5/10) = 0.5
			// score = 1.0 * 0.5 = 0.5
			expectedScore: 0.5,
		},
		{
			name:         "50% series coverage, 50% query coverage, uniform distribution, enough values",
			seriesCount:  500,
			valuesCount:  10,
			distribution: []uint64{50, 50, 50, 50, 50, 50, 50, 50, 50, 50},
			queryMatches: 5,
			totalQueries: 10,
			// baseScore = 0.40*(50/100) + 0.40*(50/100) + 0.20*1.0 = 0.60
			// valueSufficiency = 1.0
			// score = 0.60
			expectedScore: 0.60,
		},
		{
			name:         "high coverage but skewed distribution",
			seriesCount:  1000,
			valuesCount:  10,
			distribution: []uint64{910, 10, 10, 10, 10, 10, 10, 10, 10, 10}, // 91% in one value
			queryMatches: 10,
			totalQueries: 10,
			// entropy ≈ 0.22 (heavily skewed)
			// baseScore = 0.40*(100/100) + 0.40*(100/100) + 0.20*0.22 ≈ 0.844
			// valueSufficiency = 1.0
			// score ≈ 0.844
			expectedScore: 0.84,
		},
		{
			name:         "single value - heavily penalized",
			seriesCount:  1000,
			valuesCount:  1,
			distribution: []uint64{1000},
			queryMatches: 10,
			totalQueries: 10,
			// baseScore = 0.40 + 0.40 + 0.20*0 = 0.80
			// valueSufficiency = 1/10 = 0.1
			// score = 0.80 * 0.1 = 0.08
			expectedScore: 0.08,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			analyzer := NewAnalyzer()

			// Process queries.
			for i := 0; i < tc.queryMatches; i++ {
				analyzer.ProcessQuery(`metric{test="value"}`, UserQuery)
			}
			for i := 0; i < tc.totalQueries-tc.queryMatches; i++ {
				analyzer.ProcessQuery(`metric`, UserQuery)
			}

			labelSeriesStats := map[string]LabelSeriesStats{
				"test": {
					SeriesCount:         tc.seriesCount,
					ValuesCount:         tc.valuesCount,
					SeriesCountPerValue: tc.distribution,
				},
			}

			stats := analyzer.GetLabelStats(labelSeriesStats, 1000, time.Hour, time.Hour)

			var testStats *LabelStats
			for i := range stats {
				if stats[i].Name == "test" {
					testStats = &stats[i]
					break
				}
			}
			require.NotNil(t, testStats, "test label not found")

			assert.InDelta(t, tc.expectedScore, testStats.Score, 0.02, "score mismatch")
		})
	}
}
