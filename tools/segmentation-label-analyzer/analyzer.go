// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"math"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func init() {
	// Enable experimental PromQL functions (like info()) so we can parse all queries.
	parser.EnableExperimentalFunctions = true
}

// QueryType represents the type of query (user or rule).
type QueryType int

const (
	UserQuery QueryType = iota
	RuleQuery
)

// LabelStats holds statistics about a label for segmentation analysis.
type LabelStats struct {
	// Name is the label name.
	Name string

	// SeriesCount is the number of series that have this label.
	SeriesCount uint64

	// ValuesCount is the number of unique values for this label.
	ValuesCount uint64

	// SeriesCoverage is the percentage of total series that have this label.
	SeriesCoverage float64

	// QueryCoverage is the percentage of all queries where this label is a segmentation candidate.
	QueryCoverage float64

	// UserQueryCoverage is the percentage of user queries where this label is a segmentation candidate.
	UserQueryCoverage float64

	// RuleQueryCoverage is the percentage of rule queries where this label is a segmentation candidate.
	RuleQueryCoverage float64

	// AvgDistinctValuesPerQuery is the average number of distinct values for this label per query.
	// Lower is better - a value of 1 means queries typically reference a single value for this label.
	AvgDistinctValuesPerQuery float64

	// SeriesValuesDistribution is the normalized Shannon entropy of series distribution across label values.
	// Range 0-1: 0 = all series have one value (bad for sharding), 1 = evenly distributed (ideal for sharding).
	SeriesValuesDistribution float64

	// QueryValuesDistribution is the normalized Shannon entropy of query distribution across label values.
	// Range 0-1: 0 = all queries use one value (bad for queries isolation), 1 = queries evenly distributed across values (ideal for queries isolation).
	QueryValuesDistribution float64

	// TopValuesSeriesPercent is the percentage of series for the top 3 label values (by cardinality).
	// E.g., [45.0, 20.0, 10.0] means the top value has 45% of series, second has 20%, etc.
	TopValuesSeriesPercent []float64

	// Score is a weighted score (0-1) for ranking candidates.
	// Composed of 40% series coverage + 30% query coverage + 15% query values distribution + 15% series values distribution.
	Score float64
}

// LabelSeriesStats holds series statistics for a label from Mimir.
type LabelSeriesStats struct {
	// SeriesCount is the total number of series with this label.
	SeriesCount uint64
	// ValuesCount is the number of unique values for this label.
	ValuesCount uint64
	// SeriesCountPerValue is the series count for label values, sorted in descending order (highest first).
	// Used for distribution uniformity calculation and top values display.
	SeriesCountPerValue []uint64
}

// computeNormalizedEntropy calculates the normalized Shannon entropy of series distribution.
// It measures how evenly series are distributed across label values.
// Returns a value between 0 and 1:
//   - 0: all series have the same value (worst for sharding)
//   - 1: series are evenly distributed across all values (ideal for sharding)
func computeNormalizedEntropy(seriesCounts []uint64) float64 {
	n := len(seriesCounts)
	if n <= 1 {
		// With 0 or 1 values, entropy is undefined/meaningless.
		// Return 0 as there's no distribution diversity.
		return 0
	}

	// Calculate total series count.
	var totalSeries uint64
	for _, count := range seriesCounts {
		totalSeries += count
	}
	if totalSeries == 0 {
		return 0
	}

	// Calculate Shannon entropy: H = -Σ(p_i × log₂(p_i))
	var entropy float64
	for _, count := range seriesCounts {
		if count == 0 {
			continue
		}
		p := float64(count) / float64(totalSeries)
		entropy -= p * math.Log2(p)
	}

	// Normalize by maximum possible entropy: log₂(n)
	maxEntropy := math.Log2(float64(n))
	if maxEntropy == 0 {
		return 0
	}

	return entropy / maxEntropy
}

// Analyzer performs segmentation label analysis.
type Analyzer struct {
	// queriesCountBySegmentationLabel tracks how many queries each segmentation label candidate appears in (total).
	queriesCountBySegmentationLabel map[string]int

	// userQueriesCountBySegmentationLabel tracks user queries per label.
	userQueriesCountBySegmentationLabel map[string]int

	// ruleQueriesCountBySegmentationLabel tracks rule queries per label.
	ruleQueriesCountBySegmentationLabel map[string]int

	// totalDistinctValuesPerLabel tracks the sum of distinct values per query for each label.
	// Used to compute average distinct values per query for penalty calculation.
	totalDistinctValuesPerLabel map[string]int

	// queriesCountByLabelValue tracks queries per label value.
	// Map structure: label_name -> value -> query_count
	queriesCountByLabelValue map[string]map[string]int

	// totalQueries is the total number of queries analyzed.
	totalQueries int

	// totalUserQueries is the number of user queries analyzed.
	totalUserQueries int

	// totalRuleQueries is the number of rule queries analyzed.
	totalRuleQueries int
}

// NewAnalyzer creates a new Analyzer.
func NewAnalyzer() *Analyzer {
	return &Analyzer{
		queriesCountBySegmentationLabel:     make(map[string]int),
		userQueriesCountBySegmentationLabel: make(map[string]int),
		ruleQueriesCountBySegmentationLabel: make(map[string]int),
		totalDistinctValuesPerLabel:         make(map[string]int),
		queriesCountByLabelValue:            make(map[string]map[string]int),
	}
}

// ProcessQuery parses a PromQL query and updates the analyzer statistics.
// It skips queries that cannot be parsed.
func (a *Analyzer) ProcessQuery(query string, queryType QueryType) {
	candidates, err := FindSegmentLabelCandidates(query)
	if err != nil {
		// Skip unparseable queries.
		return
	}

	a.totalQueries++

	switch queryType {
	case UserQuery:
		a.totalUserQueries++
	case RuleQuery:
		a.totalRuleQueries++
	}

	// Update counts for each segment label candidate.
	for _, candidate := range candidates {
		a.queriesCountBySegmentationLabel[candidate.Name]++
		a.totalDistinctValuesPerLabel[candidate.Name] += len(candidate.Values)

		// Track queries per label value.
		if a.queriesCountByLabelValue[candidate.Name] == nil {
			a.queriesCountByLabelValue[candidate.Name] = make(map[string]int)
		}
		for _, v := range candidate.Values {
			a.queriesCountByLabelValue[candidate.Name][v]++
		}

		switch queryType {
		case UserQuery:
			a.userQueriesCountBySegmentationLabel[candidate.Name]++
		case RuleQuery:
			a.ruleQueriesCountBySegmentationLabel[candidate.Name]++
		}
	}
}

// GetLabelStats returns combined statistics for all labels.
// The userQueryDuration and ruleQueryDuration are used to weight the "All queries" coverage,
// since user and rule queries may cover different time ranges.
func (a *Analyzer) GetLabelStats(labelSeriesStats map[string]LabelSeriesStats, totalSeriesCount uint64, userQueryDuration, ruleQueryDuration time.Duration) []LabelStats {
	// Combine data from Mimir (series stats) and query analysis.
	statsMap := make(map[string]*LabelStats)

	// Add series stats from Mimir.
	for name, stats := range labelSeriesStats {
		coverage := min(100, max(0, float64(stats.SeriesCount)/float64(totalSeriesCount)*100))

		// Compute top 3 values series percentages.
		var topValuesPercent []float64
		if stats.SeriesCount > 0 && len(stats.SeriesCountPerValue) > 0 {
			for i := 0; i < min(3, len(stats.SeriesCountPerValue)); i++ {
				pct := float64(stats.SeriesCountPerValue[i]) / float64(stats.SeriesCount) * 100
				topValuesPercent = append(topValuesPercent, pct)
			}
		}

		statsMap[name] = &LabelStats{
			Name:                     name,
			SeriesCount:              stats.SeriesCount,
			ValuesCount:              stats.ValuesCount,
			SeriesCoverage:           coverage,
			SeriesValuesDistribution: computeNormalizedEntropy(stats.SeriesCountPerValue),
			TopValuesSeriesPercent:   topValuesPercent,
		}
	}

	// Calculate scale factors to normalize query counts to the longer observation window.
	// If user queries cover 1h and rule queries cover 5m, we extrapolate rule query counts
	// by 12x to estimate what we'd see over 1h.
	var userScale, ruleScale float64 = 1, 1
	if userQueryDuration > 0 && ruleQueryDuration > 0 {
		if userQueryDuration > ruleQueryDuration {
			ruleScale = float64(userQueryDuration) / float64(ruleQueryDuration)
		} else {
			userScale = float64(ruleQueryDuration) / float64(userQueryDuration)
		}
	}

	// Calculate extrapolated totals for "All queries" coverage.
	scaledTotalQueries := float64(a.totalUserQueries)*userScale + float64(a.totalRuleQueries)*ruleScale

	// Add query coverage stats.
	for label := range a.queriesCountBySegmentationLabel {
		if _, exists := statsMap[label]; !exists {
			// Label appears in queries but not in series stats.
			statsMap[label] = &LabelStats{
				Name: label,
			}
		}

		stats := statsMap[label]
		if a.totalUserQueries > 0 {
			stats.UserQueryCoverage = min(100, max(0, float64(a.userQueriesCountBySegmentationLabel[label])/float64(a.totalUserQueries)*100))
		}
		if a.totalRuleQueries > 0 {
			stats.RuleQueryCoverage = min(100, max(0, float64(a.ruleQueriesCountBySegmentationLabel[label])/float64(a.totalRuleQueries)*100))
		}

		// Calculate "All queries" coverage by extrapolating counts to the longer observation window.
		if scaledTotalQueries > 0 {
			scaledMatchingQueries := float64(a.userQueriesCountBySegmentationLabel[label])*userScale +
				float64(a.ruleQueriesCountBySegmentationLabel[label])*ruleScale
			stats.QueryCoverage = min(100, max(0, scaledMatchingQueries/scaledTotalQueries*100))
		}

		// Calculate average distinct values per query.
		// If queries typically reference multiple values for this label, the label is
		// less effective for query routing.
		queriesWithLabel := a.queriesCountBySegmentationLabel[label]
		if queriesWithLabel > 0 {
			stats.AvgDistinctValuesPerQuery = float64(a.totalDistinctValuesPerLabel[label]) / float64(queriesWithLabel)
		}

		// Calculate query values distribution (entropy of queries per label value).
		if counts, ok := a.queriesCountByLabelValue[label]; ok && len(counts) > 0 {
			queryCounts := make([]uint64, 0, len(counts))
			for _, count := range counts {
				queryCounts = append(queryCounts, uint64(count))
			}
			stats.QueryValuesDistribution = computeNormalizedEntropy(queryCounts)
		}
	}

	// Calculate scores and convert to slice.
	result := make([]LabelStats, 0, len(statsMap))
	for _, stats := range statsMap {
		// Score is weighted combination of:
		// - 40% series coverage: we need to shard the series to compartments by the segmentation label,
		//   so to have an effective sharding we need that the majority of series have the label.
		// - 30% query coverage: the % of queries for which we can deterministically know the compartment
		//   when series are sharded by that label. Penalized by avg distinct values per query.
		// - 15% query values distribution: queries should be evenly distributed across label values
		//   to ensure balanced query load. A label where 99% of queries use one value means unbalanced load.
		// - 15% series values distribution: series should be evenly distributed across label values
		//   to ensure balanced sharding. A label where 99% of series have one value is bad for sharding.

		// Apply penalty to query coverage score if queries typically reference multiple values.
		// Exponential decay: 0.8^(avg-1). Penalty table:
		//   avg=1 → 1.00 (no penalty)    avg=6 → 0.33
		//   avg=2 → 0.80 (20% penalty)   avg=7 → 0.26
		//   avg=3 → 0.64                 avg=8 → 0.21
		//   avg=4 → 0.51                 avg=9 → 0.17
		//   avg=5 → 0.41                 avg=10 → 0.13
		queryCoverageScore := stats.QueryCoverage / 100
		if stats.AvgDistinctValuesPerQuery > 0 {
			queryValuesPenalty := math.Pow(0.8, stats.AvgDistinctValuesPerQuery-1)
			queryCoverageScore *= queryValuesPenalty
		}

		baseScore := 0.40*(stats.SeriesCoverage/100) + 0.30*queryCoverageScore + 0.15*stats.QueryValuesDistribution + 0.15*stats.SeriesValuesDistribution

		// Apply penalty if the label has fewer unique values than the minimum required for effective sharding.
		// A label with only 5 values can't support 10-way sharding regardless of distribution.
		const minRequiredValues = 10
		valueSufficiency := min(1.0, float64(stats.ValuesCount)/float64(minRequiredValues))
		stats.Score = baseScore * valueSufficiency

		result = append(result, *stats)
	}

	return result
}

// TotalQueries returns the total number of queries analyzed.
func (a *Analyzer) TotalQueries() int {
	return a.totalQueries
}

// TotalUserQueries returns the number of user queries analyzed.
func (a *Analyzer) TotalUserQueries() int {
	return a.totalUserQueries
}

// TotalRuleQueries returns the number of rule queries analyzed.
func (a *Analyzer) TotalRuleQueries() int {
	return a.totalRuleQueries
}

// SegmentLabelCandidate represents a label that is a valid segmentation candidate for a query.
type SegmentLabelCandidate struct {
	// Name is the label name.
	Name string
	// Values is the list of distinct values for this label across all selectors in the query.
	// For example, if a query has `metric1{cluster="a"} + metric2{cluster="b"}`, the cluster label
	// has values ["a", "b"]. Fewer values is better for query routing (fewer compartments to hit).
	Values []string
}

// FindSegmentLabelCandidates analyzes a PromQL query and returns label names that
// are valid segmentation label candidates, along with the count of distinct values
// for each label in the query.
//
// A label is a valid segment label candidate if:
//  1. Every vector selector in the query has a matcher on that label
//  2. Each matcher on that label is either an equal matcher (=) or a regexp
//     matcher with set matches (e.g., =~"a|b|c")
//  3. The query does not use the info() function (which implicitly queries additional metrics)
//
// Returns a slice of candidates that meet all criteria, or an error if the
// query cannot be parsed.
func FindSegmentLabelCandidates(query string) ([]SegmentLabelCandidate, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, err
	}

	// Check if query uses info() function - if so, no labels are valid candidates
	// because info() implicitly queries additional metrics under the hood.
	if containsInfoFunction(expr) {
		return nil, nil
	}

	selectors := parser.ExtractSelectors(expr)
	if len(selectors) == 0 {
		return nil, nil
	}

	// For each selector, find labels with equality or set matchers.
	// We want labels that appear in ALL selectors with valid matchers.
	// Also track the distinct values for each label.
	var selectorLabels []map[string]struct{}
	labelValues := make(map[string]map[string]struct{}) // label -> set of distinct values

	for _, matchers := range selectors {
		labelsInSelector := make(map[string]struct{})
		for _, m := range matchers {
			if isValidSegmentMatcher(m) {
				labelsInSelector[m.Name] = struct{}{}

				// Track distinct values for this label.
				if labelValues[m.Name] == nil {
					labelValues[m.Name] = make(map[string]struct{})
				}
				for _, v := range getMatcherValues(m) {
					labelValues[m.Name][v] = struct{}{}
				}
			}
		}
		selectorLabels = append(selectorLabels, labelsInSelector)
	}

	// Find intersection: labels that appear in ALL selectors.
	if len(selectorLabels) == 0 {
		return nil, nil
	}

	// Start with the first selector's labels.
	candidates := make(map[string]struct{})
	for label := range selectorLabels[0] {
		candidates[label] = struct{}{}
	}

	// Intersect with remaining selectors.
	for i := 1; i < len(selectorLabels); i++ {
		for label := range candidates {
			if _, exists := selectorLabels[i][label]; !exists {
				delete(candidates, label)
			}
		}
	}

	// Convert to slice with values.
	result := make([]SegmentLabelCandidate, 0, len(candidates))
	for label := range candidates {
		values := make([]string, 0, len(labelValues[label]))
		for v := range labelValues[label] {
			values = append(values, v)
		}
		result = append(result, SegmentLabelCandidate{
			Name:   label,
			Values: values,
		})
	}

	return result, nil
}

// getMatcherValues returns the distinct values matched by a matcher.
// For equal matchers, returns the single value.
// For regex set matchers (e.g., =~"a|b|c"), returns all values in the set.
func getMatcherValues(m *labels.Matcher) []string {
	switch m.Type {
	case labels.MatchEqual:
		return []string{m.Value}
	case labels.MatchRegexp:
		return m.SetMatches()
	default:
		return nil
	}
}

// isValidSegmentMatcher returns true if the matcher is valid for segmentation:
// - Equal matcher (=)
// - Regexp matcher with set matches (=~"a|b|c")
func isValidSegmentMatcher(m *labels.Matcher) bool {
	switch m.Type {
	case labels.MatchEqual:
		return true
	case labels.MatchRegexp:
		// Check if it's a set matcher (like =~"a|b|c").
		// SetMatches() returns the set of values if the regex is an alternation.
		return len(m.SetMatches()) > 0
	default:
		return false
	}
}

// containsInfoFunction returns true if the expression contains an info() function call.
// The info() function implicitly queries additional metrics, so queries using it
// cannot have deterministic segmentation label candidates.
func containsInfoFunction(expr parser.Expr) bool {
	var found bool
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if call, ok := node.(*parser.Call); ok && call.Func.Name == "info" {
			found = true
			return nil
		}
		return nil
	})
	return found
}
