// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

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

	// Score is a weighted score (0-1) for ranking candidates.
	Score float64
}

// Analyzer performs segmentation label analysis.
type Analyzer struct {
	// queriesCountBySegmentationLabel tracks how many queries each segmentation label candidate appears in (total).
	queriesCountBySegmentationLabel map[string]int

	// userQueriesCountBySegmentationLabel tracks user queries per label.
	userQueriesCountBySegmentationLabel map[string]int

	// ruleQueriesCountBySegmentationLabel tracks rule queries per label.
	ruleQueriesCountBySegmentationLabel map[string]int

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
	for _, label := range candidates {
		a.queriesCountBySegmentationLabel[label]++

		switch queryType {
		case UserQuery:
			a.userQueriesCountBySegmentationLabel[label]++
		case RuleQuery:
			a.ruleQueriesCountBySegmentationLabel[label]++
		}
	}
}

// GetLabelStats returns combined statistics for all labels.
// The userQueryDuration and ruleQueryDuration are used to weight the "All queries" coverage,
// since user and rule queries may cover different time ranges.
func (a *Analyzer) GetLabelStats(labelSeriesStats map[string]struct {
	seriesCount uint64
	valuesCount uint64
}, totalSeriesCount uint64, userQueryDuration, ruleQueryDuration time.Duration) []LabelStats {
	// Combine data from Mimir (series stats) and query analysis.
	statsMap := make(map[string]*LabelStats)

	// Add series stats from Mimir.
	for name, stats := range labelSeriesStats {
		coverage := min(100, max(0, float64(stats.seriesCount)/float64(totalSeriesCount)*100))
		statsMap[name] = &LabelStats{
			Name:           name,
			SeriesCount:    stats.seriesCount,
			ValuesCount:    stats.valuesCount,
			SeriesCoverage: coverage,
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
	}

	// Calculate scores and convert to slice.
	result := make([]LabelStats, 0, len(statsMap))
	for _, stats := range statsMap {
		// Score is weighted combination of:
		// - The % of series having that label
		//   We need to shard the series to compartments by the segmentation label, so to have an effective
		//   sharding we need that the majority of series have the label.
		// - The % of queries for which we can deterministically know the compartment when series are sharded by that label
		stats.Score = 0.75*(stats.SeriesCoverage/100) + 0.25*(stats.QueryCoverage/100)
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

// FindSegmentLabelCandidates analyzes a PromQL query and returns label names that
// are valid segmentation label candidates.
//
// A label is a valid segment label candidate if:
//  1. Every vector selector in the query has a matcher on that label
//  2. Each matcher on that label is either an equal matcher (=) or a regexp
//     matcher with set matches (e.g., =~"a|b|c")
//
// Returns a slice of label names that meet both criteria, or an error if the
// query cannot be parsed.
func FindSegmentLabelCandidates(query string) ([]string, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, err
	}

	selectors := parser.ExtractSelectors(expr)
	if len(selectors) == 0 {
		return nil, nil
	}

	// For each selector, find labels with equality or set matchers.
	// We want labels that appear in ALL selectors with valid matchers.
	var selectorLabels []map[string]struct{}

	for _, matchers := range selectors {
		labelsInSelector := make(map[string]struct{})
		for _, m := range matchers {
			if isValidSegmentMatcher(m) {
				labelsInSelector[m.Name] = struct{}{}
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

	// Convert to slice.
	result := make([]string, 0, len(candidates))
	for label := range candidates {
		result = append(result, label)
	}

	return result, nil
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
