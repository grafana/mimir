// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

type planPredicate struct {
	config  CostConfig
	matcher *labels.Matcher

	// selectivity is between 0 and 1. 1 indicates that the matcher will match all label values, 0 indicates it will match no values. NB: label values, not series
	selectivity float64
	// cardinality is the estimation of how many series this matcher matches on its own. NB: series, not label values
	cardinality         uint64
	labelNameUniqueVals uint64
	// singleMatchCost is how much it costs to run this matcher against an arbitrary label value.
	singleMatchCost float64
	// indexScanCost is the cost of traversing the postings offset table. This is the singleMatchCost to run the matcher against all label values (or at least enough to know all the values it matches).
	// This naively equals singleMatchCost * labelNameUniqueVals, but it might be lower if the matcher is a prefix matcher or an exact matcher.
	indexScanCost float64
}

func newPlanPredicate(ctx context.Context, m *labels.Matcher, stats index.Statistics, config CostConfig) planPredicate {
	pred := planPredicate{
		matcher:         m,
		singleMatchCost: m.SingleMatchCost(),
		config:          config,
	}
	pred.labelNameUniqueVals = stats.LabelValuesCount(ctx, m.Name)
	pred.selectivity = m.EstimateSelectivity(pred.labelNameUniqueVals)

	pred.cardinality = estimatePredicateCardinality(ctx, m, stats, pred.selectivity)
	pred.indexScanCost = estimatePredicateIndexScanCost(pred, m)

	return pred
}

func estimatePredicateIndexScanCost(pred planPredicate, m *labels.Matcher) float64 {
	switch pred.matcher.Type {
	case labels.MatchEqual, labels.MatchNotEqual:
		if m.Value == "" {
			// For this we need to get the postings of all label values
			return pred.singleMatchCost * float64(pred.labelNameUniqueVals)
		}
		return pred.singleMatchCost
	case labels.MatchRegexp, labels.MatchNotRegexp:
		// Prometheus doesn't optimize if the matcher has a prefix, so we don't take it into account for this cost estimation.
		// https://github.com/prometheus/prometheus/issues/16889
		// The store-gateway has this optimization in PostingOffsetTableV2.LabelValuesOffsets,
		// so if this planner is used in the store-gateway we might want to account for it.
		if setMatches := m.SetMatches(); len(setMatches) > 0 {
			// If we know the exact matches, then we will go ~directly to them.
			// This excludes the calculations for the postings offset table sampling with tsdb.DefaultPostingOffsetInMemorySampling
			// because we don't know the distribution of the matches in the postings offset table and to keep the code simpler.
			return pred.singleMatchCost * float64(len(setMatches))
		}
		// Assume we will have to scan all label values to find a match.
		return pred.singleMatchCost * float64(pred.labelNameUniqueVals)
	}

	panic("estimatePredicateIndexScanCost called with unhandled matcher type: " + m.Type.String() + m.String())
}

func estimatePredicateCardinality(ctx context.Context, m *labels.Matcher, stats index.Statistics, selectivity float64) uint64 {
	switch m.Type {
	case labels.MatchEqual:
		return estimateEqualMatcherCardinality(ctx, m, stats)
	case labels.MatchNotEqual:
		return estimateNotEqualMatcherCardinality(ctx, m, stats)
	case labels.MatchRegexp:
		return estimateRegexMatcherCardinality(ctx, m, stats, selectivity)
	case labels.MatchNotRegexp:
		return estimateNotRegexMatcherCardinality(ctx, m, stats, selectivity)
	default:
		panic("estimatePredicateCardinality called with unhandled matcher type: " + m.Type.String() + m.String())
	}
}

func estimateEqualMatcherCardinality(ctx context.Context, m *labels.Matcher, stats index.Statistics) uint64 {
	if m.Matches("") { // foo=""
		return numSeriesWithoutLabel(ctx, m.Name, stats)
	}
	// foo="bar"
	return stats.LabelValuesCardinality(ctx, m.Name, m.Value)
}

func estimateNotEqualMatcherCardinality(ctx context.Context, m *labels.Matcher, stats index.Statistics) uint64 {
	if m.Value == "" { // foo!=""
		return stats.LabelValuesCardinality(ctx, m.Name)
	}
	// foo!="bar" matches all series except those with foo="bar"
	return stats.TotalSeries() - stats.LabelValuesCardinality(ctx, m.Name, m.Value)
}

func estimateRegexMatcherCardinality(ctx context.Context, m *labels.Matcher, stats index.Statistics, selectivity float64) uint64 {
	var matchedSeries uint64
	if setMatches := m.SetMatches(); len(setMatches) > 0 { // foo=~"bar|baz", foo=~"|bar"
		matchedSeries = stats.LabelValuesCardinality(ctx, m.Name, setMatches...)
	} else {
		// Generic regex - estimate using selectivity
		labelNameCardinality := stats.LabelValuesCardinality(ctx, m.Name)
		matchedSeries = uint64(float64(labelNameCardinality) * selectivity)
	}
	if m.Matches("") { // foo=~"", foo=~"|bar", foo=~"bar?", foo=~".*"
		// Regex matches empty string. This matches all series which don't have this label name.
		matchedSeries += numSeriesWithoutLabel(ctx, m.Name, stats)
	}
	return matchedSeries
}

func estimateNotRegexMatcherCardinality(ctx context.Context, m *labels.Matcher, stats index.Statistics, selectivity float64) uint64 {
	matchedSeries := uint64(0)

	// Calculate how many series are matched by the regex as if the regex was positive.
	if setMatches := m.SetMatches(); len(setMatches) > 0 { // foo!~"bar|baz", foo!~"|bar"
		matchedSeries = stats.LabelValuesCardinality(ctx, m.Name, setMatches...)
	} else {
		// Generic regex - estimate using selectivity
		labelNameCardinality := stats.LabelValuesCardinality(ctx, m.Name)
		matchedSeries = uint64(float64(labelNameCardinality) * selectivity)
	}
	// Account for the negation
	matchedSeries = stats.TotalSeries() - matchedSeries

	if m.Matches("") { // foo!~"bar", foo!~"bar|baz"
		// Regex matches empty string. This matches all series which don't have this label name.
		matchedSeries += numSeriesWithoutLabel(ctx, m.Name, stats)
	}

	return matchedSeries
}

func numSeriesWithoutLabel(ctx context.Context, labelName string, stats index.Statistics) uint64 {
	return stats.TotalSeries() - stats.LabelValuesCardinality(ctx, labelName)
}

func (pr planPredicate) indexLookupCost() float64 {
	cost := 0.0
	// Running the matcher against all label values.
	cost += pr.indexScanCost

	// Retrieving each posting list (e.g. checksumming, disk seeking)
	cost += pr.config.RetrievedPostingListCost * float64(pr.labelNameUniqueVals) * pr.selectivity

	return cost
}

// filterCost is the singleMatchCost to run the matcher against all series.
func (pr planPredicate) filterCost(series uint64) float64 {
	return float64(series) * pr.singleMatchCost
}
