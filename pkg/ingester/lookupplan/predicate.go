// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
)

const costPerPostingListRetrieval = 10.0

type planPredicate struct {
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

func newPlanPredicate(ctx context.Context, m *labels.Matcher, stats Statistics) (planPredicate, error) {
	var err error
	pred := planPredicate{
		matcher:         m,
		singleMatchCost: m.SingleMatchCost(),
	}
	pred.labelNameUniqueVals, err = stats.LabelValuesCount(ctx, m.Name)
	if err != nil {
		return planPredicate{}, fmt.Errorf("error getting label values count for label %s: %w", m.Name, err)
	}
	pred.selectivity = m.EstimateSelectivity(pred.labelNameUniqueVals)

	pred.cardinality, err = estimatePredicateCardinality(ctx, m, stats, pred.selectivity)
	if err != nil {
		return planPredicate{}, fmt.Errorf("error estimating cardinality for label %s: %w", m.Name, err)
	}

	pred.indexScanCost = estimatePredicateIndexScanCost(pred, m)

	return pred, nil
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

func estimatePredicateCardinality(ctx context.Context, m *labels.Matcher, stats Statistics, selectivity float64) (uint64, error) {
	var (
		seriesBehindSelectedValues uint64
		matchesAnyValues           bool
		err                        error
	)

	switch m.Type {
	case labels.MatchEqual, labels.MatchNotEqual:
		if m.Value == "" {
			seriesBehindSelectedValues, err = stats.LabelValuesCardinality(ctx, m.Name)
			// The matcher selects all series, which don't have this label.
			seriesBehindSelectedValues = stats.TotalSeries() - seriesBehindSelectedValues
		} else {
			seriesBehindSelectedValues, err = stats.LabelValuesCardinality(ctx, m.Name, m.Value)
		}
		matchesAnyValues = seriesBehindSelectedValues > 0
	case labels.MatchRegexp, labels.MatchNotRegexp:
		if setMatches := m.SetMatches(); len(setMatches) > 0 {
			seriesBehindSelectedValues, err = stats.LabelValuesCardinality(ctx, m.Name, setMatches...)
			matchesAnyValues = seriesBehindSelectedValues > 0
		} else {
			var labelNameCardinality uint64
			labelNameCardinality, err = stats.LabelValuesCardinality(ctx, m.Name)
			matchesAnyValues = labelNameCardinality > 0
			if m.Matches("") {
				// The matcher selects all series, which don't have this label.
				seriesBehindSelectedValues += stats.TotalSeries() - labelNameCardinality
			}
			// The matcher selects some series with this label.
			seriesBehindSelectedValues += uint64(float64(labelNameCardinality) * selectivity)
		}
	}
	if err != nil {
		return 0, fmt.Errorf("error getting series per label value for label %s: %w", m.Name, err)
	}
	switch m.Type {
	case labels.MatchNotEqual, labels.MatchNotRegexp:
		if !matchesAnyValues {
			// This label name doesn't exist. This means that negating this will select everything.
			return stats.TotalSeries(), nil
		}
		return stats.TotalSeries() - seriesBehindSelectedValues, nil
	}
	return seriesBehindSelectedValues, nil
}

func (pr planPredicate) indexLookupCost() float64 {
	cost := 0.0
	// Running the matcher against all label values.
	cost += pr.indexScanCost

	// Retrieving each posting list (e.g. checksumming, disk seeking)
	cost += costPerPostingListRetrieval * float64(pr.labelNameUniqueVals) * pr.selectivity

	return cost
}

// filterCost is the singleMatchCost to run the matcher against all series.
func (pr planPredicate) filterCost(series uint64) float64 {
	return float64(series) * pr.singleMatchCost
}
