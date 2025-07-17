// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
)

type planPredicate struct {
	matcher *labels.Matcher

	// selectivity is between 0 and 1. 1 indicates that the matcher will match all label values, 0 indicates it will match no values. NB: label values, not series
	selectivity float64
	// cardinality is the estimation of how many series this matcher matches on its own.
	cardinality         int64
	labelNameUniqueVals int64
	// perMatchCost is how much it costs to run this matcher against an arbitrary label value.
	perMatchCost float64
	// indexScanCost is the perMatchCost to run the matcher against all label values (or at least enough to know all the values it matches).
	// This is naively perMatchCost * labelNameUniqueVals, but it might be lower if the matcher is a prefix matcher or an exact matcher.
	indexScanCost float64
}

func newPlanPredicate(ctx context.Context, m *labels.Matcher, stats Statistics) (planPredicate, error) {
	var err error
	pred := planPredicate{
		matcher:      m,
		perMatchCost: m.SingleMatchCost(),
	}
	pred.labelNameUniqueVals, err = stats.LabelValuesCount(ctx, m.Name)
	if err != nil {
		return planPredicate{}, fmt.Errorf("error getting label values count for label %s: %w", m.Name, err)
	}
	pred.selectivity = m.EstimateSelectivity(pred.labelNameUniqueVals)

	seriesBehindSelectedValues := int64(0)
	setMatches := m.SetMatches()
	switch m.Type {
	case labels.MatchEqual, labels.MatchNotEqual:
		if m.Value != "" {
			seriesBehindSelectedValues, err = stats.LabelValuesCardinality(ctx, m.Name, m.Value)
		} else {
			seriesBehindSelectedValues, err = stats.LabelValuesCardinality(ctx, m.Name)
		}
	case labels.MatchRegexp, labels.MatchNotRegexp:
		if len(setMatches) > 0 {
			seriesBehindSelectedValues, err = stats.LabelValuesCardinality(ctx, m.Name, setMatches...)
		} else {
			seriesBehindSelectedValues, err = stats.LabelValuesCardinality(ctx, m.Name)
			seriesBehindSelectedValues = int64(float64(seriesBehindSelectedValues) * pred.selectivity)
		}
	}
	if m.Type == labels.MatchNotEqual || m.Type == labels.MatchNotRegexp {
		pred.cardinality = stats.TotalSeries() - seriesBehindSelectedValues
	} else {
		pred.cardinality = seriesBehindSelectedValues
	}
	if err != nil {
		return planPredicate{}, fmt.Errorf("error getting series per label value for label %s: %w", m.Name, err)
	}

	switch pred.matcher.Type {
	case labels.MatchEqual, labels.MatchNotEqual:
		if m.Value == "" {
			pred.indexScanCost = pred.perMatchCost * float64(pred.labelNameUniqueVals)
		} else {
			pred.indexScanCost = pred.perMatchCost * 32 // for on-disk index we'd scan through 32 label values and compare them to the needle before returning.
		}
	case labels.MatchRegexp, labels.MatchNotRegexp:
		// TODO benchmark relative cost
		switch {
		case pred.matcher.Prefix() != "":
			pred.indexScanCost = pred.perMatchCost * float64(pred.labelNameUniqueVals) * 0.1
		case pred.matcher.IsRegexOptimized():
			if len(setMatches) > 0 {
				pred.indexScanCost = pred.perMatchCost * float64(len(setMatches))
			} else {
				pred.indexScanCost = pred.perMatchCost * float64(pred.labelNameUniqueVals) / 10 // Optimized regexes are expected to be faster.
			}
		default:
			pred.indexScanCost = pred.perMatchCost * float64(pred.labelNameUniqueVals)
		}
	}

	return pred, nil
}
