// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"fmt"
	"slices"

	"github.com/prometheus/prometheus/model/labels"
)

const (
	// TODO verify relative costs here; they will be different for on-disk and in-memory
	costPerIteratedPosting      = 0.01
	costPerPostingListRetrieval = 10.0
)

type plan struct {
	predicates  []planPredicate
	applied     []bool
	totalSeries int64

	indexLookupCost  float64
	intersectionCost float64
	filterCost       float64
	totalCost        float64
}

func newPlanWithoutEstimation(ctx context.Context, matchers []*labels.Matcher, stats Statistics) (plan, error) {
	p := plan{
		predicates:  make([]planPredicate, 0, len(matchers)),
		applied:     make([]bool, 0, len(matchers)),
		totalSeries: stats.TotalSeries(),
	}
	for _, m := range matchers {
		pred, err := newPlanPredicate(ctx, m, stats)
		if err != nil {
			return plan{}, fmt.Errorf("error converting matcher to plan predicate: %w", err)
		}
		p.predicates = append(p.predicates, pred)
		p.applied = append(p.applied, false)
	}

	return p, nil
}

func (p plan) IndexMatchers() []*labels.Matcher {
	var matchers []*labels.Matcher
	for i, pr := range p.predicates {
		if p.applied[i] {
			matchers = append(matchers, pr.matcher)
		}
	}
	return matchers
}

func (p plan) ScanMatchers() []*labels.Matcher {
	var matchers []*labels.Matcher
	for i, pr := range p.predicates {
		if !p.applied[i] {
			matchers = append(matchers, pr.matcher)
		}
	}
	return matchers
}

func (p plan) applyPredicate(predicateIdx int) plan {
	p.applied = slices.Clone(p.applied)
	p.applied[predicateIdx] = true
	return estimateTotalCost(p)
}

func (p plan) unapplyPredicate(predicateIdx int) plan {
	p.applied = slices.Clone(p.applied)
	p.applied[predicateIdx] = false
	return estimateTotalCost(p)
}

func estimateTotalCost(p plan) plan {
	p.indexLookupCost = 0
	p.intersectionCost = 0
	p.filterCost = 0
	p.totalCost = 0

	for i, pr := range p.predicates {
		if p.applied[i] {
			p.indexLookupCost += p.calculateIndexLookupCost(pr)
		}
	}

	p.intersectionCost = p.calculateIntersectionCost()

	fetchedSeries := p.intersectionSize()

	for i, m := range p.predicates {
		// In reality we will apply all the predicates for each series and stop once one predicate doesn't match.
		// But we calculate for the worst case where we have to run all predicates for all series.
		if !p.applied[i] {
			p.filterCost += p.calculateFilterCost(fetchedSeries, m)
		}
	}

	p.totalCost = p.indexLookupCost + p.intersectionCost + p.filterCost

	return p
}

func (p plan) calculateIndexLookupCost(pr planPredicate) float64 {
	cost := 0.0
	// Runing the matcher against all label values.
	cost += pr.indexScanCost

	// Retrieving each posting list (e.g. checksumming, disk seeking)
	cost += costPerPostingListRetrieval * float64(pr.labelNameUniqueVals) * pr.selectivity

	return cost
}

func (p plan) calculateIntersectionCost() float64 {
	iteratedPostings := int64(0)
	for i, pr := range p.predicates {
		if !p.applied[i] {
			continue
		}

		iteratedPostings += pr.cardinality
	}

	return float64(iteratedPostings) * costPerIteratedPosting
}

func (p plan) intersectionSize() int64 {
	finalSelectivity := 1.0
	for i, pr := range p.predicates {
		if !p.applied[i] {
			continue
		}

		// We use the selectivity across all series instead of the selectivity across label values.
		// For example, if {protocol=~.*} matches all values, it doesn't mean it won't reduce the result set after intersection.
		//
		// We also assume idependence between the predicates. This is a simplification.
		// For example, the selectivity of {pod=~prometheus.*} doesn't depend if we have already applied {statefulset=prometheus}.
		finalSelectivity *= float64(pr.cardinality) / float64(p.totalSeries)
	}
	return int64(finalSelectivity * float64(p.totalSeries))
}

// filterCost is the perMatchCost to run the matcher against all series.
func (p plan) calculateFilterCost(series int64, m planPredicate) float64 {
	return float64(series) * m.perMatchCost
}
