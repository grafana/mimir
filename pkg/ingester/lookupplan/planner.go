package lookupplan

import (
	"context"
	"fmt"
	"slices"

	"github.com/prometheus/prometheus/model/labels"
)

// TODO dimitarvdimitrov break up into the files
type CostBasedPlanner struct {
	stats Statistics

	metrics metrics
}

// TODO dimitarvdimitrov add constructor

func (p CostBasedPlanner) PlanIndexLookup(ctx context.Context, plan LookupPlan, _, _ int64) (LookupPlan, error) {
	// Repartition the matchers. We don't trust other planners.
	matchers := append(plan.IndexMatchers(), plan.ScanMatchers()...)
	if len(matchers) > 10 {
		// TODO dimitarvdimitrov metrics & spanlogger DebugLog
		return plan, nil
	}

	allPlans, err := p.generatePlans(ctx, matchers)
	if err != nil {
		return nil, fmt.Errorf("error generating plans: %w", err)
	}

	lowestCostPlan := allPlans[0]
	for _, plan := range allPlans {
		if plan.totalCost < lowestCostPlan.totalCost {
			lowestCostPlan = plan
		}
	}

	return lowestCostPlan, nil
}

const (
	// TODO verify relative costs here; they will be different for on-disk and in-memory
	costPerIteratedPosting      = 0.01
	costPerPostingListRetrieval = 10.0
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

func (p CostBasedPlanner) generatePlans(ctx context.Context, matchers []*labels.Matcher) ([]plan, error) {
	labelMustBeSet := make(map[string]bool, len(matchers))
	for _, m := range matchers {
		if !m.Matches("") {
			labelMustBeSet[m.Name] = true
		}
	}

	noopPlan, err := newPlanWithoutEstimation(ctx, matchers, p.stats)
	if err != nil {
		return nil, fmt.Errorf("error generating index lookup plan: %w", err)
	}
	allPlans := make([]plan, 0, 1<<uint(len(matchers)))

	return generatePredicateCombinations(allPlans, noopPlan, 0), nil
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

func generatePredicateCombinations(plans []plan, currentPlan plan, decidedPredicates int) []plan {
	if decidedPredicates == len(currentPlan.predicates) {
		return append(plans, currentPlan)
	}

	// Generate two plans, one with the current predicate applied and one without.
	// This is done by copying the current plan and applying the predicate to the copy.
	// The copy is then added to the list of plans to be returned.
	plans = generatePredicateCombinations(plans, currentPlan, decidedPredicates+1)

	p := currentPlan.applyPredicate(decidedPredicates)
	plans = generatePredicateCombinations(plans, p, decidedPredicates+1)

	return plans
}
