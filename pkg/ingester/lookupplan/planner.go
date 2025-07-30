// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

type NoopPlanner struct{}

func (i NoopPlanner) PlanIndexLookup(_ context.Context, plan index.LookupPlan, _, _ int64) (index.LookupPlan, error) {
	return plan, nil
}

type CostBasedPlanner struct {
	stats Statistics

	metrics Metrics
}

func NewCostBasedPlanner(metrics Metrics, statistics Statistics) *CostBasedPlanner {
	return &CostBasedPlanner{
		metrics: metrics,
		stats:   statistics,
	}
}

func (p CostBasedPlanner) PlanIndexLookup(ctx context.Context, plan index.LookupPlan, _, _ int64) (_ index.LookupPlan, retErr error) {
	defer func(start time.Time) {
		var abortedEarly bool
		retErr, abortedEarly = mapPlanningOutcomeError(retErr)
		p.recordPlanningOutcome(start, abortedEarly, retErr)
	}(time.Now())

	// Repartition the matchers. We don't trust other planners.
	// Allocate a new slice so that we don't mess up the slice of the caller.
	matchers := slices.Concat(plan.IndexMatchers(), plan.ScanMatchers())
	if len(matchers) > 10 {
		return plan, errTooManyMatchers
	}

	allPlans, err := p.generatePlans(ctx, matchers)
	if err != nil {
		return nil, fmt.Errorf("error generating plans: %w", err)
	}

	cheapestPlan, lowestCost := allPlans[0], allPlans[0].totalCost()
	for _, plan := range allPlans[1:] {
		if pCost := plan.totalCost(); pCost < lowestCost {
			cheapestPlan, lowestCost = plan, pCost
		}
	}

	return cheapestPlan, nil
}

var errTooManyMatchers = errors.New("too many matchers to generate plans")

func mapPlanningOutcomeError(err error) (mappedError error, tooManyMatchers bool) {
	if errors.Is(err, errTooManyMatchers) {
		return nil, true
	}
	return err, false
}

func (p CostBasedPlanner) recordPlanningOutcome(start time.Time, abortedEarly bool, retErr error) {
	outcome := "success"
	switch {
	case abortedEarly:
		outcome = "aborted_due_to_too_many_matchers"
	case retErr != nil:
		outcome = "error"
	}
	p.metrics.planningDuration.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
}

func (p CostBasedPlanner) generatePlans(ctx context.Context, matchers []*labels.Matcher) ([]plan, error) {
	noopPlan, err := newScanOnlyPlan(ctx, matchers, p.stats)
	if err != nil {
		return nil, fmt.Errorf("error generating index lookup plan: %w", err)
	}
	allPlans := make([]plan, 0, 1<<uint(len(matchers)))

	return generatePredicateCombinations(allPlans, noopPlan, 0), nil
}

func generatePredicateCombinations(plans []plan, currentPlan plan, decidedPredicates int) []plan {
	if decidedPredicates == len(currentPlan.predicates) {
		return append(plans, currentPlan)
	}

	// Generate two plans, one with the current predicate applied and one without.
	// This is done by copying the current plan and applying the predicate to the copy.
	// The copy is then added to the list of plans to be returned.
	plans = generatePredicateCombinations(plans, currentPlan, decidedPredicates+1)

	p := currentPlan.useIndexFor(decidedPredicates)
	plans = generatePredicateCombinations(plans, p, decidedPredicates+1)

	return plans
}
