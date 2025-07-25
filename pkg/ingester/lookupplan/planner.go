// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

type NoopPlanner struct{}

func (i NoopPlanner) PlanIndexLookup(_ context.Context, plan index.LookupPlan, _, _ int64) (index.LookupPlan, error) {
    return plan, nil
}

// TODO dimitarvdimitrov break up into the files
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

func (p CostBasedPlanner) PlanIndexLookup(ctx context.Context, plan LookupPlan, _, _ int64) (_ LookupPlan, retErr error) {
	defer func(start time.Time) {
		var abortedEarly bool
		retErr, abortedEarly = mapPlanningOutcomeError(retErr)
		p.recordPlanningOutcome(start, abortedEarly, retErr)
	}(time.Now())

	// Repartition the matchers. We don't trust other planners.
	matchers := append(plan.IndexMatchers(), plan.ScanMatchers()...)
	if len(matchers) > 10 {
		return plan, errTooManyMatchers
	}

	allPlans, err := p.generatePlans(ctx, matchers)
	if err != nil {
		return nil, fmt.Errorf("error generating plans: %w", err)
	}

	lowestCostPlan := allPlans[0]
	for _, plan := range allPlans {
		if plan.totalCost() < lowestCostPlan.totalCost() {
			lowestCostPlan = plan
		}
	}

	return lowestCostPlan, nil
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
	if abortedEarly {
		outcome = "aborted_due_to_too_many_matchers"
	}
	if retErr != nil {
		outcome = "error"
	}
	p.metrics.planningDuration.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
}

func (p CostBasedPlanner) generatePlans(ctx context.Context, matchers []*labels.Matcher) ([]plan, error) {
	labelMustBeSet := make(map[string]bool, len(matchers))
	for _, m := range matchers {
		if !m.Matches("") {
			labelMustBeSet[m.Name] = true
		}
	}

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

	// Generate two plans, one with the current predicate using the index and one using a sequential scan.
	// This is done by copying the current plan and applying the predicate to the copy.
	// The copy is then added to the list of plans to be returned.
	plans = generatePredicateCombinations(plans, currentPlan, decidedPredicates+1)

	p := currentPlan.useIndexFor(decidedPredicates)
	plans = generatePredicateCombinations(plans, p, decidedPredicates+1)

	return plans
}
