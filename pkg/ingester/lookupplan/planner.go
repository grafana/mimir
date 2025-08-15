// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type NoopPlanner struct{}

func (i NoopPlanner) PlanIndexLookup(_ context.Context, plan index.LookupPlan, _, _ int64) (index.LookupPlan, error) {
	return plan, nil
}

type CostBasedPlanner struct {
	statistician Statistician

	metrics Metrics
}

func NewCostBasedPlanner(metrics Metrics, statistician Statistician) *CostBasedPlanner {
	return &CostBasedPlanner{
		metrics:      metrics,
		statistician: statistician,
	}
}

func (p CostBasedPlanner) PlanIndexLookup(ctx context.Context, inPlan index.LookupPlan, _, _ int64) (_ index.LookupPlan, retErr error) {
	var allPlans []plan
	defer func(start time.Time) {
		var abortedEarly bool
		retErr, abortedEarly = mapPlanningOutcomeError(retErr)
		p.recordPlanningOutcome(ctx, start, abortedEarly, retErr, allPlans)
	}(time.Now())

	// Obtain a copy of the statistics and use the same statistics for the entire planning process.
	stats, err := p.statistician.Statistics(ctx)
	if err != nil {
		return nil, fmt.Errorf("error obtaining statistics: %w", err)
	}

	// Repartition the matchers. We don't trust other planners.
	// Allocate a new slice so that we don't mess up the slice of the caller.
	matchers := slices.Concat(inPlan.IndexMatchers(), inPlan.ScanMatchers())
	if len(matchers) > 10 {
		return inPlan, errTooManyMatchers
	}

	allPlans, err = p.generatePlans(ctx, stats, matchers)
	if err != nil {
		return nil, fmt.Errorf("error generating plans: %w", err)
	}

	slices.SortFunc(allPlans, func(a, b plan) int {
		return cmp.Compare(a.totalCost(), b.totalCost())
	})

	// Select the cheapest plan that has at least one index matcher.
	// PostingsForMatchers will return incorrect results if there are no matchers.
	for _, p := range allPlans {
		if len(p.IndexMatchers()) > 0 {
			return p, nil
		}
	}

	return nil, fmt.Errorf("no plan with index matchers found out of %d plans", len(allPlans))
}

var errTooManyMatchers = errors.New("too many matchers to generate plans")

func (p CostBasedPlanner) generatePlans(ctx context.Context, stats Statistics, matchers []*labels.Matcher) ([]plan, error) {
	noopPlan, err := newScanOnlyPlan(ctx, matchers, stats)
	if err != nil {
		return nil, fmt.Errorf("error generating index lookup plan: %w", err)
	}
	allPlans := make([]plan, 0, 1<<uint(len(matchers)))

	return generatePredicateCombinations(allPlans, noopPlan, 0), nil
}

// generatePredicateCombinations recursively generates all possible plans with their predicates toggled as index or as scan predicates.
// It generates 2^n plans for n predicates and appends them to the plans slice.
// It also returns the plans slice with all the generated plans.
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

func mapPlanningOutcomeError(err error) (mappedError error, tooManyMatchers bool) {
	if errors.Is(err, errTooManyMatchers) {
		return nil, true
	}
	return err, false
}

func (p CostBasedPlanner) recordPlanningOutcome(ctx context.Context, start time.Time, abortedEarly bool, retErr error, allPlans []plan) {
	logger := spanlogger.FromContext(ctx, log.NewNopLogger())

	var outcome string
	switch {
	case abortedEarly:
		outcome = "aborted_due_to_too_many_matchers"
		logger.DebugLog("msg", "aborted creating lookup plan because there are too many matchers")
	case retErr != nil:
		outcome = "error"
		logger.DebugLog("msg", "failed to create lookup plan", "err", retErr)
	default:
		outcome = "success"
		const topKPlans = 3
		logKvs := make([]any, 0, 2 /* msg */ +topKPlans*2 /* key+value */ *(5 /* cost */ +len(allPlans[0].predicates) /* matchers */))
		logKvs = append(logKvs,
			"msg", "selected lookup plan",
		)
		for i, plan := range allPlans[:min(topKPlans, len(allPlans))] {
			planFieldPrefix := "selected_plan"
			if i > 0 {
				planFieldPrefix = fmt.Sprintf("plan_%d", i+1)
			}
			logKvs = plan.appendLogKVs(logKvs, planFieldPrefix)
		}
		logger.DebugLog(logKvs...)
	}
	p.metrics.planningDuration.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
}
