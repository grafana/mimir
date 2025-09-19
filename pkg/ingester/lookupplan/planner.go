// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/grafana/dskit/tracing"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type NoopPlanner struct{}

func (i NoopPlanner) PlanIndexLookup(_ context.Context, plan index.LookupPlan, _, _ int64) (index.LookupPlan, error) {
	return plan, nil
}

type CostBasedPlanner struct {
	stats   index.Statistics
	metrics Metrics
}

func NewCostBasedPlanner(metrics Metrics, statistics index.Statistics) *CostBasedPlanner {
	return &CostBasedPlanner{
		metrics: metrics,
		stats:   statistics,
	}
}

func (p CostBasedPlanner) PlanIndexLookup(ctx context.Context, inPlan index.LookupPlan, _, _ int64) (_ index.LookupPlan, retErr error) {
	if planningDisabled(ctx) {
		return inPlan, nil
	}

	var allPlans []plan
	defer func(start time.Time) {
		var abortedEarly bool
		retErr, abortedEarly = mapPlanningOutcomeError(retErr)
		p.recordPlanningOutcome(ctx, start, abortedEarly, retErr, allPlans)
	}(time.Now())

	// Repartition the matchers. We don't trust other planners.
	// Allocate a new slice so that we don't mess up the slice of the caller.
	matchers := slices.Concat(inPlan.IndexMatchers(), inPlan.ScanMatchers())
	if len(matchers) > 10 {
		return inPlan, errTooManyMatchers
	}

	allPlansUnordered := p.generatePlans(ctx, p.stats, matchers)

	type planWithCost struct {
		plan
		totalCost float64
	}
	// calculate the cost of all plans once, instead of calculating them every time we compare during sort
	allPlansWithCosts := make([]planWithCost, len(allPlansUnordered))
	for i, p := range allPlansUnordered {
		allPlansWithCosts[i] = planWithCost{plan: p, totalCost: p.totalCost()}
	}

	slices.SortFunc(allPlansWithCosts, func(a, b planWithCost) int {
		return cmp.Compare(a.totalCost, b.totalCost)
	})

	// build the sorted slice of plans
	for _, pwc := range allPlansWithCosts {
		allPlans = append(allPlans, pwc.plan)
	}

	// Select the cheapest plan that has at least one index matcher.
	// PostingsForMatchers will return incorrect results if there are no matchers.
	for i, p := range allPlans {
		if len(p.IndexMatchers()) > 0 {
			allPlans = allPlans[i:]
			return p, nil
		}
	}

	return nil, fmt.Errorf("no plan with index matchers found out of %d plans", len(allPlans))
}

var errTooManyMatchers = errors.New("too many matchers to generate plans")

func (p CostBasedPlanner) generatePlans(ctx context.Context, statistics index.Statistics, matchers []*labels.Matcher) []plan {
	noopPlan := newScanOnlyPlan(ctx, statistics, matchers)
	allPlans := make([]plan, 0, 1<<uint(len(matchers)))

	return generatePredicateCombinations(allPlans, noopPlan, 0)
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
	span, _, traceSampled := tracing.SpanFromContext(ctx)

	var outcome string
	switch {
	case abortedEarly:
		outcome = "aborted_due_to_too_many_matchers"
		if span != nil {
			span.AddEvent("aborted creating lookup plan because there are too many matchers")
		}
	case retErr != nil:
		outcome = "error"
		if span != nil {
			span.AddEvent("failed to create lookup plan", trace.WithAttributes(
				attribute.String("error", retErr.Error()),
			))
		}
	default:
		outcome = "success"
		if !traceSampled {
			// Avoid logging overhead if the events aren't going to make it into a span.
			break
		}
		span.AddEvent("selected lookup plan", trace.WithAttributes(
			attribute.Stringer("duration", time.Since(start)),
		))
		const topKPlans = 2
		allPlans[0].addPredicatesToSpan(span)
		for i, plan := range allPlans[:min(topKPlans, len(allPlans))] {
			planName := "selected_plan"
			if i > 0 {
				planName = strconv.Itoa(i + 1)
			}
			plan.addSpanEvent(span, planName)
		}
	}
	p.metrics.planningDuration.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
}

type contextKey string

const disabledPlanningContextKey contextKey = "disabled_planning"

func ContextWithDisabledPlanning(ctx context.Context) context.Context {
	return context.WithValue(ctx, disabledPlanningContextKey, true)
}

// planningDisabled checks if planning is disabled in the context
func planningDisabled(ctx context.Context) bool {
	val := ctx.Value(disabledPlanningContextKey)
	disabled, ok := val.(bool)
	return ok && disabled
}
