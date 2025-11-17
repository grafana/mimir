// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/grafana/dskit/tracing"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util/pool"
)

type NoopPlanner struct{}

func (i NoopPlanner) PlanIndexLookup(_ context.Context, plan index.LookupPlan, _ *storage.SelectHints) (index.LookupPlan, error) {
	return plan, nil
}

var (
	rawPlansWithCostPool   = &sync.Pool{}
	rawPartialPlansPool    = &sync.Pool{}
	rawPlansPool           = &sync.Pool{}
	rawIndexPredicatesPool = &sync.Pool{}
)

const (
	// maxMatchersForPlanning is arbitrary. Since we generate 2^N plans for N matchers, we want to avoid allocating too many plans.
	maxMatchersForPlanning = 10
	maxPlansForPlanning    = 1 << maxMatchersForPlanning

	// predicateIndexSlicesTotalLen is used to size the pools for the booleans which say if a predicate is an index predicate or not.
	// We want to size the pool so we can fit a plan with numMatchersForSingleAlloc matchers by taking a single slab.
	// We'd need 2^numMatchersForSingleAlloc plans, and each plan will have numMatchersForSingleAlloc number of matchers.
	predicateIndexSlicesTotalLen = 1 << numMatchersForSingleAlloc * numMatchersForSingleAlloc
	// numMatchersForSingleAlloc is an arbitrary number so we don't leave a lot of empty space in predicateIndexSlicesTotalLen
	// while accounting for most of the queries that may come in.
	numMatchersForSingleAlloc = 6
)

type costBasedPlannerPools struct {
	partialPlansPool    *pool.SlabPool[partialPlan]
	plansWithCostPool   *pool.SlabPool[planWithCost]
	plansPool           *pool.SlabPool[plan]
	indexPredicatesPool *pool.SlabPool[bool]
}

func newCostBasedPlannerPools() *costBasedPlannerPools {
	return &costBasedPlannerPools{
		partialPlansPool:    pool.NewSlabPool[partialPlan](rawPartialPlansPool, maxPlansForPlanning),
		plansWithCostPool:   pool.NewSlabPool[planWithCost](rawPlansWithCostPool, maxPlansForPlanning),
		plansPool:           pool.NewSlabPool[plan](rawPlansPool, maxPlansForPlanning),
		indexPredicatesPool: pool.NewSlabPool[bool](rawIndexPredicatesPool, predicateIndexSlicesTotalLen),
	}
}

func (p *costBasedPlannerPools) Release() {
	p.partialPlansPool.Release()
	p.plansWithCostPool.Release()
	p.plansPool.Release()
	p.indexPredicatesPool.Release()
}

type planWithCost struct {
	plan
	totalCost float64
}

type CostBasedPlanner struct {
	config  CostConfig
	stats   index.Statistics
	metrics Metrics
}

func NewCostBasedPlanner(metrics Metrics, statistics index.Statistics, config CostConfig) *CostBasedPlanner {
	return &CostBasedPlanner{
		config:  config,
		metrics: metrics,
		stats:   statistics,
	}
}

var (
	BnBTotalCosts        = atomic.NewFloat64(0)
	BnBProperTotalCosts  = atomic.NewFloat64(0)
	ExhaustiveTotalCosts = atomic.NewFloat64(0)
	ThirdBestTotalCosts  = atomic.NewFloat64(0)
)

func (p CostBasedPlanner) PlanIndexLookup(ctx context.Context, inPlan index.LookupPlan, hints *storage.SelectHints) (retPlan index.LookupPlan, retErr error) {
	if planningDisabled(ctx) {
		return inPlan, nil
	}

	// Extract shard information from hints
	var shard *sharding.ShardSelector
	if hints != nil && hints.ShardCount > 0 {
		shard = &sharding.ShardSelector{
			ShardIndex: hints.ShardIndex,
			ShardCount: hints.ShardCount,
		}
	}

	memPools := newCostBasedPlannerPools()
	defer memPools.Release()
	defer func() {
		if plan, ok := retPlan.(plan); ok {
			retPlan = plan.withoutMemoryPool()
		}
	}()

	startTime := time.Now()
	var allPlans iter.Seq[plan]
	defer func() {
		var selectedPlan *plan
		if p, ok := retPlan.(*plan); ok {
			selectedPlan = p
		}
		p.recordPlanningOutcome(ctx, startTime, retErr, selectedPlan, allPlans)
	}()

	// Repartition the matchers. We don't trust other planners.
	// Allocate a new slice so that we don't mess up the slice of the caller.
	matchers := slices.Concat(inPlan.IndexMatchers(), inPlan.ScanMatchers())
	//allPlans = p.generatePlansBranchAndBound(ctx, p.stats, matchers, memPools, shard)

	//allPlans = p.generatePlansProperBranchAndBound(ctx, p.stats, matchers, memPools, shard)

	allPlans = plansIteratorFromSlice(p.generateExhaustivePlans(ctx, p.stats, matchers, memPools, shard))
	//allPlansExhaustive = p.sortPlansByCost(memPools, allPlansExhaustive)
	//bestPlanOverall := p.chooseBestPlan(plansIteratorFromSlice(allPlansExhaustive))

	lookupPlan := p.chooseBestPlan(allPlans)
	if lookupPlan == nil {
		return nil, fmt.Errorf("no plan with index matchers found")
	}

	BnBTotalCosts.Add(lookupPlan.TotalCost())
	//BnBProperTotalCosts.Add(bestPlanBnB.TotalCost())
	//ExhaustiveTotalCosts.Add(bestPlanOverall.TotalCost())
	//ThirdBestTotalCosts.Add(allPlansExhaustive[min(2, len(allPlansExhaustive)-1)].TotalCost())

	return lookupPlan, nil
}

func (p CostBasedPlanner) chooseBestPlan(allPlans iter.Seq[plan]) *plan {
	// Select the first plan that has at least one index matcher.
	// PostingsForMatchers will return incorrect results if there are no matchers.
	for p := range allPlans {
		if len(p.IndexMatchers()) > 0 {
			return &p
		}
	}
	return nil
}

func (p CostBasedPlanner) generateExhaustivePlans(ctx context.Context, statistics index.Statistics, matchers []*labels.Matcher, pools *costBasedPlannerPools, shard *sharding.ShardSelector) []plan {
	noopPlan := newScanOnlyPlan(ctx, statistics, p.config, matchers, pools.indexPredicatesPool, shard)
	allPlans := pools.plansPool.Get(1 << uint(len(matchers)))[:0]

	return generateExhaustivePlans(allPlans, noopPlan, 0)
}

// generateExhaustivePlans recursively generates all possible plans with their predicates toggled as index or as scan predicates.
// It generates 2^n plans for n predicates and appends them to the plans slice.
// It also returns the plans slice with all the generated plans.
func generateExhaustivePlans(plans []plan, currentPlan plan, decidedPredicates int) []plan {
	if decidedPredicates == len(currentPlan.predicates) {
		return append(plans, currentPlan)
	}

	// Generate two plans, one with the current predicate applied and one without.
	// This is done by copying the current plan and applying the predicate to the copy.
	// The copy is then added to the list of plans to be returned.
	plans = generateExhaustivePlans(plans, currentPlan, decidedPredicates+1)

	p := currentPlan.UseIndexFor(decidedPredicates)
	plans = generateExhaustivePlans(plans, p, decidedPredicates+1)

	return plans
}

func (p CostBasedPlanner) recordPlanningOutcome(ctx context.Context, start time.Time, retErr error, selectedPlan *plan, remainingPlans iter.Seq[plan]) {
	span, _, traceSampled := tracing.SpanFromContext(ctx)

	var outcome string
	if retErr != nil {
		outcome = "error"
		if span != nil {
			span.AddEvent("failed to create lookup plan", trace.WithAttributes(
				attribute.String("error", retErr.Error()),
			))
		}
	} else {
		if selectedPlan != nil {
			if queryStats := QueryStatsFromContext(ctx); queryStats != nil {
				queryStats.SetEstimatedSelectedPostings(selectedPlan.NumSelectedPostings())
				queryStats.SetEstimatedFinalCardinality(selectedPlan.FinalCardinality())
			}

			if traceSampled {
				// Only add span events when tracing is sampled to avoid unnecessary overhead.
				p.addSpanEvents(span, start, *selectedPlan, remainingPlans)
			}
		}
		outcome = "success"
	}
	p.metrics.planningDuration.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
}

func (p CostBasedPlanner) addSpanEvents(span trace.Span, start time.Time, selectedPlan plan, remainingPlans iter.Seq[plan]) {
	span.AddEvent("selected lookup plan", trace.WithAttributes(
		attribute.Stringer("duration", time.Since(start)),
	))
	selectedPlan.AddPredicatesToSpan(span)

	const topKPlans = 2
	var i int
	for plan := range remainingPlans {
		if i >= topKPlans {
			break
		}
		planName := "selected_plan"
		if i > 0 {
			planName = strconv.Itoa(i + 1)
		}
		plan.AddSpanEvent(span, planName)
		i++
	}
}

func (p CostBasedPlanner) sortPlansByCost(memPools *costBasedPlannerPools, allPlansUnordered []plan) []plan {
	// calculate the cost of all plans once, instead of calculating them every time we compare during sort
	allPlansWithCosts := memPools.plansWithCostPool.Get(len(allPlansUnordered))
	for i, p := range allPlansUnordered {
		allPlansWithCosts[i] = planWithCost{plan: p, totalCost: p.TotalCost()}
	}

	slices.SortFunc(allPlansWithCosts, func(a, b planWithCost) int {
		return cmp.Compare(a.totalCost, b.totalCost)
	})

	// build the sorted slice of plans
	allPlans := memPools.plansPool.Get(len(allPlansWithCosts))[:0]
	for _, pwc := range allPlansWithCosts {
		allPlans = append(allPlans, pwc.plan)
	}

	return allPlans
}

func plansIteratorFromSlice(plans []plan) iter.Seq[plan] {
	return func(f func(plan) bool) {
		for _, p := range plans {
			if !f(p) {
				return
			}
		}
	}
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
