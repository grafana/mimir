// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/grafana/dskit/tracing"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util/pool"
)

type NoopPlanner struct{}

func (i NoopPlanner) PlanIndexLookup(_ context.Context, plan index.LookupPlan, _ *storage.SelectHints) (index.LookupPlan, error) {
	return plan, nil
}

var (
	rawPlansWithCostPool   = &sync.Pool{}
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
	plansWithCostPool   *pool.SlabPool[planWithCost]
	indexPredicatesPool *pool.SlabPool[bool]
}

func newCostBasedPlannerPools() *costBasedPlannerPools {
	return &costBasedPlannerPools{
		plansWithCostPool:   pool.NewSlabPool[planWithCost](rawPlansWithCostPool, maxPlansForPlanning),
		indexPredicatesPool: pool.NewSlabPool[bool](rawIndexPredicatesPool, predicateIndexSlicesTotalLen),
	}
}

func (p *costBasedPlannerPools) GetPlans(num int) *plans {
	pls := p.plansWithCostPool.Get(num)[:0]
	return (*plans)(&pls)
}

func (p *costBasedPlannerPools) Release() {
	p.plansWithCostPool.Release()
	p.indexPredicatesPool.Release()
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

	var allPlans iter.Seq[plan]
	memPools := newCostBasedPlannerPools()
	defer memPools.Release()
	defer func() {
		if plan, ok := retPlan.(plan); ok {
			retPlan = plan.withoutMemoryPool()
		}
	}()

	defer func(start time.Time) {
		var selectedPlan *plan
		if p, ok := retPlan.(*plan); ok {
			selectedPlan = p
		}
		p.recordPlanningOutcome(ctx, start, retErr, selectedPlan, allPlans)
	}(time.Now())

	// Repartition the matchers. We don't trust other planners.
	// Allocate a new slice so that we don't mess up the slice of the caller.
	matchers := slices.Concat(inPlan.IndexMatchers(), inPlan.ScanMatchers())

	allPlans = p.generatePlansBranchAndBound(ctx, p.stats, matchers, memPools, shard)

	lookupPlan := p.chooseBestPlan(allPlans)
	if lookupPlan == nil {
		return nil, fmt.Errorf("no plan with index matchers found")
	}

	return *lookupPlan, nil
}

func (p CostBasedPlanner) chooseBestPlan(allPlans iter.Seq[plan]) *plan {
	// Select the first plan that has at least one index matcher.
	// PostingsForMatchers will return incorrect results if there are no matchers.
	for p := range allPlans {
		p := p // make a copy of the plan because the iterator might be reusing the same memory
		if len(p.IndexMatchers()) > 0 {
			return &p
		}
	}
	return nil
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
