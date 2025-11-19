// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"container/heap"
	"context"
	"iter"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/sharding"
)

type planWithCost struct {
	plan
	cost float64
}

func newPlanWithCost(p plan, numDecidedPredicates int) planWithCost {
	p.numDecidedPredicates = numDecidedPredicates
	return planWithCost{
		plan: p,
		cost: p.TotalCost(),
	}
}

// plans implements heap.Interface for a min-heap of plans ordered by lower bound cost.
type plans []planWithCost

func (pq plans) Len() int { return len(pq) }

func (pq plans) Less(i, j int) bool {
	return pq[i].cost < pq[j].cost
}

func (pq plans) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *plans) Push(x interface{}) {
	*pq = append(*pq, x.(planWithCost))
}

func (pq *plans) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func (pq plans) Iterator() iter.Seq[plan] {
	return func(f func(plan) bool) {
		for _, p := range pq {
			if !f(p.plan) {
				return
			}
		}
	}
}

// generatePlansBranchAndBound uses branch-and-bound to explore the space of possible plans.
// It prunes branches that cannot possibly lead to a better plan than the current best.
func (p CostBasedPlanner) generatePlansBranchAndBound(ctx context.Context, statistics index.Statistics, matchers []*labels.Matcher, pools *costBasedPlannerPools, shard *sharding.ShardSelector) iter.Seq[plan] {
	// Initialize priority queue with the root plan (all predicates undecided)
	prospectPlans := pools.GetPlans(maxPlansForPlanning)

	scanOnlyPlan := newScanOnlyPlan(ctx, statistics, p.config, matchers, pools.indexPredicatesPool, shard)
	heap.Push(prospectPlans, newPlanWithCost(scanOnlyPlan, 0))
	numPredicates := len(scanOnlyPlan.predicates)

	// Fall back to index-only plan to ensure that our code doesn't choose a more expensive plan than the naive plan.
	// Also include this in the initial prospect plans to pick a better upper bound on the first iteration.
	indexOnlyPlan := newIndexOnlyPlan(ctx, statistics, p.config, matchers, pools.indexPredicatesPool, shard)
	heap.Push(prospectPlans, newPlanWithCost(indexOnlyPlan, numPredicates))

	completePlans := pools.GetPlans(maxPlansForPlanning)
	bestCompleteCost := math.MaxFloat64

	for i := maxPlansForPlanning; prospectPlans.Len() > 0 && i > 0; i-- {
		current := heap.Pop(prospectPlans).(planWithCost)

		// Prune: if lower bound is worse than best complete plan, skip this branch
		if current.cost >= bestCompleteCost {
			continue
		}

		// Check if this is a complete plan (all predicates decided)
		if current.numDecidedPredicates == numPredicates {
			if !current.hasAnyIndexPredicate() {
				// We only want plans with at least one index predicate here.
				// Plans without index predicates will return no postings.
				// This means we should also not use scan-only plans for pruning because their low cost is not a cost we can actually achieve.
				continue
			}
			heap.Push(completePlans, current)

			// Update best complete cost for pruning
			if current.cost < bestCompleteCost {
				bestCompleteCost = current.cost
			}
			continue
		}

		// Branch: create children by deciding the next undecided predicate
		indexChild := current.UseIndexFor(current.numDecidedPredicates)
		heap.Push(prospectPlans, newPlanWithCost(indexChild, current.numDecidedPredicates+1))
		heap.Push(prospectPlans, newPlanWithCost(current.plan, current.numDecidedPredicates+1))
	}

	// Push all plans from the smaller heap into the larger one
	// We need this because we will need to find a plan with at least one index matcher later,
	// and we might not find that in either of the heaps alone.
	return mergePlans(completePlans, prospectPlans).Iterator()
}

func mergePlans(completePlans, prospectPlans *plans) *plans {
	for prospectPlans.Len() > 0 {
		p := heap.Pop(prospectPlans).(planWithCost)
		// Ensure plan is marked as complete for proper cost calculation
		p.numDecidedPredicates = len(p.predicates)
		p.cost = p.TotalCost()
		heap.Push(completePlans, p)
	}
	return completePlans
}
