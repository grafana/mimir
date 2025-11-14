// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"container/heap"
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/sharding"
)

// partialPlan represents a plan where only some predicates have been decided.
type partialPlan struct {
	planWithCost
	decidedPredicates int
}

// partialPlans implements heap.Interface for a min-heap of partial plans ordered by lower bound.
type partialPlans []partialPlan

func (pq partialPlans) AsPlans() []plan {
	plans := make([]plan, len(pq))
	for i, pp := range pq {
		plans[i] = pp.plan.withoutMemoryPool()
	}
	return plans
}

func (pq partialPlans) Len() int { return len(pq) }

func (pq partialPlans) Less(i, j int) bool {
	return pq[i].totalCost < pq[j].totalCost
}

func (pq partialPlans) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *partialPlans) Push(x interface{}) {
	*pq = append(*pq, x.(partialPlan))
}

func (pq *partialPlans) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// generatePlansBranchAndBound uses branch-and-bound to explore the space of possible plans.
// It prunes branches that cannot possibly lead to a better plan than the current best.
func (p CostBasedPlanner) generatePlansBranchAndBound(ctx context.Context, statistics index.Statistics, matchers []*labels.Matcher, pools *costBasedPlannerPools, shard *sharding.ShardSelector) []plan {
	basePlan := newScanOnlyPlan(ctx, statistics, p.config, matchers, pools.indexPredicatesPool, shard)

	// Initialize the priority queue with the base plan
	prospectPlans := &partialPlans{partialPlan{
		planWithCost:      planWithCost{plan: basePlan, totalCost: basePlan.TotalCost()},
		decidedPredicates: 0,
	}}
	heap.Init(prospectPlans)
	// add one plan where each predicate uses the index and the rest use scan
	for i := 0; i < len(basePlan.predicates); i++ {
		indexOnlyPlan := basePlan.UseIndexFor(i)
		heap.Push(prospectPlans, partialPlan{
			planWithCost:      planWithCost{plan: indexOnlyPlan, totalCost: indexOnlyPlan.TotalCost()},
			decidedPredicates: 0,
		})
	}

	decidedPlans := &partialPlans{}

	for i := 0; i < maxPlansForPlanning && prospectPlans.Len() > 0; i++ {
		current := heap.Pop(prospectPlans).(partialPlan)

		if current.decidedPredicates == len(current.plan.predicates) {
			// All predicates have been decided, add to decided plans
			heap.Push(decidedPlans, current)
			continue
		}

		// Branch: try both scan (false) and index (true) for the next predicate
		for _, useIndex := range []bool{false, true} {
			childPlan := current.plan
			if useIndex {
				childPlan = childPlan.UseIndexFor(current.decidedPredicates)
			}

			// Calculate lower bound for the child plan
			heap.Push(prospectPlans, partialPlan{
				planWithCost: planWithCost{
					plan:      childPlan,
					totalCost: childPlan.TotalCost(),
				},
				decidedPredicates: current.decidedPredicates + 1,
			})
		}
	}

	const maxReturnedPlans = 3
	// If we don't have enough decided plans, fill from prospect plans, they're the best we have.
	for decidedPlans.Len() < maxReturnedPlans && prospectPlans.Len() > 0 {
		heap.Push(decidedPlans, heap.Pop(prospectPlans).(partialPlan))
	}
	// Return only the top maxReturnedPlans plans
	for decidedPlans.Len() > maxReturnedPlans {
		heap.Remove(decidedPlans, decidedPlans.Len()-1)
	}

	return decidedPlans.AsPlans()
}
