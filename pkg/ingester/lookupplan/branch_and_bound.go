// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"container/heap"
	"context"
	"iter"
	"slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/sharding"
)

// partialPlan represents a plan where only some predicates have been decided.
type partialPlan struct {
	planWithCost
	decidedPredicates []bool
}

// partialPlans implements heap.Interface for a min-heap of partial plans ordered by lower bound.
type partialPlans []partialPlan

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

// Iterator returns an iterator that yields plans from the heap in order of cost (lowest first).
func (pq *partialPlans) Iterator() iter.Seq[plan] {
	return func(yield func(plan) bool) {
		for pq.Len() > 0 {
			partial := heap.Pop(pq).(partialPlan)
			if !yield(partial.plan) {
				return
			}
		}
	}
}

// PartialIterator returns an iterator that yields partial plans from the heap in order of cost (lowest first).
func (pq *partialPlans) PartialIterator() iter.Seq[partialPlan] {
	return func(yield func(plans partialPlan) bool) {
		for pq.Len() > 0 {
			partial := heap.Pop(pq).(partialPlan)
			if !yield(partial) {
				return
			}
		}
	}
}

// generatePlansBranchAndBound uses branch-and-bound to explore the space of possible plans.
// It prunes branches that cannot possibly lead to a better plan than the current best.
func (p CostBasedPlanner) generatePlansBranchAndBound(ctx context.Context, statistics index.Statistics, matchers []*labels.Matcher, pools *costBasedPlannerPools, shard *sharding.ShardSelector) iter.Seq[plan] {
	basePlan := newScanOnlyPlan(ctx, statistics, p.config, matchers, pools.indexPredicatesPool, shard)

	// Initialize the priority queue with the base plan
	prospectPlans := &partialPlans{partialPlan{
		planWithCost:      planWithCost{plan: basePlan, totalCost: basePlan.TotalCost()},
		decidedPredicates: make([]bool, len(basePlan.predicates)),
	}}
	heap.Init(prospectPlans)
	// add one plan where each predicate uses the index and the rest use scan
	for i := 0; i < len(basePlan.predicates); i++ {
		indexOnlyPlan := basePlan.UseIndexFor(i)
		decidedPredicates := make([]bool, len(basePlan.predicates))
		decidedPredicates[i] = true
		heap.Push(prospectPlans, partialPlan{
			planWithCost:      planWithCost{plan: indexOnlyPlan, totalCost: indexOnlyPlan.TotalCost()},
			decidedPredicates: decidedPredicates,
		})
	}

	decidedPlans := &partialPlans{}
	allDecided := slices.Repeat([]bool{true}, len(basePlan.predicates))

	for i := 0; i < maxPlansForPlanning && prospectPlans.Len() > 0; i++ {
		current := heap.Pop(prospectPlans).(partialPlan)

		if slices.Equal(current.decidedPredicates, allDecided) {
			// All predicates have been decided, add to decided plans
			heap.Push(decidedPlans, current)
			continue
		}

		// Branch: try both scan (false) and index (true) for the next predicate
		alreadyDecidedOneFalse := false
		for firstUndecidedPredicate, decided := range current.decidedPredicates {
			if decided {
				continue
			}
			decidedPredicates := slices.Clone(current.decidedPredicates)
			decidedPredicates[firstUndecidedPredicate] = true
			for _, useIndex := range []bool{false, true} {
				if !useIndex && alreadyDecidedOneFalse {
					// We already added a plan where this predicate uses scan, skip adding another one
					continue
				}
				alreadyDecidedOneFalse = true
				childPlan := current.plan
				if useIndex {
					childPlan = childPlan.UseIndexFor(firstUndecidedPredicate)
				}
				// Calculate lower bound for the child plan
				heap.Push(prospectPlans, partialPlan{
					planWithCost: planWithCost{
						plan:      childPlan,
						totalCost: childPlan.TotalCost(),
					},
					decidedPredicates: decidedPredicates,
				})
			}
		}
	}
	var (
		fewerPlans *partialPlans
		morePlans  *partialPlans
	)
	if decidedPlans.Len() > prospectPlans.Len() {
		fewerPlans, morePlans = prospectPlans, decidedPlans
	} else {
		fewerPlans, morePlans = decidedPlans, prospectPlans
	}
	// Push all plans from the smaller heap into the larger one until we reach maxReturnedPlans
	// We need this because we will need to find a plan with at least one index matcher later,
	// and we might not find that in either of the heaps alone.
	// We also don't just concatenate the two heaps because nothing guarantees that the decided plans are cheaper than the prospective ones.
	for p := range fewerPlans.PartialIterator() {
		heap.Push(morePlans, p)
	}

	return morePlans.Iterator()
}
