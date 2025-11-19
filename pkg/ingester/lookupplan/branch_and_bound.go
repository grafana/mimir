// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"container/heap"
	"context"
	"iter"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/sharding"
)

// partialPlan represents a plan where only some predicates have been decided.
// Predicates are decided in order from 0 to len(predicates)-1.
type partialPlan struct {
	plan

	// lowerBoundCost is the value of LowerBoundCost() cached for efficiency.
	lowerBoundCost float64
	// numDecidedPredicates tracks how many predicates have been decided (0 to len(predicates)).
	// Predicates [0, numDecidedPredicates) have been decided.
	numDecidedPredicates int
}

func partialPlanWithLowerBound(p plan, numDecided int) partialPlan {
	partial := partialPlan{
		plan:                 p,
		numDecidedPredicates: numDecided,
	}
	partial.lowerBoundCost = partial.LowerBoundCost()
	return partial
}

func (p partialPlan) hasAnyIndexPredicate() bool {
	for _, useIndex := range p.indexPredicate {
		if useIndex {
			return true
		}
	}
	return false
}

func (p partialPlan) LowerBoundCost() float64 {
	return p.indexLookupCost() + p.intersectionCost() + p.seriesRetrievalCost() + p.filterCost()
}

// indexLookupCost returns the cost of performing index lookups for all predicates that use the index
func (p partialPlan) indexLookupCost() float64 {
	cost := 0.0
	for i := range p.predicates {
		pr, ok := p.virtualPredicate(i)
		if !ok {
			continue
		}

		cost += pr.indexLookupCost()
	}
	return cost
}

// virtualPredicate returns the predicate at idx and whether it's an index predicate.
// For undecided predicates:
// - The first undecided predicate is treated as an index predicate for lower bound calculation
// - All other undecided predicates are treated as scan predicates with minimal cost
// This goal of virtual undecided predicates is to minimize the cost of the whole plan.
func (p partialPlan) virtualPredicate(idx int) (planPredicate, bool) {
	if idx < p.numDecidedPredicates {
		return p.predicates[idx], p.indexPredicate[idx]
	}

	virtualPred := p.predicates[idx]
	// Very cheap single match cost, but still non-zero so that there is a difference between using index and not using index for a predicate.
	virtualPred.singleMatchCost = 1
	// Don't assume 0 cardinality because that might make the whole plan have 0 cardinality which is unrealistic.
	virtualPred.cardinality = 1
	// Don't assume 0 unique label values because that might make the whole plan have 0 cardinality which is unrealistic.
	virtualPred.labelNameUniqueVals = 1
	// We don't want selectivity of 0 because then the cost of the rest of the predicates might not matter.
	virtualPred.selectivity = 1
	// Assume extremely cheap index scan cost.
	virtualPred.indexScanCost = 1

	return virtualPred, idx == p.numDecidedPredicates
}

// intersectionCost returns the cost of intersecting posting lists from multiple index predicates
// This includes retrieving the series' labels from the index.
func (p partialPlan) intersectionCost() float64 {
	iteratedPostings := uint64(0)
	for i := range p.predicates {
		pred, ok := p.virtualPredicate(i)
		if !ok {
			continue
		}

		iteratedPostings += pred.cardinality
	}

	return float64(iteratedPostings) * p.config.RetrievedPostingCost
}

// seriesRetrievalCost returns the cost of retrieving series from the index after intersecting posting lists.
// This includes retrieving the series' labels from the index and checking if the series belongs to the query's shard.
// Realistically we don't retrieve every series because we have the series hash cache, but we ignore that for simplicity.
func (p partialPlan) seriesRetrievalCost() float64 {
	return float64(p.NumSelectedPostings()) * p.config.RetrievedSeriesCost
}

// filterCost returns the cost of applying scan predicates to the fetched series.
// The sequence is: intersection → retrieve series → check shard → apply scan matchers.
func (p partialPlan) filterCost() float64 {
	cost := 0.0
	seriesToFilter := p.numSelectedPostingsInOurShard()
	for i := range p.predicates {
		// In reality, we will apply all the predicates for each series and stop once one predicate doesn't match.
		// But we calculate for the worst case where we have to run all predicates for all series.
		pred, ok := p.virtualPredicate(i)
		if ok {
			continue
		}

		cost += pred.filterCost(seriesToFilter)
	}
	return cost
}

func (p partialPlan) numSelectedPostingsInOurShard() uint64 {
	return shardedCardinality(p.NumSelectedPostings(), p.shard)
}

func (p partialPlan) NumSelectedPostings() uint64 {
	finalSelectivity := 1.0
	for i := range p.predicates {
		pred, ok := p.virtualPredicate(i)
		if !ok {
			continue
		}

		// We use the selectivity across all series instead of the selectivity across label values.
		// For example, if {protocol=~.*} matches all values, it doesn't mean it won't reduce the result set after intersection.
		//
		// We also assume independence between the predicates. This is a simplification.
		// For example, the selectivity of {pod=~prometheus.*} doesn't depend on if we have already applied {statefulset=prometheus}.
		// While finalSelectivity is neither an upper bound nor a lower bound, assuming independence allows us to come up with cost estimates comparable between plans.
		finalSelectivity *= float64(pred.cardinality) / float64(p.totalSeries)
	}
	return uint64(finalSelectivity * float64(p.totalSeries))
}

// nonShardedCardinality returns an estimate of the total number of series before query sharding is applied.
// This is the base cardinality considering only the selectivity of all predicates.
func (p partialPlan) nonShardedCardinality() uint64 {
	finalSelectivity := 1.0
	for i := range p.predicates {
		pred, _ := p.virtualPredicate(i)
		// We use the selectivity across all series instead of the selectivity across label values.
		// For example, if {protocol=~.*} matches all values, it could still reduce the result set after intersection.
		//
		// We also assume independence between the predicates. This is a simplification.
		// For example, the selectivity of {pod=~prometheus.*} doesn't depend on if we have already applied {statefulset=prometheus}.
		finalSelectivity *= float64(pred.cardinality) / float64(p.totalSeries)
	}
	return uint64(finalSelectivity * float64(p.totalSeries))
}

// FinalCardinality returns an estimate of the total number of series that this plan would return.
func (p partialPlan) FinalCardinality() uint64 {
	return shardedCardinality(p.nonShardedCardinality(), p.shard)
}

// partialPlans implements heap.Interface for a min-heap of partial plans ordered by lower bound.
type partialPlans []partialPlan

func (pq partialPlans) Len() int { return len(pq) }

func (pq partialPlans) Less(i, j int) bool {
	return pq[i].lowerBoundCost < pq[j].lowerBoundCost
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

func (pq partialPlans) Iterator() iter.Seq[plan] {
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
	// Initialize priority queue with the root partial plan (all predicates undecided)
	prospectPlans := pools.GetPartialPlans(maxPlansForPlanning)
	scanOnlyPlan := newScanOnlyPlan(ctx, statistics, p.config, matchers, pools.indexPredicatesPool, shard)
	heap.Push(prospectPlans, partialPlanWithLowerBound(scanOnlyPlan, 0))

	completePlans := pools.GetPartialPlans(maxPlansForPlanning)
	bestCompleteCost := float64(1<<63 - 1) // Start with max float64
	numPredicates := len(scanOnlyPlan.predicates)

	for i := maxPlansForPlanning; prospectPlans.Len() > 0 && i > 0; i-- {
		current := heap.Pop(prospectPlans).(partialPlan)

		// Prune: if lower bound is worse than best complete plan, skip this branch
		if current.lowerBoundCost >= bestCompleteCost {
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
			actualCost := current.plan.TotalCost()
			current.lowerBoundCost = actualCost
			heap.Push(completePlans, current)

			// Update best complete cost for pruning
			if actualCost < bestCompleteCost {
				bestCompleteCost = actualCost
			}
			continue
		}

		// Branch: create children by deciding the next undecided predicate
		nextPredicateIdx := current.numDecidedPredicates

		indexChild := current.plan.UseIndexFor(nextPredicateIdx)
		heap.Push(prospectPlans, partialPlanWithLowerBound(indexChild, nextPredicateIdx+1))
		heap.Push(prospectPlans, partialPlanWithLowerBound(current.plan, nextPredicateIdx+1))
	}

	// Fall back to index-only plan to ensure that our code doesn't choose a more expensive plan than the naive plan.
	indexOnlyPlan := newIndexOnlyPlan(ctx, statistics, p.config, matchers, pools.indexPredicatesPool, shard)
	heap.Push(completePlans, partialPlanWithLowerBound(indexOnlyPlan, numPredicates))

	// Push all plans from the smaller heap into the larger one
	// We need this because we will need to find a plan with at least one index matcher later,
	// and we might not find that in either of the heaps alone.
	return mergePlans(completePlans, prospectPlans).Iterator()
}

func mergePlans(completePlans, prospectPlans *partialPlans) *partialPlans {
	for prospectPlans.Len() > 0 {
		p := heap.Pop(prospectPlans).(partialPlan)
		// At this point we'll be choosing the cheapest plan. we shouldn't be considering the lower bound as the cost of the plan.
		p.lowerBoundCost = p.plan.TotalCost()
		heap.Push(completePlans, p)
	}
	return completePlans
}
