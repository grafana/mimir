// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"cmp"
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

	var candidates []partialPlan
	for i := 0; i < maxPlansForPlanning && prospectPlans.Len() > 0; i++ {
		current := heap.Pop(prospectPlans).(partialPlan)

		if slices.Equal(current.decidedPredicates, allDecided) {
			// All predicates have been decided, add to decided plans
			heap.Push(decidedPlans, current)
			continue
		}

		// Branch: try both scan (false) and index (true) for the next predicate
		alreadyDecidedOneFalse := false
		candidates = candidates[:0]
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
				candidates = append(candidates, partialPlan{
					planWithCost: planWithCost{
						plan:      childPlan,
						totalCost: childPlan.TotalCost(),
					},
					decidedPredicates: decidedPredicates,
				})
			}
		}
		slices.SortFunc(candidates, func(a, b partialPlan) int {
			return cmp.Compare(a.totalCost, b.totalCost)
		})
		// push only the top half of candidates to limit the branching factor
		for _, candidate := range candidates[:max(1, len(candidates)/2)] {
			heap.Push(prospectPlans, candidate)
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

func (p partialPlan) LowerBoundCost() float64 {
	return p.indexLookupCost() + p.intersectionCost() + p.seriesRetrievalCost() + p.filterCost()
}

// indexLookupCost returns the cost of performing index lookups for all predicates that use the index
func (p partialPlan) indexLookupCost() float64 {
	cost := 0.0
	for i := range p.predicates {
		pr, ok := p.pretendDecidedPredicate(i)
		if ok {
			cost += pr.indexLookupCost()
		}
	}
	return cost
}

// pretendDecidedPredicate returns the predicate at idx and whether it's a scan predicate.
// The first undecided predicate is always treated as a decided index predicate for lower bound calculation purposes.
// All other undecided predicates are treated as decided scan predicates.
func (p partialPlan) pretendDecidedPredicate(idx int) (planPredicate, bool) {
	if p.decidedPredicates[idx] {
		return p.predicates[idx], p.indexPredicate[idx]
	}

	pretendPredicate := p.predicates[idx]
	pretendPredicate.singleMatchCost = 1
	pretendPredicate.cardinality = 1
	pretendPredicate.labelNameUniqueVals = 1
	pretendPredicate.selectivity = 1
	pretendPredicate.indexScanCost = 1

	return pretendPredicate, idx == p.firstUndecidedPredicateIdx()
}

// intersectionCost returns the cost of intersecting posting lists from multiple index predicates
// This includes retrieving the series' labels from the index.
func (p partialPlan) intersectionCost() float64 {
	iteratedPostings := uint64(0)
	for i := range p.predicates {
		pred, ok := p.pretendDecidedPredicate(i)
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
		pred, ok := p.pretendDecidedPredicate(i)
		if !ok {
			cost += pred.filterCost(seriesToFilter)
		}
	}
	return cost
}

func (p partialPlan) numSelectedPostingsInOurShard() uint64 {
	return shardedCardinality(p.NumSelectedPostings(), p.shard)
}

func (p partialPlan) NumSelectedPostings() uint64 {
	finalSelectivity := 1.0
	for i := range p.predicates {
		pred, ok := p.pretendDecidedPredicate(i)
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
		pred, _ := p.pretendDecidedPredicate(i)
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

func (p partialPlan) firstUndecidedPredicateIdx() int {
	for i, decided := range p.decidedPredicates {
		if !decided {
			return i
		}
	}
	return len(p.decidedPredicates)
}
