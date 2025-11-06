// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/pool"
)

// plan is a representation of one way of executing a vector selector.
type plan struct {
	// predicates contains all the matchers converted to plan predicates with their statistics.
	predicates []planPredicate
	// indexPredicate tracks which predicates should use index lookups vs sequential scans.
	// indexPredicate[i] == true means predicates[i] should use the index
	indexPredicate []bool
	// totalSeries is the total number of series in the index.
	totalSeries uint64
	// shard contains query sharding information. nil means no sharding.
	shard *sharding.ShardSelector

	config              CostConfig
	indexPredicatesPool *pool.SlabPool[bool]
}

// newScanOnlyPlan returns a plan in which all predicates would be used to scan and none to reach from the index.
func newScanOnlyPlan(ctx context.Context, stats index.Statistics, config CostConfig, matchers []*labels.Matcher, predicatesPool *pool.SlabPool[bool], shard *sharding.ShardSelector) plan {
	p := plan{
		predicates:          make([]planPredicate, 0, len(matchers)),
		indexPredicate:      make([]bool, 0, len(matchers)),
		totalSeries:         stats.TotalSeries(),
		shard:               shard,
		config:              config,
		indexPredicatesPool: predicatesPool,
	}
	for _, m := range matchers {
		pred := newPlanPredicate(ctx, m, stats, config)
		p.predicates = append(p.predicates, pred)
		p.indexPredicate = append(p.indexPredicate, false)
	}

	return p
}

func (p plan) IndexMatchers() []*labels.Matcher {
	var matchers []*labels.Matcher
	for i, pred := range p.predicates {
		if p.indexPredicate[i] {
			matchers = append(matchers, pred.matcher)
		}
	}
	return matchers
}

func (p plan) ScanMatchers() []*labels.Matcher {
	var matchers []*labels.Matcher
	for i, pred := range p.predicates {
		if !p.indexPredicate[i] {
			matchers = append(matchers, pred.matcher)
		}
	}
	return matchers
}

// useIndexFor returns a copy of this plan where predicate predicateIdx is used on the index.
func (p plan) useIndexFor(predicateIdx int) plan {
	var copied []bool
	if p.indexPredicatesPool != nil {
		copied = p.indexPredicatesPool.Get(len(p.indexPredicate))
		copy(copied, p.indexPredicate)
	} else {
		copied = slices.Clone(p.indexPredicate)
	}
	p.indexPredicate = copied
	p.indexPredicate[predicateIdx] = true
	return p
}

func (p plan) withoutMemoryPool() plan {
	p.indexPredicate = slices.Clone(p.indexPredicate)
	p.indexPredicatesPool = nil
	return p
}

// totalCost returns the sum of indexLookupCost + intersectionCost + filterCost
func (p plan) totalCost() float64 {
	return p.indexLookupCost() + p.intersectionCost() + p.seriesRetrievalCost() + p.filterCost()
}

// indexLookupCost returns the cost of performing index lookups for all predicates that use the index
func (p plan) indexLookupCost() float64 {
	cost := 0.0
	for i, pr := range p.predicates {
		if p.indexPredicate[i] {
			cost += pr.indexLookupCost()
		}
	}
	return cost
}

// intersectionCost returns the cost of intersecting posting lists from multiple index predicates
// This includes retrieving the series' labels from the index.
func (p plan) intersectionCost() float64 {
	iteratedPostings := uint64(0)
	for i, pred := range p.predicates {
		if !p.indexPredicate[i] {
			continue
		}

		iteratedPostings += pred.cardinality
	}

	return float64(iteratedPostings) * p.config.RetrievedPostingCost
}

// seriesRetrievalCost returns the cost of retrieving series from the index after intersecting posting lists.
// This includes retrieving the series' labels from the index and checking if the series belongs to the query's shard.
// Realistically we don't retrieve every series because we have the series hash cache, but we ignore that for simplicity.
func (p plan) seriesRetrievalCost() float64 {
	return float64(p.numSelectedPostings()) * p.config.RetrievedSeriesCost
}

// filterCost returns the cost of applying scan predicates to the fetched series.
// The sequence is: intersection → retrieve series → check shard → apply scan matchers.
func (p plan) filterCost() float64 {
	cost := 0.0
	seriesToFilter := p.numSelectedPostingsInOurShard()
	for i, pred := range p.predicates {
		// In reality, we will apply all the predicates for each series and stop once one predicate doesn't match.
		// But we calculate for the worst case where we have to run all predicates for all series.
		if !p.indexPredicate[i] {
			cost += pred.filterCost(seriesToFilter)
		}
	}
	return cost
}

func (p plan) numSelectedPostingsInOurShard() uint64 {
	postings := p.numSelectedPostings()

	if p.shard == nil || p.shard.ShardCount == 0 {
		return postings
	}

	// If query sharding is enabled, divide by the shard count since only series in this shard will be returned.
	shardedPostings := postings / p.shard.ShardCount
	// Ensure we return at least 1 if postings > 0, to avoid returning 0 when we have series.
	if postings > 0 && shardedPostings == 0 {
		return 1
	}
	return shardedPostings
}

func (p plan) numSelectedPostings() uint64 {
	finalSelectivity := 1.0
	for i, pred := range p.predicates {
		if !p.indexPredicate[i] {
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
func (p plan) nonShardedCardinality() uint64 {
	finalSelectivity := 1.0
	for _, pred := range p.predicates {
		// We use the selectivity across all series instead of the selectivity across label values.
		// For example, if {protocol=~.*} matches all values, it could still reduce the result set after intersection.
		//
		// We also assume independence between the predicates. This is a simplification.
		// For example, the selectivity of {pod=~prometheus.*} doesn't depend on if we have already applied {statefulset=prometheus}.
		finalSelectivity *= float64(pred.cardinality) / float64(p.totalSeries)
	}
	return uint64(finalSelectivity * float64(p.totalSeries))
}

// finalCardinality returns an estimate of the total number of series that this plan would return.
func (p plan) finalCardinality() uint64 {
	baseCardinality := p.nonShardedCardinality()

	if p.shard == nil || p.shard.ShardCount == 0 {
		return baseCardinality
	}

	// If query sharding is enabled, divide by the shard count since only series in this shard will be returned.
	shardedCardinality := baseCardinality / p.shard.ShardCount
	// Ensure we return at least 1 if baseCardinality > 0, to avoid returning 0 when we have series.
	if baseCardinality > 0 && shardedCardinality == 0 {
		return 1
	}
	return shardedCardinality
}

func (p plan) addPredicatesToSpan(span trace.Span) {
	// Preallocate the attributes. Use an array to ensure the capacity is correct at compile time.
	const numAttributesPerPredicate = 7
	attributes := make([]attribute.KeyValue, 0, len(p.predicates)*numAttributesPerPredicate)

	for _, pred := range p.predicates {
		predAttr := [numAttributesPerPredicate]attribute.KeyValue{
			attribute.Stringer("matcher", pred.matcher),
			attribute.Float64("selectivity", pred.selectivity),
			attribute.Int64("cardinality", int64(pred.cardinality)),
			attribute.Int64("label_name_unique_values", int64(pred.labelNameUniqueVals)),
			attribute.Float64("single_match_cost", pred.singleMatchCost),
			attribute.Float64("index_scan_cost", pred.indexScanCost),
			attribute.Float64("index_lookup_cost", pred.indexLookupCost()),
		}
		attributes = append(attributes, predAttr[:]...)
	}
	span.AddEvent("lookup_plan_predicate", trace.WithAttributes(attributes...))
}

func (p plan) addSpanEvent(span trace.Span, planName string) {
	span.AddEvent("lookup plan", trace.WithAttributes(attribute.String("plan_name", planName),
		attribute.Int64("shard_count", int64(maybe(p.shard).ShardCount)),
		attribute.Float64("total_cost", p.totalCost()),
		attribute.Float64("index_lookup_cost", p.indexLookupCost()),
		attribute.Float64("intersection_cost", p.intersectionCost()),
		attribute.Int64("estimated_retrieved_series", int64(p.numSelectedPostings())),
		attribute.Float64("series_retrieval_cost", p.seriesRetrievalCost()),
		attribute.Int64("estimated_series_after_sharding", int64(p.numSelectedPostingsInOurShard())),
		attribute.Float64("filter_cost", p.filterCost()),
		attribute.Int64("estimated_final_cardinality", int64(p.finalCardinality())),

		attribute.Stringer("index_matchers", util.MatchersStringer(p.IndexMatchers())),
		attribute.Stringer("scan_matchers", util.MatchersStringer(p.ScanMatchers()))))
}

func maybe[T any](ptr *T) T {
	if ptr != nil {
		return *ptr
	}
	var zero T
	return zero
}
