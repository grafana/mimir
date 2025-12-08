// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"math"
	"slices"
	"sort"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

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

	ctx                 context.Context
	config              CostConfig
	indexPredicatesPool *pool.SlabPool[bool]
}

// newScanOnlyPlan returns a plan in which all predicates would be used to scan and none to reach from the index.
func newScanOnlyPlan(ctx context.Context, stats index.Statistics, config CostConfig, matchers []*labels.Matcher, predicatesPool *pool.SlabPool[bool]) plan {
	p := plan{
		predicates:     make([]planPredicate, 0, len(matchers)),
		indexPredicate: make([]bool, 0, len(matchers)),
		totalSeries:    stats.TotalSeries(),

		ctx:                 ctx,
		config:              config,
		indexPredicatesPool: predicatesPool,
	}
	for _, m := range matchers {
		pred := newPlanPredicate(ctx, m, stats, config)
		p.predicates = append(p.predicates, pred)
		p.indexPredicate = append(p.indexPredicate, false)
	}

	// Compute sqrtSelectivity for all predicates.
	// Sort predicates by series selectivity (cardinality / totalSeries) and apply progressively
	// higher roots to account for correlation between predicates.
	computeSqrtSelectivities(p.predicates, p.totalSeries)

	return p
}

// computeSqrtSelectivities computes the sqrtSelectivity for each predicate.
// It sorts predicates by their series selectivity and applies progressively higher roots
// (1, sqrt, 4th root, 8th root, ...) to account for correlation between predicates.
func computeSqrtSelectivities(predicates []planPredicate, totalSeries uint64) {
	if len(predicates) == 0 || totalSeries == 0 {
		return
	}

	// Create indices sorted by series selectivity (cardinality / totalSeries)
	indices := make([]int, len(predicates))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		// Sort by selectivity (lower selectivity = more selective = applied first)
		selI := float64(predicates[indices[i]].cardinality) / float64(totalSeries)
		selJ := float64(predicates[indices[j]].cardinality) / float64(totalSeries)
		return selI < selJ
	})

	// Assign sqrtSelectivity based on sorted position
	for pos, idx := range indices {
		sel := float64(predicates[idx].cardinality) / float64(totalSeries)
		// Apply progressively higher roots: 1, sqrt, 4th root, 8th root, ...
		// power = 1/2^pos: pos=0 -> 1, pos=1 -> 0.5, pos=2 -> 0.25, ...
		power := 1.0 / float64(int(1)<<pos)
		predicates[idx].sqrtSelectivity = math.Pow(sel, power)
	}
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
func (p plan) seriesRetrievalCost() float64 {
	return float64(p.intersectionSize()) * p.config.RetrievedSeriesCost
}

// filterCost returns the cost of applying scan predicates to the fetched series
func (p plan) filterCost() float64 {
	cost := 0.0
	fetchedSeries := p.intersectionSize()
	for i, pred := range p.predicates {
		// In reality, we will apply all the predicates for each series and stop once one predicate doesn't match.
		// But we calculate for the worst case where we have to run all predicates for all series.
		if !p.indexPredicate[i] {
			cost += pred.filterCost(fetchedSeries)
		}
	}
	return cost
}

func (p plan) intersectionSize() uint64 {
	finalSelectivity := 1.0
	for i, pred := range p.predicates {
		if !p.indexPredicate[i] {
			continue
		}

		// We use sqrtSelectivity which accounts for correlation between predicates.
		// The sqrtSelectivity is computed once when the plan is created by sorting all predicates
		// by series selectivity and applying progressively higher roots to less selective predicates.
		// This approach assumes that more selective predicates are applied first during intersection,
		// so less selective ones have diminishing impact on the result size.
		finalSelectivity *= pred.sqrtSelectivity
	}
	return uint64(finalSelectivity * float64(p.totalSeries))
}

// cardinality returns an estimate of the total number of series that this plan would return.
func (p plan) cardinality() uint64 {
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
	span.AddEvent("lookup plan", trace.WithAttributes(
		attribute.String("plan_name", planName),
		attribute.Float64("total_cost", p.totalCost()),
		attribute.Float64("index_lookup_cost", p.indexLookupCost()),
		attribute.Float64("filter_cost", p.filterCost()),
		attribute.Float64("intersection_cost", p.intersectionCost()),
		attribute.Float64("series_retrieval_cost", p.seriesRetrievalCost()),
		attribute.Int64("cardinality", int64(p.cardinality())),
		attribute.Stringer("index_matchers", util.MatchersStringer(p.IndexMatchers())),
		attribute.Stringer("scan_matchers", util.MatchersStringer(p.ScanMatchers())),
	))
}
