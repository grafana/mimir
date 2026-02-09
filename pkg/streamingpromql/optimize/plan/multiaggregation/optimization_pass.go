// SPDX-License-Identifier: AGPL-3.0-only

package multiaggregation

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

type OptimizationPass struct {
	aggregationNodesReplaced prometheus.Counter
	duplicateNodesExamined   prometheus.Counter
	duplicateNodesReplaced   prometheus.Counter
}

func NewOptimizationPass(reg prometheus.Registerer) *OptimizationPass {
	return &OptimizationPass{
		aggregationNodesReplaced: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_multiaggregation_aggregation_nodes_replaced_total",
			Help: "Number of aggregation nodes replaced by multi-aggregation consumer nodes by the multi-aggregation optimization pass.",
		}),
		duplicateNodesExamined: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_multiaggregation_duplicate_nodes_examined_total",
			Help: "Number of duplicate nodes examined by the multi-aggregation optimization pass.",
		}),
		duplicateNodesReplaced: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_multiaggregation_duplicate_nodes_replaced_total",
			Help: "Number of duplicate nodes replaced by multi-aggregation group nodes by the multi-aggregation optimization pass.",
		}),
	}
}

func (o *OptimizationPass) Name() string {
	return "Use multi-aggregation where possible"
}

type aggregateOverDuplicate struct {
	aggregate                *core.AggregateExpression
	aggregateParent          planning.Node
	indexOfAggregateInParent int
	filters                  []*core.LabelMatcher
}

func (o *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	if maximumSupportedQueryPlanVersion < planning.QueryPlanV5 {
		return plan, nil
	}

	ineligibleDuplicateNodes := make(map[*commonsubexpressionelimination.Duplicate]struct{})
	candidateDuplicateNodes := make(map[*commonsubexpressionelimination.Duplicate][]aggregateOverDuplicate)

	err := optimize.Walk(plan.Root, optimize.VisitorFunc(func(node planning.Node, path []planning.Node) error {
		// If we've reached a Duplicate node, check that its parent on this path is a supported aggregation.
		// If it's not, then we can't apply this optimisation to this Duplicate node.
		duplicate, isDuplicate := node.(*commonsubexpressionelimination.Duplicate)
		if isDuplicate {
			_, isIneligible := ineligibleDuplicateNodes[duplicate]
			if isIneligible {
				// We already know this Duplicate node is ineligible, so there's nothing more to do.
				return nil
			}

			parent := path[len(path)-1]

			// If the parent is a DuplicateFilter, we need to look at the grandparent.
			if _, isDuplicateFilter := parent.(*commonsubexpressionelimination.DuplicateFilter); isDuplicateFilter {
				parent = path[len(path)-2]
			}

			aggregate, isAggregate := parent.(*core.AggregateExpression)
			if !isAggregate {
				ineligibleDuplicateNodes[duplicate] = struct{}{}
				delete(candidateDuplicateNodes, duplicate)
			} else if supported, err := IsSupportedAggregationOperation(aggregate.Op); err != nil || !supported {
				ineligibleDuplicateNodes[duplicate] = struct{}{}
				delete(candidateDuplicateNodes, duplicate)
			}
		}

		// Check if any of this node's children are supported aggregate expressions over Duplicate nodes.
		for idx := range node.ChildCount() {
			child := node.Child(idx)

			aggregate, isAggregate := child.(*core.AggregateExpression)
			if !isAggregate {
				continue
			} else if supported, err := IsSupportedAggregationOperation(aggregate.Op); err != nil || !supported {
				continue
			}

			duplicate, isDuplicate := aggregate.Inner.(*commonsubexpressionelimination.Duplicate)
			var filters []*core.LabelMatcher

			if !isDuplicate {
				duplicateFilter, isDuplicateFilter := aggregate.Inner.(*commonsubexpressionelimination.DuplicateFilter)

				if !isDuplicateFilter {
					continue
				}

				filters = duplicateFilter.Filters
				duplicate = duplicateFilter.Inner
			}

			if _, isIneligible := ineligibleDuplicateNodes[duplicate]; isIneligible {
				continue
			}

			candidateDuplicateNodes[duplicate] = append(candidateDuplicateNodes[duplicate], aggregateOverDuplicate{
				aggregate:                aggregate,
				aggregateParent:          node,
				indexOfAggregateInParent: idx,
				filters:                  filters,
			})
		}

		return nil
	}))

	if err != nil {
		return nil, err
	}

	aggregateNodesReplaced := 0

	for duplicate, aggregates := range candidateDuplicateNodes {
		if aggregates == nil {
			continue
		}

		if err := o.replaceWithMultiAggregation(duplicate, aggregates); err != nil {
			return nil, err
		}

		aggregateNodesReplaced += len(aggregates)
	}

	o.aggregationNodesReplaced.Add(float64(aggregateNodesReplaced))
	o.duplicateNodesExamined.Add(float64(len(candidateDuplicateNodes) + len(ineligibleDuplicateNodes)))
	o.duplicateNodesReplaced.Add(float64(len(candidateDuplicateNodes)))

	return plan, nil
}

func (o *OptimizationPass) replaceWithMultiAggregation(duplicate *commonsubexpressionelimination.Duplicate, aggregateOverDuplicates []aggregateOverDuplicate) error {
	group := &MultiAggregationGroup{
		MultiAggregationGroupDetails: &MultiAggregationGroupDetails{},
		Inner:                        duplicate.Inner,
	}

	for _, aggregateOverDuplicate := range aggregateOverDuplicates {
		consumer := &MultiAggregationInstance{
			MultiAggregationInstanceDetails: &MultiAggregationInstanceDetails{
				Aggregation: aggregateOverDuplicate.aggregate.AggregateExpressionDetails,
				Filters:     aggregateOverDuplicate.filters,
			},
			Group: group,
		}

		if err := aggregateOverDuplicate.aggregateParent.ReplaceChild(aggregateOverDuplicate.indexOfAggregateInParent, consumer); err != nil {
			return err
		}
	}

	return nil
}

func IsSupportedAggregationOperation(o core.AggregationOperation) (bool, error) {
	switch o {
	case core.AGGREGATION_SUM:
		return true, nil
	case core.AGGREGATION_COUNT:
		return true, nil
	case core.AGGREGATION_MIN:
		return true, nil
	case core.AGGREGATION_MAX:
		return true, nil
	case core.AGGREGATION_AVG:
		return true, nil
	case core.AGGREGATION_GROUP:
		return true, nil
	case core.AGGREGATION_STDVAR:
		return true, nil
	case core.AGGREGATION_STDDEV:
		return true, nil

	case core.AGGREGATION_QUANTILE:
		return false, nil
	case core.AGGREGATION_COUNT_VALUES:
		return false, nil
	case core.AGGREGATION_TOPK:
		return false, nil
	case core.AGGREGATION_BOTTOMK:
		return false, nil
	case core.AGGREGATION_LIMITK:
		return false, nil
	case core.AGGREGATION_LIMIT_RATIO:
		return false, nil

	default:
		return false, fmt.Errorf("multiaggregation.IsSupportedAggregationOperation: unknown aggregation operation: %s", o.String())
	}
}
