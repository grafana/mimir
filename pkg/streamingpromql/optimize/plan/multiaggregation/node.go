// SPDX-License-Identifier: AGPL-3.0-only

package multiaggregation

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	planning.RegisterNodeFactory(func() planning.Node {
		return &MultiAggregationGroup{MultiAggregationGroupDetails: &MultiAggregationGroupDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &MultiAggregationInstance{MultiAggregationInstanceDetails: &MultiAggregationInstanceDetails{}}
	})
}

type MultiAggregationGroup struct {
	*MultiAggregationGroupDetails
	Inner planning.Node
}

func (g *MultiAggregationGroup) Details() proto.Message {
	return g.MultiAggregationGroupDetails
}

func (g *MultiAggregationGroup) NodeType() planning.NodeType {
	return planning.NODE_TYPE_MULTI_AGGREGATION_GROUP
}

func (g *MultiAggregationGroup) Child(idx int) planning.Node {
	if idx != 0 {
		panic(fmt.Sprintf("node of type MultiAggregationGroup supports 1 child, but attempted to get child at index %d", idx))
	}

	return g.Inner
}

func (g *MultiAggregationGroup) ChildCount() int {
	return 1
}

func (g *MultiAggregationGroup) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type MultiAggregationGroup supports 1 child, but got %d", len(children))
	}

	g.Inner = children[0]
	return nil
}

func (g *MultiAggregationGroup) ReplaceChild(idx int, node planning.Node) error {
	if idx != 0 {
		return fmt.Errorf("node of type MultiAggregationGroup supports 1 child, but attempted to replace child at index %d", idx)
	}

	g.Inner = node
	return nil
}

func (g *MultiAggregationGroup) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	_, ok := other.(*MultiAggregationGroup)

	return ok
}

func (g *MultiAggregationGroup) MergeHints(other planning.Node) error {
	_, ok := other.(*MultiAggregationGroup)
	if !ok {
		return fmt.Errorf("cannot merge hints from %T into %T", other, g)
	}

	return nil
}

func (g *MultiAggregationGroup) Describe() string {
	return ""
}

func (g *MultiAggregationGroup) ChildrenLabels() []string {
	return []string{""}
}

func (g *MultiAggregationGroup) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (g *MultiAggregationGroup) ResultType() (parser.ValueType, error) {
	return g.Inner.ResultType()
}

func (g *MultiAggregationGroup) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return g.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (g *MultiAggregationGroup) ExpressionPosition() (posrange.PositionRange, error) {
	return g.Inner.ExpressionPosition()
}

func (g *MultiAggregationGroup) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return planning.QueryPlanV5
}

type MultiAggregationInstance struct {
	*MultiAggregationInstanceDetails
	Group *MultiAggregationGroup
}

func (a *MultiAggregationInstance) Details() proto.Message {
	return a.MultiAggregationInstanceDetails
}

func (a *MultiAggregationInstance) NodeType() planning.NodeType {
	return planning.NODE_TYPE_MULTI_AGGREGATION_INSTANCE
}

func (a *MultiAggregationInstance) Child(idx int) planning.Node {
	if idx != 0 {
		panic(fmt.Sprintf("node of type MultiAggregationInstance supports 1 child, but attempted to get child at index %d", idx))
	}

	return a.Group
}

func (a *MultiAggregationInstance) ChildCount() int {
	return 1
}

func (a *MultiAggregationInstance) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type MultiAggregationInstance supports 1 child, but got %d", len(children))
	}

	return a.ReplaceChild(0, children[0])
}

func (a *MultiAggregationInstance) ReplaceChild(idx int, node planning.Node) error {
	if idx != 0 {
		return fmt.Errorf("node of type MultiAggregationInstance supports 1 child, but attempted to replace child at index %d", idx)
	}

	group, ok := node.(*MultiAggregationGroup)
	if !ok {
		return fmt.Errorf("node of type MultiAggregationInstance requires child of type MultiAggregationGroup, but got %T", node)
	}

	a.Group = group
	return nil
}

func (a *MultiAggregationInstance) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherInstance, ok := other.(*MultiAggregationInstance)

	return ok &&
		a.Aggregation.EquivalentTo(otherInstance.Aggregation) &&
		slices.EqualFunc(a.Filters, otherInstance.Filters, func(a *core.LabelMatcher, b *core.LabelMatcher) bool {
			return a.Equal(b)
		})

}

func (a *MultiAggregationInstance) MergeHints(other planning.Node) error {
	otherInstance, ok := other.(*MultiAggregationInstance)
	if !ok {
		return fmt.Errorf("cannot merge hints from %T into %T", other, a)
	}

	return a.Aggregation.MergeHints(otherInstance.Aggregation)
}

func (a *MultiAggregationInstance) Describe() string {
	builder := &strings.Builder{}
	a.Aggregation.DescribeTo(builder)

	if len(a.Filters) > 0 {
		builder.WriteString(", filters: ")
		core.FormatMatchers(builder, a.Filters)
	}

	return builder.String()
}

func (a *MultiAggregationInstance) ChildrenLabels() []string {
	return []string{""}
}

func (a *MultiAggregationInstance) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (a *MultiAggregationInstance) ResultType() (parser.ValueType, error) {
	return a.Group.ResultType()
}

func (a *MultiAggregationInstance) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return a.Group.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (a *MultiAggregationInstance) ExpressionPosition() (posrange.PositionRange, error) {
	return a.Group.ExpressionPosition()
}

func (a *MultiAggregationInstance) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	if len(a.Filters) > 0 {
		return planning.QueryPlanV7
	}

	return planning.QueryPlanV5
}

func MaterializeMultiAggregationGroup(node *MultiAggregationGroup, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	inner, err := materializer.ConvertNodeToInstantVectorOperator(node.Inner, timeRange)
	if err != nil {
		return nil, err
	}

	evaluator := NewMultiAggregatorGroupEvaluator(inner, params.MemoryConsumptionTracker)

	return &MultiAggregationInstanceFactory{group: evaluator}, nil
}

type MultiAggregationInstanceFactory struct {
	group *MultiAggregatorGroupEvaluator
}

func (m *MultiAggregationInstanceFactory) Produce() (types.Operator, error) {
	return m.group.AddInstance(), nil
}

func MaterializeMultiAggregationInstance(node *MultiAggregationInstance, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	operator, err := materializer.ConvertNodeToInstantVectorOperator(node.Group, timeRange)
	if err != nil {
		return nil, err
	}

	instance, ok := operator.(*MultiAggregatorInstanceOperator)
	if !ok {
		return nil, fmt.Errorf("expected MultiAggregatorInstanceOperator, got %T", operator)
	}

	op, ok := node.Aggregation.Op.ToItemType()
	if !ok {
		return nil, fmt.Errorf("unknown aggregation operation %s", node.Aggregation.Op.String())
	}

	matchers, err := core.LabelMatchersToPrometheusType(node.Filters)
	if err != nil {
		return nil, err
	}

	err = instance.Configure(
		op,
		node.Aggregation.Grouping,
		node.Aggregation.Without,
		matchers,
		params.MemoryConsumptionTracker,
		params.Annotations,
		timeRange,
		node.Aggregation.ExpressionPosition.ToPrometheusType(),
	)

	if err != nil {
		return nil, err
	}

	return planning.NewSingleUseOperatorFactory(instance), nil
}
