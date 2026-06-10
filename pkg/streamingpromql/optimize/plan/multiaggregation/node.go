// SPDX-License-Identifier: AGPL-3.0-only

package multiaggregation

import (
	"fmt"
	"slices"
	"strconv"
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

//node:generate
type MultiAggregationGroup struct {
	*MultiAggregationGroupDetails
	Inner planning.Node `node:"child"`
}

func (g *MultiAggregationGroup) Details() proto.Message {
	return g.MultiAggregationGroupDetails
}

func (g *MultiAggregationGroup) NodeType() planning.NodeType {
	return planning.NODE_TYPE_MULTI_AGGREGATION_GROUP
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

func (g *MultiAggregationGroup) MinimumRequiredPlanVersion(types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanV5, nil
}

type MultiAggregationInstance struct {
	*MultiAggregationInstanceDetails
	Group *MultiAggregationGroup
	Param planning.Node // nil for non-parameterized aggregations (eg. sum), set for quantile.
}

func (a *MultiAggregationInstance) Details() proto.Message {
	return a.MultiAggregationInstanceDetails
}

func (a *MultiAggregationInstance) NodeType() planning.NodeType {
	return planning.NODE_TYPE_MULTI_AGGREGATION_INSTANCE
}

func (a *MultiAggregationInstance) Child(idx int) planning.Node {
	switch idx {
	case 0:
		return a.Group
	case 1:
		if a.Param == nil {
			panic("cannot get MultiAggregationInstance child at index 1 if there is no parameter")
		}
		return a.Param
	default:
		panic(fmt.Sprintf("node of type MultiAggregationInstance supports at most 2 children, but attempted to get child at index %d", idx))
	}
}

func (a *MultiAggregationInstance) ChildCount() int {
	if a.Param == nil {
		return 1
	}

	return 2
}

func (a *MultiAggregationInstance) SetChildren(children []planning.Node) error {
	switch len(children) {
	case 1:
		a.Param = nil
		return a.ReplaceChild(0, children[0])
	case 2:
		if err := a.ReplaceChild(0, children[0]); err != nil {
			return err
		}
		a.Param = children[1]
		return nil
	default:
		return fmt.Errorf("node of type MultiAggregationInstance expects 1 or 2 children, but got %d", len(children))
	}
}

func (a *MultiAggregationInstance) ReplaceChild(idx int, node planning.Node) error {
	switch idx {
	case 0:
		group, ok := node.(*MultiAggregationGroup)
		if !ok {
			return fmt.Errorf("node of type MultiAggregationInstance requires child at index 0 of type MultiAggregationGroup, but got %T", node)
		}
		a.Group = group
		return nil
	case 1:
		a.Param = node
		return nil
	default:
		return fmt.Errorf("node of type MultiAggregationInstance expects at most 2 children, but attempted to replace child at index %d", idx)
	}
}

func (a *MultiAggregationInstance) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherInstance, ok := other.(*MultiAggregationInstance)

	return ok &&
		a.Aggregation.EquivalentTo(otherInstance.Aggregation) &&
		slices.EqualFunc(a.Filters, otherInstance.Filters, func(a *core.LabelMatcher, b *core.LabelMatcher) bool {
			return a.Equal(b)
		}) &&
		a.SubsetIndex == otherInstance.SubsetIndex
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

		builder.WriteString(", subset index: ")
		builder.WriteString(strconv.FormatInt(a.SubsetIndex, 10))
	}

	return builder.String()
}

func (a *MultiAggregationInstance) ChildrenLabels() []string {
	if a.Param == nil {
		return []string{""}
	}

	return []string{"", "parameter"}
}

func (a *MultiAggregationInstance) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (a *MultiAggregationInstance) ResultType() (parser.ValueType, error) {
	return a.Group.ResultType()
}

func (a *MultiAggregationInstance) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	groupRange, err := a.Group.QueriedTimeRange(queryTimeRange, lookbackDelta)
	if err != nil {
		return planning.NoDataQueried(), err
	}

	if a.Param == nil {
		return groupRange, nil
	}

	paramRange, err := a.Param.QueriedTimeRange(queryTimeRange, lookbackDelta)
	if err != nil {
		return planning.NoDataQueried(), err
	}

	return groupRange.Union(paramRange), nil
}

func (a *MultiAggregationInstance) ExpressionPosition() (posrange.PositionRange, error) {
	return a.Group.ExpressionPosition()
}

func (a *MultiAggregationInstance) MinimumRequiredPlanVersion(types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	if a.Param != nil {
		return planning.QueryPlanV15, nil
	}

	if len(a.Filters) > 0 {
		return planning.QueryPlanV8, nil
	}

	return planning.QueryPlanV5, nil
}

func MaterializeMultiAggregationGroup(node *MultiAggregationGroup, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	inner, err := materializer.ConvertNodeToInstantVectorOperator(node.Inner, timeRange)
	if err != nil {
		return nil, err
	}

	evaluator := NewMultiAggregatorGroupEvaluator(inner, params.MemoryConsumptionTracker, timeRange, params.Logger)

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
		int(node.SubsetIndex),
		params.MemoryConsumptionTracker,
		timeRange,
		node.Aggregation.ExpressionPosition.ToPrometheusType(),
	)

	if err != nil {
		return nil, err
	}

	if node.Param != nil {
		param, err := materializer.ConvertNodeToScalarOperator(node.Param, timeRange)
		if err != nil {
			return nil, fmt.Errorf("could not create parameter operator for MultiAggregationInstance %s: %w", node.Aggregation.Op.String(), err)
		}

		instance.SetParam(param)
	}

	return planning.NewSingleUseOperatorFactory(instance), nil
}
