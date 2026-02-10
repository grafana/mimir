// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	planning.RegisterNodeFactory(func() planning.Node {
		return &Duplicate{DuplicateDetails: &DuplicateDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &DuplicateFilter{DuplicateFilterDetails: &DuplicateFilterDetails{}}
	})
}

type Duplicate struct {
	*DuplicateDetails
	Inner planning.Node
}

func (d *Duplicate) Details() proto.Message {
	return d.DuplicateDetails
}

func (d *Duplicate) NodeType() planning.NodeType {
	return planning.NODE_TYPE_DUPLICATE
}

func (d *Duplicate) Child(idx int) planning.Node {
	if idx != 0 {
		panic(fmt.Sprintf("node of type Duplicate supports 1 child, but attempted to get child at index %d", idx))
	}

	return d.Inner
}

func (d *Duplicate) ChildCount() int {
	return 1
}

func (d *Duplicate) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type Duplicate supports 1 child, but got %d", len(children))
	}

	d.Inner = children[0]

	return nil
}

func (d *Duplicate) ReplaceChild(idx int, node planning.Node) error {
	if idx != 0 {
		return fmt.Errorf("node of type Duplicate supports 1 child, but attempted to replace child at index %d", idx)
	}

	d.Inner = node
	return nil
}

func (d *Duplicate) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	_, ok := other.(*Duplicate)

	return ok
}

func (d *Duplicate) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func (d *Duplicate) Describe() string {
	return ""
}

func (d *Duplicate) ChildrenLabels() []string {
	return []string{""}
}

func (d *Duplicate) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (d *Duplicate) ResultType() (parser.ValueType, error) {
	return d.Inner.ResultType()
}

func (d *Duplicate) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return d.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (d *Duplicate) ExpressionPosition() (posrange.PositionRange, error) {
	return d.Inner.ExpressionPosition()
}

func (d *Duplicate) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return planning.QueryPlanVersionZero
}

func MaterializeDuplicate(d *Duplicate, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	inner, err := materializer.ConvertNodeToOperator(d.Inner, timeRange)
	if err != nil {
		return nil, err
	}

	switch inner := inner.(type) {
	case types.InstantVectorOperator:
		return &InstantVectorDuplicationConsumerOperatorFactory{
			Buffer: NewInstantVectorDuplicationBuffer(inner, params.MemoryConsumptionTracker),
		}, nil
	case types.RangeVectorOperator:
		return &RangeVectorDuplicationConsumerOperatorFactory{
			Buffer: NewRangeVectorDuplicationBuffer(inner, params.MemoryConsumptionTracker),
		}, nil
	default:
		return nil, fmt.Errorf("expected InstantVectorOperator or RangeVectorOperator as child of Duplicate, got %T", inner)
	}
}

type InstantVectorDuplicationConsumerOperatorFactory struct {
	Buffer *InstantVectorDuplicationBuffer
}

func (d *InstantVectorDuplicationConsumerOperatorFactory) Produce() (types.Operator, error) {
	return d.Buffer.AddConsumer(), nil
}

type RangeVectorDuplicationConsumerOperatorFactory struct {
	Buffer *RangeVectorDuplicationBuffer
}

func (d *RangeVectorDuplicationConsumerOperatorFactory) Produce() (types.Operator, error) {
	return d.Buffer.AddConsumer(), nil
}

type DuplicateFilter struct {
	*DuplicateFilterDetails
	Inner *Duplicate
}

func (f *DuplicateFilter) Details() proto.Message {
	return f.DuplicateFilterDetails
}

func (f *DuplicateFilter) NodeType() planning.NodeType {
	return planning.NODE_TYPE_DUPLICATE_FILTER
}

func (f *DuplicateFilter) Child(idx int) planning.Node {
	if idx != 0 {
		panic(fmt.Sprintf("node of type DuplicateFilter supports 1 child, but attempted to get child at index %d", idx))
	}

	return f.Inner
}

func (f *DuplicateFilter) ChildCount() int {
	return 1
}

func (f *DuplicateFilter) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type DuplicateFilter supports 1 child, but got %d", len(children))
	}

	return f.ReplaceChild(0, children[0])
}

func (f *DuplicateFilter) ReplaceChild(idx int, node planning.Node) error {
	if idx != 0 {
		return fmt.Errorf("node of type DuplicateFilter supports 1 child, but attempted to replace child at index %d", idx)
	}

	duplicate, ok := node.(*Duplicate)

	if !ok {
		return fmt.Errorf("node of type DuplicateFilter only supports Duplicate nodes as child, got %T", node)
	}

	f.Inner = duplicate

	return nil
}

func (f *DuplicateFilter) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherFilter, ok := other.(*DuplicateFilter)

	return ok && slices.EqualFunc(f.Filters, otherFilter.Filters, func(a *core.LabelMatcher, b *core.LabelMatcher) bool {
		return a.Equal(b)
	})
}

func (f *DuplicateFilter) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func (f *DuplicateFilter) Describe() string {
	builder := &strings.Builder{}
	core.FormatMatchers(builder, f.Filters)

	return builder.String()
}

func (f *DuplicateFilter) ChildrenLabels() []string {
	return []string{""}
}

func (f *DuplicateFilter) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (f *DuplicateFilter) ResultType() (parser.ValueType, error) {
	return f.Inner.ResultType()
}

func (f *DuplicateFilter) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return f.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (f *DuplicateFilter) ExpressionPosition() (posrange.PositionRange, error) {
	return f.Inner.ExpressionPosition()
}

func (f *DuplicateFilter) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return planning.QueryPlanV6
}

func MaterializeDuplicateFilter(f *DuplicateFilter, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	operator, err := materializer.ConvertNodeToOperator(f.Inner, timeRange)
	if err != nil {
		return nil, err
	}

	filterable, ok := operator.(Filterable)
	if !ok {
		return nil, fmt.Errorf("expected operator that supports filtering as child of DuplicateFilter, got %T", operator)
	}

	filters, err := core.LabelMatchersToPrometheusType(f.Filters)
	if err != nil {
		return nil, err
	}

	filterable.SetFilters(filters)

	return planning.NewSingleUseOperatorFactory(operator), nil
}

type Filterable interface {
	SetFilters(filters []*labels.Matcher)
}
