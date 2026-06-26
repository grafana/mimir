// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"fmt"
	"strconv"
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

//node:generate
type Duplicate struct {
	*DuplicateDetails
	Inner planning.Node `node:"child"`
}

func (d *Duplicate) Details() proto.Message {
	return d.DuplicateDetails
}

func (d *Duplicate) NodeType() planning.NodeType {
	return planning.NODE_TYPE_DUPLICATE
}

func (d *Duplicate) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func (d *Duplicate) Describe() string {
	return ""
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

func (d *Duplicate) MinimumRequiredPlanVersion(timeRange types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	innerResultType, err := d.Inner.ResultType()
	if err != nil {
		return 0, err
	}

	if !timeRange.IsInstant && innerResultType == parser.ValueTypeMatrix {
		// Range vector expression in a range query
		return planning.QueryPlanV11, nil
	}

	return planning.QueryPlanVersionZero, nil
}

func (d *Duplicate) IsSplittable() bool {
	splitNode, ok := d.Inner.(planning.SplitNode)
	if !ok {
		return false
	}
	return splitNode.IsSplittable()
}

func (d *Duplicate) GetRangeParams() planning.RangeParams {
	splitNode, ok := d.Inner.(planning.SplitNode)
	if !ok {
		return planning.RangeParams{}
	}
	return splitNode.GetRangeParams()
}

var _ planning.SplitNode = &Duplicate{}

func MaterializeDuplicate(ctx context.Context, d *Duplicate, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters, overrideTimeParams planning.RangeParams) (planning.OperatorFactory, error) {
	inner, err := materializer.ConvertNodeToOperatorWithSubRange(ctx, d.Inner, timeRange, overrideTimeParams)
	if err != nil {
		return nil, err
	}

	switch inner := inner.(type) {
	case types.InstantVectorOperator:
		return &InstantVectorDuplicationConsumerOperatorFactory{
			Buffer: NewInstantVectorDuplicationBuffer(inner, params.MemoryConsumptionTracker, timeRange, params.Logger),
		}, nil
	case types.RangeVectorOperator:
		return &RangeVectorDuplicationConsumerOperatorFactory{
			Buffer: NewRangeVectorDuplicationBuffer(inner, params.MemoryConsumptionTracker, timeRange, params.Logger),
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

//node:generate
type DuplicateFilter struct {
	*DuplicateFilterDetails
	Inner *Duplicate `node:"child"`
}

func (f *DuplicateFilter) Details() proto.Message {
	return f.DuplicateFilterDetails
}

func (f *DuplicateFilter) NodeType() planning.NodeType {
	return planning.NODE_TYPE_DUPLICATE_FILTER
}

func (f *DuplicateFilter) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func (f *DuplicateFilter) Describe() string {
	builder := &strings.Builder{}
	core.FormatMatchers(builder, f.Filters)

	builder.WriteString(", subset index: ")
	builder.WriteString(strconv.FormatInt(f.SubsetIndex, 10))

	return builder.String()
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

func (f *DuplicateFilter) MinimumRequiredPlanVersion(types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanV7, nil
}

func (f *DuplicateFilter) IsSplittable() bool {
	return f.Inner.IsSplittable()
}

func (f *DuplicateFilter) GetRangeParams() planning.RangeParams {
	return f.Inner.GetRangeParams()
}

var _ planning.SplitNode = &DuplicateFilter{}

func MaterializeDuplicateFilter(ctx context.Context, f *DuplicateFilter, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters, overrideTimeParams planning.RangeParams) (planning.OperatorFactory, error) {
	operator, err := materializer.ConvertNodeToOperatorWithSubRange(ctx, f.Inner, timeRange, overrideTimeParams)
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

	filterable.SetFilters(filters, int(f.SubsetIndex))

	return planning.NewSingleUseOperatorFactory(operator), nil
}

type Filterable interface {
	SetFilters(filters []*labels.Matcher, subsetIndex int)
}
