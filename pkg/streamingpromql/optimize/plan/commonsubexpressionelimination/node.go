// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	planning.RegisterNodeFactory(func() planning.Node {
		return &Duplicate{DuplicateDetails: &DuplicateDetails{}}
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

func MaterializeDuplicate(d *Duplicate, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters, overrideTimeParams planning.RangeParams) (planning.OperatorFactory, error) {
	inner, err := materializer.ConvertNodeToOperatorWithSubRange(d.Inner, timeRange, overrideTimeParams)
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
