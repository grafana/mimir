// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"

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

func (d *Duplicate) Children() []planning.Node {
	return []planning.Node{d.Inner}
}

func (d *Duplicate) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type Duplicate supports 1 child, but got %d", len(children))
	}

	d.Inner = children[0]

	return nil
}

func (d *Duplicate) EquivalentTo(other planning.Node) bool {
	otherDuplicate, ok := other.(*Duplicate)

	return ok && d.Inner.EquivalentTo(otherDuplicate.Inner)
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

func (d *Duplicate) OperatorFactory(children []types.Operator, _ types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("expected exactly 1 child for Duplicate, got %v", len(children))
	}

	switch inner := children[0].(type) {
	case types.InstantVectorOperator:
		return &InstantVectorDuplicationConsumerOperatorFactory{
			Buffer: NewInstantVectorDuplicationBuffer(inner, params.MemoryConsumptionTracker),
		}, nil
	case types.RangeVectorOperator:
		return &RangeVectorDuplicationConsumerOperatorFactory{
			Buffer: NewRangeVectorDuplicationBuffer(inner, params.MemoryConsumptionTracker),
		}, nil
	default:
		return nil, fmt.Errorf("expected InstantVectorOperator as child of Duplicate, got %T", children[0])
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
