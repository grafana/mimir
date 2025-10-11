// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type NameDrop struct {
	*NameDropDetails
	Inner planning.Node
}

func (n *NameDrop) Details() proto.Message {
	return n.NameDropDetails
}

func (n *NameDrop) NodeType() planning.NodeType {
	return planning.NODE_TYPE_NAME_DROP
}

func (n *NameDrop) Children() []planning.Node {
	return []planning.Node{n.Inner}
}

func (n *NameDrop) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type NameDrop supports 1 child, but got %d", len(children))
	}

	n.Inner = children[0]

	return nil
}

func (n *NameDrop) EquivalentTo(other planning.Node) bool {
	otherNameDrop, ok := other.(*NameDrop)

	return ok && n.Inner.EquivalentTo(otherNameDrop.Inner)
}

func (n *NameDrop) Describe() string {
	return "drop metric names"
}

func (n *NameDrop) ChildrenLabels() []string {
	return []string{""}
}

func (n *NameDrop) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (n *NameDrop) ResultType() (parser.ValueType, error) {
	return n.Inner.ResultType()
}

func (n *NameDrop) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) planning.QueriedTimeRange {
	return n.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (n *NameDrop) ExpressionPosition() posrange.PositionRange {
	return n.Inner.ExpressionPosition()
}

func MaterializeNameDrop(n *NameDrop, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	inner, err := materializer.ConvertNodeToInstantVectorOperator(n.Inner, timeRange)
	if err != nil {
		return nil, err
	}

	o := operators.NewNameDrop(inner, params.MemoryConsumptionTracker)

	return planning.NewSingleUseOperatorFactory(o), nil
}
