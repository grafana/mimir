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

type DropName struct {
	*DropNameDetails
	Inner planning.Node
}

func (n *DropName) Details() proto.Message {
	return n.DropNameDetails
}

func (n *DropName) NodeType() planning.NodeType {
	return planning.NODE_TYPE_DROP_NAME
}

func (n *DropName) Children() []planning.Node {
	return []planning.Node{n.Inner}
}

func (n *DropName) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type DropName supports 1 child, but got %d", len(children))
	}

	n.Inner = children[0]

	return nil
}

func (n *DropName) EquivalentTo(other planning.Node) bool {
	otherDropName, ok := other.(*DropName)

	return ok && n.Inner.EquivalentTo(otherDropName.Inner)
}

func (n *DropName) Describe() string {
	return ""
}

func (n *DropName) ChildrenLabels() []string {
	return []string{""}
}

func (n *DropName) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (n *DropName) ResultType() (parser.ValueType, error) {
	return n.Inner.ResultType()
}

func (n *DropName) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) planning.QueriedTimeRange {
	return n.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (n *DropName) ExpressionPosition() posrange.PositionRange {
	return n.Inner.ExpressionPosition()
}

func (n *DropName) MinimumRequiredPlanVersion() int64 {
	return planning.QueryPlanV1
}

func MaterializeDropName(n *DropName, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	inner, err := materializer.ConvertNodeToInstantVectorOperator(n.Inner, timeRange)
	if err != nil {
		return nil, err
	}

	o := operators.NewDropName(inner, params.MemoryConsumptionTracker)

	return planning.NewSingleUseOperatorFactory(o), nil
}
