// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/scalars"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type NumberLiteral struct {
	*NumberLiteralDetails
}

func (n *NumberLiteral) Describe() string {
	return strconv.FormatFloat(n.Value, 'g', -1, 64)
}

func (n *NumberLiteral) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (n *NumberLiteral) Details() proto.Message {
	return n.NumberLiteralDetails
}

func (n *NumberLiteral) NodeType() planning.NodeType {
	return planning.NODE_TYPE_NUMBER_LITERAL
}

func (n *NumberLiteral) Children() []planning.Node {
	return nil
}

func (n *NumberLiteral) SetChildren(children []planning.Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type NumberLiteral expects 0 children, but got %d", len(children))
	}

	return nil
}

func (n *NumberLiteral) EquivalentTo(other planning.Node) bool {
	otherLiteral, ok := other.(*NumberLiteral)

	return ok && n.Value == otherLiteral.Value
}

func (n *NumberLiteral) ChildrenLabels() []string {
	return nil
}

func MaterializeNumberLiteral(n *NumberLiteral, _ *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	o := scalars.NewScalarConstant(n.Value, timeRange, params.MemoryConsumptionTracker, n.ExpressionPosition.ToPrometheusType())

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (n *NumberLiteral) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeScalar, nil
}
