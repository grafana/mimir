// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

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

func (n *NumberLiteral) Child(idx int) planning.Node {
	panic(fmt.Sprintf("node of type NumberLiteral has no children, but attempted to get child at index %d", idx))
}

func (n *NumberLiteral) ChildCount() int {
	return 0
}

func (n *NumberLiteral) SetChildren(children []planning.Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type NumberLiteral expects 0 children, but got %d", len(children))
	}

	return nil
}

func (n *NumberLiteral) ReplaceChild(idx int, node planning.Node) error {
	return fmt.Errorf("node of type NumberLiteral supports no children, but attempted to replace child at index %d", idx)
}

func (n *NumberLiteral) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherLiteral, ok := other.(*NumberLiteral)

	return ok && n.Value == otherLiteral.Value
}

func (n *NumberLiteral) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func (n *NumberLiteral) ChildrenLabels() []string {
	return nil
}

func MaterializeNumberLiteral(n *NumberLiteral, _ *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	o := scalars.NewScalarConstant(n.Value, timeRange, params.MemoryConsumptionTracker, n.ExpressionPosition())

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (n *NumberLiteral) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeScalar, nil
}

func (n *NumberLiteral) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) planning.QueriedTimeRange {
	return planning.NoDataQueried()
}

func (n *NumberLiteral) ExpressionPosition() posrange.PositionRange {
	return n.GetExpressionPosition().ToPrometheusType()
}

func (n *NumberLiteral) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return planning.QueryPlanVersionZero
}
