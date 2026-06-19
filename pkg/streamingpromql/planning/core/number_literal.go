// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/scalars"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

//node:generate
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

func (n *NumberLiteral) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherLiteral, ok := other.(*NumberLiteral)

	return ok && n.Value == otherLiteral.Value
}

func (n *NumberLiteral) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func MaterializeNumberLiteral(n *NumberLiteral, _ *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	o := scalars.NewScalarConstant(n.Value, timeRange, params.MemoryConsumptionTracker, n.GetExpressionPosition().ToPrometheusType())

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (n *NumberLiteral) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeScalar, nil
}

func (n *NumberLiteral) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return planning.NoDataQueried(), nil
}

func (n *NumberLiteral) ExpressionPosition() (posrange.PositionRange, error) {
	return n.GetExpressionPosition().ToPrometheusType(), nil
}

func (n *NumberLiteral) MinimumRequiredPlanVersion(types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanVersionZero, nil
}
