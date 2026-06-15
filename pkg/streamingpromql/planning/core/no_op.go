// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// NoOp is a query plan node that always returns an empty instant vector.
// It is used by optimization passes to replace subexpressions that can be statically
// determined to return no results.
//
//node:generate
type NoOp struct {
	*NoOpDetails
}

func (n *NoOp) Details() proto.Message {
	return n.NoOpDetails
}

func (n *NoOp) NodeType() planning.NodeType {
	return planning.NODE_TYPE_NO_OP
}

func (n *NoOp) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	o, ok := other.(*NoOp)
	return ok && n.MatrixSelector == o.MatrixSelector
}

func (n *NoOp) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func (n *NoOp) Describe() string {
	if n.MatrixSelector {
		return "matrix"
	}
	return ""
}

func (n *NoOp) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (n *NoOp) ResultType() (parser.ValueType, error) {
	if n.MatrixSelector {
		return parser.ValueTypeMatrix, nil
	}
	return parser.ValueTypeVector, nil
}

func (n *NoOp) QueriedTimeRange(_ types.QueryTimeRange, _ time.Duration) (planning.QueriedTimeRange, error) {
	return planning.NoDataQueried(), nil
}

func (n *NoOp) ExpressionPosition() (posrange.PositionRange, error) {
	return posrange.PositionRange{}, nil
}

func (n *NoOp) MinimumRequiredPlanVersion(types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanV10, nil
}

func MaterializeNoOp(n *NoOp, _ *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	var o types.Operator

	if n.MatrixSelector {
		o = operators.NewNoOpRange(timeRange, params.MemoryConsumptionTracker)
	} else {
		o = operators.NewNoOpInstant(timeRange, params.MemoryConsumptionTracker)
	}

	return planning.NewSingleUseOperatorFactory(o), nil
}
