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

// NoOp is a query plan node that always returns an empty instant vector.
// It is used by optimization passes to replace subexpressions that can be statically
// determined to return no results.
type NoOp struct {
	*NoOpDetails
}

func (n *NoOp) Details() proto.Message {
	return n.NoOpDetails
}

func (n *NoOp) NodeType() planning.NodeType {
	return planning.NODE_TYPE_NO_OP
}

func (n *NoOp) Child(idx int) planning.Node {
	panic(fmt.Sprintf("node of type NoOp has no children, but attempted to get child at index %d", idx))
}

func (n *NoOp) ChildCount() int {
	return 0
}

func (n *NoOp) SetChildren(children []planning.Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type NoOp expects 0 children, but got %d", len(children))
	}

	return nil
}

func (n *NoOp) ReplaceChild(idx int, _ planning.Node) error {
	return fmt.Errorf("node of type NoOp supports no children, but attempted to replace child at index %d", idx)
}

func (n *NoOp) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	_, ok := other.(*NoOp)
	return ok
}

func (n *NoOp) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func (n *NoOp) Describe() string {
	return ""
}

func (n *NoOp) ChildrenLabels() []string {
	return nil
}

func (n *NoOp) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (n *NoOp) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeVector, nil
}

func (n *NoOp) QueriedTimeRange(_ types.QueryTimeRange, _ time.Duration) (planning.QueriedTimeRange, error) {
	return planning.NoDataQueried(), nil
}

func (n *NoOp) ExpressionPosition() (posrange.PositionRange, error) {
	return posrange.PositionRange{}, nil
}

func (n *NoOp) MinimumRequiredPlanVersion(types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanV9, nil
}

func MaterializeNoOp(_ *NoOp, _ *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	o := operators.NewNoOp(timeRange, params.MemoryConsumptionTracker)
	return planning.NewSingleUseOperatorFactory(o), nil
}
