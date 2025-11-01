// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"reflect"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// StepInvariantExpression is a query which evaluates to the same result irrelevant of the evaluation time.
// To optimise for this, a single value is determined and used to populate the vector for each step of the given series.
type StepInvariantExpression struct {
	*StepInvariantExpressionDetails
	Inner planning.Node `json:"-"`
}

func (s *StepInvariantExpression) Describe() string {
	return ""
}

func (s *StepInvariantExpression) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	// note that we set the StartT == EndT with a single step count.
	return types.NewInstantQueryTimeRange(timestamp.Time(timeRange.StartT))
}

func (s *StepInvariantExpression) Details() proto.Message {
	return s.StepInvariantExpressionDetails
}

func (s *StepInvariantExpression) NodeType() planning.NodeType {
	return planning.NODE_TYPE_STEP_INVARIANT_EXPRESSION
}

func (s *StepInvariantExpression) Child(idx int) planning.Node {
	if idx != 0 {
		panic(fmt.Sprintf("node of type StepInvariantExpression supports 1 child, but attempted to get child at index %d", idx))
	}

	return s.Inner
}

func (s *StepInvariantExpression) ChildCount() int {
	return 1
}

func (s *StepInvariantExpression) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return planning.QueryPlanV1
}

func (s *StepInvariantExpression) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type StepInvariantExpression expects 1 child, but got %d", len(children))
	}

	s.Inner = children[0]

	return nil
}

func (s *StepInvariantExpression) ReplaceChild(idx int, node planning.Node) error {
	if idx != 0 {
		return fmt.Errorf("node of type StepInvariantExpression supports 1 child, but attempted to replace child at index %d", idx)
	}

	s.Inner = node
	return nil
}

func (s *StepInvariantExpression) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	_, ok := other.(*StepInvariantExpression)
	return ok
}

func (s *StepInvariantExpression) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func (s *StepInvariantExpression) ChildrenLabels() []string {
	return []string{""}
}

func MaterializeStepInvariantExpression(s *StepInvariantExpression, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	adjustedTimeRange := s.ChildrenTimeRange(timeRange)

	op, err := materializer.ConvertNodeToOperator(s.Inner, adjustedTimeRange)
	if err != nil {
		return nil, err
	}

	switch op := op.(type) {
	case types.InstantVectorOperator:
		return planning.NewSingleUseOperatorFactory(operators.NewStepInvariantInstantVectorOperator(op, timeRange, params.MemoryConsumptionTracker)), nil
	case types.ScalarOperator:
		return planning.NewSingleUseOperatorFactory(operators.NewStepInvariantScalarOperator(op, timeRange, params.MemoryConsumptionTracker)), nil
	case types.RangeVectorOperator:
		// Notes on range vector handling:
		//
		// If the query was wrapped in a step-invariant expression and this branch is reached,
		// the inner expression must be a subquery or range vector selector that returns a range vector at a fixed evaluation
		// timestamp — for example: metric[3m:1m] @ 100 or metric[3m] @ 100.
		//
		// Since range queries cannot directly produce range-vector results for step evaluation,
		// this must be an instant query; therefore, no special step-invariant operator is required.
		//
		// Other step-invariant expressions that wrap range-vector-producing queries are handled
		// above in the InstantVectorOperator case. For example, a query such as
		// max_over_time(metric[3m] @ 100) would have the entire function wrapped as a step-invariant,
		// and its inner operation (the range function call) would be materialized via the
		// InstantVectorOperator path.

		return planning.NewSingleUseOperatorFactory(op), nil
	}

	return nil, fmt.Errorf("unable to materialize step invariant expression because of unhandled operator, operator=%v", reflect.TypeOf(op))
}

func (s *StepInvariantExpression) ResultType() (parser.ValueType, error) {
	return s.Inner.ResultType()
}

func (s *StepInvariantExpression) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) planning.QueriedTimeRange {
	return s.Inner.QueriedTimeRange(s.ChildrenTimeRange(queryTimeRange), lookbackDelta)
}

func (s *StepInvariantExpression) ExpressionPosition() posrange.PositionRange {
	return s.Inner.ExpressionPosition()
}
