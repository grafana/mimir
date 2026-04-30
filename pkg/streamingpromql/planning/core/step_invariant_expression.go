// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
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

func (s *StepInvariantExpression) MinimumRequiredPlanVersion(types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanV1, nil
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
	resultType, err := s.Inner.ResultType()
	if err != nil {
		return nil, err
	}

	if resultType == parser.ValueTypeMatrix {
		// Earlier versions incorrectly wrapped range vector expressions in step-invariant expression nodes.
		// This has since been fixed, but new queriers might still get incorrect plans from old query-frontends.
		// If this happens, materialize the inner node as if the step-invariant node does not exist: both the range
		// vector selector operator and subquery operator behave correctly in this case.
		op, err := materializer.ConvertNodeToRangeVectorOperator(s.Inner, timeRange)
		if err != nil {
			return nil, err
		}

		return planning.NewSingleUseOperatorFactory(op), nil
	}

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
	}

	return nil, fmt.Errorf("unable to materialize step invariant expression with inner node type %T", op)
}

func (s *StepInvariantExpression) ResultType() (parser.ValueType, error) {
	return s.Inner.ResultType()
}

func (s *StepInvariantExpression) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return s.Inner.QueriedTimeRange(s.ChildrenTimeRange(queryTimeRange), lookbackDelta)
}

func (s *StepInvariantExpression) ExpressionPosition() (posrange.PositionRange, error) {
	return s.Inner.ExpressionPosition()
}
