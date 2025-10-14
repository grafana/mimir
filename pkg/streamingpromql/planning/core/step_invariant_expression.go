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

func (s *StepInvariantExpression) Children() []planning.Node {
	return []planning.Node{s.Inner}
}

func (s *StepInvariantExpression) MinimumRequiredPlanVersion() int64 {
	return planning.QueryPlanVersionOne
}

func (s *StepInvariantExpression) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type StepInvariantExpression expects 1 child, but got %d", len(children))
	}

	s.Inner = children[0]

	return nil
}

func (s *StepInvariantExpression) EquivalentTo(other planning.Node) bool {
	otherExpr, ok := other.(*StepInvariantExpression)
	return ok && s.Inner.EquivalentTo(otherExpr.Inner)
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
	case *operators.Subquery:
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
