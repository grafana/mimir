// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
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
//
//node:generate
type StepInvariantExpression struct {
	*StepInvariantExpressionDetails
	Inner planning.Node `json:"-" node:"child"`
}

func (s *StepInvariantExpression) Describe() string {
	return ""
}

func (s *StepInvariantExpression) ChildrenTimeRange(_ types.QueryTimeRange) types.QueryTimeRange {
	// The time range used doesn't matter as long as it is a single step, given the expression is step invariant.
	// So use T=0, to ensure there's no accidental dependency on the actual time range.
	return types.NewInstantQueryTimeRange(timestamp.Time(0))
}

func (s *StepInvariantExpression) Details() proto.Message {
	return s.StepInvariantExpressionDetails
}

func (s *StepInvariantExpression) NodeType() planning.NodeType {
	return planning.NODE_TYPE_STEP_INVARIANT_EXPRESSION
}

func (s *StepInvariantExpression) MinimumRequiredPlanVersion(types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanV1, nil
}

func (s *StepInvariantExpression) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
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

func MaterializeStepInvariantExpression(ctx context.Context, s *StepInvariantExpression, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	resultType, err := s.Inner.ResultType()
	if err != nil {
		return nil, err
	}

	if resultType == parser.ValueTypeMatrix {
		// Earlier versions incorrectly wrapped range vector expressions in step-invariant expression nodes.
		// This has since been fixed, but new queriers might still get incorrect plans from old query-frontends.
		// If this happens, materialize the inner node as if the step-invariant node does not exist: both the range
		// vector selector operator and subquery operator behave correctly in this case.
		op, err := materializer.ConvertNodeToRangeVectorOperator(ctx, s.Inner, timeRange)
		if err != nil {
			return nil, err
		}

		return planning.NewSingleUseOperatorFactory(op), nil
	}

	adjustedTimeRange := s.ChildrenTimeRange(timeRange)

	op, err := materializer.ConvertNodeToOperator(ctx, s.Inner, adjustedTimeRange)
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
