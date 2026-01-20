// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type ScalarScalarBinaryOperation struct {
	Left                     types.ScalarOperator
	Right                    types.ScalarOperator
	Op                       parser.ItemType
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	opFunc             binaryOperationFunc
	emitAnnotation     types.EmitAnnotationFunc
	expressionPosition posrange.PositionRange
	annotations        *annotations.Annotations
}

var _ types.ScalarOperator = &ScalarScalarBinaryOperation{}

func NewScalarScalarBinaryOperation(
	left, right types.ScalarOperator,
	op parser.ItemType,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) (*ScalarScalarBinaryOperation, error) {
	s := &ScalarScalarBinaryOperation{
		Left:                     left,
		Right:                    right,
		Op:                       op,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		annotations:              annotations,
		expressionPosition:       expressionPosition,
	}

	if op.IsComparisonOperator() {
		s.opFunc = boolComparisonOperationFuncs[op]
	} else {
		s.opFunc = arithmeticAndComparisonOperationFuncs[op]
	}

	if s.opFunc == nil {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("binary expression with '%s'", op))
	}

	s.emitAnnotation = func(generator types.AnnotationGenerator) {
		s.annotations.Add(generator("", s.expressionPosition))
	}

	return s, nil
}

func (s *ScalarScalarBinaryOperation) GetValues(ctx context.Context) (types.ScalarData, error) {
	leftValues, err := s.Left.GetValues(ctx)
	if err != nil {
		return types.ScalarData{}, err
	}

	rightValues, err := s.Right.GetValues(ctx)
	if err != nil {
		return types.ScalarData{}, err
	}

	// Binary operations between two scalars always produce a float value, as only arithmetic operators or comparison
	// operators (with the bool keyword) are supported between two scalars.
	//
	// Furthermore, scalar values always have a value at each step.
	//
	// So we can just compute the result of each pairwise operation without examining the timestamps of each sample.
	//
	// We store the result in the slice from the left operator, and return the right operator's slice once we're done.
	for i, left := range leftValues.Samples {
		right := rightValues.Samples[i]

		f, h, ok, valid, err := s.opFunc(left.F, right.F, nil, nil, true, true, s.emitAnnotation)

		if err != nil {
			return types.ScalarData{}, err
		}

		if !ok {
			panic(fmt.Sprintf("%v binary operation between two scalars (%v and %v) did not produce a result, this should never happen", s.Op.String(), left.F, right.F))
		}

		if !valid {
			panic(fmt.Sprintf("%v binary operation between two scalars (%v and %v) is not considered a valid operation, this should never happen", s.Op.String(), left.F, right.F))
		}

		if h != nil {
			panic(fmt.Sprintf("%v binary operation between two scalars (%v and %v) produced a histogram result, this should never happen", s.Op.String(), left.F, right.F))
		}

		leftValues.Samples[i].F = f
	}

	types.FPointSlicePool.Put(&rightValues.Samples, s.MemoryConsumptionTracker)

	return leftValues, nil
}

func (s *ScalarScalarBinaryOperation) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *ScalarScalarBinaryOperation) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := s.Left.Prepare(ctx, params); err != nil {
		return err
	}

	return s.Right.Prepare(ctx, params)
}

func (s *ScalarScalarBinaryOperation) AfterPrepare(ctx context.Context) error {
	if err := s.Left.AfterPrepare(ctx); err != nil {
		return err
	}

	return s.Right.AfterPrepare(ctx)
}

func (s *ScalarScalarBinaryOperation) Finalize(ctx context.Context) error {
	if err := s.Left.Finalize(ctx); err != nil {
		return err
	}

	return s.Right.Finalize(ctx)
}

func (s *ScalarScalarBinaryOperation) Close() {
	s.Left.Close()
	s.Right.Close()
}
