// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/scalars"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type InstantVectorFunctionOperatorFactory func(
	args []types.Operator,
	absentLabels labels.Labels, // Only used by absent and absent_over_time.
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
) (types.InstantVectorOperator, error)

type ScalarFunctionOperatorFactory func(
	args []types.Operator,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
) (types.ScalarOperator, error)

// SingleInputVectorFunctionOperatorFactory creates an InstantVectorFunctionOperatorFactory for functions
// that have exactly 1 argument (v instant-vector).
//
// Parameters:
//   - name: The name of the function
//   - f: The function implementation
func SingleInputVectorFunctionOperatorFactory(name string, f FunctionOverInstantVectorDefinition) InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", name, len(args))
		}

		inner, ok := args[0].(types.InstantVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected an instant vector argument for %s, got %T", name, args[0])
		}

		var o types.InstantVectorOperator = NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition, timeRange)

		if f.SeriesMetadataFunction.NeedsSeriesDeduplication {
			o = operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
		}

		return o, nil
	}
}

// InstantVectorTransformationFunctionOperatorFactory creates an InstantVectorFunctionOperatorFactory for functions
// that have exactly 1 argument (v instant-vector), and drop the series __name__ label.
//
// Parameters:
//   - name: The name of the function
//   - seriesDataFunc: The function to handle series data
func InstantVectorTransformationFunctionOperatorFactory(name string, seriesDataFunc InstantVectorSeriesFunction) InstantVectorFunctionOperatorFactory {
	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         seriesDataFunc,
		SeriesMetadataFunction: DropSeriesName,
	}

	return SingleInputVectorFunctionOperatorFactory(name, f)
}

func TimeTransformationFunctionOperatorFactory(name string, seriesDataFunc InstantVectorSeriesFunction) InstantVectorFunctionOperatorFactory {
	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         seriesDataFunc,
		SeriesMetadataFunction: DropSeriesName,
	}

	return func(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
		var inner types.InstantVectorOperator
		if len(args) == 0 {
			// if the argument is not provided, it will default to vector(time())
			inner = scalars.NewScalarToInstantVector(operators.NewTime(timeRange, memoryConsumptionTracker, expressionPosition), expressionPosition, memoryConsumptionTracker)
		} else if len(args) == 1 {
			// if one argument is provided, it must be an instant vector
			var ok bool
			inner, ok = args[0].(types.InstantVectorOperator)
			if !ok {
				// Should be caught by the PromQL parser, but we check here for safety.
				return nil, fmt.Errorf("expected an instant vector argument for %s, got %T", name, args[0])
			}
		} else {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected 0 or 1 argument for %s, got %v", name, len(args))
		}

		var o types.InstantVectorOperator = NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition, timeRange)
		if f.SeriesMetadataFunction.NeedsSeriesDeduplication {
			o = operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
		}

		return o, nil
	}
}

// InstantVectorLabelManipulationFunctionOperatorFactory creates an InstantVectorFunctionOperator for functions
// that have exactly 1 argument (v instant-vector), and need to manipulate the labels of
// each series without manipulating the returned samples.
// The values of v are passed through.
//
// Parameters:
//   - name: The name of the function
//   - metadataFunc: The function for handling metadata
func InstantVectorLabelManipulationFunctionOperatorFactory(name string, metadataFunc SeriesMetadataFunctionDefinition) InstantVectorFunctionOperatorFactory {
	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         PassthroughData,
		SeriesMetadataFunction: metadataFunc,
	}

	return SingleInputVectorFunctionOperatorFactory(name, f)
}

// FunctionOverRangeVectorOperatorFactory creates an InstantVectorFunctionOperatorFactory for functions
// that have exactly 1 argument (v range-vector).
//
// Parameters:
//   - name: The name of the function
//   - f: The function implementation
func FunctionOverRangeVectorOperatorFactory(
	name string,
	f FunctionOverRangeVectorDefinition,
) InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", name, len(args))
		}

		inner, ok := args[0].(types.RangeVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected a range vector argument for %s, got %T", name, args[0])
		}

		var o types.InstantVectorOperator = NewFunctionOverRangeVector(inner, nil, memoryConsumptionTracker, f, annotations, expressionPosition, timeRange)

		if f.SeriesMetadataFunction.NeedsSeriesDeduplication {
			o = operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
		}

		return o, nil
	}
}

func PredictLinearFactory(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	f := PredictLinear

	if len(args) != 2 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 2 arguments for predict_linear, got %v", len(args))
	}

	inner, ok := args[0].(types.RangeVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected first argument for predict_linear to be a range vector, got %T", args[0])
	}

	arg, ok := args[1].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected second argument for predict_linear to be a scalar, got %T", args[1])
	}

	var o types.InstantVectorOperator = NewFunctionOverRangeVector(inner, []types.ScalarOperator{arg}, memoryConsumptionTracker, f, annotations, expressionPosition, timeRange)

	if f.SeriesMetadataFunction.NeedsSeriesDeduplication {
		o = operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
	}

	return o, nil
}

func QuantileOverTimeFactory(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	f := QuantileOverTime

	if len(args) != 2 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 2 arguments for quantile_over_time, got %v", len(args))
	}

	arg, ok := args[0].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected first argument for quantile_over_time to be a scalar, got %T", args[1])
	}

	inner, ok := args[1].(types.RangeVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected second argument for quantile_over_time to be a range vector, got %T", args[0])
	}

	var o types.InstantVectorOperator = NewFunctionOverRangeVector(inner, []types.ScalarOperator{arg}, memoryConsumptionTracker, f, annotations, expressionPosition, timeRange)

	if f.SeriesMetadataFunction.NeedsSeriesDeduplication {
		o = operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
	}

	return o, nil
}

func scalarToInstantVectorOperatorFactory(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, _ types.QueryTimeRange) (types.InstantVectorOperator, error) {
	if len(args) != 1 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 1 argument for vector, got %v", len(args))
	}

	inner, ok := args[0].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar argument for vector, got %T", args[0])
	}

	return scalars.NewScalarToInstantVector(inner, expressionPosition, memoryConsumptionTracker), nil
}

func LabelJoinFunctionOperatorFactory(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	// It is valid for label_join to have no source label names. ie, only 3 arguments are actually required.
	if len(args) < 3 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected 3 or more arguments for label_join, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 1st argument for label_join, got %T", args[0])
	}

	dstLabel, ok := args[1].(types.StringOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a string for 2nd argument for label_join, got %T", args[1])
	}

	separator, ok := args[2].(types.StringOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a string for 3rd argument for label_join, got %T", args[2])
	}

	srcLabels := make([]types.StringOperator, len(args)-3)
	for i := 3; i < len(args); i++ {
		srcLabel, ok := args[i].(types.StringOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected a string for %dth argument for label_join, got %T", i+1, args[i])
		}
		srcLabels[i-3] = srcLabel
	}

	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc: PassthroughData,
		SeriesMetadataFunction: SeriesMetadataFunctionDefinition{
			Func:                     LabelJoinFactory(dstLabel, separator, srcLabels),
			NeedsSeriesDeduplication: true,
		},
	}

	o := NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition, timeRange)

	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
}

func LabelReplaceFunctionOperatorFactory(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	if len(args) != 5 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 5 arguments for label_replace, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 1st argument for label_replace, got %T", args[0])
	}

	dstLabel, ok := args[1].(types.StringOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a string for 2nd argument for label_replace, got %T", args[1])
	}

	replacement, ok := args[2].(types.StringOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a string for 3rd argument for label_replace, got %T", args[2])
	}

	srcLabel, ok := args[3].(types.StringOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a string for 4th argument for label_replace, got %T", args[3])
	}

	regex, ok := args[4].(types.StringOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a string for 5th argument for label_replace, got %T", args[4])
	}

	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc: PassthroughData,
		SeriesMetadataFunction: SeriesMetadataFunctionDefinition{
			Func:                     LabelReplaceFactory(dstLabel, replacement, srcLabel, regex),
			NeedsSeriesDeduplication: true,
		},
	}

	o := NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition, timeRange)

	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
}

func AbsentOperatorFactory(args []types.Operator, labels labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("expected exactly 1 parameter for 'absent', got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		return nil, fmt.Errorf("expected InstantVectorOperator as parameter of 'absent' function call, got %T", args[0])
	}

	return NewAbsent(inner, labels, timeRange, memoryConsumptionTracker, expressionPosition), nil
}

func AbsentOverTimeOperatorFactory(args []types.Operator, labels labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("expected exactly 1 parameter for 'absent_over_time', got %v", len(args))
	}

	inner, ok := args[0].(types.RangeVectorOperator)
	if !ok {
		return nil, fmt.Errorf("expected RangeVectorOperator as parameter of 'absent_over_time' function call, got %T", args[0])
	}

	return NewAbsentOverTime(inner, labels, timeRange, memoryConsumptionTracker, expressionPosition), nil
}

func ClampFunctionOperatorFactory(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	if len(args) != 3 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 3 arguments for clamp, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 1st argument for clamp, got %T", args[0])
	}

	min, ok := args[1].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar for 2nd argument for clamp, got %T", args[1])
	}

	max, ok := args[2].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar for 3rd argument for clamp, got %T", args[2])
	}

	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         Clamp,
		SeriesMetadataFunction: DropSeriesName,
	}

	o := NewFunctionOverInstantVector(inner, []types.ScalarOperator{min, max}, memoryConsumptionTracker, f, expressionPosition, timeRange)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
}

func ClampMinMaxFunctionOperatorFactory(functionName string, isMin bool) InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
		if len(args) != 2 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 2 arguments for %s, got %v", functionName, len(args))
		}

		inner, ok := args[0].(types.InstantVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected an instant vector for 1st argument for %s, got %T", functionName, args[0])
		}

		clampTo, ok := args[1].(types.ScalarOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected a scalar for 2nd argument for %s, got %T", functionName, args[1])
		}

		f := FunctionOverInstantVectorDefinition{
			SeriesDataFunc:         ClampMinMaxFactory(isMin),
			SeriesMetadataFunction: DropSeriesName,
		}

		o := NewFunctionOverInstantVector(inner, []types.ScalarOperator{clampTo}, memoryConsumptionTracker, f, expressionPosition, timeRange)
		return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
	}
}

func RoundFunctionOperatorFactory(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	if len(args) != 1 && len(args) != 2 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected 1 or 2 arguments for round, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 1st argument for round, got %T", args[0])
	}

	var toNearest types.ScalarOperator
	if len(args) == 2 {
		toNearest, ok = args[1].(types.ScalarOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected a scalar for 2nd argument for round, got %T", args[1])
		}
	} else {
		toNearest = scalars.NewScalarConstant(float64(1), timeRange, memoryConsumptionTracker, expressionPosition)
	}

	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         Round,
		SeriesMetadataFunction: DropSeriesName,
	}

	o := NewFunctionOverInstantVector(inner, []types.ScalarOperator{toNearest}, memoryConsumptionTracker, f, expressionPosition, timeRange)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
}

func HistogramQuantileFunctionOperatorFactory(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	if len(args) != 2 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 2 arguments for histogram_quantile, got %v", len(args))
	}

	ph, ok := args[0].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar for 1st argument for histogram_quantile, got %T", args[0])
	}

	inner, ok := args[1].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 2nd argument for histogram_quantile, got %T", args[1])
	}

	o := NewHistogramQuantileFunction(ph, inner, memoryConsumptionTracker, annotations, expressionPosition, timeRange)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
}

func HistogramFractionFunctionOperatorFactory(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	if len(args) != 3 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 3 arguments for histogram_fraction, got %v", len(args))
	}

	lower, ok := args[0].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar for 1st argument for histogram_fraction, got %T", args[0])
	}

	upper, ok := args[1].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar for 2nd argument for histogram_fraction, got %T", args[1])
	}

	inner, ok := args[2].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 3rd argument for histogram_fraction, got %T", args[2])
	}

	o := NewHistogramFractionFunction(lower, upper, inner, memoryConsumptionTracker, annotations, expressionPosition, timeRange)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
}

func TimestampFunctionOperatorFactory(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	if len(args) != 1 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 1 argument for timestamp, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 1st argument for timestamp, got %T", args[0])
	}

	f := Timestamp
	_, isSelector := args[0].(*selectors.InstantVectorSelector)

	if isSelector {
		// We'll have already set ReturnSampleTimestamps on the InstantVectorSelector during the planning process, so we don't need to do that here.
		f.SeriesDataFunc = PassthroughData
	}

	o := NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition, timeRange)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
}

func SortOperatorFactory(descending bool) InstantVectorFunctionOperatorFactory {
	functionName := "sort"

	if descending {
		functionName = "sort_desc"
	}

	return func(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", functionName, len(args))
		}

		inner, ok := args[0].(types.InstantVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected an instant vector for 1st argument for %s, got %T", functionName, args[0])
		}

		if timeRange.StepCount != 1 {
			// If this is a range query, sort / sort_desc does not reorder series, but does drop all histograms like it would for an instant query.
			f := FunctionOverInstantVectorDefinition{SeriesDataFunc: DropHistograms}
			return NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition, timeRange), nil
		}

		return NewSort(inner, descending, memoryConsumptionTracker, expressionPosition), nil
	}
}

// InstantVectorFunctionOperatorFactories contains operator factories for each function that returns an instant vector.
//
// Do not modify this map directly. Instead, call RegisterInstantVectorFunctionOperatorFactory.
var InstantVectorFunctionOperatorFactories = map[Function]InstantVectorFunctionOperatorFactory{}

func RegisterInstantVectorFunctionOperatorFactory(function Function, name string, factory InstantVectorFunctionOperatorFactory) error {
	if existing, exists := functionsToPromQLNames[function]; exists && name != existing {
		return fmt.Errorf("function with ID %d has already been registered with a different name: %s", function, existing)
	}

	if _, exists := InstantVectorFunctionOperatorFactories[function]; exists {
		return fmt.Errorf("function '%s' (%d) has already been registered", name, function)
	}

	if existing, exists := promQLNamesToFunctions[name]; exists {
		return fmt.Errorf("function with name '%s' has already been registered as a function with ID %d returning a different type of result", name, existing)
	}

	InstantVectorFunctionOperatorFactories[function] = factory
	promQLNamesToFunctions[name] = function
	functionsToPromQLNames[function] = name

	return nil
}

// ScalarFunctionOperatorFactories contains operator factories for each function that returns a scalar.
//
// Do not modify this map directly. Instead, call RegisterScalarFunctionOperatorFactory.
var ScalarFunctionOperatorFactories = map[Function]ScalarFunctionOperatorFactory{}

func RegisterScalarFunctionOperatorFactory(function Function, name string, factory ScalarFunctionOperatorFactory) error {
	if existing, exists := functionsToPromQLNames[function]; exists && name != existing {
		return fmt.Errorf("function with ID %d has already been registered with a different name: %s", function, existing)
	}

	if _, exists := ScalarFunctionOperatorFactories[function]; exists {
		return fmt.Errorf("function '%s' (%d) has already been registered", name, function)
	}

	if existing, exists := promQLNamesToFunctions[name]; exists {
		return fmt.Errorf("function with name '%s' has already been registered as a function with ID %d returning a different type of result", name, existing)
	}

	ScalarFunctionOperatorFactories[function] = factory
	promQLNamesToFunctions[name] = function
	functionsToPromQLNames[function] = name

	return nil
}

func piOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.ScalarOperator, error) {
	if len(args) != 0 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 0 arguments for pi, got %v", len(args))
	}

	return scalars.NewScalarConstant(math.Pi, timeRange, memoryConsumptionTracker, expressionPosition), nil
}

func timeOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.ScalarOperator, error) {
	if len(args) != 0 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 0 arguments for time, got %v", len(args))
	}

	return operators.NewTime(timeRange, memoryConsumptionTracker, expressionPosition), nil
}

func instantVectorToScalarOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.ScalarOperator, error) {
	if len(args) != 1 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 1 argument for scalar, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector argument for scalar, got %T", args[0])
	}

	return scalars.NewInstantVectorToScalar(inner, timeRange, memoryConsumptionTracker, expressionPosition), nil
}

func UnaryNegationOfInstantVectorOperatorFactory(inner types.InstantVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) types.InstantVectorOperator {
	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         UnaryNegation,
		SeriesMetadataFunction: DropSeriesName,
	}

	o := NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition, timeRange)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
}

func DoubleExponentialSmoothingFunctionOperatorFactory(args []types.Operator, _ labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	f := DoubleExponentialSmoothing

	functionName := "double_exponential_smoothing"
	if len(args) != 3 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 3 arguments for %s, got %v", functionName, len(args))
	}

	inner, ok := args[0].(types.RangeVectorOperator)
	if !ok {
		return nil, fmt.Errorf("expected a range vector argument for %s, got %T", functionName, args[0])
	}

	smoothingFactor, ok := args[1].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected second argument for %s to be a scalar, got %T", functionName, args[1])
	}

	trendFactor, ok := args[2].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected third argument for %s to be a scalar, got %T", functionName, args[2])
	}

	var o types.InstantVectorOperator = NewFunctionOverRangeVector(inner, []types.ScalarOperator{smoothingFactor, trendFactor}, memoryConsumptionTracker, f, annotations, expressionPosition, timeRange)

	if f.SeriesMetadataFunction.NeedsSeriesDeduplication {
		o = operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
	}

	return o, nil
}

func init() {
	//lint:sorted
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_ABS, "abs", InstantVectorTransformationFunctionOperatorFactory("abs", Abs)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_ABSENT, "absent", AbsentOperatorFactory))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_ABSENT_OVER_TIME, "absent_over_time", AbsentOverTimeOperatorFactory))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_ACOS, "acos", InstantVectorTransformationFunctionOperatorFactory("acos", Acos)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_ACOSH, "acosh", InstantVectorTransformationFunctionOperatorFactory("acosh", Acosh)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_ASIN, "asin", InstantVectorTransformationFunctionOperatorFactory("asin", Asin)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_ASINH, "asinh", InstantVectorTransformationFunctionOperatorFactory("asinh", Asinh)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_ATAN, "atan", InstantVectorTransformationFunctionOperatorFactory("atan", Atan)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_ATANH, "atanh", InstantVectorTransformationFunctionOperatorFactory("atanh", Atanh)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_AVG_OVER_TIME, "avg_over_time", FunctionOverRangeVectorOperatorFactory("avg_over_time", AvgOverTime)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_CEIL, "ceil", InstantVectorTransformationFunctionOperatorFactory("ceil", Ceil)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_CHANGES, "changes", FunctionOverRangeVectorOperatorFactory("changes", Changes)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_CLAMP, "clamp", ClampFunctionOperatorFactory))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_CLAMP_MAX, "clamp_max", ClampMinMaxFunctionOperatorFactory("clamp_max", false)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_CLAMP_MIN, "clamp_min", ClampMinMaxFunctionOperatorFactory("clamp_min", true)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_COS, "cos", InstantVectorTransformationFunctionOperatorFactory("cos", Cos)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_COSH, "cosh", InstantVectorTransformationFunctionOperatorFactory("cosh", Cosh)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_COUNT_OVER_TIME, "count_over_time", FunctionOverRangeVectorOperatorFactory("count_over_time", CountOverTime)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_DAY_OF_MONTH, "day_of_month", TimeTransformationFunctionOperatorFactory("day_of_month", DayOfMonth)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_DAY_OF_WEEK, "day_of_week", TimeTransformationFunctionOperatorFactory("day_of_week", DayOfWeek)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_DAY_OF_YEAR, "day_of_year", TimeTransformationFunctionOperatorFactory("day_of_year", DayOfYear)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_DAYS_IN_MONTH, "days_in_month", TimeTransformationFunctionOperatorFactory("days_in_month", DaysInMonth)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_DEG, "deg", InstantVectorTransformationFunctionOperatorFactory("deg", Deg)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_DELTA, "delta", FunctionOverRangeVectorOperatorFactory("delta", Delta)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_DERIV, "deriv", FunctionOverRangeVectorOperatorFactory("deriv", Deriv)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_DOUBLE_EXPONENTIAL_SMOOTHING, "double_exponential_smoothing", DoubleExponentialSmoothingFunctionOperatorFactory))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_EXP, "exp", InstantVectorTransformationFunctionOperatorFactory("exp", Exp)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_FLOOR, "floor", InstantVectorTransformationFunctionOperatorFactory("floor", Floor)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_HISTOGRAM_AVG, "histogram_avg", InstantVectorTransformationFunctionOperatorFactory("histogram_avg", HistogramAvg)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_HISTOGRAM_COUNT, "histogram_count", InstantVectorTransformationFunctionOperatorFactory("histogram_count", HistogramCount)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_HISTOGRAM_FRACTION, "histogram_fraction", HistogramFractionFunctionOperatorFactory))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_HISTOGRAM_QUANTILE, "histogram_quantile", HistogramQuantileFunctionOperatorFactory))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_HISTOGRAM_STDDEV, "histogram_stddev", InstantVectorTransformationFunctionOperatorFactory("histogram_stddev", HistogramStdDevStdVar(true))))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_HISTOGRAM_STDVAR, "histogram_stdvar", InstantVectorTransformationFunctionOperatorFactory("histogram_stdvar", HistogramStdDevStdVar(false))))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_HISTOGRAM_SUM, "histogram_sum", InstantVectorTransformationFunctionOperatorFactory("histogram_sum", HistogramSum)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_HOUR, "hour", TimeTransformationFunctionOperatorFactory("hour", Hour)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_IDELTA, "idelta", FunctionOverRangeVectorOperatorFactory("idelta", Idelta)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_INCREASE, "increase", FunctionOverRangeVectorOperatorFactory("increase", Increase)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_IRATE, "irate", FunctionOverRangeVectorOperatorFactory("irate", Irate)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_LABEL_JOIN, "label_join", LabelJoinFunctionOperatorFactory))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_LABEL_REPLACE, "label_replace", LabelReplaceFunctionOperatorFactory))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_LAST_OVER_TIME, "last_over_time", FunctionOverRangeVectorOperatorFactory("last_over_time", LastOverTime)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_LN, "ln", InstantVectorTransformationFunctionOperatorFactory("ln", Ln)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_LOG10, "log10", InstantVectorTransformationFunctionOperatorFactory("log10", Log10)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_LOG2, "log2", InstantVectorTransformationFunctionOperatorFactory("log2", Log2)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_MAX_OVER_TIME, "max_over_time", FunctionOverRangeVectorOperatorFactory("max_over_time", MaxOverTime)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_MIN_OVER_TIME, "min_over_time", FunctionOverRangeVectorOperatorFactory("min_over_time", MinOverTime)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_MINUTE, "minute", TimeTransformationFunctionOperatorFactory("minute", Minute)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_MONTH, "month", TimeTransformationFunctionOperatorFactory("month", Month)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_PREDICT_LINEAR, "predict_linear", PredictLinearFactory))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_PRESENT_OVER_TIME, "present_over_time", FunctionOverRangeVectorOperatorFactory("present_over_time", PresentOverTime)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_QUANTILE_OVER_TIME, "quantile_over_time", QuantileOverTimeFactory))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_RAD, "rad", InstantVectorTransformationFunctionOperatorFactory("rad", Rad)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_RATE, "rate", FunctionOverRangeVectorOperatorFactory("rate", Rate)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_RESETS, "resets", FunctionOverRangeVectorOperatorFactory("resets", Resets)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_ROUND, "round", RoundFunctionOperatorFactory))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_SGN, "sgn", InstantVectorTransformationFunctionOperatorFactory("sgn", Sgn)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_SIN, "sin", InstantVectorTransformationFunctionOperatorFactory("sin", Sin)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_SINH, "sinh", InstantVectorTransformationFunctionOperatorFactory("sinh", Sinh)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_SORT, "sort", SortOperatorFactory(false)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_SORT_DESC, "sort_desc", SortOperatorFactory(true)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_SQRT, "sqrt", InstantVectorTransformationFunctionOperatorFactory("sqrt", Sqrt)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_STDDEV_OVER_TIME, "stddev_over_time", FunctionOverRangeVectorOperatorFactory("stddev_over_time", StddevOverTime)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_STDVAR_OVER_TIME, "stdvar_over_time", FunctionOverRangeVectorOperatorFactory("stdvar_over_time", StdvarOverTime)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_SUM_OVER_TIME, "sum_over_time", FunctionOverRangeVectorOperatorFactory("sum_over_time", SumOverTime)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_TAN, "tan", InstantVectorTransformationFunctionOperatorFactory("tan", Tan)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_TANH, "tanh", InstantVectorTransformationFunctionOperatorFactory("tanh", Tanh)))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_TIMESTAMP, "timestamp", TimestampFunctionOperatorFactory))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_VECTOR, "vector", scalarToInstantVectorOperatorFactory))
	must(RegisterInstantVectorFunctionOperatorFactory(FUNCTION_YEAR, "year", TimeTransformationFunctionOperatorFactory("year", Year)))

	//lint:sorted
	must(RegisterScalarFunctionOperatorFactory(FUNCTION_PI, "pi", piOperatorFactory))
	must(RegisterScalarFunctionOperatorFactory(FUNCTION_SCALAR, "scalar", instantVectorToScalarOperatorFactory))
	must(RegisterScalarFunctionOperatorFactory(FUNCTION_TIME, "time", timeOperatorFactory))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
