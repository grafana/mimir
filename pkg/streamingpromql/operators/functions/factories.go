// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"fmt"
	"math"

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
	return func(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

	return func(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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
	return func(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

func PredictLinearFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

func QuantileOverTimeFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

func scalarToInstantVectorOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, _ types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

func LabelJoinFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

func LabelReplaceFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

func ClampFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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
	return func(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

func RoundFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

func HistogramQuantileFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

func HistogramFractionFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

func TimestampFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

	return func(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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
// Do not modify this map directly at runtime. Instead, call RegisterInstantVectorFunctionOperatorFactory.
var InstantVectorFunctionOperatorFactories = map[Function]InstantVectorFunctionOperatorFactory{
	// absent and absent_over_time are handled as special cases.
	//lint:sorted
	FUNCTION_ABS:                          InstantVectorTransformationFunctionOperatorFactory("abs", Abs),
	FUNCTION_ACOS:                         InstantVectorTransformationFunctionOperatorFactory("acos", Acos),
	FUNCTION_ACOSH:                        InstantVectorTransformationFunctionOperatorFactory("acosh", Acosh),
	FUNCTION_ASIN:                         InstantVectorTransformationFunctionOperatorFactory("asin", Asin),
	FUNCTION_ASINH:                        InstantVectorTransformationFunctionOperatorFactory("asinh", Asinh),
	FUNCTION_ATAN:                         InstantVectorTransformationFunctionOperatorFactory("atan", Atan),
	FUNCTION_ATANH:                        InstantVectorTransformationFunctionOperatorFactory("atanh", Atanh),
	FUNCTION_AVG_OVER_TIME:                FunctionOverRangeVectorOperatorFactory("avg_over_time", AvgOverTime),
	FUNCTION_CEIL:                         InstantVectorTransformationFunctionOperatorFactory("ceil", Ceil),
	FUNCTION_CHANGES:                      FunctionOverRangeVectorOperatorFactory("changes", Changes),
	FUNCTION_CLAMP:                        ClampFunctionOperatorFactory,
	FUNCTION_CLAMP_MAX:                    ClampMinMaxFunctionOperatorFactory("clamp_max", false),
	FUNCTION_CLAMP_MIN:                    ClampMinMaxFunctionOperatorFactory("clamp_min", true),
	FUNCTION_COS:                          InstantVectorTransformationFunctionOperatorFactory("cos", Cos),
	FUNCTION_COSH:                         InstantVectorTransformationFunctionOperatorFactory("cosh", Cosh),
	FUNCTION_COUNT_OVER_TIME:              FunctionOverRangeVectorOperatorFactory("count_over_time", CountOverTime),
	FUNCTION_DAY_OF_MONTH:                 TimeTransformationFunctionOperatorFactory("day_of_month", DayOfMonth),
	FUNCTION_DAY_OF_WEEK:                  TimeTransformationFunctionOperatorFactory("day_of_week", DayOfWeek),
	FUNCTION_DAY_OF_YEAR:                  TimeTransformationFunctionOperatorFactory("day_of_year", DayOfYear),
	FUNCTION_DAYS_IN_MONTH:                TimeTransformationFunctionOperatorFactory("days_in_month", DaysInMonth),
	FUNCTION_DEG:                          InstantVectorTransformationFunctionOperatorFactory("deg", Deg),
	FUNCTION_DELTA:                        FunctionOverRangeVectorOperatorFactory("delta", Delta),
	FUNCTION_DERIV:                        FunctionOverRangeVectorOperatorFactory("deriv", Deriv),
	FUNCTION_DOUBLE_EXPONENTIAL_SMOOTHING: DoubleExponentialSmoothingFunctionOperatorFactory,
	FUNCTION_EXP:                          InstantVectorTransformationFunctionOperatorFactory("exp", Exp),
	FUNCTION_FLOOR:                        InstantVectorTransformationFunctionOperatorFactory("floor", Floor),
	FUNCTION_HISTOGRAM_AVG:                InstantVectorTransformationFunctionOperatorFactory("histogram_avg", HistogramAvg),
	FUNCTION_HISTOGRAM_COUNT:              InstantVectorTransformationFunctionOperatorFactory("histogram_count", HistogramCount),
	FUNCTION_HISTOGRAM_FRACTION:           HistogramFractionFunctionOperatorFactory,
	FUNCTION_HISTOGRAM_QUANTILE:           HistogramQuantileFunctionOperatorFactory,
	FUNCTION_HISTOGRAM_STDDEV:             InstantVectorTransformationFunctionOperatorFactory("histogram_stddev", HistogramStdDevStdVar(true)),
	FUNCTION_HISTOGRAM_STDVAR:             InstantVectorTransformationFunctionOperatorFactory("histogram_stdvar", HistogramStdDevStdVar(false)),
	FUNCTION_HISTOGRAM_SUM:                InstantVectorTransformationFunctionOperatorFactory("histogram_sum", HistogramSum),
	FUNCTION_HOUR:                         TimeTransformationFunctionOperatorFactory("hour", Hour),
	FUNCTION_IDELTA:                       FunctionOverRangeVectorOperatorFactory("idelta", Idelta),
	FUNCTION_INCREASE:                     FunctionOverRangeVectorOperatorFactory("increase", Increase),
	FUNCTION_IRATE:                        FunctionOverRangeVectorOperatorFactory("irate", Irate),
	FUNCTION_LABEL_JOIN:                   LabelJoinFunctionOperatorFactory,
	FUNCTION_LABEL_REPLACE:                LabelReplaceFunctionOperatorFactory,
	FUNCTION_LAST_OVER_TIME:               FunctionOverRangeVectorOperatorFactory("last_over_time", LastOverTime),
	FUNCTION_LN:                           InstantVectorTransformationFunctionOperatorFactory("ln", Ln),
	FUNCTION_LOG10:                        InstantVectorTransformationFunctionOperatorFactory("log10", Log10),
	FUNCTION_LOG2:                         InstantVectorTransformationFunctionOperatorFactory("log2", Log2),
	FUNCTION_MAX_OVER_TIME:                FunctionOverRangeVectorOperatorFactory("max_over_time", MaxOverTime),
	FUNCTION_MIN_OVER_TIME:                FunctionOverRangeVectorOperatorFactory("min_over_time", MinOverTime),
	FUNCTION_MINUTE:                       TimeTransformationFunctionOperatorFactory("minute", Minute),
	FUNCTION_MONTH:                        TimeTransformationFunctionOperatorFactory("month", Month),
	FUNCTION_PREDICT_LINEAR:               PredictLinearFactory,
	FUNCTION_PRESENT_OVER_TIME:            FunctionOverRangeVectorOperatorFactory("present_over_time", PresentOverTime),
	FUNCTION_QUANTILE_OVER_TIME:           QuantileOverTimeFactory,
	FUNCTION_RAD:                          InstantVectorTransformationFunctionOperatorFactory("rad", Rad),
	FUNCTION_RATE:                         FunctionOverRangeVectorOperatorFactory("rate", Rate),
	FUNCTION_RESETS:                       FunctionOverRangeVectorOperatorFactory("resets", Resets),
	FUNCTION_ROUND:                        RoundFunctionOperatorFactory,
	FUNCTION_SGN:                          InstantVectorTransformationFunctionOperatorFactory("sgn", Sgn),
	FUNCTION_SIN:                          InstantVectorTransformationFunctionOperatorFactory("sin", Sin),
	FUNCTION_SINH:                         InstantVectorTransformationFunctionOperatorFactory("sinh", Sinh),
	FUNCTION_SORT:                         SortOperatorFactory(false),
	FUNCTION_SORT_DESC:                    SortOperatorFactory(true),
	FUNCTION_SQRT:                         InstantVectorTransformationFunctionOperatorFactory("sqrt", Sqrt),
	FUNCTION_STDDEV_OVER_TIME:             FunctionOverRangeVectorOperatorFactory("stddev_over_time", StddevOverTime),
	FUNCTION_STDVAR_OVER_TIME:             FunctionOverRangeVectorOperatorFactory("stdvar_over_time", StdvarOverTime),
	FUNCTION_SUM_OVER_TIME:                FunctionOverRangeVectorOperatorFactory("sum_over_time", SumOverTime),
	FUNCTION_TAN:                          InstantVectorTransformationFunctionOperatorFactory("tan", Tan),
	FUNCTION_TANH:                         InstantVectorTransformationFunctionOperatorFactory("tanh", Tanh),
	FUNCTION_TIMESTAMP:                    TimestampFunctionOperatorFactory,
	FUNCTION_VECTOR:                       scalarToInstantVectorOperatorFactory,
	FUNCTION_YEAR:                         TimeTransformationFunctionOperatorFactory("year", Year),
}

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
// Do not modify this map directly at runtime. Instead, call RegisterScalarFunctionOperatorFactory.
var ScalarFunctionOperatorFactories = map[Function]ScalarFunctionOperatorFactory{
	// Please keep this list sorted alphabetically.
	FUNCTION_PI:     piOperatorFactory,
	FUNCTION_SCALAR: instantVectorToScalarOperatorFactory,
	FUNCTION_TIME:   timeOperatorFactory,
}

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

func DoubleExponentialSmoothingFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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
