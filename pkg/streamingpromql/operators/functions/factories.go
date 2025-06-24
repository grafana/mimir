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
var InstantVectorFunctionOperatorFactories = map[string]InstantVectorFunctionOperatorFactory{
	// absent and absent_over_time are handled as special cases.
	//lint:sorted
	"abs":                          InstantVectorTransformationFunctionOperatorFactory("abs", Abs),
	"acos":                         InstantVectorTransformationFunctionOperatorFactory("acos", Acos),
	"acosh":                        InstantVectorTransformationFunctionOperatorFactory("acosh", Acosh),
	"asin":                         InstantVectorTransformationFunctionOperatorFactory("asin", Asin),
	"asinh":                        InstantVectorTransformationFunctionOperatorFactory("asinh", Asinh),
	"atan":                         InstantVectorTransformationFunctionOperatorFactory("atan", Atan),
	"atanh":                        InstantVectorTransformationFunctionOperatorFactory("atanh", Atanh),
	"avg_over_time":                FunctionOverRangeVectorOperatorFactory("avg_over_time", AvgOverTime),
	"ceil":                         InstantVectorTransformationFunctionOperatorFactory("ceil", Ceil),
	"changes":                      FunctionOverRangeVectorOperatorFactory("changes", Changes),
	"clamp":                        ClampFunctionOperatorFactory,
	"clamp_max":                    ClampMinMaxFunctionOperatorFactory("clamp_max", false),
	"clamp_min":                    ClampMinMaxFunctionOperatorFactory("clamp_min", true),
	"cos":                          InstantVectorTransformationFunctionOperatorFactory("cos", Cos),
	"cosh":                         InstantVectorTransformationFunctionOperatorFactory("cosh", Cosh),
	"count_over_time":              FunctionOverRangeVectorOperatorFactory("count_over_time", CountOverTime),
	"day_of_month":                 TimeTransformationFunctionOperatorFactory("day_of_month", DayOfMonth),
	"day_of_week":                  TimeTransformationFunctionOperatorFactory("day_of_week", DayOfWeek),
	"day_of_year":                  TimeTransformationFunctionOperatorFactory("day_of_year", DayOfYear),
	"days_in_month":                TimeTransformationFunctionOperatorFactory("days_in_month", DaysInMonth),
	"deg":                          InstantVectorTransformationFunctionOperatorFactory("deg", Deg),
	"delta":                        FunctionOverRangeVectorOperatorFactory("delta", Delta),
	"deriv":                        FunctionOverRangeVectorOperatorFactory("deriv", Deriv),
	"double_exponential_smoothing": DoubleExponentialSmoothingFunctionOperatorFactory,
	"exp":                          InstantVectorTransformationFunctionOperatorFactory("exp", Exp),
	"floor":                        InstantVectorTransformationFunctionOperatorFactory("floor", Floor),
	"histogram_avg":                InstantVectorTransformationFunctionOperatorFactory("histogram_avg", HistogramAvg),
	"histogram_count":              InstantVectorTransformationFunctionOperatorFactory("histogram_count", HistogramCount),
	"histogram_fraction":           HistogramFractionFunctionOperatorFactory,
	"histogram_quantile":           HistogramQuantileFunctionOperatorFactory,
	"histogram_stddev":             InstantVectorTransformationFunctionOperatorFactory("histogram_stddev", HistogramStdDevStdVar(true)),
	"histogram_stdvar":             InstantVectorTransformationFunctionOperatorFactory("histogram_stdvar", HistogramStdDevStdVar(false)),
	"histogram_sum":                InstantVectorTransformationFunctionOperatorFactory("histogram_sum", HistogramSum),
	"hour":                         TimeTransformationFunctionOperatorFactory("hour", Hour),
	"idelta":                       FunctionOverRangeVectorOperatorFactory("idelta", Idelta),
	"increase":                     FunctionOverRangeVectorOperatorFactory("increase", Increase),
	"irate":                        FunctionOverRangeVectorOperatorFactory("irate", Irate),
	"label_join":                   LabelJoinFunctionOperatorFactory,
	"label_replace":                LabelReplaceFunctionOperatorFactory,
	"last_over_time":               FunctionOverRangeVectorOperatorFactory("last_over_time", LastOverTime),
	"ln":                           InstantVectorTransformationFunctionOperatorFactory("ln", Ln),
	"log10":                        InstantVectorTransformationFunctionOperatorFactory("log10", Log10),
	"log2":                         InstantVectorTransformationFunctionOperatorFactory("log2", Log2),
	"max_over_time":                FunctionOverRangeVectorOperatorFactory("max_over_time", MaxOverTime),
	"min_over_time":                FunctionOverRangeVectorOperatorFactory("min_over_time", MinOverTime),
	"minute":                       TimeTransformationFunctionOperatorFactory("minute", Minute),
	"month":                        TimeTransformationFunctionOperatorFactory("month", Month),
	"predict_linear":               PredictLinearFactory,
	"present_over_time":            FunctionOverRangeVectorOperatorFactory("present_over_time", PresentOverTime),
	"quantile_over_time":           QuantileOverTimeFactory,
	"rad":                          InstantVectorTransformationFunctionOperatorFactory("rad", Rad),
	"rate":                         FunctionOverRangeVectorOperatorFactory("rate", Rate),
	"resets":                       FunctionOverRangeVectorOperatorFactory("resets", Resets),
	"round":                        RoundFunctionOperatorFactory,
	"sgn":                          InstantVectorTransformationFunctionOperatorFactory("sgn", Sgn),
	"sin":                          InstantVectorTransformationFunctionOperatorFactory("sin", Sin),
	"sinh":                         InstantVectorTransformationFunctionOperatorFactory("sinh", Sinh),
	"sort":                         SortOperatorFactory(false),
	"sort_desc":                    SortOperatorFactory(true),
	"sqrt":                         InstantVectorTransformationFunctionOperatorFactory("sqrt", Sqrt),
	"stddev_over_time":             FunctionOverRangeVectorOperatorFactory("stddev_over_time", StddevOverTime),
	"stdvar_over_time":             FunctionOverRangeVectorOperatorFactory("stdvar_over_time", StdvarOverTime),
	"sum_over_time":                FunctionOverRangeVectorOperatorFactory("sum_over_time", SumOverTime),
	"tan":                          InstantVectorTransformationFunctionOperatorFactory("tan", Tan),
	"tanh":                         InstantVectorTransformationFunctionOperatorFactory("tanh", Tanh),
	"timestamp":                    TimestampFunctionOperatorFactory,
	"vector":                       scalarToInstantVectorOperatorFactory,
	"year":                         TimeTransformationFunctionOperatorFactory("year", Year),
}

func RegisterInstantVectorFunctionOperatorFactory(functionName string, factory InstantVectorFunctionOperatorFactory) error {
	if _, exists := InstantVectorFunctionOperatorFactories[functionName]; exists {
		return fmt.Errorf("function '%s' has already been registered", functionName)
	}

	InstantVectorFunctionOperatorFactories[functionName] = factory
	return nil
}

// ScalarFunctionOperatorFactories contains operator factories for each function that returns a scalar.
//
// Do not modify this map directly at runtime. Instead, call RegisterScalarFunctionOperatorFactory.
var ScalarFunctionOperatorFactories = map[string]ScalarFunctionOperatorFactory{
	// Please keep this list sorted alphabetically.
	"pi":     piOperatorFactory,
	"scalar": instantVectorToScalarOperatorFactory,
	"time":   timeOperatorFactory,
}

func RegisterScalarFunctionOperatorFactory(functionName string, factory ScalarFunctionOperatorFactory) error {
	if _, exists := ScalarFunctionOperatorFactories[functionName]; exists {
		return fmt.Errorf("function '%s' has already been registered", functionName)
	}

	ScalarFunctionOperatorFactories[functionName] = factory
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
