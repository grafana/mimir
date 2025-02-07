// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"fmt"
	"math"

	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/scalars"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type InstantVectorFunctionOperatorFactory func(
	args []types.Operator,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
) (types.InstantVectorOperator, error)

type ScalarFunctionOperatorFactory func(
	args []types.Operator,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
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
func SingleInputVectorFunctionOperatorFactory(name string, f functions.FunctionOverInstantVectorDefinition) InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", name, len(args))
		}

		inner, ok := args[0].(types.InstantVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected an instant vector argument for %s, got %T", name, args[0])
		}

		var o types.InstantVectorOperator = functions.NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition, timeRange)

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
func InstantVectorTransformationFunctionOperatorFactory(name string, seriesDataFunc functions.InstantVectorSeriesFunction) InstantVectorFunctionOperatorFactory {
	f := functions.FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         seriesDataFunc,
		SeriesMetadataFunction: functions.DropSeriesName,
	}

	return SingleInputVectorFunctionOperatorFactory(name, f)
}

func TimeTransformationFunctionOperatorFactory(name string, seriesDataFunc functions.InstantVectorSeriesFunction) InstantVectorFunctionOperatorFactory {
	f := functions.FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         seriesDataFunc,
		SeriesMetadataFunction: functions.DropSeriesName,
	}

	return func(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
		var inner types.InstantVectorOperator
		if len(args) == 0 {
			// if the argument is not provided, it will default to vector(time())
			inner = scalars.NewScalarToInstantVector(operators.NewTime(timeRange, memoryConsumptionTracker, expressionPosition), expressionPosition)
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

		var o types.InstantVectorOperator = functions.NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition, timeRange)
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
func InstantVectorLabelManipulationFunctionOperatorFactory(name string, metadataFunc functions.SeriesMetadataFunctionDefinition) InstantVectorFunctionOperatorFactory {
	f := functions.FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         functions.PassthroughData,
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
	f functions.FunctionOverRangeVectorDefinition,
) InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", name, len(args))
		}

		inner, ok := args[0].(types.RangeVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected a range vector argument for %s, got %T", name, args[0])
		}

		var o types.InstantVectorOperator = functions.NewFunctionOverRangeVector(inner, nil, memoryConsumptionTracker, f, annotations, expressionPosition, timeRange)

		if f.SeriesMetadataFunction.NeedsSeriesDeduplication {
			o = operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
		}

		return o, nil
	}
}

func PredictLinearFactory(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	f := functions.PredictLinear

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

	var o types.InstantVectorOperator = functions.NewFunctionOverRangeVector(inner, []types.ScalarOperator{arg}, memoryConsumptionTracker, f, annotations, expressionPosition, timeRange)

	if f.SeriesMetadataFunction.NeedsSeriesDeduplication {
		o = operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
	}

	return o, nil
}

func scalarToInstantVectorOperatorFactory(args []types.Operator, _ *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, _ types.QueryTimeRange) (types.InstantVectorOperator, error) {
	if len(args) != 1 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 1 argument for vector, got %v", len(args))
	}

	inner, ok := args[0].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar argument for vector, got %T", args[0])
	}

	return scalars.NewScalarToInstantVector(inner, expressionPosition), nil
}

func LabelReplaceFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

	f := functions.FunctionOverInstantVectorDefinition{
		SeriesDataFunc: functions.PassthroughData,
		SeriesMetadataFunction: functions.SeriesMetadataFunctionDefinition{
			Func:                     functions.LabelReplaceFactory(dstLabel, replacement, srcLabel, regex),
			NeedsSeriesDeduplication: true,
		},
	}

	o := functions.NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition, timeRange)

	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
}

func ClampFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

	f := functions.FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         functions.Clamp,
		SeriesMetadataFunction: functions.DropSeriesName,
	}

	o := functions.NewFunctionOverInstantVector(inner, []types.ScalarOperator{min, max}, memoryConsumptionTracker, f, expressionPosition, timeRange)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
}

func ClampMinMaxFunctionOperatorFactory(functionName string, isMin bool) InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

		f := functions.FunctionOverInstantVectorDefinition{
			SeriesDataFunc:         functions.ClampMinMaxFactory(isMin),
			SeriesMetadataFunction: functions.DropSeriesName,
		}

		o := functions.NewFunctionOverInstantVector(inner, []types.ScalarOperator{clampTo}, memoryConsumptionTracker, f, expressionPosition, timeRange)
		return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
	}
}

func RoundFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

	f := functions.FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         functions.Round,
		SeriesMetadataFunction: functions.DropSeriesName,
	}

	o := functions.NewFunctionOverInstantVector(inner, []types.ScalarOperator{toNearest}, memoryConsumptionTracker, f, expressionPosition, timeRange)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
}

func HistogramQuantileFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

	o := functions.NewHistogramQuantileFunction(ph, inner, memoryConsumptionTracker, annotations, expressionPosition, timeRange)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
}

func HistogramFractionFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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

	f := functions.FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         functions.HistogramFraction,
		SeriesMetadataFunction: functions.DropSeriesName,
	}

	o := functions.NewFunctionOverInstantVector(inner, []types.ScalarOperator{lower, upper}, memoryConsumptionTracker, f, expressionPosition, timeRange)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
}

func TimestampFunctionOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	if len(args) != 1 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 1 argument for timestamp, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 1st argument for timestamp, got %T", args[0])
	}

	f := functions.Timestamp
	selector, isSelector := args[0].(*selectors.InstantVectorSelector)

	if isSelector {
		selector.ReturnSampleTimestamps = true
		f.SeriesDataFunc = functions.PassthroughData
	}

	o := functions.NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition, timeRange)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
}

func SortOperatorFactory(descending bool) InstantVectorFunctionOperatorFactory {
	functionName := "sort"

	if descending {
		functionName = "sort_desc"
	}

	return func(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
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
			// If this is a range query, sort / sort_desc have no effect, so we might as well just skip straight to the inner operator.
			return inner, nil
		}

		return functions.NewSort(inner, descending, memoryConsumptionTracker, expressionPosition), nil
	}
}

// These functions return an instant-vector.
var instantVectorFunctionOperatorFactories = map[string]InstantVectorFunctionOperatorFactory{
	// Please keep this list sorted alphabetically.
	"abs":                InstantVectorTransformationFunctionOperatorFactory("abs", functions.Abs),
	"acos":               InstantVectorTransformationFunctionOperatorFactory("acos", functions.Acos),
	"acosh":              InstantVectorTransformationFunctionOperatorFactory("acosh", functions.Acosh),
	"asin":               InstantVectorTransformationFunctionOperatorFactory("asin", functions.Asin),
	"asinh":              InstantVectorTransformationFunctionOperatorFactory("asinh", functions.Asinh),
	"atan":               InstantVectorTransformationFunctionOperatorFactory("atan", functions.Atan),
	"atanh":              InstantVectorTransformationFunctionOperatorFactory("atanh", functions.Atanh),
	"avg_over_time":      FunctionOverRangeVectorOperatorFactory("avg_over_time", functions.AvgOverTime),
	"ceil":               InstantVectorTransformationFunctionOperatorFactory("ceil", functions.Ceil),
	"changes":            FunctionOverRangeVectorOperatorFactory("changes", functions.Changes),
	"clamp":              ClampFunctionOperatorFactory,
	"clamp_max":          ClampMinMaxFunctionOperatorFactory("clamp_max", false),
	"clamp_min":          ClampMinMaxFunctionOperatorFactory("clamp_min", true),
	"cos":                InstantVectorTransformationFunctionOperatorFactory("cos", functions.Cos),
	"cosh":               InstantVectorTransformationFunctionOperatorFactory("cosh", functions.Cosh),
	"count_over_time":    FunctionOverRangeVectorOperatorFactory("count_over_time", functions.CountOverTime),
	"days_in_month":      TimeTransformationFunctionOperatorFactory("days_in_month", functions.DaysInMonth),
	"day_of_month":       TimeTransformationFunctionOperatorFactory("day_of_month", functions.DayOfMonth),
	"day_of_week":        TimeTransformationFunctionOperatorFactory("day_of_week", functions.DayOfWeek),
	"day_of_year":        TimeTransformationFunctionOperatorFactory("day_of_year", functions.DayOfYear),
	"deg":                InstantVectorTransformationFunctionOperatorFactory("deg", functions.Deg),
	"delta":              FunctionOverRangeVectorOperatorFactory("delta", functions.Delta),
	"deriv":              FunctionOverRangeVectorOperatorFactory("deriv", functions.Deriv),
	"exp":                InstantVectorTransformationFunctionOperatorFactory("exp", functions.Exp),
	"floor":              InstantVectorTransformationFunctionOperatorFactory("floor", functions.Floor),
	"histogram_avg":      InstantVectorTransformationFunctionOperatorFactory("histogram_avg", functions.HistogramAvg),
	"histogram_count":    InstantVectorTransformationFunctionOperatorFactory("histogram_count", functions.HistogramCount),
	"histogram_fraction": HistogramFractionFunctionOperatorFactory,
	"histogram_quantile": HistogramQuantileFunctionOperatorFactory,
	"histogram_stddev":   InstantVectorTransformationFunctionOperatorFactory("histogram_stddev", functions.HistogramStdDevStdVar(true)),
	"histogram_stdvar":   InstantVectorTransformationFunctionOperatorFactory("histogram_stdvar", functions.HistogramStdDevStdVar(false)),
	"histogram_sum":      InstantVectorTransformationFunctionOperatorFactory("histogram_sum", functions.HistogramSum),
	"hour":               TimeTransformationFunctionOperatorFactory("hour", functions.Hour),
	"idelta":             FunctionOverRangeVectorOperatorFactory("idelta", functions.Idelta),
	"increase":           FunctionOverRangeVectorOperatorFactory("increase", functions.Increase),
	"irate":              FunctionOverRangeVectorOperatorFactory("irate", functions.Irate),
	"label_replace":      LabelReplaceFunctionOperatorFactory,
	"last_over_time":     FunctionOverRangeVectorOperatorFactory("last_over_time", functions.LastOverTime),
	"ln":                 InstantVectorTransformationFunctionOperatorFactory("ln", functions.Ln),
	"log10":              InstantVectorTransformationFunctionOperatorFactory("log10", functions.Log10),
	"log2":               InstantVectorTransformationFunctionOperatorFactory("log2", functions.Log2),
	"max_over_time":      FunctionOverRangeVectorOperatorFactory("max_over_time", functions.MaxOverTime),
	"min_over_time":      FunctionOverRangeVectorOperatorFactory("min_over_time", functions.MinOverTime),
	"minute":             TimeTransformationFunctionOperatorFactory("minute", functions.Minute),
	"month":              TimeTransformationFunctionOperatorFactory("month", functions.Month),
	"predict_linear":     PredictLinearFactory,
	"present_over_time":  FunctionOverRangeVectorOperatorFactory("present_over_time", functions.PresentOverTime),
	"rad":                InstantVectorTransformationFunctionOperatorFactory("rad", functions.Rad),
	"rate":               FunctionOverRangeVectorOperatorFactory("rate", functions.Rate),
	"resets":             FunctionOverRangeVectorOperatorFactory("resets", functions.Resets),
	"round":              RoundFunctionOperatorFactory,
	"sgn":                InstantVectorTransformationFunctionOperatorFactory("sgn", functions.Sgn),
	"sin":                InstantVectorTransformationFunctionOperatorFactory("sin", functions.Sin),
	"sinh":               InstantVectorTransformationFunctionOperatorFactory("sinh", functions.Sinh),
	"sort":               SortOperatorFactory(false),
	"sort_desc":          SortOperatorFactory(true),
	"sqrt":               InstantVectorTransformationFunctionOperatorFactory("sqrt", functions.Sqrt),
	"sum_over_time":      FunctionOverRangeVectorOperatorFactory("sum_over_time", functions.SumOverTime),
	"tan":                InstantVectorTransformationFunctionOperatorFactory("tan", functions.Tan),
	"tanh":               InstantVectorTransformationFunctionOperatorFactory("tanh", functions.Tanh),
	"timestamp":          TimestampFunctionOperatorFactory,
	"vector":             scalarToInstantVectorOperatorFactory,
	"year":               TimeTransformationFunctionOperatorFactory("year", functions.Year),
}

func RegisterInstantVectorFunctionOperatorFactory(functionName string, factory InstantVectorFunctionOperatorFactory) error {
	if _, exists := instantVectorFunctionOperatorFactories[functionName]; exists {
		return fmt.Errorf("function '%s' has already been registered", functionName)
	}

	instantVectorFunctionOperatorFactories[functionName] = factory
	return nil
}

// These functions return a scalar.
var scalarFunctionOperatorFactories = map[string]ScalarFunctionOperatorFactory{
	// Please keep this list sorted alphabetically.
	"pi":     piOperatorFactory,
	"scalar": instantVectorToScalarOperatorFactory,
	"time":   timeOperatorFactory,
}

func RegisterScalarFunctionOperatorFactory(functionName string, factory ScalarFunctionOperatorFactory) error {
	if _, exists := scalarFunctionOperatorFactories[functionName]; exists {
		return fmt.Errorf("function '%s' has already been registered", functionName)
	}

	scalarFunctionOperatorFactories[functionName] = factory
	return nil
}

func piOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.ScalarOperator, error) {
	if len(args) != 0 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 0 arguments for pi, got %v", len(args))
	}

	return scalars.NewScalarConstant(math.Pi, timeRange, memoryConsumptionTracker, expressionPosition), nil
}

func timeOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.ScalarOperator, error) {
	if len(args) != 0 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 0 arguments for time, got %v", len(args))
	}

	return operators.NewTime(timeRange, memoryConsumptionTracker, expressionPosition), nil
}

func instantVectorToScalarOperatorFactory(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.ScalarOperator, error) {
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

func unaryNegationOfInstantVectorOperatorFactory(inner types.InstantVectorOperator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) types.InstantVectorOperator {
	f := functions.FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         functions.UnaryNegation,
		SeriesMetadataFunction: functions.DropSeriesName,
	}

	o := functions.NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition, timeRange)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
}
