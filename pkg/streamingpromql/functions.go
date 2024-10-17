// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"fmt"
	"math"

	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
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
func SingleInputVectorFunctionOperatorFactory(name string, f functions.FunctionOverInstantVector) InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, _ types.QueryTimeRange) (types.InstantVectorOperator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", name, len(args))
		}

		inner, ok := args[0].(types.InstantVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected an instant vector argument for %s, got %T", name, args[0])
		}

		var o types.InstantVectorOperator = operators.NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition)

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
	f := functions.FunctionOverInstantVector{
		SeriesDataFunc:         seriesDataFunc,
		SeriesMetadataFunction: functions.DropSeriesName,
	}

	return SingleInputVectorFunctionOperatorFactory(name, f)
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
	f := functions.FunctionOverInstantVector{
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
	f functions.FunctionOverRangeVector,
) InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange, _ types.QueryTimeRange) (types.InstantVectorOperator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", name, len(args))
		}

		inner, ok := args[0].(types.RangeVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected a range vector argument for %s, got %T", name, args[0])
		}

		var o types.InstantVectorOperator = operators.NewFunctionOverRangeVector(inner, memoryConsumptionTracker, f, annotations, expressionPosition)

		if f.SeriesMetadataFunction.NeedsSeriesDeduplication {
			o = operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
		}

		return o, nil
	}
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

	return operators.NewScalarToInstantVector(inner, expressionPosition), nil
}

func LabelReplaceFunctionOperatorFactory() InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, _ types.QueryTimeRange) (types.InstantVectorOperator, error) {
		if len(args) != 5 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 5 argument for label_replace, got %v", len(args))
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

		f := functions.FunctionOverInstantVector{
			SeriesDataFunc: functions.PassthroughData,
			SeriesMetadataFunction: functions.SeriesMetadataFunctionDefinition{
				Func:                     functions.LabelReplaceFactory(dstLabel, replacement, srcLabel, regex),
				NeedsSeriesDeduplication: true,
			},
		}

		o := operators.NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition)

		return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker), nil
	}
}

func ClampFunctionOperatorFactory() InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, _ types.QueryTimeRange) (types.InstantVectorOperator, error) {
		if len(args) != 3 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 3 argument for clamp, got %v", len(args))
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

		f := functions.FunctionOverInstantVector{
			SeriesDataFunc:         functions.Clamp,
			SeriesMetadataFunction: functions.DropSeriesName,
		}

		return operators.NewFunctionOverInstantVector(inner, []types.ScalarOperator{min, max}, memoryConsumptionTracker, f, expressionPosition), nil
	}
}

func ClampMinMaxFunctionOperatorFactory(functionName string, isMin bool) InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, _ types.QueryTimeRange) (types.InstantVectorOperator, error) {
		if len(args) != 2 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 2 argument for %s, got %v", functionName, len(args))
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

		f := functions.FunctionOverInstantVector{
			SeriesDataFunc:         functions.ClampMinMaxFactory(isMin),
			SeriesMetadataFunction: functions.DropSeriesName,
		}

		return operators.NewFunctionOverInstantVector(inner, []types.ScalarOperator{clampTo}, memoryConsumptionTracker, f, expressionPosition), nil
	}
}

func RoundFunctionOperatorFactory() InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
		if len(args) != 1 && len(args) != 2 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 or 2 argument for round, got %v", len(args))
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
			toNearest = operators.NewScalarConstant(float64(1), timeRange, memoryConsumptionTracker, expressionPosition)
		}

		f := functions.FunctionOverInstantVector{
			SeriesDataFunc: functions.Round,
			// TODO(jhesketh): With the currently vendored prometheus, round does not consistently drop the __name__ label
			//                 (as verified by our tests). We match this for consistency, but will
			//                 need to drop them once prometheus 3.0 is vendored in.
			SeriesMetadataFunction: functions.SeriesMetadataFunctionDefinition{},
		}

		return operators.NewFunctionOverInstantVector(inner, []types.ScalarOperator{toNearest}, memoryConsumptionTracker, f, expressionPosition), nil
	}
}

// These functions return an instant-vector.
var instantVectorFunctionOperatorFactories = map[string]InstantVectorFunctionOperatorFactory{
	// Please keep this list sorted alphabetically.

	"abs":               InstantVectorTransformationFunctionOperatorFactory("abs", functions.Abs),
	"acos":              InstantVectorTransformationFunctionOperatorFactory("acos", functions.Acos),
	"acosh":             InstantVectorTransformationFunctionOperatorFactory("acosh", functions.Acosh),
	"asin":              InstantVectorTransformationFunctionOperatorFactory("asin", functions.Asin),
	"asinh":             InstantVectorTransformationFunctionOperatorFactory("asinh", functions.Asinh),
	"atan":              InstantVectorTransformationFunctionOperatorFactory("atan", functions.Atan),
	"atanh":             InstantVectorTransformationFunctionOperatorFactory("atanh", functions.Atanh),
	"avg_over_time":     FunctionOverRangeVectorOperatorFactory("avg_over_time", functions.AvgOverTime),
	"ceil":              InstantVectorTransformationFunctionOperatorFactory("ceil", functions.Ceil),
	"clamp":             ClampFunctionOperatorFactory(),
	"clamp_max":         ClampMinMaxFunctionOperatorFactory("clamp_max", false),
	"clamp_min":         ClampMinMaxFunctionOperatorFactory("clamp_min", true),
	"cos":               InstantVectorTransformationFunctionOperatorFactory("cos", functions.Cos),
	"cosh":              InstantVectorTransformationFunctionOperatorFactory("cosh", functions.Cosh),
	"count_over_time":   FunctionOverRangeVectorOperatorFactory("count_over_time", functions.CountOverTime),
	"deg":               InstantVectorTransformationFunctionOperatorFactory("deg", functions.Deg),
	"exp":               InstantVectorTransformationFunctionOperatorFactory("exp", functions.Exp),
	"floor":             InstantVectorTransformationFunctionOperatorFactory("floor", functions.Floor),
	"histogram_count":   InstantVectorTransformationFunctionOperatorFactory("histogram_count", functions.HistogramCount),
	"histogram_sum":     InstantVectorTransformationFunctionOperatorFactory("histogram_sum", functions.HistogramSum),
	"increase":          FunctionOverRangeVectorOperatorFactory("increase", functions.Increase),
	"label_replace":     LabelReplaceFunctionOperatorFactory(),
	"last_over_time":    FunctionOverRangeVectorOperatorFactory("last_over_time", functions.LastOverTime),
	"ln":                InstantVectorTransformationFunctionOperatorFactory("ln", functions.Ln),
	"log10":             InstantVectorTransformationFunctionOperatorFactory("log10", functions.Log10),
	"log2":              InstantVectorTransformationFunctionOperatorFactory("log2", functions.Log2),
	"max_over_time":     FunctionOverRangeVectorOperatorFactory("max_over_time", functions.MaxOverTime),
	"min_over_time":     FunctionOverRangeVectorOperatorFactory("min_over_time", functions.MinOverTime),
	"present_over_time": FunctionOverRangeVectorOperatorFactory("present_over_time", functions.PresentOverTime),
	"rad":               InstantVectorTransformationFunctionOperatorFactory("rad", functions.Rad),
	"rate":              FunctionOverRangeVectorOperatorFactory("rate", functions.Rate),
	"round":             RoundFunctionOperatorFactory(),
	"sgn":               InstantVectorTransformationFunctionOperatorFactory("sgn", functions.Sgn),
	"sin":               InstantVectorTransformationFunctionOperatorFactory("sin", functions.Sin),
	"sinh":              InstantVectorTransformationFunctionOperatorFactory("sinh", functions.Sinh),
	"sqrt":              InstantVectorTransformationFunctionOperatorFactory("sqrt", functions.Sqrt),
	"sum_over_time":     FunctionOverRangeVectorOperatorFactory("sum_over_time", functions.SumOverTime),
	"tan":               InstantVectorTransformationFunctionOperatorFactory("tan", functions.Tan),
	"tanh":              InstantVectorTransformationFunctionOperatorFactory("tanh", functions.Tanh),
	"vector":            scalarToInstantVectorOperatorFactory,
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

	return operators.NewScalarConstant(math.Pi, timeRange, memoryConsumptionTracker, expressionPosition), nil
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

	return operators.NewInstantVectorToScalar(inner, timeRange, memoryConsumptionTracker, expressionPosition), nil
}

func unaryNegationOfInstantVectorOperatorFactory(inner types.InstantVectorOperator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, expressionPosition posrange.PositionRange) types.InstantVectorOperator {
	f := functions.FunctionOverInstantVector{
		SeriesDataFunc:         functions.UnaryNegation,
		SeriesMetadataFunction: functions.DropSeriesName,
	}

	o := operators.NewFunctionOverInstantVector(inner, nil, memoryConsumptionTracker, f, expressionPosition)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
}
