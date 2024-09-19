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
	return func(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange) (types.InstantVectorOperator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", name, len(args))
		}

		inner, ok := args[0].(types.InstantVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected an instant vector argument for %s, got %T", name, args[0])
		}

		var o types.InstantVectorOperator = operators.NewFunctionOverInstantVector(inner, memoryConsumptionTracker, f, expressionPosition)

		if f.NeedsSeriesDeduplication {
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
		SeriesDataFunc:           seriesDataFunc,
		SeriesMetadataFunc:       functions.DropSeriesName,
		NeedsSeriesDeduplication: true,
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
//   - needsSeriesDeduplication: Set to true if metadataFunc may produce multiple series with the same labels and therefore deduplication is required
func InstantVectorLabelManipulationFunctionOperatorFactory(name string, metadataFunc functions.SeriesMetadataFunction, needsSeriesDeduplication bool) InstantVectorFunctionOperatorFactory {
	f := functions.FunctionOverInstantVector{
		SeriesDataFunc:           functions.PassthroughData,
		SeriesMetadataFunc:       metadataFunc,
		NeedsSeriesDeduplication: needsSeriesDeduplication,
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
	return func(args []types.Operator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, annotations *annotations.Annotations, expressionPosition posrange.PositionRange) (types.InstantVectorOperator, error) {
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

		if f.NeedsSeriesDeduplication {
			o = operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
		}

		return o, nil
	}
}

func scalarToInstantVectorOperatorFactory(args []types.Operator, _ *limiting.MemoryConsumptionTracker, _ *annotations.Annotations, expressionPosition posrange.PositionRange) (types.InstantVectorOperator, error) {
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
	"cos":               InstantVectorTransformationFunctionOperatorFactory("cos", functions.Cos),
	"cosh":              InstantVectorTransformationFunctionOperatorFactory("cosh", functions.Cosh),
	"count_over_time":   FunctionOverRangeVectorOperatorFactory("count_over_time", functions.CountOverTime),
	"deg":               InstantVectorTransformationFunctionOperatorFactory("deg", functions.Deg),
	"exp":               InstantVectorTransformationFunctionOperatorFactory("exp", functions.Exp),
	"floor":             InstantVectorTransformationFunctionOperatorFactory("floor", functions.Floor),
	"histogram_count":   InstantVectorTransformationFunctionOperatorFactory("histogram_count", functions.HistogramCount),
	"histogram_sum":     InstantVectorTransformationFunctionOperatorFactory("histogram_sum", functions.HistogramSum),
	"last_over_time":    FunctionOverRangeVectorOperatorFactory("last_over_time", functions.LastOverTime),
	"max_over_time":     FunctionOverRangeVectorOperatorFactory("max_over_time", functions.MaxOverTime),
	"min_over_time":     FunctionOverRangeVectorOperatorFactory("min_over_time", functions.MinOverTime),
	"ln":                InstantVectorTransformationFunctionOperatorFactory("ln", functions.Ln),
	"log10":             InstantVectorTransformationFunctionOperatorFactory("log10", functions.Log10),
	"log2":              InstantVectorTransformationFunctionOperatorFactory("log2", functions.Log2),
	"present_over_time": FunctionOverRangeVectorOperatorFactory("present_over_time", functions.PresentOverTime),
	"rad":               InstantVectorTransformationFunctionOperatorFactory("rad", functions.Rad),
	"rate":              FunctionOverRangeVectorOperatorFactory("rate", functions.Rate),
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
		SeriesDataFunc:           functions.UnaryNegation,
		SeriesMetadataFunc:       functions.DropSeriesName,
		NeedsSeriesDeduplication: true,
	}

	o := operators.NewFunctionOverInstantVector(inner, memoryConsumptionTracker, f, expressionPosition)
	return operators.NewDeduplicateAndMerge(o, memoryConsumptionTracker)
}
