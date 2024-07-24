// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"fmt"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type InstantVectorFunctionOperatorFactory func(args []types.Operator, pool *pooling.LimitingPool) (types.InstantVectorOperator, error)

// SingleInputVectorFunctionOperatorFactory creates an InstantVectorFunctionOperatorFactory for functions
// that have exactly 1 argument (v instant-vector).
//
// Parameters:
//   - name: The name of the function.
//   - metadataFunc: The function for handling metadata
//   - seriesDataFunc: The function to handle series data
func SingleInputVectorFunctionOperatorFactory(name string, metadataFunc functions.SeriesMetadataFunction, seriesDataFunc functions.InstantVectorFunction) InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, pool *pooling.LimitingPool) (types.InstantVectorOperator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", name, len(args))
		}

		inner, ok := args[0].(types.InstantVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected an instant vector argument for %s, got %T", name, args[0])
		}

		return &operators.FunctionOverInstantVector{
			Inner: inner,
			Pool:  pool,

			MetadataFunc:   metadataFunc,
			SeriesDataFunc: seriesDataFunc,
		}, nil
	}
}

// InstantVectorTransformationFunctionOperatorFactory creates an InstantVectorFunctionOperatorFactory for functions
// that have exactly 1 argument (v instant-vector), and drop the series __name__ label.
//
// Parameters:
//   - name: The name of the function.
//   - seriesDataFunc: The function to handle series data
func InstantVectorTransformationFunctionOperatorFactory(name string, seriesDataFunc functions.InstantVectorFunction) InstantVectorFunctionOperatorFactory {
	return SingleInputVectorFunctionOperatorFactory(name, functions.DropSeriesName, seriesDataFunc)
}

// InstantVectorLabelManipulationFunctionOperatorFactory creates an InstantVectorFunctionOperator for functions
// that have exactly 1 argument (v instant-vector), and need to manipulate the labels of
// each series without manipulating the returned samples.
// The values of v are passed through.
//
// Parameters:
//   - name: The name of the function.
//   - metadataFunc: The function for handling metadata
//
// Returns:
//
//	An InstantVectorFunctionOperator.
func InstantVectorLabelManipulationFunctionOperatorFactory(name string, metadataFunc functions.SeriesMetadataFunction) InstantVectorFunctionOperatorFactory {
	return SingleInputVectorFunctionOperatorFactory(name, metadataFunc, functions.Passthrough)
}

// SingleRangeVectorFunctionOperatorFactory creates an InstantVectorFunctionOperatorFactory for functions
// that have exactly 1 argument (v range-vector).
//
// Parameters:
//   - name: The name of the function.
//   - metadataFunc: The function for handling metadata
//   - rangeStepFunc: The function to handle a range vector step
func SingleRangeVectorFunctionOperatorFactory(name string, metadataFunc functions.SeriesMetadataFunction, rangeStepFunc functions.RangeVectorStepFunction) InstantVectorFunctionOperatorFactory {
	return func(args []types.Operator, pool *pooling.LimitingPool) (types.InstantVectorOperator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", name, len(args))
		}

		inner, ok := args[0].(types.RangeVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected a range vector argument for %s, got %T", name, args[0])
		}

		return &operators.FunctionOverRangeVector{
			Inner:               inner,
			Pool:                pool,
			MetadataFunc:        functions.DropSeriesName,
			RangeVectorStepFunc: rangeStepFunc,
		}, nil
	}
}

// SingleRangeVectorTransformationFunctionOperatorFactory creates an InstantVectorFunctionOperatorFactory for functions
// that have exactly 1 argument (v range-vector), and drop the series __name__ label.
//
// Parameters:
//   - name: The name of the function.
//   - rangeStepFunc: The function to handle a range vector step

func SingleRangeVectorTransformationFunctionOperatorFactory(name string, rangeStepFunc functions.RangeVectorStepFunction) InstantVectorFunctionOperatorFactory {
	return SingleRangeVectorFunctionOperatorFactory(name, functions.DropSeriesName, rangeStepFunc)
}

// These functions return an instant-vector.
var instantVectorFunctionOperatorFactories = map[string]InstantVectorFunctionOperatorFactory{
	"abs":             InstantVectorTransformationFunctionOperatorFactory("abs", functions.Abs),
	"acos":            InstantVectorTransformationFunctionOperatorFactory("acos", functions.Acos),
	"acosh":           InstantVectorTransformationFunctionOperatorFactory("acosh", functions.Acosh),
	"asin":            InstantVectorTransformationFunctionOperatorFactory("asin", functions.Asin),
	"asinh":           InstantVectorTransformationFunctionOperatorFactory("asinh", functions.Asinh),
	"atan":            InstantVectorTransformationFunctionOperatorFactory("atan", functions.Atan),
	"atanh":           InstantVectorTransformationFunctionOperatorFactory("atanh", functions.Atanh),
	"ceil":            InstantVectorTransformationFunctionOperatorFactory("ceil", functions.Ceil),
	"cos":             InstantVectorTransformationFunctionOperatorFactory("cos", functions.Cos),
	"cosh":            InstantVectorTransformationFunctionOperatorFactory("cosh", functions.Cosh),
	"deg":             InstantVectorTransformationFunctionOperatorFactory("deg", functions.Deg),
	"exp":             InstantVectorTransformationFunctionOperatorFactory("exp", functions.Exp),
	"floor":           InstantVectorTransformationFunctionOperatorFactory("floor", functions.Floor),
	"histogram_count": InstantVectorTransformationFunctionOperatorFactory("histogram_count", functions.HistogramCount),
	"histogram_sum":   InstantVectorTransformationFunctionOperatorFactory("histogram_sum", functions.HistogramSum),
	"ln":              InstantVectorTransformationFunctionOperatorFactory("ln", functions.Ln),
	"log10":           InstantVectorTransformationFunctionOperatorFactory("log10", functions.Log10),
	"log2":            InstantVectorTransformationFunctionOperatorFactory("log2", functions.Log2),
	"rad":             InstantVectorTransformationFunctionOperatorFactory("rad", functions.Rad),
	"rate":            SingleRangeVectorTransformationFunctionOperatorFactory("rate", functions.Rate),
	"sgn":             InstantVectorTransformationFunctionOperatorFactory("sgn", functions.Sgn),
	"sin":             InstantVectorTransformationFunctionOperatorFactory("sin", functions.Sin),
	"sinh":            InstantVectorTransformationFunctionOperatorFactory("sinh", functions.Sinh),
	"sqrt":            InstantVectorTransformationFunctionOperatorFactory("sqrt", functions.Sqrt),
	"tan":             InstantVectorTransformationFunctionOperatorFactory("tan", functions.Tan),
	"tanh":            InstantVectorTransformationFunctionOperatorFactory("tanh", functions.Tanh),
}

func RegisterInstantVectorFunctionOperatorFactory(functionName string, factory InstantVectorFunctionOperatorFactory) error {
	if _, exists := instantVectorFunctionOperatorFactories[functionName]; exists {
		return fmt.Errorf("function '%s' has already been registered", functionName)
	}

	instantVectorFunctionOperatorFactories[functionName] = factory
	return nil
}
