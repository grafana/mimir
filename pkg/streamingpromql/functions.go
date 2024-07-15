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

// TransformationFunctionOperatorFactory creates an InstantVectorFunctionOperatorFactory for functions
// that have exactly 1 argument (v instant-vector), and drop the series __name__ label.
//
// Parameters:
//   - name: The name of the function.
//   - seriesDataFunc: The function to handle series data
func TransformationFunctionOperatorFactory(name string, seriesDataFunc functions.InstantVectorFunction) InstantVectorFunctionOperatorFactory {
	return SingleInputVectorFunctionOperatorFactory(name, functions.DropSeriesName, seriesDataFunc)
}

// LabelManipulationFunctionOperatorFactory creates an InstantVectorFunctionOperator for functions
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
func LabelManipulationFunctionOperatorFactory(name string, metadataFunc functions.SeriesMetadataFunction) InstantVectorFunctionOperatorFactory {
	return SingleInputVectorFunctionOperatorFactory(name, metadataFunc, functions.Passthrough)
}

func createRateFunctionOperator(args []types.Operator, pool *pooling.LimitingPool) (types.InstantVectorOperator, error) {
	if len(args) != 1 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 1 argument for rate, got %v", len(args))
	}

	inner, ok := args[0].(types.RangeVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a range vector argument for rate, got %T", args[0])
	}

	return &operators.FunctionOverRangeVector{
		Inner: inner,
		Pool:  pool,
	}, nil
}

// These functions return an instant-vector.
var instantVectorFunctionOperatorFactories = map[string]InstantVectorFunctionOperatorFactory{
	"abs":             TransformationFunctionOperatorFactory("abs", functions.Abs),
	"acos":            TransformationFunctionOperatorFactory("acos", functions.Acos),
	"acosh":           TransformationFunctionOperatorFactory("acosh", functions.Acosh),
	"asin":            TransformationFunctionOperatorFactory("asin", functions.Asin),
	"asinh":           TransformationFunctionOperatorFactory("asinh", functions.Asinh),
	"atan":            TransformationFunctionOperatorFactory("atan", functions.Atan),
	"atanh":           TransformationFunctionOperatorFactory("atanh", functions.Atanh),
	"ceil":            TransformationFunctionOperatorFactory("ceil", functions.Ceil),
	"cos":             TransformationFunctionOperatorFactory("cos", functions.Cos),
	"cosh":            TransformationFunctionOperatorFactory("cosh", functions.Cosh),
	"deg":             TransformationFunctionOperatorFactory("deg", functions.Deg),
	"exp":             TransformationFunctionOperatorFactory("exp", functions.Exp),
	"floor":           TransformationFunctionOperatorFactory("floor", functions.Floor),
	"histogram_count": TransformationFunctionOperatorFactory("histogram_count", functions.HistogramCount),
	"histogram_sum":   TransformationFunctionOperatorFactory("histogram_sum", functions.HistogramSum),
	"ln":              TransformationFunctionOperatorFactory("ln", functions.Ln),
	"log10":           TransformationFunctionOperatorFactory("log10", functions.Log10),
	"log2":            TransformationFunctionOperatorFactory("log2", functions.Log2),
	"rad":             TransformationFunctionOperatorFactory("rad", functions.Rad),
	"rate":            createRateFunctionOperator,
	"sgn":             TransformationFunctionOperatorFactory("sgn", functions.Sgn),
	"sin":             TransformationFunctionOperatorFactory("sin", functions.Sin),
	"sinh":            TransformationFunctionOperatorFactory("sinh", functions.Sinh),
	"sqrt":            TransformationFunctionOperatorFactory("sqrt", functions.Sqrt),
	"tan":             TransformationFunctionOperatorFactory("tan", functions.Tan),
	"tanh":            TransformationFunctionOperatorFactory("tanh", functions.Tanh),
}

func RegisterInstantVectorFunctionOperatorFactory(functionName string, factory InstantVectorFunctionOperatorFactory) error {
	if _, exists := instantVectorFunctionOperatorFactories[functionName]; exists {
		return fmt.Errorf("function '%s' has already been registered", functionName)
	}

	instantVectorFunctionOperatorFactories[functionName] = factory
	return nil
}
