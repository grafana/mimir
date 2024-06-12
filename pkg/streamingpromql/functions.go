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

// SingleInputVectorFunctionOperator creates an InstantVectorFunctionOperatorFactory for functions
// that have exactly 1 argument (v instant-vector).
//
// Parameters:
//   - name: The name of the function.
//   - metadataFunc: The function for handling metadata
//   - seriesDataFunc: The function to handle series data
func SingleInputVectorFunctionOperator(name string, metadataFunc functions.SeriesMetadataFunction, seriesDataFunc functions.InstantVectorFunction) InstantVectorFunctionOperatorFactory {
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

// TransformationFunctionOperator creates an InstantVectorFunctionOperatorFactory for functions
// that have exactly 1 argument (v instant-vector), and drop the series __name__ label.
//
// Parameters:
//   - name: The name of the function.
//   - seriesDataFunc: The function to handle series data
func TransformationFunctionOperator(name string, seriesDataFunc functions.InstantVectorFunction) InstantVectorFunctionOperatorFactory {
	return SingleInputVectorFunctionOperator(name, functions.DropSeriesName, seriesDataFunc)
}

// LabelManipulationFunctionOperator creates an InstantVectorFunctionOperator for functions
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
func LabelManipulationFunctionOperator(name string, metadataFunc functions.SeriesMetadataFunction) InstantVectorFunctionOperatorFactory {
	return SingleInputVectorFunctionOperator(name, metadataFunc, functions.Passthrough)
}

func rateFunctionOperator(args []types.Operator, pool *pooling.LimitingPool) (types.InstantVectorOperator, error) {
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
	"acos":            TransformationFunctionOperator("acos", functions.Acos),
	"histogram_count": TransformationFunctionOperator("histogram_count", functions.HistogramCount),
	"histogram_sum":   TransformationFunctionOperator("histogram_sum", functions.HistogramSum),
	"rate":            rateFunctionOperator,
}

func RegisterInstantVectorFunctionOperator(functionName string, functionOperator InstantVectorFunctionOperatorFactory) error {
	if _, exists := instantVectorFunctionOperatorFactories[functionName]; exists {
		return fmt.Errorf("function '%s' has already been registered", functionName)
	}

	instantVectorFunctionOperatorFactories[functionName] = functionOperator
	return nil
}
