// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"fmt"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type instantVectorFunctionOperator func(args []types.Operator, pool *pooling.LimitingPool) (types.InstantVectorOperator, error)

// transformationFunctionOperatorFactory creates an instantVectorFunctionOperator for functions
// that have exactly 1 argument and drop the series name.
//
// Parameters:
//   - name: The name of the function.
//   - seriesDataFunc: The function to be wrapped.
//
// Returns:
//
//	An instantVectorFunctionOperator.
func transformationFunctionOperatorFactory(name string, seriesDataFunc functions.InstantVectorFunction) instantVectorFunctionOperator {
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

			MetadataFunc:   functions.DropSeriesName,
			SeriesDataFunc: seriesDataFunc,
		}, nil
	}
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

var _ instantVectorFunctionOperator = rateFunctionOperator

// These functions return an instant-vector.
var instantVectorFunctions = map[string]instantVectorFunctionOperator{
	"acos":            transformationFunctionOperatorFactory("acos", functions.Acos),
	"histogram_count": transformationFunctionOperatorFactory("histogram_count", functions.HistogramCount),
	"histogram_sum":   transformationFunctionOperatorFactory("histogram_sum", functions.HistogramSum),
	"rate":            rateFunctionOperator,
}
