// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"fmt"
	"math"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type InstantVectorFunctionOperatorFactory func(args []types.Operator, pool *pooling.LimitingPool) (types.InstantVectorOperator, error)

func SimpleFunctionFactory(name string, seriesDataFunc functions.InstantVectorFunction) InstantVectorFunctionOperatorFactory {
	// Simple Function Factory is for functions that have exactly 1 argument and drop the series name.
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

func rateFunction(args []types.Operator, pool *pooling.LimitingPool) (types.InstantVectorOperator, error) {
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

var _ InstantVectorFunctionOperatorFactory = rateFunction

func clampFunction(args []types.Operator, pool *pooling.LimitingPool) (types.InstantVectorOperator, error) {
	if len(args) != 3 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 3 arguments for rate, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector argument for clamp, got %T", args[0])
	}

	min, ok := args[1].(*operators.Scalar)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar argument for clamp, got %T", args[1])
	}

	max, ok := args[2].(*operators.Scalar)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar argument for clamp, got %T", args[2])
	}

	// Special cases: - Return an empty vector if min > max - Return NaN if min or max is NaN
	if max.GetFloat() < min.GetFloat() {
		return &operators.FunctionOverInstantVector{
			Inner: inner,
			Pool:  pool,

			MetadataFunc: functions.DropSeriesName,
			SeriesDataFunc: func(seriesData types.InstantVectorSeriesData, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error) {
				pool.PutInstantVectorSeriesData(seriesData)
				seriesData = types.InstantVectorSeriesData{
					Floats: nil,
				}
				return seriesData, nil
			},
		}, nil
	}

	return &operators.FunctionOverInstantVector{
		Inner: inner,
		Pool:  pool,

		MetadataFunc: functions.DropSeriesName,
		SeriesDataFunc: func(seriesData types.InstantVectorSeriesData, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error) {
			for i := range seriesData.Floats {
				seriesData.Floats[i].F = math.Max(min.GetFloat(), math.Min(max.GetFloat(), seriesData.Floats[i].F))
			}

			return seriesData, nil
		},
	}, nil
}

var _ InstantVectorFunctionOperatorFactory = clampFunction

// These functions return an instant-vector.
var instantVectorFunctions = map[string]InstantVectorFunctionOperatorFactory{
	"acos":            SimpleFunctionFactory("acos", functions.Acos),
	"clamp":           clampFunction,
	"histogram_count": SimpleFunctionFactory("histogram_count", functions.HistogramCount),
	"histogram_sum":   SimpleFunctionFactory("histogram_sum", functions.HistogramSum),
	"rate":            rateFunction,
}
