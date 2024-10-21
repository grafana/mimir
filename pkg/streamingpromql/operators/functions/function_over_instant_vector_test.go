// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Most of the functionality of functions is tested through the test scripts in
// pkg/streamingpromql/testdata.

func TestFunctionOverInstantVector(t *testing.T) {
	inner := &operators.TestOperator{
		Series: []labels.Labels{
			labels.FromStrings("series", "0"),
			labels.FromStrings("series", "1"),
		},
		Data: []types.InstantVectorSeriesData{
			{Floats: []promql.FPoint{{T: 0, F: 1}}},
			{Floats: []promql.FPoint{{T: 0, F: 2}}},
		},
	}

	metadataFuncCalled := false
	mustBeCalledMetadata := func(seriesMetadata []types.SeriesMetadata, _ *limiting.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
		require.Equal(t, len(inner.Series), len(seriesMetadata))
		metadataFuncCalled = true
		return nil, nil
	}

	seriesDataFuncCalledTimes := 0
	mustBeCalledSeriesData := func(types.InstantVectorSeriesData, []types.ScalarData, *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
		seriesDataFuncCalledTimes++
		return types.InstantVectorSeriesData{}, nil
	}

	operator := &FunctionOverInstantVector{
		Inner:                    inner,
		MemoryConsumptionTracker: limiting.NewMemoryConsumptionTracker(0, nil),
		Func: FunctionOverInstantVectorDefinition{
			SeriesDataFunc: mustBeCalledSeriesData,
			SeriesMetadataFunction: SeriesMetadataFunctionDefinition{
				Func: mustBeCalledMetadata,
			},
		},
	}

	ctx := context.TODO()
	_, err := operator.SeriesMetadata(ctx)
	require.NoError(t, err)
	_, err = operator.NextSeries(ctx)
	require.NoError(t, err)
	require.True(t, metadataFuncCalled, "Supplied MetadataFunc must be called matching the signature")
	require.Equal(t, len(inner.Data), seriesDataFuncCalledTimes, "Supplied SeriesDataFunc was called once for each Series")
}

func TestFunctionOverInstantVectorWithScalarArgs(t *testing.T) {
	inner := &operators.TestOperator{
		Series: []labels.Labels{
			labels.FromStrings("series", "0"),
			labels.FromStrings("series", "1"),
		},
		Data: []types.InstantVectorSeriesData{
			{Floats: []promql.FPoint{{T: 0, F: 1}}},
			{Floats: []promql.FPoint{{T: 0, F: 2}}},
		},
	}

	scalarOperator1 := &testScalarOperator{
		value: types.ScalarData{Samples: []promql.FPoint{{T: 0, F: 3}}},
	}

	scalarOperator2 := &testScalarOperator{
		value: types.ScalarData{Samples: []promql.FPoint{{T: 60, F: 4}}},
	}

	seriesDataFuncCalledTimes := 0
	mustBeCalledSeriesData := func(_ types.InstantVectorSeriesData, scalarArgs []types.ScalarData, _ *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
		seriesDataFuncCalledTimes++
		// Verify that the scalar arguments are correctly passed and in the order we expect
		require.Equal(t, 2, len(scalarArgs))
		require.Equal(t, types.ScalarData{Samples: []promql.FPoint{{T: 0, F: 3}}}, scalarArgs[0])
		require.Equal(t, types.ScalarData{Samples: []promql.FPoint{{T: 60, F: 4}}}, scalarArgs[1])
		return types.InstantVectorSeriesData{}, nil
	}

	operator := &FunctionOverInstantVector{
		Inner:                    inner,
		ScalarArgs:               []types.ScalarOperator{scalarOperator1, scalarOperator2},
		MemoryConsumptionTracker: limiting.NewMemoryConsumptionTracker(0, nil),
		Func: FunctionOverInstantVectorDefinition{
			SeriesDataFunc:         mustBeCalledSeriesData,
			SeriesMetadataFunction: DropSeriesName,
		},
	}

	ctx := context.TODO()
	// SeriesMetadata should process scalar args
	_, err := operator.SeriesMetadata(ctx)
	require.NoError(t, err)

	// NextSeries should pass scalarArgsData to SeriesDataFunc, which validates the arguments
	_, err = operator.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, len(inner.Data), seriesDataFuncCalledTimes, "Supplied SeriesDataFunc was called once for each Series")
}

type testScalarOperator struct {
	value types.ScalarData
}

func (t *testScalarOperator) GetValues(_ context.Context) (types.ScalarData, error) {
	return t.value, nil
}

func (t *testScalarOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (t *testScalarOperator) Close() {}
