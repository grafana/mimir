// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Most of the functionality of functions is tested through the test scripts in
// pkg/streamingpromql/testdata.

func TestFunctionOverInstantVector(t *testing.T) {
	inner := &testOperator{
		series: []labels.Labels{
			labels.FromStrings("series", "0"),
			labels.FromStrings("series", "1"),
		},
		data: []types.InstantVectorSeriesData{
			{Floats: []promql.FPoint{{T: 0, F: 1}}},
			{Floats: []promql.FPoint{{T: 0, F: 2}}},
		},
	}

	metadataFuncCalled := false
	mustBeCalledMetadata := func(seriesMetadata []types.SeriesMetadata, _ *limiting.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
		require.Equal(t, len(inner.series), len(seriesMetadata))
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
		Func: functions.FunctionOverInstantVector{
			SeriesDataFunc: mustBeCalledSeriesData,
			SeriesMetadataFunction: functions.SeriesMetadataFunctionDefinition{
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
	require.Equal(t, len(inner.data), seriesDataFuncCalledTimes, "Supplied SeriesDataFunc was called once for each Series")
}
