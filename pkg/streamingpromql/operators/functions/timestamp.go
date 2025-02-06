// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

var Timestamp = FunctionOverInstantVectorDefinition{
	SeriesMetadataFunction: DropSeriesName,
	SeriesDataFunc:         timestamp,
}

func timestamp(data types.InstantVectorSeriesData, _ []types.ScalarData, _ types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	output := types.InstantVectorSeriesData{}

	defer types.HPointSlicePool.Put(data.Histograms, memoryConsumptionTracker)

	if len(data.Histograms) > 0 {
		defer types.FPointSlicePool.Put(data.Floats, memoryConsumptionTracker)

		var err error
		output.Floats, err = types.FPointSlicePool.Get(len(data.Floats)+len(data.Histograms), memoryConsumptionTracker)

		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	} else {
		// Only have floats, so it's safe to reuse the input float slice.
		output.Floats = data.Floats[:0]
	}

	it := types.InstantVectorSeriesDataIterator{}
	it.Reset(data)

	t, _, _, _, ok := it.Next()

	for ok {
		output.Floats = append(output.Floats, promql.FPoint{T: t, F: float64(t) / 1000})
		t, _, _, _, ok = it.Next()
	}

	return output, nil
}
