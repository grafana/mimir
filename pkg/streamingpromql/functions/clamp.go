// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"math"

	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func ClampFactory(min float64, max float64) InstantVectorFunction {
	// Special cases: - Return an empty vector if min > max - Return NaN if min or max is NaN
	if max < min {
		return func(seriesData types.InstantVectorSeriesData, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error) {
			pool.PutInstantVectorSeriesData(seriesData)
			seriesData = types.InstantVectorSeriesData{
				Floats: nil,
			}
			return seriesData, nil
		}
	}

	return func(seriesData types.InstantVectorSeriesData, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error) {
		for i := range seriesData.Floats {
			seriesData.Floats[i].F = math.Max(min, math.Min(max, seriesData.Floats[i].F))
		}

		return seriesData, nil
	}
}
