// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

var _ types.InstantVectorOperator = &Sort{}

type Sort struct {
	sortBase
}

func NewSort(
	inner types.InstantVectorOperator,
	descending bool,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *Sort {
	return &Sort{newSortBase(inner, descending, memoryConsumptionTracker, expressionPosition)}
}

func (s *Sort) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	allSeries, err := s.inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	s.allData = make([]types.InstantVectorSeriesData, len(allSeries))

	for idx := range allSeries {
		d, err := s.inner.NextSeries(ctx)
		if err != nil {
			return nil, err
		}

		// sort() and sort_desc() ignore histograms.
		types.HPointSlicePool.Put(&d.Histograms, s.memoryConsumptionTracker)
		pointCount := len(d.Floats)

		if pointCount > 1 {
			return nil, fmt.Errorf("expected series %v to have at most one point, but it had %v", allSeries[idx], pointCount)
		}

		s.allData[idx] = d
	}

	var lessFn lessFunc
	if s.descending {
		lessFn = sortLessDescending
	} else {
		lessFn = sortLessAscending
	}

	sort.Sort(&sortSeriesAndData{
		data:   s.allData,
		series: allSeries,
		less:   lessFn,
	})

	return allSeries, nil
}

func sortLessAscending(_ []types.SeriesMetadata, data []types.InstantVectorSeriesData, i, j int) bool {
	v1 := getValueForSorting(data, i)
	v2 := getValueForSorting(data, j)

	// NaNs always sort to the end of the list, regardless of the sort order.
	if math.IsNaN(v1) {
		return false
	} else if math.IsNaN(v2) {
		return true
	}

	return v1 < v2
}

func sortLessDescending(_ []types.SeriesMetadata, data []types.InstantVectorSeriesData, i, j int) bool {
	v1 := getValueForSorting(data, i)
	v2 := getValueForSorting(data, j)

	// NaNs always sort to the end of the list, regardless of the sort order.
	if math.IsNaN(v1) {
		return false
	} else if math.IsNaN(v2) {
		return true
	}

	return v1 > v2
}

func getValueForSorting(allData []types.InstantVectorSeriesData, seriesIdx int) float64 {
	series := allData[seriesIdx]

	if len(series.Floats) == 1 {
		return series.Floats[0].F
	}

	return 0 // The value we use for empty series doesn't matter, as long as we're consistent: we'll still return an empty set of data in NextSeries().
}
