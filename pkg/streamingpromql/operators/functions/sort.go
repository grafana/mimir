// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type Sort struct {
	Inner                    types.InstantVectorOperator
	Descending               bool
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange

	allData        []types.InstantVectorSeriesData // Series data, in the order to be returned
	seriesReturned int                             // Number of series already returned by NextSeries
}

var _ types.InstantVectorOperator = &Sort{}

func NewSort(
	inner types.InstantVectorOperator,
	descending bool,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *Sort {
	return &Sort{
		Inner:                    inner,
		Descending:               descending,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (s *Sort) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	allSeries, err := s.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	s.allData = make([]types.InstantVectorSeriesData, len(allSeries))

	for idx := range allSeries {
		d, err := s.Inner.NextSeries(ctx)
		if err != nil {
			return nil, err
		}

		pointCount := len(d.Floats) + len(d.Histograms)

		if pointCount > 1 {
			return nil, fmt.Errorf("expected series %v to have at most one point, but it had %v", allSeries[idx], pointCount)
		}

		s.allData[idx] = d
	}

	if s.Descending {
		sort.Sort(&sortDescending{data: s.allData, series: allSeries})
	} else {
		sort.Sort(&sortAscending{data: s.allData, series: allSeries})
	}

	return allSeries, nil
}

type sortAscending struct {
	data   []types.InstantVectorSeriesData
	series []types.SeriesMetadata
}

func (s *sortAscending) Len() int {
	return len(s.data)
}

func (s *sortAscending) Less(idx1, idx2 int) bool {
	v1 := getValueForSorting(s.data, idx1)
	v2 := getValueForSorting(s.data, idx2)

	// NaNs always sort to the end of the list, regardless of the sort order.
	if math.IsNaN(v1) {
		return false
	} else if math.IsNaN(v2) {
		return true
	}

	return v1 < v2
}

func (s *sortAscending) Swap(i, j int) {
	s.data[i], s.data[j] = s.data[j], s.data[i]
	s.series[i], s.series[j] = s.series[j], s.series[i]
}

type sortDescending struct {
	data   []types.InstantVectorSeriesData
	series []types.SeriesMetadata
}

func (s *sortDescending) Len() int {
	return len(s.data)
}

func (s *sortDescending) Less(idx1, idx2 int) bool {
	v1 := getValueForSorting(s.data, idx1)
	v2 := getValueForSorting(s.data, idx2)

	// NaNs always sort to the end of the list, regardless of the sort order.
	if math.IsNaN(v1) {
		return false
	} else if math.IsNaN(v2) {
		return true
	}

	return v1 > v2
}

func (s *sortDescending) Swap(i, j int) {
	s.data[i], s.data[j] = s.data[j], s.data[i]
	s.series[i], s.series[j] = s.series[j], s.series[i]
}

func getValueForSorting(allData []types.InstantVectorSeriesData, seriesIdx int) float64 {
	series := allData[seriesIdx]

	if len(series.Floats) == 1 {
		return series.Floats[0].F
	}

	if len(series.Histograms) == 1 {
		return series.Histograms[0].H.Sum
	}

	return 0 // The value we use for empty series doesn't matter, as long as we're consistent: we'll still return an empty set of data in NextSeries().
}

func (s *Sort) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	if s.seriesReturned >= len(s.allData) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	data := s.allData[s.seriesReturned]
	s.seriesReturned++

	return data, nil
}

func (s *Sort) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *Sort) Close() {
	s.Inner.Close()

	// We don't need to do anything with s.allData here: we passed ownership of the data to the calling operator when we returned it in NextSeries.
}
