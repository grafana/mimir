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

type Sort struct {
	Inner                    types.InstantVectorOperator
	Descending               bool
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange

	allData        []types.InstantVectorSeriesData // Series data, in the order to be returned
	seriesReturned int                             // Number of series already returned by NextSeries
}

var _ types.InstantVectorOperator = &Sort{}

func NewSort(
	inner types.InstantVectorOperator,
	descending bool,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
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

		// sort() and sort_desc() ignore histograms.
		types.HPointSlicePool.Put(d.Histograms, s.MemoryConsumptionTracker)
		d.Histograms = nil

		pointCount := len(d.Floats)

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

	return 0 // The value we use for empty series doesn't matter, as long as we're consistent: we'll still return an empty set of data in NextSeries().
}

func (s *Sort) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	if s.seriesReturned >= len(s.allData) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	data := s.allData[s.seriesReturned]
	s.allData[s.seriesReturned] = types.InstantVectorSeriesData{} // Clear our reference to the data, so it can be garbage collected.
	s.seriesReturned++

	return data, nil
}

func (s *Sort) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *Sort) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return s.Inner.Prepare(ctx, params)
}

func (s *Sort) Close() {
	s.Inner.Close()

	// Return any remaining data to the pool.
	// Any data in allData that was previously passed to the calling operator by NextSeries does not need to be returned to the pool,
	// as the calling operator is responsible for returning it to the pool.
	for s.seriesReturned < len(s.allData) {
		types.PutInstantVectorSeriesData(s.allData[s.seriesReturned], s.MemoryConsumptionTracker)
		s.seriesReturned++
	}

	s.allData = nil
}
