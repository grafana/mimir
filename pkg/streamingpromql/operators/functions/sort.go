// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type Sort struct {
	Inner                    types.InstantVectorOperator
	Descending               bool
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange

	allData           []types.InstantVectorSeriesData // Series data, in the order returned by Inner
	seriesOutputOrder []int                           // Series indices into allData, in the order they should be returned
	seriesReturned    int                             // Number of series already returned by NextSeries
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
	innerSeries, err := s.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	defer types.PutSeriesMetadataSlice(innerSeries)

	s.seriesOutputOrder, err = types.IntSlicePool.Get(len(innerSeries), s.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	s.allData = make([]types.InstantVectorSeriesData, len(innerSeries))

	for idx := range innerSeries {
		d, err := s.Inner.NextSeries(ctx)
		if err != nil {
			return nil, err
		}

		pointCount := len(d.Floats) + len(d.Histograms)

		if pointCount > 1 {
			return nil, fmt.Errorf("expected series %v to have at most one point, but it had %v", innerSeries[idx], pointCount)
		}

		if pointCount == 1 {
			s.allData[idx] = d
			s.seriesOutputOrder = append(s.seriesOutputOrder, idx)
		} else {
			types.PutInstantVectorSeriesData(d, s.MemoryConsumptionTracker)
		}
	}

	if s.Descending {
		slices.SortFunc(s.seriesOutputOrder, s.compareDescending)
	} else {
		slices.SortFunc(s.seriesOutputOrder, s.compareAscending)
	}

	// Why can't we just reorder innerSeries? This would be very difficult to do, as there's no
	// guarantee that all the input series will be returned.
	outputSeries := types.GetSeriesMetadataSlice(len(s.seriesOutputOrder))

	for _, seriesIdx := range s.seriesOutputOrder {
		outputSeries = append(outputSeries, innerSeries[seriesIdx])
	}

	return outputSeries, nil
}

func (s *Sort) compareAscending(s1 int, s2 int) int {
	v1 := s.getValueForSorting(s1)
	v2 := s.getValueForSorting(s2)

	// NaNs always sort to the end of the list, regardless of the sort order.
	if math.IsNaN(v1) {
		return 1
	} else if math.IsNaN(v2) {
		return -1
	}

	if v1 < v2 {
		return -1
	} else if v1 > v2 {
		return 1
	}

	return 0
}

func (s *Sort) compareDescending(s1 int, s2 int) int {
	v1 := s.getValueForSorting(s1)
	v2 := s.getValueForSorting(s2)

	// NaNs always sort to the end of the list, regardless of the sort order.
	if math.IsNaN(v1) {
		return 1
	} else if math.IsNaN(v2) {
		return -1
	}

	if v1 < v2 {
		return 1
	} else if v1 > v2 {
		return -1
	}

	return 0
}

func (s *Sort) getValueForSorting(idx int) float64 {
	series := s.allData[idx]

	if len(series.Floats) == 1 {
		return series.Floats[0].F
	}

	return series.Histograms[0].H.Sum
}

func (s *Sort) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	if s.seriesReturned >= len(s.seriesOutputOrder) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	data := s.allData[s.seriesOutputOrder[s.seriesReturned]]
	s.seriesReturned++

	return data, nil
}

func (s *Sort) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *Sort) Close() {
	s.Inner.Close()

	types.IntSlicePool.Put(s.seriesOutputOrder, s.MemoryConsumptionTracker)

	// We don't need to do anything with s.allData here: we passed ownership of the data to the calling operator when we returned it in NextSeries.
}
