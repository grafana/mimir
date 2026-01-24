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
	inner                    types.InstantVectorOperator
	descending               bool
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	expressionPosition       posrange.PositionRange

	allData        []types.InstantVectorSeriesData // Series data, in the order to be returned
	seriesReturned int                             // Number of series already returned by NextSeries
}

func NewSort(
	inner types.InstantVectorOperator,
	descending bool,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *Sort {
	return &Sort{
		inner:                    inner,
		descending:               descending,
		memoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (s *Sort) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	allSeries, err := s.inner.SeriesMetadata(ctx, matchers)
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

type lessFunc func(series []types.SeriesMetadata, data []types.InstantVectorSeriesData, i, j int) bool

// sortSeriesAndData sorts series metadata and values together based on a supplied
// lessFunc which is used as part of the sort.Interface contract.
type sortSeriesAndData struct {
	series []types.SeriesMetadata
	data   []types.InstantVectorSeriesData
	less   lessFunc
}

func (s sortSeriesAndData) Len() int {
	return len(s.data)
}

func (s sortSeriesAndData) Less(i, j int) bool {
	return s.less(s.series, s.data, i, j)
}

func (s sortSeriesAndData) Swap(i, j int) {
	s.data[i], s.data[j] = s.data[j], s.data[i]
	s.series[i], s.series[j] = s.series[j], s.series[i]
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
	return s.inner.Prepare(ctx, params)
}

func (s *Sort) AfterPrepare(ctx context.Context) error {
	return s.inner.AfterPrepare(ctx)
}

func (s *Sort) Finalize(ctx context.Context) error {
	return s.inner.Finalize(ctx)
}

func (s *Sort) Close() {
	s.inner.Close()

	// Return any remaining data to the pool.
	// Any data in allData that was previously passed to the calling operator by NextSeries does not need to be returned to the pool,
	// as the calling operator is responsible for returning it to the pool.
	for s.seriesReturned < len(s.allData) {
		types.PutInstantVectorSeriesData(s.allData[s.seriesReturned], s.memoryConsumptionTracker)
		s.seriesReturned++
	}

	s.allData = nil
}
