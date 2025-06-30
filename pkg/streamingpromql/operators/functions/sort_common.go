package functions

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

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

// sortBase is the common base for sort/sort_desc and sort_by_label/sort_by_label_desc.
// Extensions that use it are expected to implement their sorting logic in SeriesMetadata
// from types.InstantVectorOperator which should set allData.
type sortBase struct {
	inner                    types.InstantVectorOperator
	descending               bool
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	expressionPosition       posrange.PositionRange

	allData        []types.InstantVectorSeriesData // Series data, in the order to be returned
	seriesReturned int                             // Number of series already returned by NextSeries
}

func newSortBase(
	inner types.InstantVectorOperator,
	descending bool,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) sortBase {
	return sortBase{
		inner:                    inner,
		descending:               descending,
		memoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (s *sortBase) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	if s.seriesReturned >= len(s.allData) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	data := s.allData[s.seriesReturned]
	s.allData[s.seriesReturned] = types.InstantVectorSeriesData{} // Clear our reference to the data, so it can be garbage collected.
	s.seriesReturned++

	return data, nil
}

func (s *sortBase) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *sortBase) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return s.inner.Prepare(ctx, params)
}

func (s *sortBase) Close() {
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
