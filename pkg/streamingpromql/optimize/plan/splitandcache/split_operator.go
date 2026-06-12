// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// TimeRangeSplitOperator evaluates a range query by concatenating the results of multiple shorter-range queries.
type TimeRangeSplitOperator struct {
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	TimeRange                types.QueryTimeRange

	ranges       []*splitRange // Sorted in descending time order.
	outputSeries []splitOrCacheOutputSeries
}

var _ types.InstantVectorOperator = &TimeRangeSplitOperator{}

type splitRange struct {
	operator types.InstantVectorOperator
	buffer   *operators.InstantVectorOperatorBuffer
}

type splitOrCacheOutputSeries struct {
	sourceSeriesIndices []int // One entry per range or extent. -1 indicates that the series is not present in the range/extent.
}

// newTimeRangeSplitOperator creates a new TimeRangeSplitOperator.
//
// ranges must be sorted in descending time order and must not overlap.
// ranges must not be empty.
func newTimeRangeSplitOperator(ranges []*splitRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, timeRange types.QueryTimeRange) *TimeRangeSplitOperator {
	return &TimeRangeSplitOperator{
		MemoryConsumptionTracker: memoryConsumptionTracker,
		TimeRange:                timeRange,
		ranges:                   ranges,
	}
}

func newSplitRange(operator types.InstantVectorOperator) *splitRange {
	return &splitRange{operator: operator}
}

func (s *TimeRangeSplitOperator) Prepare(ctx context.Context, params *types.PrepareParams) error {
	queryStats := stats.FromContext(ctx)
	queryStats.AddSplitQueries(uint32(len(s.ranges)))

	for _, r := range s.ranges {
		if err := r.operator.Prepare(ctx, params); err != nil {
			return err
		}
	}
	return nil
}

func (s *TimeRangeSplitOperator) AfterPrepare(ctx context.Context) error {
	for _, r := range s.ranges {
		if err := r.operator.AfterPrepare(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *TimeRangeSplitOperator) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	// Use the slice from the first range as the base for the returned series metadata.
	allSeries, err := s.ranges[0].operator.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	s.ranges[0].buffer = operators.NewInstantVectorOperatorBuffer(s.ranges[0].operator, nil, len(allSeries)-1, s.MemoryConsumptionTracker)

	seriesIndices := make(map[string]int, len(allSeries))
	labelBytesBuf := make([]byte, 0, 1024)
	for seriesIdx, series := range allSeries {
		labelBytesBuf = series.Labels.Bytes(labelBytesBuf)
		seriesIndices[string(labelBytesBuf)] = seriesIdx // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if err := s.addNewOutputSeries(0, seriesIdx); err != nil {
			return nil, err
		}
	}

	// Now go through the remaining ranges.
	for rangeIdx := 1; rangeIdx < len(s.ranges); rangeIdx++ {
		r := s.ranges[rangeIdx]
		rangeSeries, err := r.operator.SeriesMetadata(ctx, matchers)
		if err != nil {
			return nil, err
		}

		for rangeSeriesIdx, series := range rangeSeries {
			labelBytesBuf = series.Labels.Bytes(labelBytesBuf)

			// Important: don't extract the string(...) call in the map lookup below - passing it directly allows us to avoid allocating it.
			if outputSeriesIdx, seenAlready := seriesIndices[string(labelBytesBuf)]; seenAlready {
				if series.DropName != allSeries[outputSeriesIdx].DropName {
					return nil, fmt.Errorf("series with labels %s has conflicting drop name values in different ranges", series.Labels.String())
				}

				s.outputSeries[outputSeriesIdx].sourceSeriesIndices[rangeIdx] = rangeSeriesIdx

				// We're not going to keep this labels instance (we already have it from a previous range), so
				// decrease memory consumption now.
				s.MemoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(series.Labels)
			} else {
				seriesIndices[string(labelBytesBuf)] = len(allSeries)
				allSeries, err = types.SeriesMetadataSlicePool.AppendToSlice(allSeries, s.MemoryConsumptionTracker, series)
				if err != nil {
					return nil, err
				}

				if err := s.addNewOutputSeries(rangeIdx, rangeSeriesIdx); err != nil {
					return nil, err
				}
			}

			// We've already accounted for the memory consumption of this series' labels with the DecreaseMemoryConsumptionForLabels
			// or AppendToSlice calls above, so clear the labels now so they're not double-decremented when we return the series
			// slice to the pool below.
			rangeSeries[rangeSeriesIdx] = types.SeriesMetadata{}
		}

		r.buffer = operators.NewInstantVectorOperatorBuffer(r.operator, nil, len(rangeSeries)-1, s.MemoryConsumptionTracker)
		types.SeriesMetadataSlicePool.Put(&rangeSeries, s.MemoryConsumptionTracker)
	}

	return allSeries, nil
}

func (s *TimeRangeSplitOperator) addNewOutputSeries(sourceRangeIndex int, sourceRangeSeriesIndex int) error {
	sourceSeriesIndices, err := types.IntSlicePool.Get(len(s.ranges), s.MemoryConsumptionTracker)
	if err != nil {
		return err
	}

	sourceSeriesIndices = sourceSeriesIndices[:len(s.ranges)]

	for idx := range s.ranges {
		if idx == sourceRangeIndex {
			sourceSeriesIndices[idx] = sourceRangeSeriesIndex
		} else {
			sourceSeriesIndices[idx] = -1
		}
	}

	s.outputSeries = append(s.outputSeries, splitOrCacheOutputSeries{sourceSeriesIndices: sourceSeriesIndices})
	return nil
}

func (s *TimeRangeSplitOperator) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(s.outputSeries) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	thisOutputSeries := s.outputSeries[0]
	s.outputSeries = s.outputSeries[1:]

	defer types.IntSlicePool.Put(&thisOutputSeries.sourceSeriesIndices, s.MemoryConsumptionTracker)

	var floats []promql.FPoint
	var histograms []promql.HPoint

	for rangeIdx, sourceSeriesIdx := range thisOutputSeries.sourceSeriesIndices {
		if sourceSeriesIdx == -1 {
			continue
		}

		r := s.ranges[rangeIdx]
		data, err := r.buffer.GetSeries(ctx, []int{sourceSeriesIdx})
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		if floats == nil {
			floats = data[0].Floats
		} else {
			floats, err = types.FPointSlicePool.AppendToSlice(floats, s.MemoryConsumptionTracker, data[0].Floats...)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			types.FPointSlicePool.Put(&data[0].Floats, s.MemoryConsumptionTracker)
		}

		if histograms == nil {
			histograms = data[0].Histograms
		} else {
			histograms, err = types.HPointSlicePool.AppendToSlice(histograms, s.MemoryConsumptionTracker, data[0].Histograms...)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			types.HPointSlicePool.Put(&data[0].Histograms, s.MemoryConsumptionTracker)
		}
	}

	return types.InstantVectorSeriesData{
		Floats:     floats,
		Histograms: histograms,
	}, nil
}

func (s *TimeRangeSplitOperator) FinishedReading(ctx context.Context) error {
	for _, r := range s.ranges {
		if err := r.operator.FinishedReading(ctx); err != nil {
			return err
		}

		r.buffer.FinishedReading()
	}

	return nil
}

func (s *TimeRangeSplitOperator) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	combinedStats, err := types.NewOperatorEvaluationStats(ctx, s.TimeRange, s.MemoryConsumptionTracker, 0)
	if err != nil {
		return nil, nil, err
	}

	var combinedAnnotations annotations.Annotations

	for _, r := range s.ranges {
		rangeStats, rangeAnnotations, err := r.operator.Finalize(ctx)
		if err != nil {
			return nil, nil, err
		}

		if err := combinedStats.AddSubRange(rangeStats); err != nil {
			return nil, nil, err
		}

		rangeStats.Close()
		combinedAnnotations.Merge(rangeAnnotations)
	}

	return combinedStats, combinedAnnotations, nil
}

func (s *TimeRangeSplitOperator) Close() {
	for _, r := range s.ranges {
		r.operator.Close()
	}
}

func (s *TimeRangeSplitOperator) ExpressionPosition() posrange.PositionRange {
	// All ranges should be derived from the same expression, so use the first one.
	// It is not valid to have a split operator with no ranges, so we don't need to check for the case where there are no ranges.
	return s.ranges[0].operator.ExpressionPosition()
}

var _ types.InstantVectorOperator = (*TimeRangeSplitOperator)(nil)
