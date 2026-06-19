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
	operator                 types.InstantVectorOperator
	buffer                   *operators.InstantVectorOperatorBuffer
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func (r *splitRange) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	series, err := r.operator.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	r.buffer = operators.NewInstantVectorOperatorBuffer(r.operator, nil, len(series)-1, r.memoryConsumptionTracker)

	return series, nil
}

type splitOrCacheOutputSeries struct {
	sourceSeriesIndices []int // One entry per range or extent. -1 indicates that the series is not present in the range/extent.
}

// newTimeRangeSplitOperator creates a new TimeRangeSplitOperator.
//
// ranges must be sorted in descending time order and must not overlap.
// ranges must contain at least two ranges: when there is only a single range, callers should use the inner operator
// directly rather than wrapping it in a TimeRangeSplitOperator.
func newTimeRangeSplitOperator(ranges []*splitRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, timeRange types.QueryTimeRange) *TimeRangeSplitOperator {
	return &TimeRangeSplitOperator{
		MemoryConsumptionTracker: memoryConsumptionTracker,
		TimeRange:                timeRange,
		ranges:                   ranges,
	}
}

func newSplitRange(operator types.InstantVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *splitRange {
	return &splitRange{operator: operator, memoryConsumptionTracker: memoryConsumptionTracker}
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
	if len(s.ranges) < 2 {
		// mergeSeriesMetadata takes a fast path for a single source that leaves outputSeries unpopulated, but
		// TimeRangeSplitOperator's NextSeries relies on outputSeries being populated. In production this is never
		// hit, as MaterializeSplit returns the inner operator directly rather than constructing a
		// TimeRangeSplitOperator when there is only one range.
		return nil, fmt.Errorf("TimeRangeSplitOperator requires at least two ranges, but has %d", len(s.ranges))
	}

	series, outputSeries, err := mergeSeriesMetadata(ctx, s.ranges, matchers, s.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	s.outputSeries = outputSeries

	return series, nil
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
	var combinedStats *types.OperatorEvaluationStats // We can't create this immediately as we don't know the subset count yet.
	var combinedAnnotations annotations.Annotations

	for _, r := range s.ranges {
		rangeStats, rangeAnnotations, err := r.operator.Finalize(ctx)
		if err != nil {
			return nil, nil, err
		}

		if combinedStats == nil {
			combinedStats, err = types.NewOperatorEvaluationStats(ctx, s.TimeRange, s.MemoryConsumptionTracker, rangeStats.GetSubsetCount())
			if err != nil {
				return nil, nil, err
			}
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
