// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestOperatorEvaluationStats_TrackSampleForInstantVectorSelector(t *testing.T) {
	start := timestamp.Time(0)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	stats, err := NewOperatorEvaluationStats(timeRange, memoryConsumptionTracker)
	require.NoError(t, err)

	samplesProcessedPerStep := newPerStepTracker("samples processed", timeRange.StepCount)
	newSamplesReadPerStep := newPerStepTracker("new samples read", timeRange.StepCount)

	stats.TrackSampleForInstantVectorSelector(timestamp.FromTime(start), 1)
	samplesProcessedPerStep.requireChange(t, stats.samplesProcessedPerStep, 1, 0, 0)
	newSamplesReadPerStep.requireChange(t, stats.newSamplesReadPerStep, 1, 0, 0)

	stats.TrackSampleForInstantVectorSelector(timestamp.FromTime(start), 2)
	samplesProcessedPerStep.requireChange(t, stats.samplesProcessedPerStep, 2, 0, 0)
	newSamplesReadPerStep.requireChange(t, stats.newSamplesReadPerStep, 2, 0, 0)

	stats.TrackSampleForInstantVectorSelector(timestamp.FromTime(start.Add(step)), 1)
	samplesProcessedPerStep.requireChange(t, stats.samplesProcessedPerStep, 0, 1, 0)
	newSamplesReadPerStep.requireChange(t, stats.newSamplesReadPerStep, 0, 1, 0)

	stats.TrackSampleForInstantVectorSelector(timestamp.FromTime(start.Add(2*step)), 4)
	samplesProcessedPerStep.requireChange(t, stats.samplesProcessedPerStep, 0, 0, 4)
	newSamplesReadPerStep.requireChange(t, stats.newSamplesReadPerStep, 0, 0, 4)

	stats.Close()
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestOperatorEvaluationStats_TrackSamplesForRangeVectorSelector(t *testing.T) {
	testCases := map[string]struct {
		append          func(ts int64, floats *FPointRingBuffer, histograms *HPointRingBuffer) error
		samplesPerPoint int64
	}{
		"floats": {
			append: func(ts int64, floats *FPointRingBuffer, histograms *HPointRingBuffer) error {
				return floats.Append(promql.FPoint{T: ts})
			},
			samplesPerPoint: 1,
		},
		"histograms": {
			append: func(ts int64, floatsf *FPointRingBuffer, histograms *HPointRingBuffer) error {
				return histograms.Append(promql.HPoint{T: ts, H: &histogram.FloatHistogram{}})
			},
			samplesPerPoint: EquivalentFloatSampleCount(&histogram.FloatHistogram{}),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			start := timestamp.Time(0)
			step := time.Minute
			end := start.Add(2 * step)
			timeRange := NewRangeQueryTimeRange(start, end, step)

			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

			stats, err := NewOperatorEvaluationStats(timeRange, memoryConsumptionTracker)
			require.NoError(t, err)

			samplesProcessedPerStep := newPerStepTracker("samples processed", timeRange.StepCount)
			newSamplesReadPerStep := newPerStepTracker("new samples read", timeRange.StepCount)
			floats := NewFPointRingBuffer(memoryConsumptionTracker)
			histograms := NewHPointRingBuffer(memoryConsumptionTracker)

			appendPoint := func(ts time.Time) {
				require.NoError(t, testCase.append(timestamp.FromTime(ts), floats, histograms))
			}
			clearPoints := func() {
				floats.Release()
				histograms.Release()
			}

			appendPoint(start.Add(-3 * time.Second))
			appendPoint(start.Add(-2 * time.Second))
			appendPoint(start.Add(-time.Second))
			appendPoint(start)

			// Samples from rangeStart up to and including rangeEnd should be included.
			stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start), floats, histograms, timestamp.FromTime(start.Add(-4*time.Second)), timestamp.FromTime(start))
			samplesProcessedPerStep.requireChange(t, stats.samplesProcessedPerStep, 4*testCase.samplesPerPoint, 0, 0)
			newSamplesReadPerStep.requireChange(t, stats.newSamplesReadPerStep, 4*testCase.samplesPerPoint, 0, 0)

			// Samples after rangeEnd should not be included.
			stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start), floats, histograms, timestamp.FromTime(start.Add(-4*time.Second)), timestamp.FromTime(start.Add(-1*time.Second)))
			samplesProcessedPerStep.requireChange(t, stats.samplesProcessedPerStep, 3*testCase.samplesPerPoint, 0, 0)
			newSamplesReadPerStep.requireChange(t, stats.newSamplesReadPerStep, 3*testCase.samplesPerPoint, 0, 0)

			// Should behave the same way for subsequent steps as well.
			stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start.Add(step)), floats, histograms, timestamp.FromTime(start.Add(-4*time.Second)), timestamp.FromTime(start))
			samplesProcessedPerStep.requireChange(t, stats.samplesProcessedPerStep, 0, 4*testCase.samplesPerPoint, 0)
			newSamplesReadPerStep.requireChange(t, stats.newSamplesReadPerStep, 0, 4*testCase.samplesPerPoint, 0)

			// If the range is greater than the step, then only samples since the previous step should be counted as new.
			clearPoints()
			appendPoint(start.Add(-2 * step).Add(-time.Millisecond))
			appendPoint(start.Add(-2 * step))
			appendPoint(start.Add(-3 * time.Second))
			appendPoint(start.Add(-2 * time.Second))
			appendPoint(start.Add(-time.Second))
			appendPoint(start)
			stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start), floats, histograms, timestamp.FromTime(start.Add(-3*step)), timestamp.FromTime(start))
			samplesProcessedPerStep.requireChange(t, stats.samplesProcessedPerStep, 6*testCase.samplesPerPoint, 0, 0)
			newSamplesReadPerStep.requireChange(t, stats.newSamplesReadPerStep, 4*testCase.samplesPerPoint, 0, 0)

			stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start.Add(step)), floats, histograms, timestamp.FromTime(start.Add(-3*step)), timestamp.FromTime(start))
			samplesProcessedPerStep.requireChange(t, stats.samplesProcessedPerStep, 0, 6*testCase.samplesPerPoint, 0)
			newSamplesReadPerStep.requireChange(t, stats.newSamplesReadPerStep, 0, 4*testCase.samplesPerPoint, 0)

			// Using empty buffers should not change anything.
			clearPoints()
			stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start), floats, histograms, timestamp.FromTime(start.Add(-4*time.Second)), timestamp.FromTime(start))
			samplesProcessedPerStep.requireNoChange(t, stats.samplesProcessedPerStep)
			newSamplesReadPerStep.requireNoChange(t, stats.newSamplesReadPerStep)

			stats.Close()
			floats.Close()
			histograms.Close()
			require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
		})
	}
}

func TestOperatorEvaluationStats_TrackSamplesForRangeVectorSelector_FloatsAndHistograms(t *testing.T) {
	start := timestamp.Time(0)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	stats, err := NewOperatorEvaluationStats(timeRange, memoryConsumptionTracker)
	require.NoError(t, err)

	samplesProcessedPerStep := newPerStepTracker("samples processed", timeRange.StepCount)
	newSamplesReadPerStep := newPerStepTracker("new samples read", timeRange.StepCount)

	floats := NewFPointRingBuffer(memoryConsumptionTracker)
	histograms := NewHPointRingBuffer(memoryConsumptionTracker)

	h := &histogram.FloatHistogram{}
	require.NoError(t, floats.Append(promql.FPoint{T: timestamp.FromTime(start.Add(-3 * time.Second))}))
	require.NoError(t, histograms.Append(promql.HPoint{T: timestamp.FromTime(start.Add(-2 * time.Second)), H: h}))
	require.NoError(t, floats.Append(promql.FPoint{T: timestamp.FromTime(start.Add(-time.Second))}))

	stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start), floats, histograms, timestamp.FromTime(start.Add(-4*time.Second)), timestamp.FromTime(start))
	samplesProcessedPerStep.requireChange(t, stats.samplesProcessedPerStep, 2+EquivalentFloatSampleCount(h), 0, 0)
	newSamplesReadPerStep.requireChange(t, stats.newSamplesReadPerStep, 2 + +EquivalentFloatSampleCount(h), 0, 0)

	stats.Close()
	floats.Close()
	histograms.Close()
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestOperatorEvaluationStats_TrackSamplesForRangeVectorSelector_InstantQuery(t *testing.T) {
	queryT := timestamp.Time(0)
	timeRange := NewInstantQueryTimeRange(queryT)

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	stats, err := NewOperatorEvaluationStats(timeRange, memoryConsumptionTracker)
	require.NoError(t, err)

	samplesProcessedPerStep := newPerStepTracker("samples processed", timeRange.StepCount)
	newSamplesReadPerStep := newPerStepTracker("new samples read", timeRange.StepCount)

	floats := NewFPointRingBuffer(memoryConsumptionTracker)
	histograms := NewHPointRingBuffer(memoryConsumptionTracker)

	h := &histogram.FloatHistogram{}
	require.NoError(t, floats.Append(promql.FPoint{T: timestamp.FromTime(queryT.Add(-3 * time.Second))}))
	require.NoError(t, histograms.Append(promql.HPoint{T: timestamp.FromTime(queryT.Add(-2 * time.Second)), H: h}))
	require.NoError(t, floats.Append(promql.FPoint{T: timestamp.FromTime(queryT.Add(-time.Second))}))

	stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(queryT), floats, histograms, timestamp.FromTime(queryT.Add(-4*time.Second)), timestamp.FromTime(queryT))
	samplesProcessedPerStep.requireChange(t, stats.samplesProcessedPerStep, 2+EquivalentFloatSampleCount(h))
	newSamplesReadPerStep.requireChange(t, stats.newSamplesReadPerStep, 2 + +EquivalentFloatSampleCount(h))

	stats.Close()
	floats.Close()
	histograms.Close()
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

type perStepTracker struct {
	name    string
	current []int64
}

func newPerStepTracker(name string, steps int) *perStepTracker {
	return &perStepTracker{
		name:    name,
		current: make([]int64, steps),
	}
}

func (p *perStepTracker) requireChange(t *testing.T, actual []int64, delta ...int64) {
	require.Len(t, delta, len(p.current), "invalid requireChange call, delta does not contain expected number of steps")
	for i := range delta {
		p.current[i] += delta[i]
	}

	require.Equalf(t, p.current, actual, "unexpected value for %s, expected change of %v", p.name, delta)
}

func (p *perStepTracker) requireNoChange(t *testing.T, actual []int64) {
	require.Equal(t, p.current, actual)
}

func TestOperatorEvaluationStats_Add(t *testing.T) {
	start := timestamp.Time(0)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	s1, err := NewOperatorEvaluationStats(timeRange, memoryConsumptionTracker)
	require.NoError(t, err)
	s2, err := NewOperatorEvaluationStats(timeRange, memoryConsumptionTracker)
	require.NoError(t, err)

	s1.samplesProcessedPerStep[0] = 10
	s1.samplesProcessedPerStep[1] = 20
	s1.samplesProcessedPerStep[2] = 30
	s1.newSamplesReadPerStep[0] = 1
	s1.newSamplesReadPerStep[1] = 2
	s1.newSamplesReadPerStep[2] = 3

	s2.samplesProcessedPerStep[0] = 40
	s2.samplesProcessedPerStep[1] = 50
	s2.samplesProcessedPerStep[2] = 60
	s2.newSamplesReadPerStep[0] = 4
	s2.newSamplesReadPerStep[1] = 5
	s2.newSamplesReadPerStep[2] = 6

	require.NoError(t, s1.Add(s2))
	require.Equal(t, []int64{50, 70, 90}, s1.samplesProcessedPerStep)
	require.Equal(t, []int64{5, 7, 9}, s1.newSamplesReadPerStep)

	// s2 should be unchanged.
	require.Equal(t, []int64{40, 50, 60}, s2.samplesProcessedPerStep)
	require.Equal(t, []int64{4, 5, 6}, s2.newSamplesReadPerStep)

	s1.Close()
	s2.Close()
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestOperatorEvaluationStats_Add_DifferentTimeRanges(t *testing.T) {
	start := timestamp.Time(0)
	step := time.Minute

	timeRange1 := NewRangeQueryTimeRange(start, start.Add(2*step), step)
	timeRange2 := NewRangeQueryTimeRange(start, start.Add(3*step), step)

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	s1, err := NewOperatorEvaluationStats(timeRange1, memoryConsumptionTracker)
	require.NoError(t, err)
	s2, err := NewOperatorEvaluationStats(timeRange2, memoryConsumptionTracker)
	require.NoError(t, err)

	require.EqualError(t, s1.Add(s2), "cannot add OperatorEvaluationStats with different time ranges")
}

func TestOperatorEvaluationStats_Clone(t *testing.T) {
	start := timestamp.Time(0)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	original, err := NewOperatorEvaluationStats(timeRange, memoryConsumptionTracker)
	require.NoError(t, err)

	original.samplesProcessedPerStep[0] = 10
	original.samplesProcessedPerStep[1] = 20
	original.samplesProcessedPerStep[2] = 30
	original.newSamplesReadPerStep[0] = 1
	original.newSamplesReadPerStep[1] = 2
	original.newSamplesReadPerStep[2] = 3

	clone, err := original.Clone()
	require.NoError(t, err)

	// Clone should have the same values.
	require.Equal(t, original.samplesProcessedPerStep, clone.samplesProcessedPerStep)
	require.Equal(t, original.newSamplesReadPerStep, clone.newSamplesReadPerStep)
	require.Equal(t, original.timeRange, clone.timeRange)

	// Modifying the clone should not affect the original.
	clone.samplesProcessedPerStep[0] = 99
	clone.newSamplesReadPerStep[0] = 99
	require.Equal(t, int64(10), original.samplesProcessedPerStep[0])
	require.Equal(t, int64(1), original.newSamplesReadPerStep[0])

	original.Close()
	clone.Close()
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestOperatorEvaluationStats_ComputeForSubquery(t *testing.T) {
	// Inner time range: steps at 0, 2, 4, 6, 8, 10 minutes (6 steps with 2 minute interval).
	innerStep := 2 * time.Minute
	innerStart := timestamp.Time(0)
	innerEnd := innerStart.Add(5 * innerStep)
	innerTimeRange := NewRangeQueryTimeRange(innerStart, innerEnd, innerStep)

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	samplesProcessedAt := func(t int) int64 { return int64(10 + t*10) }
	newSamplesReadAt := func(t int) int64 { return int64(5 + t*10) }

	createInnerStats := func(t *testing.T) *OperatorEvaluationStats {
		inner, err := NewOperatorEvaluationStats(innerTimeRange, memoryConsumptionTracker)
		require.NoError(t, err)

		for i := range innerTimeRange.StepCount {
			inner.samplesProcessedPerStep[i] = samplesProcessedAt(i)
			inner.newSamplesReadPerStep[i] = newSamplesReadAt(i)
		}

		return inner
	}

	t.Run("range query, outer ranges overlap", func(t *testing.T) {
		parentStep := 4 * time.Minute
		parentStart := innerStart.Add(6 * time.Minute)
		parentEnd := parentStart.Add(parentStep)
		parentTimeRange := NewRangeQueryTimeRange(parentStart, parentEnd, parentStep)

		inner := createInnerStats(t)
		subqueryRange := 6 * time.Minute
		result, err := inner.ComputeForSubquery(parentTimeRange, subqueryRange.Milliseconds(), nil, 0)
		require.NoError(t, err)

		// At t=6min: inner steps in (0m, 6m] = {2m, 4m, 6m} (idx 1, 2, 3)
		// new samples start after 0m, inner steps in (0m, 6m] = {2m, 4m, 6m} (idx 1, 2, 3)
		//
		// At t=10min: inner steps in (4m, 10m] = {6m, 8m, 10m} (idx 3, 4, 5)
		// new samples start after 6m, inner steps in (6m, 10m] = {8m, 10m} (idx 4, 5)
		require.Equal(t, []int64{samplesProcessedAt(1) + samplesProcessedAt(2) + samplesProcessedAt(3), samplesProcessedAt(3) + samplesProcessedAt(4) + samplesProcessedAt(5)}, result.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(1) + newSamplesReadAt(2) + newSamplesReadAt(3), newSamplesReadAt(4) + newSamplesReadAt(5)}, result.newSamplesReadPerStep)

		inner.Close()
		result.Close()
		require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	})

	t.Run("range query, outer ranges correspond 1:1 with inner steps", func(t *testing.T) {
		parentStep := 2 * time.Minute
		parentStart := innerStart.Add(6 * time.Minute)
		parentEnd := parentStart.Add(parentStep)
		parentTimeRange := NewRangeQueryTimeRange(parentStart, parentEnd, parentStep)

		inner := createInnerStats(t)
		subqueryRange := parentStep
		result, err := inner.ComputeForSubquery(parentTimeRange, subqueryRange.Milliseconds(), nil, 0)
		require.NoError(t, err)

		// At t=6min: inner steps in (4m, 6m] = {6m} (idx 3)
		// new samples start after 4m, inner steps in (4m, 6m] = {6m} (same)
		//
		// At t=8min: inner steps in (6m, 8m] = {8m} (idx 4)
		// new samples start after 6m, inner steps in (6m, 8m] = {8m} (same)
		require.Equal(t, []int64{samplesProcessedAt(3), samplesProcessedAt(4)}, result.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(3), newSamplesReadAt(4)}, result.newSamplesReadPerStep)

		inner.Close()
		result.Close()
		require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	})

	t.Run("range query, outer ranges don't use all inner steps", func(t *testing.T) {
		parentStep := 6 * time.Minute
		parentStart := innerStart.Add(4 * time.Minute)
		parentEnd := parentStart.Add(parentStep)
		parentTimeRange := NewRangeQueryTimeRange(parentStart, parentEnd, parentStep)

		inner := createInnerStats(t)
		subqueryRange := 3 * time.Minute
		result, err := inner.ComputeForSubquery(parentTimeRange, subqueryRange.Milliseconds(), nil, 0)
		require.NoError(t, err)

		// At t=4min: inner steps in (1m, 4m] = {2m, 4m} (idx 1, 2)
		// new samples start after 1m, inner steps in (1m, 4m] = {2m, 4m} (same)
		//
		// At t=10min: inner steps in (7m, 10m] = {8m, 10m} (idx 4, 5)
		// new samples start after 7m, inner steps in (7m, 10m] = {8m, 10m} (same)
		require.Equal(t, []int64{samplesProcessedAt(1) + samplesProcessedAt(2), samplesProcessedAt(4) + samplesProcessedAt(5)}, result.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(1) + newSamplesReadAt(2), newSamplesReadAt(4) + newSamplesReadAt(5)}, result.newSamplesReadPerStep)

		inner.Close()
		result.Close()
		require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	})

	t.Run("range query with @", func(t *testing.T) {
		parentStep := 4 * time.Minute
		parentStart := innerStart.Add(6 * time.Minute)
		parentEnd := parentStart.Add(parentStep)
		parentTimeRange := NewRangeQueryTimeRange(parentStart, parentEnd, parentStep)

		inner := createInnerStats(t)
		subqueryRange := 6 * time.Minute
		fixedTimestampMs := (8 * time.Minute).Milliseconds()
		result, err := inner.ComputeForSubquery(parentTimeRange, subqueryRange.Milliseconds(), &fixedTimestampMs, 0)
		require.NoError(t, err)

		// For all steps, the inner steps are (2m, 8m] = {4m, 6m, 8m} (idx 2, 3, 4)
		samplesProcessed := samplesProcessedAt(2) + samplesProcessedAt(3) + samplesProcessedAt(4)
		require.Equal(t, []int64{samplesProcessed, samplesProcessed}, result.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(2) + newSamplesReadAt(3) + newSamplesReadAt(4), 0}, result.newSamplesReadPerStep) // No new samples are read for subsequent steps.

		inner.Close()
		result.Close()
		require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	})

	t.Run("range query with offset", func(t *testing.T) {
		parentStep := 4 * time.Minute
		parentStart := innerStart.Add(10 * time.Minute)
		parentEnd := parentStart.Add(parentStep)
		parentTimeRange := NewRangeQueryTimeRange(parentStart, parentEnd, parentStep)

		inner := createInnerStats(t)
		subqueryRange := 6 * time.Minute
		offset := 4 * time.Minute
		result, err := inner.ComputeForSubquery(parentTimeRange, subqueryRange.Milliseconds(), nil, offset.Milliseconds())
		require.NoError(t, err)

		// At t=10min: inner steps in (0m, 6m] = {2m, 4m, 6m} (idx 1, 2, 3)
		// At t=14min: inner steps in (4m, 10m] = {6m, 8m, 10m} (idx 3, 4, 5)
		require.Equal(t, []int64{samplesProcessedAt(1) + samplesProcessedAt(2) + samplesProcessedAt(3), samplesProcessedAt(3) + samplesProcessedAt(4) + samplesProcessedAt(5)}, result.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(1) + newSamplesReadAt(2) + newSamplesReadAt(3), newSamplesReadAt(4) + newSamplesReadAt(5)}, result.newSamplesReadPerStep)

		inner.Close()
		result.Close()
		require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	})

	t.Run("range query with @ and offset", func(t *testing.T) {
		parentStep := 4 * time.Minute
		parentStart := innerStart.Add(10 * time.Minute)
		parentEnd := parentStart.Add(parentStep)
		parentTimeRange := NewRangeQueryTimeRange(parentStart, parentEnd, parentStep)

		inner := createInnerStats(t)
		subqueryRange := 6 * time.Minute
		offset := 2 * time.Minute
		fixedTimestampMs := (8 * time.Minute).Milliseconds()
		result, err := inner.ComputeForSubquery(parentTimeRange, subqueryRange.Milliseconds(), &fixedTimestampMs, offset.Milliseconds())
		require.NoError(t, err)

		// For all steps, the inner steps are (0m, 6m] = {2m, 4m, 6m} (idx 1, 2, 3)
		samplesProcessed := samplesProcessedAt(1) + samplesProcessedAt(2) + samplesProcessedAt(3)
		require.Equal(t, []int64{samplesProcessed, samplesProcessed}, result.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(1) + newSamplesReadAt(2) + newSamplesReadAt(3), 0}, result.newSamplesReadPerStep)

		inner.Close()
		result.Close()
		require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	})

	t.Run("no inner steps in range", func(t *testing.T) {
		parentStep := 4 * time.Minute
		parentStart := innerStart.Add(20 * time.Minute)
		parentEnd := parentStart.Add(parentStep)
		parentTimeRange := NewRangeQueryTimeRange(parentStart, parentEnd, parentStep)

		inner := createInnerStats(t)
		subqueryRange := 2 * time.Minute
		result, err := inner.ComputeForSubquery(parentTimeRange, subqueryRange.Milliseconds(), nil, 0)
		require.NoError(t, err)

		require.Equal(t, []int64{0, 0}, result.samplesProcessedPerStep)
		require.Equal(t, []int64{0, 0}, result.newSamplesReadPerStep)

		inner.Close()
		result.Close()
		require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	})

	t.Run("instant query", func(t *testing.T) {
		parentTimeRange := NewInstantQueryTimeRange(innerStart.Add(10 * time.Minute))

		inner := createInnerStats(t)
		subqueryRange := 6 * time.Minute
		result, err := inner.ComputeForSubquery(parentTimeRange, subqueryRange.Milliseconds(), nil, 0)
		require.NoError(t, err)

		// At t=10m: inner steps in (4m, 10m] = {6m, 8m, 10m} (idx 3, 4, 5)
		require.Equal(t, []int64{samplesProcessedAt(3) + samplesProcessedAt(4) + samplesProcessedAt(5)}, result.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(3) + newSamplesReadAt(4) + newSamplesReadAt(5)}, result.newSamplesReadPerStep)

		inner.Close()
		result.Close()
		require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	})

	t.Run("instant query with @", func(t *testing.T) {
		parentTimeRange := NewInstantQueryTimeRange(innerStart.Add(10 * time.Minute))

		inner := createInnerStats(t)
		subqueryRange := 6 * time.Minute
		fixedTimestampMs := (8 * time.Minute).Milliseconds()
		result, err := inner.ComputeForSubquery(parentTimeRange, subqueryRange.Milliseconds(), &fixedTimestampMs, 0)
		require.NoError(t, err)

		// At t=10m, subquery timestamp is 8m: inner steps in (2m, 8m] = {4m, 6m, 8m} (idx 2, 3, 4)
		require.Equal(t, []int64{samplesProcessedAt(2) + samplesProcessedAt(3) + samplesProcessedAt(4)}, result.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(2) + newSamplesReadAt(3) + newSamplesReadAt(4)}, result.newSamplesReadPerStep)

		inner.Close()
		result.Close()
		require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	})

	t.Run("instant query with offset", func(t *testing.T) {
		parentTimeRange := NewInstantQueryTimeRange(innerStart.Add(12 * time.Minute))

		inner := createInnerStats(t)
		subqueryRange := 6 * time.Minute
		offset := 2 * time.Minute
		result, err := inner.ComputeForSubquery(parentTimeRange, subqueryRange.Milliseconds(), nil, offset.Milliseconds())
		require.NoError(t, err)

		// At t=12m: inner steps in (4m, 10m] = {6m, 8m, 10m} (idx 3, 4, 5)
		require.Equal(t, []int64{samplesProcessedAt(3) + samplesProcessedAt(4) + samplesProcessedAt(5)}, result.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(3) + newSamplesReadAt(4) + newSamplesReadAt(5)}, result.newSamplesReadPerStep)

		inner.Close()
		result.Close()
		require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	})

	t.Run("instant query with @ and offset", func(t *testing.T) {
		parentTimeRange := NewInstantQueryTimeRange(innerStart.Add(20 * time.Minute))

		inner := createInnerStats(t)
		subqueryRange := 6 * time.Minute
		fixedTimestampMs := (10 * time.Minute).Milliseconds()
		offset := 2 * time.Minute
		result, err := inner.ComputeForSubquery(parentTimeRange, subqueryRange.Milliseconds(), &fixedTimestampMs, offset.Milliseconds())
		require.NoError(t, err)

		// At t=12m, subquery timestamp is 10m-2m=8m: inner steps in (2m, 8m] = {4m, 6m, 8m} (idx 2, 3, 4)
		require.Equal(t, []int64{samplesProcessedAt(2) + samplesProcessedAt(3) + samplesProcessedAt(4)}, result.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(2) + newSamplesReadAt(3) + newSamplesReadAt(4)}, result.newSamplesReadPerStep)

		inner.Close()
		result.Close()
		require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	})
}

func TestOperatorEvaluationStats_ExtendStepInvariant(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	// Create a set of stats corresponding to the single evaluated step.
	stepInvariant, err := NewOperatorEvaluationStats(NewInstantQueryTimeRange(timestamp.Time(10000)), memoryConsumptionTracker)
	require.NoError(t, err)

	stepInvariant.samplesProcessedPerStep[0] = 100
	stepInvariant.newSamplesReadPerStep[0] = 40

	// Extend it to the full time range.
	start := timestamp.Time(20000)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)
	extended, err := stepInvariant.ExtendStepInvariantToFullRange(timeRange)
	require.NoError(t, err)

	require.Equal(t, []int64{100, 100, 100}, extended.samplesProcessedPerStep)
	require.Equal(t, []int64{40, 0, 0}, extended.newSamplesReadPerStep)

	// Make sure everything was returned to the pool.
	extended.Close()
	stepInvariant.Close()
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}
