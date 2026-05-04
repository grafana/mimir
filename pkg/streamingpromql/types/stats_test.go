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

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestOperatorEvaluationStats_TrackSampleForInstantVectorSelector(t *testing.T) {
	start := timestamp.Time(0)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)

	queryStats, ctx := stats.ContextWithEmptyStats(context.Background())
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	stats, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 0)
	require.NoError(t, err)

	samplesProcessedPerStep := newPerStepTracker("samples processed", timeRange.StepCount)
	newSamplesReadPerStep := newPerStepTracker("new samples read", timeRange.StepCount)

	stats.TrackSampleForInstantVectorSelector(timestamp.FromTime(start), 1, nil)
	samplesProcessedPerStep.requireChange(t, stats.allSeries.samplesProcessedPerStep, 1, 0, 0)
	newSamplesReadPerStep.requireChange(t, stats.allSeries.newSamplesReadPerStep, 1, 0, 0)

	stats.TrackSampleForInstantVectorSelector(timestamp.FromTime(start), 2, nil)
	samplesProcessedPerStep.requireChange(t, stats.allSeries.samplesProcessedPerStep, 2, 0, 0)
	newSamplesReadPerStep.requireChange(t, stats.allSeries.newSamplesReadPerStep, 2, 0, 0)

	stats.TrackSampleForInstantVectorSelector(timestamp.FromTime(start.Add(step)), 1, nil)
	samplesProcessedPerStep.requireChange(t, stats.allSeries.samplesProcessedPerStep, 0, 1, 0)
	newSamplesReadPerStep.requireChange(t, stats.allSeries.newSamplesReadPerStep, 0, 1, 0)

	stats.TrackSampleForInstantVectorSelector(timestamp.FromTime(start.Add(2*step)), 4, nil)
	samplesProcessedPerStep.requireChange(t, stats.allSeries.samplesProcessedPerStep, 0, 0, 4)
	newSamplesReadPerStep.requireChange(t, stats.allSeries.newSamplesReadPerStep, 0, 0, 4)

	stats.Close()
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	require.Equal(t, uint64(1+2+1+4), queryStats.LoadPhysicalSamplesRead())
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

			queryStats, ctx := stats.ContextWithEmptyStats(context.Background())
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

			stats, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 0)
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
			stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start), floats, histograms, timestamp.FromTime(start.Add(-4*time.Second)), timestamp.FromTime(start), nil)
			samplesProcessedPerStep.requireChange(t, stats.allSeries.samplesProcessedPerStep, 4*testCase.samplesPerPoint, 0, 0)
			newSamplesReadPerStep.requireChange(t, stats.allSeries.newSamplesReadPerStep, 4*testCase.samplesPerPoint, 0, 0)

			// Samples after rangeEnd should not be included.
			stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start), floats, histograms, timestamp.FromTime(start.Add(-4*time.Second)), timestamp.FromTime(start.Add(-1*time.Second)), nil)
			samplesProcessedPerStep.requireChange(t, stats.allSeries.samplesProcessedPerStep, 3*testCase.samplesPerPoint, 0, 0)
			newSamplesReadPerStep.requireChange(t, stats.allSeries.newSamplesReadPerStep, 3*testCase.samplesPerPoint, 0, 0)

			// Should behave the same way for subsequent steps as well.
			stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start.Add(step)), floats, histograms, timestamp.FromTime(start.Add(-4*time.Second)), timestamp.FromTime(start), nil)
			samplesProcessedPerStep.requireChange(t, stats.allSeries.samplesProcessedPerStep, 0, 4*testCase.samplesPerPoint, 0)
			newSamplesReadPerStep.requireChange(t, stats.allSeries.newSamplesReadPerStep, 0, 4*testCase.samplesPerPoint, 0)

			// If the range is greater than the step, then only samples since the previous step should be counted as new.
			clearPoints()
			appendPoint(start.Add(-2 * step).Add(-time.Millisecond))
			appendPoint(start.Add(-2 * step))
			appendPoint(start.Add(-3 * time.Second))
			appendPoint(start.Add(-2 * time.Second))
			appendPoint(start.Add(-time.Second))
			appendPoint(start)
			stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start), floats, histograms, timestamp.FromTime(start.Add(-3*step)), timestamp.FromTime(start), nil)
			samplesProcessedPerStep.requireChange(t, stats.allSeries.samplesProcessedPerStep, 6*testCase.samplesPerPoint, 0, 0)
			newSamplesReadPerStep.requireChange(t, stats.allSeries.newSamplesReadPerStep, 4*testCase.samplesPerPoint, 0, 0)

			stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start.Add(step)), floats, histograms, timestamp.FromTime(start.Add(-3*step)), timestamp.FromTime(start), nil)
			samplesProcessedPerStep.requireChange(t, stats.allSeries.samplesProcessedPerStep, 0, 6*testCase.samplesPerPoint, 0)
			newSamplesReadPerStep.requireChange(t, stats.allSeries.newSamplesReadPerStep, 0, 4*testCase.samplesPerPoint, 0)

			// Using empty buffers should not change anything.
			clearPoints()
			stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start), floats, histograms, timestamp.FromTime(start.Add(-4*time.Second)), timestamp.FromTime(start), nil)
			samplesProcessedPerStep.requireNoChange(t, stats.allSeries.samplesProcessedPerStep)
			newSamplesReadPerStep.requireNoChange(t, stats.allSeries.newSamplesReadPerStep)

			stats.Close()
			floats.Close()
			histograms.Close()
			require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

			require.Equal(t, uint64((4+3+4+4+4)*testCase.samplesPerPoint), queryStats.LoadPhysicalSamplesRead())
		})
	}
}

func TestOperatorEvaluationStats_TrackSamplesForRangeVectorSelector_FloatsAndHistograms(t *testing.T) {
	start := timestamp.Time(0)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)

	queryStats, ctx := stats.ContextWithEmptyStats(context.Background())
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	stats, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 0)
	require.NoError(t, err)

	samplesProcessedPerStep := newPerStepTracker("samples processed", timeRange.StepCount)
	newSamplesReadPerStep := newPerStepTracker("new samples read", timeRange.StepCount)

	floats := NewFPointRingBuffer(memoryConsumptionTracker)
	histograms := NewHPointRingBuffer(memoryConsumptionTracker)

	h := &histogram.FloatHistogram{}
	require.NoError(t, floats.Append(promql.FPoint{T: timestamp.FromTime(start.Add(-3 * time.Second))}))
	require.NoError(t, histograms.Append(promql.HPoint{T: timestamp.FromTime(start.Add(-2 * time.Second)), H: h}))
	require.NoError(t, floats.Append(promql.FPoint{T: timestamp.FromTime(start.Add(-time.Second))}))

	stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start), floats, histograms, timestamp.FromTime(start.Add(-4*time.Second)), timestamp.FromTime(start), nil)
	samplesProcessedPerStep.requireChange(t, stats.allSeries.samplesProcessedPerStep, 2+EquivalentFloatSampleCount(h), 0, 0)
	newSamplesReadPerStep.requireChange(t, stats.allSeries.newSamplesReadPerStep, 2 + +EquivalentFloatSampleCount(h), 0, 0)

	stats.Close()
	floats.Close()
	histograms.Close()
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	require.Equal(t, uint64(2+EquivalentFloatSampleCount(h)), queryStats.LoadPhysicalSamplesRead())
}

func TestOperatorEvaluationStats_Subsets_TrackSampleForInstantVectorSelector(t *testing.T) {
	start := timestamp.Time(0)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)

	queryStats, ctx := stats.ContextWithEmptyStats(context.Background())
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	stats, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 2)
	require.NoError(t, err)
	require.Len(t, stats.subsets, 2)

	overallProcessed := newPerStepTracker("overall samples processed", timeRange.StepCount)
	overallRead := newPerStepTracker("overall new samples read", timeRange.StepCount)
	subset0Processed := newPerStepTracker("subset[0] samples processed", timeRange.StepCount)
	subset0Read := newPerStepTracker("subset[0] new samples read", timeRange.StepCount)
	subset1Processed := newPerStepTracker("subset[1] samples processed", timeRange.StepCount)
	subset1Read := newPerStepTracker("subset[1] new samples read", timeRange.StepCount)

	// Test series that matches first subset only.
	stats.TrackSampleForInstantVectorSelector(timestamp.FromTime(start), 3, []bool{true, false})
	overallProcessed.requireChange(t, stats.allSeries.samplesProcessedPerStep, 3, 0, 0)
	overallRead.requireChange(t, stats.allSeries.newSamplesReadPerStep, 3, 0, 0)
	subset0Processed.requireChange(t, stats.subsets[0].samplesProcessedPerStep, 3, 0, 0)
	subset0Read.requireChange(t, stats.subsets[0].newSamplesReadPerStep, 3, 0, 0)
	subset1Processed.requireNoChange(t, stats.subsets[1].samplesProcessedPerStep)
	subset1Read.requireNoChange(t, stats.subsets[1].newSamplesReadPerStep)

	// Test series that matches second subset only.
	stats.TrackSampleForInstantVectorSelector(timestamp.FromTime(start.Add(step)), 2, []bool{false, true})
	overallProcessed.requireChange(t, stats.allSeries.samplesProcessedPerStep, 0, 2, 0)
	overallRead.requireChange(t, stats.allSeries.newSamplesReadPerStep, 0, 2, 0)
	subset0Processed.requireNoChange(t, stats.subsets[0].samplesProcessedPerStep)
	subset0Read.requireNoChange(t, stats.subsets[0].newSamplesReadPerStep)
	subset1Processed.requireChange(t, stats.subsets[1].samplesProcessedPerStep, 0, 2, 0)
	subset1Read.requireChange(t, stats.subsets[1].newSamplesReadPerStep, 0, 2, 0)

	// Test series that matches neither subset.
	stats.TrackSampleForInstantVectorSelector(timestamp.FromTime(start.Add(2*step)), 1, []bool{false, false})
	overallProcessed.requireChange(t, stats.allSeries.samplesProcessedPerStep, 0, 0, 1)
	overallRead.requireChange(t, stats.allSeries.newSamplesReadPerStep, 0, 0, 1)
	subset0Processed.requireNoChange(t, stats.subsets[0].samplesProcessedPerStep)
	subset0Read.requireNoChange(t, stats.subsets[0].newSamplesReadPerStep)
	subset1Processed.requireNoChange(t, stats.subsets[1].samplesProcessedPerStep)
	subset1Read.requireNoChange(t, stats.subsets[1].newSamplesReadPerStep)

	stats.Close()
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	require.Equal(t, uint64(3+2+1), queryStats.LoadPhysicalSamplesRead())
}

func TestOperatorEvaluationStats_Subsets_TrackSamplesForRangeVectorSelector(t *testing.T) {
	start := timestamp.Time(0)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)

	queryStats, ctx := stats.ContextWithEmptyStats(context.Background())
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	stats, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 1)
	require.NoError(t, err)

	floats := NewFPointRingBuffer(memoryConsumptionTracker)
	histograms := NewHPointRingBuffer(memoryConsumptionTracker)
	require.NoError(t, floats.Append(promql.FPoint{T: timestamp.FromTime(start.Add(-3 * time.Second))}))
	require.NoError(t, floats.Append(promql.FPoint{T: timestamp.FromTime(start.Add(-2 * time.Second))}))
	require.NoError(t, floats.Append(promql.FPoint{T: timestamp.FromTime(start.Add(-time.Second))}))
	require.NoError(t, floats.Append(promql.FPoint{T: timestamp.FromTime(start)}))

	overallProcessed := newPerStepTracker("overall samples processed", timeRange.StepCount)
	overallRead := newPerStepTracker("overall new samples read", timeRange.StepCount)
	subsetProcessed := newPerStepTracker("subset samples processed", timeRange.StepCount)
	subsetRead := newPerStepTracker("subset new samples read", timeRange.StepCount)

	// Matching series: both overall and subset should be updated.
	stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start), floats, histograms, timestamp.FromTime(start.Add(-4*time.Second)), timestamp.FromTime(start), []bool{true})
	overallProcessed.requireChange(t, stats.allSeries.samplesProcessedPerStep, 4, 0, 0)
	overallRead.requireChange(t, stats.allSeries.newSamplesReadPerStep, 4, 0, 0)
	subsetProcessed.requireChange(t, stats.subsets[0].samplesProcessedPerStep, 4, 0, 0)
	subsetRead.requireChange(t, stats.subsets[0].newSamplesReadPerStep, 4, 0, 0)

	// Non-matching series: only overall should be updated.
	stats.TrackSamplesForRangeVectorSelector(timestamp.FromTime(start.Add(step)), floats, histograms, timestamp.FromTime(start.Add(-4*time.Second)), timestamp.FromTime(start), []bool{false})
	overallProcessed.requireChange(t, stats.allSeries.samplesProcessedPerStep, 0, 4, 0)
	overallRead.requireChange(t, stats.allSeries.newSamplesReadPerStep, 0, 4, 0)
	subsetProcessed.requireNoChange(t, stats.subsets[0].samplesProcessedPerStep)
	subsetRead.requireNoChange(t, stats.subsets[0].newSamplesReadPerStep)

	stats.Close()
	floats.Close()
	histograms.Close()
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	require.Equal(t, uint64(4+4), queryStats.LoadPhysicalSamplesRead())
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

	s1, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 0)
	require.NoError(t, err)
	s2, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 0)
	require.NoError(t, err)

	s1.allSeries.samplesProcessedPerStep[0] = 10
	s1.allSeries.samplesProcessedPerStep[1] = 20
	s1.allSeries.samplesProcessedPerStep[2] = 30
	s1.allSeries.newSamplesReadPerStep[0] = 1
	s1.allSeries.newSamplesReadPerStep[1] = 2
	s1.allSeries.newSamplesReadPerStep[2] = 3

	s2.allSeries.samplesProcessedPerStep[0] = 40
	s2.allSeries.samplesProcessedPerStep[1] = 50
	s2.allSeries.samplesProcessedPerStep[2] = 60
	s2.allSeries.newSamplesReadPerStep[0] = 4
	s2.allSeries.newSamplesReadPerStep[1] = 5
	s2.allSeries.newSamplesReadPerStep[2] = 6

	require.NoError(t, s1.Add(s2))
	require.Equal(t, []int64{50, 70, 90}, s1.allSeries.samplesProcessedPerStep)
	require.Equal(t, []int64{5, 7, 9}, s1.allSeries.newSamplesReadPerStep)

	// s2 should be unchanged.
	require.Equal(t, []int64{40, 50, 60}, s2.allSeries.samplesProcessedPerStep)
	require.Equal(t, []int64{4, 5, 6}, s2.allSeries.newSamplesReadPerStep)

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

	s1, err := NewOperatorEvaluationStats(ctx, timeRange1, memoryConsumptionTracker, 0)
	require.NoError(t, err)
	s2, err := NewOperatorEvaluationStats(ctx, timeRange2, memoryConsumptionTracker, 0)
	require.NoError(t, err)

	require.EqualError(t, s1.Add(s2), "cannot add OperatorEvaluationStats with different time ranges")

	s1.Close()
	s2.Close()
}

func TestOperatorEvaluationStats_Add_WithSubsets(t *testing.T) {
	start := timestamp.Time(0)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	t.Run("receiver has subsets, other does not", func(t *testing.T) {
		withSubsets, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 1)
		require.NoError(t, err)
		withoutSubsets, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 0)
		require.NoError(t, err)

		withSubsets.allSeries.samplesProcessedPerStep[0] = 10
		withSubsets.allSeries.samplesProcessedPerStep[1] = 20
		withSubsets.allSeries.newSamplesReadPerStep[0] = 1
		withSubsets.allSeries.newSamplesReadPerStep[1] = 2
		withSubsets.subsets[0].samplesProcessedPerStep[0] = 5
		withSubsets.subsets[0].newSamplesReadPerStep[0] = 3

		withoutSubsets.allSeries.samplesProcessedPerStep[0] = 40
		withoutSubsets.allSeries.samplesProcessedPerStep[1] = 50
		withoutSubsets.allSeries.newSamplesReadPerStep[0] = 4
		withoutSubsets.allSeries.newSamplesReadPerStep[1] = 5

		require.NoError(t, withSubsets.Add(withoutSubsets))

		// Overall stats should be summed.
		require.Equal(t, []int64{50, 70, 0}, withSubsets.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{5, 7, 0}, withSubsets.allSeries.newSamplesReadPerStep)

		// other's overall should be added to each subset.
		require.Equal(t, []int64{45, 50, 0}, withSubsets.subsets[0].samplesProcessedPerStep)
		require.Equal(t, []int64{7, 5, 0}, withSubsets.subsets[0].newSamplesReadPerStep)

		withSubsets.Close()
		withoutSubsets.Close()
	})

	t.Run("receiver has no subsets, other does", func(t *testing.T) {
		withoutSubsets, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 0)
		require.NoError(t, err)
		withSubsets, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 1)
		require.NoError(t, err)

		withoutSubsets.allSeries.samplesProcessedPerStep[0] = 10
		withoutSubsets.allSeries.newSamplesReadPerStep[0] = 1

		withSubsets.allSeries.samplesProcessedPerStep[0] = 40
		withSubsets.allSeries.samplesProcessedPerStep[1] = 50
		withSubsets.allSeries.newSamplesReadPerStep[0] = 4
		withSubsets.allSeries.newSamplesReadPerStep[1] = 5
		withSubsets.subsets[0].samplesProcessedPerStep[0] = 30
		withSubsets.subsets[0].newSamplesReadPerStep[0] = 3

		require.NoError(t, withoutSubsets.Add(withSubsets))

		// Only overall stats are updated; withoutSubsets has no subsets to carry over.
		require.Equal(t, []int64{50, 50, 0}, withoutSubsets.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{5, 5, 0}, withoutSubsets.allSeries.newSamplesReadPerStep)

		withoutSubsets.Close()
		withSubsets.Close()
	})

	t.Run("both have subsets returns error", func(t *testing.T) {
		s1, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 1)
		require.NoError(t, err)
		s2, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 1)
		require.NoError(t, err)

		require.EqualError(t, s1.Add(s2), "cannot add two OperatorEvaluationStats instances that both have subsets")

		s1.Close()
		s2.Close()
	})

	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestOperatorEvaluationStats_Clone(t *testing.T) {
	start := timestamp.Time(0)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)

	stats, ctx := stats.ContextWithEmptyStats(context.Background())
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	original, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 0)
	require.NoError(t, err)

	original.allSeries.samplesProcessedPerStep[0] = 10
	original.allSeries.samplesProcessedPerStep[1] = 20
	original.allSeries.samplesProcessedPerStep[2] = 30
	original.allSeries.newSamplesReadPerStep[0] = 1
	original.allSeries.newSamplesReadPerStep[1] = 2
	original.allSeries.newSamplesReadPerStep[2] = 3

	clone, err := original.Clone()
	require.NoError(t, err)

	// Clone should have the same values.
	require.Equal(t, original.allSeries.samplesProcessedPerStep, clone.allSeries.samplesProcessedPerStep)
	require.Equal(t, original.allSeries.newSamplesReadPerStep, clone.allSeries.newSamplesReadPerStep)
	require.Equal(t, original.timeRange, clone.timeRange)
	require.Equal(t, stats, clone.queryStats)

	// Modifying the clone should not affect the original.
	clone.allSeries.samplesProcessedPerStep[0] = 99
	clone.allSeries.newSamplesReadPerStep[0] = 99
	require.Equal(t, int64(10), original.allSeries.samplesProcessedPerStep[0])
	require.Equal(t, int64(1), original.allSeries.newSamplesReadPerStep[0])

	original.Close()
	clone.Close()
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestOperatorEvaluationStats_Clone_WithSubsets(t *testing.T) {
	start := timestamp.Time(0)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	original, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 1)
	require.NoError(t, err)

	original.allSeries.samplesProcessedPerStep[0] = 10
	original.allSeries.newSamplesReadPerStep[0] = 1
	original.subsets[0].samplesProcessedPerStep[0] = 7
	original.subsets[0].newSamplesReadPerStep[0] = 4

	clone, err := original.Clone()
	require.NoError(t, err)

	// Clone should have the same values including subsets.
	require.Equal(t, original.allSeries.samplesProcessedPerStep, clone.allSeries.samplesProcessedPerStep)
	require.Equal(t, original.allSeries.newSamplesReadPerStep, clone.allSeries.newSamplesReadPerStep)
	require.Len(t, clone.subsets, 1)
	require.Equal(t, original.subsets[0].samplesProcessedPerStep, clone.subsets[0].samplesProcessedPerStep)
	require.Equal(t, original.subsets[0].newSamplesReadPerStep, clone.subsets[0].newSamplesReadPerStep)

	// Modifying the clone's subset should not affect the original.
	clone.subsets[0].samplesProcessedPerStep[0] = 99
	clone.subsets[0].newSamplesReadPerStep[0] = 99
	require.Equal(t, int64(7), original.subsets[0].samplesProcessedPerStep[0])
	require.Equal(t, int64(4), original.subsets[0].newSamplesReadPerStep[0])

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
		inner, err := NewOperatorEvaluationStats(ctx, innerTimeRange, memoryConsumptionTracker, 0)
		require.NoError(t, err)

		for i := range innerTimeRange.StepCount {
			inner.allSeries.samplesProcessedPerStep[i] = samplesProcessedAt(i)
			inner.allSeries.newSamplesReadPerStep[i] = newSamplesReadAt(i)
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
		require.Equal(t, []int64{samplesProcessedAt(1) + samplesProcessedAt(2) + samplesProcessedAt(3), samplesProcessedAt(3) + samplesProcessedAt(4) + samplesProcessedAt(5)}, result.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(1) + newSamplesReadAt(2) + newSamplesReadAt(3), newSamplesReadAt(4) + newSamplesReadAt(5)}, result.allSeries.newSamplesReadPerStep)

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
		require.Equal(t, []int64{samplesProcessedAt(3), samplesProcessedAt(4)}, result.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(3), newSamplesReadAt(4)}, result.allSeries.newSamplesReadPerStep)

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
		require.Equal(t, []int64{samplesProcessedAt(1) + samplesProcessedAt(2), samplesProcessedAt(4) + samplesProcessedAt(5)}, result.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(1) + newSamplesReadAt(2), newSamplesReadAt(4) + newSamplesReadAt(5)}, result.allSeries.newSamplesReadPerStep)

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
		require.Equal(t, []int64{samplesProcessed, samplesProcessed}, result.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(2) + newSamplesReadAt(3) + newSamplesReadAt(4), 0}, result.allSeries.newSamplesReadPerStep) // No new samples are read for subsequent steps.

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
		require.Equal(t, []int64{samplesProcessedAt(1) + samplesProcessedAt(2) + samplesProcessedAt(3), samplesProcessedAt(3) + samplesProcessedAt(4) + samplesProcessedAt(5)}, result.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(1) + newSamplesReadAt(2) + newSamplesReadAt(3), newSamplesReadAt(4) + newSamplesReadAt(5)}, result.allSeries.newSamplesReadPerStep)

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
		require.Equal(t, []int64{samplesProcessed, samplesProcessed}, result.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(1) + newSamplesReadAt(2) + newSamplesReadAt(3), 0}, result.allSeries.newSamplesReadPerStep)

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

		require.Equal(t, []int64{0, 0}, result.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{0, 0}, result.allSeries.newSamplesReadPerStep)

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
		require.Equal(t, []int64{samplesProcessedAt(3) + samplesProcessedAt(4) + samplesProcessedAt(5)}, result.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(3) + newSamplesReadAt(4) + newSamplesReadAt(5)}, result.allSeries.newSamplesReadPerStep)

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
		require.Equal(t, []int64{samplesProcessedAt(2) + samplesProcessedAt(3) + samplesProcessedAt(4)}, result.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(2) + newSamplesReadAt(3) + newSamplesReadAt(4)}, result.allSeries.newSamplesReadPerStep)

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
		require.Equal(t, []int64{samplesProcessedAt(3) + samplesProcessedAt(4) + samplesProcessedAt(5)}, result.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(3) + newSamplesReadAt(4) + newSamplesReadAt(5)}, result.allSeries.newSamplesReadPerStep)

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
		require.Equal(t, []int64{samplesProcessedAt(2) + samplesProcessedAt(3) + samplesProcessedAt(4)}, result.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(2) + newSamplesReadAt(3) + newSamplesReadAt(4)}, result.allSeries.newSamplesReadPerStep)

		inner.Close()
		result.Close()
		require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	})
}

func TestOperatorEvaluationStats_ComputeForSubquery_WithSubsets(t *testing.T) {
	// Inner time range: steps at 0, 2, 4, 6, 8, 10 minutes (6 steps with 2 minute interval).
	innerStep := 2 * time.Minute
	innerStart := timestamp.Time(0)
	innerEnd := innerStart.Add(5 * innerStep)
	innerTimeRange := NewRangeQueryTimeRange(innerStart, innerEnd, innerStep)

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	samplesProcessedAt := func(t int) int64 { return int64(10 + t*10) }
	newSamplesReadAt := func(t int) int64 { return int64(5 + t*10) }
	subsetSamplesProcessedAt := func(t int) int64 { return int64(2 + t*3) }
	subsetNewSamplesReadAt := func(t int) int64 { return int64(1 + t*2) }

	createInnerStats := func(t *testing.T) *OperatorEvaluationStats {
		inner, err := NewOperatorEvaluationStats(ctx, innerTimeRange, memoryConsumptionTracker, 1)
		require.NoError(t, err)

		for i := range innerTimeRange.StepCount {
			inner.allSeries.samplesProcessedPerStep[i] = samplesProcessedAt(i)
			inner.allSeries.newSamplesReadPerStep[i] = newSamplesReadAt(i)
			inner.subsets[0].samplesProcessedPerStep[i] = subsetSamplesProcessedAt(i)
			inner.subsets[0].newSamplesReadPerStep[i] = subsetNewSamplesReadAt(i)
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
		require.Equal(t, []int64{samplesProcessedAt(1) + samplesProcessedAt(2) + samplesProcessedAt(3), samplesProcessedAt(3) + samplesProcessedAt(4) + samplesProcessedAt(5)}, result.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(1) + newSamplesReadAt(2) + newSamplesReadAt(3), newSamplesReadAt(4) + newSamplesReadAt(5)}, result.allSeries.newSamplesReadPerStep)

		// Subset should be computed using the same time-range logic as allSeries.
		require.Len(t, result.subsets, 1)
		require.Equal(t, []int64{subsetSamplesProcessedAt(1) + subsetSamplesProcessedAt(2) + subsetSamplesProcessedAt(3), subsetSamplesProcessedAt(3) + subsetSamplesProcessedAt(4) + subsetSamplesProcessedAt(5)}, result.subsets[0].samplesProcessedPerStep)
		require.Equal(t, []int64{subsetNewSamplesReadAt(1) + subsetNewSamplesReadAt(2) + subsetNewSamplesReadAt(3), subsetNewSamplesReadAt(4) + subsetNewSamplesReadAt(5)}, result.subsets[0].newSamplesReadPerStep)

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
		require.Equal(t, []int64{samplesProcessedAt(3) + samplesProcessedAt(4) + samplesProcessedAt(5)}, result.allSeries.samplesProcessedPerStep)
		require.Equal(t, []int64{newSamplesReadAt(3) + newSamplesReadAt(4) + newSamplesReadAt(5)}, result.allSeries.newSamplesReadPerStep)

		// Subset should be computed using the same time-range logic as allSeries.
		require.Len(t, result.subsets, 1)
		require.Equal(t, []int64{subsetSamplesProcessedAt(3) + subsetSamplesProcessedAt(4) + subsetSamplesProcessedAt(5)}, result.subsets[0].samplesProcessedPerStep)
		require.Equal(t, []int64{subsetNewSamplesReadAt(3) + subsetNewSamplesReadAt(4) + subsetNewSamplesReadAt(5)}, result.subsets[0].newSamplesReadPerStep)

		inner.Close()
		result.Close()
		require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	})
}

func TestOperatorEvaluationStats_ExtendStepInvariant(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	// Create a set of stats corresponding to the single evaluated step.
	stepInvariant, err := NewOperatorEvaluationStats(ctx, NewInstantQueryTimeRange(timestamp.Time(10000)), memoryConsumptionTracker, 0)
	require.NoError(t, err)

	stepInvariant.allSeries.samplesProcessedPerStep[0] = 100
	stepInvariant.allSeries.newSamplesReadPerStep[0] = 40

	// Extend it to the full time range.
	start := timestamp.Time(20000)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)
	extended, err := stepInvariant.ExtendStepInvariantToFullRange(timeRange)
	require.NoError(t, err)

	require.Equal(t, []int64{100, 100, 100}, extended.allSeries.samplesProcessedPerStep)
	require.Equal(t, []int64{40, 0, 0}, extended.allSeries.newSamplesReadPerStep)

	// Make sure everything was returned to the pool.
	extended.Close()
	stepInvariant.Close()
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestOperatorEvaluationStats_ExtendStepInvariant_WithSubsets(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	stepInvariant, err := NewOperatorEvaluationStats(ctx, NewInstantQueryTimeRange(timestamp.Time(10000)), memoryConsumptionTracker, 1)
	require.NoError(t, err)

	stepInvariant.allSeries.samplesProcessedPerStep[0] = 100
	stepInvariant.allSeries.newSamplesReadPerStep[0] = 40
	stepInvariant.subsets[0].samplesProcessedPerStep[0] = 60
	stepInvariant.subsets[0].newSamplesReadPerStep[0] = 25

	start := timestamp.Time(20000)
	step := time.Minute
	end := start.Add(2 * step)
	timeRange := NewRangeQueryTimeRange(start, end, step)
	extended, err := stepInvariant.ExtendStepInvariantToFullRange(timeRange)
	require.NoError(t, err)

	// Overall stats should be expanded as normal.
	require.Equal(t, []int64{100, 100, 100}, extended.allSeries.samplesProcessedPerStep)
	require.Equal(t, []int64{40, 0, 0}, extended.allSeries.newSamplesReadPerStep)

	// Subset stats should be expanded the same way.
	require.Len(t, extended.subsets, 1)
	require.Equal(t, []int64{60, 60, 60}, extended.subsets[0].samplesProcessedPerStep)
	require.Equal(t, []int64{25, 0, 0}, extended.subsets[0].newSamplesReadPerStep)

	extended.Close()
	stepInvariant.Close()
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestOperatorEvaluationStats_EncodingAndDecoding(t *testing.T) {
	testCases := map[string]func(t *testing.T, ctx context.Context, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *OperatorEvaluationStats{
		"instant query, no subsets": func(t *testing.T, ctx context.Context, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *OperatorEvaluationStats {
			timeRange := NewInstantQueryTimeRange(timestamp.Time(1000))
			stats, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 0)
			require.NoError(t, err)

			stats.allSeries.samplesProcessedPerStep[0] = 100
			stats.allSeries.newSamplesReadPerStep[0] = 200
			require.Empty(t, stats.subsets)

			return stats
		},
		"instant query, with subsets": func(t *testing.T, ctx context.Context, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *OperatorEvaluationStats {
			timeRange := NewInstantQueryTimeRange(timestamp.Time(1000))
			stats, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 2)
			require.NoError(t, err)

			stats.allSeries.samplesProcessedPerStep[0] = 100
			stats.allSeries.newSamplesReadPerStep[0] = 200

			stats.subsets[0].samplesProcessedPerStep[0] = 300
			stats.subsets[0].newSamplesReadPerStep[0] = 400
			stats.subsets[1].samplesProcessedPerStep[0] = 500
			stats.subsets[1].newSamplesReadPerStep[0] = 600

			return stats
		},
		"range query, no subsets": func(t *testing.T, ctx context.Context, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *OperatorEvaluationStats {
			startT := timestamp.Time(1000)
			timeRange := NewRangeQueryTimeRange(startT, startT.Add(2*time.Second), time.Second)
			stats, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 0)
			require.NoError(t, err)

			stats.allSeries.samplesProcessedPerStep[0] = 100
			stats.allSeries.samplesProcessedPerStep[1] = 101
			stats.allSeries.samplesProcessedPerStep[2] = 102
			stats.allSeries.newSamplesReadPerStep[0] = 200
			stats.allSeries.newSamplesReadPerStep[1] = 201
			stats.allSeries.newSamplesReadPerStep[2] = 202
			require.Empty(t, stats.subsets)

			return stats
		},
		"range query, with subsets": func(t *testing.T, ctx context.Context, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *OperatorEvaluationStats {
			startT := timestamp.Time(1000)
			timeRange := NewRangeQueryTimeRange(startT, startT.Add(2*time.Second), time.Second)
			stats, err := NewOperatorEvaluationStats(ctx, timeRange, memoryConsumptionTracker, 2)
			require.NoError(t, err)

			stats.allSeries.samplesProcessedPerStep[0] = 100
			stats.allSeries.samplesProcessedPerStep[1] = 101
			stats.allSeries.samplesProcessedPerStep[2] = 102
			stats.allSeries.newSamplesReadPerStep[0] = 200
			stats.allSeries.newSamplesReadPerStep[1] = 201
			stats.allSeries.newSamplesReadPerStep[2] = 202

			stats.subsets[0].samplesProcessedPerStep[0] = 300
			stats.subsets[0].samplesProcessedPerStep[1] = 301
			stats.subsets[0].samplesProcessedPerStep[2] = 302
			stats.subsets[0].newSamplesReadPerStep[0] = 400
			stats.subsets[0].newSamplesReadPerStep[1] = 401
			stats.subsets[0].newSamplesReadPerStep[2] = 402

			stats.subsets[1].samplesProcessedPerStep[0] = 500
			stats.subsets[1].samplesProcessedPerStep[1] = 501
			stats.subsets[1].samplesProcessedPerStep[2] = 502
			stats.subsets[1].newSamplesReadPerStep[0] = 600
			stats.subsets[1].newSamplesReadPerStep[1] = 601
			stats.subsets[1].newSamplesReadPerStep[2] = 602

			return stats
		},
	}

	for name, factory := range testCases {
		t.Run(name, func(t *testing.T) {
			stats, ctx := stats.ContextWithEmptyStats(context.Background())
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			original := factory(t, ctx, memoryConsumptionTracker)

			encodedBytes, err := original.Encode().Marshal()
			require.NoError(t, err)
			encoded := &EncodedOperatorEvaluationStats{}
			require.NoError(t, encoded.Unmarshal(encodedBytes))

			memoryConsumptionBeforeDecoding := memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes()
			decoded, err := encoded.Decode(ctx, original.timeRange, memoryConsumptionTracker)
			require.NoError(t, err)
			require.Equal(t, original, decoded)
			require.Equal(t, stats, decoded.queryStats)

			require.Greater(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), memoryConsumptionBeforeDecoding, "decoding a stats instance should increase the memory consumption estimate")

			decoded.Close()
			original.Close()
			require.Zerof(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "expected all instances to be returned to pool, current memory consumption is:\n%v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
		})
	}
}

func TestOperatorEvaluationStats_DecodingInvalidValues(t *testing.T) {
	testCases := map[string]struct {
		encoded       *EncodedOperatorEvaluationStats
		expectedError string
	}{
		"unfiltered set has different number of samples processed steps": {
			encoded: &EncodedOperatorEvaluationStats{
				AllSeries: EncodedSubsetStats{
					SamplesProcessedPerStep: []int64{1, 2},
					NewSamplesReadPerStep:   []int64{4, 5, 6},
				},
			},
			expectedError: "number of samples processed steps in encoded form (2) does not match expected (3)",
		},
		"unfiltered set has different number of new samples read steps": {
			encoded: &EncodedOperatorEvaluationStats{
				AllSeries: EncodedSubsetStats{
					SamplesProcessedPerStep: []int64{1, 2, 3},
					NewSamplesReadPerStep:   []int64{4, 5},
				},
			},
			expectedError: "number of new samples read steps in encoded form (2) does not match expected (3)",
		},
		"subset has different number of samples processed steps": {
			encoded: &EncodedOperatorEvaluationStats{
				AllSeries: EncodedSubsetStats{
					SamplesProcessedPerStep: []int64{1, 2, 3},
					NewSamplesReadPerStep:   []int64{4, 5, 6},
				},
				Subsets: []EncodedSubsetStats{
					{
						SamplesProcessedPerStep: []int64{7, 8},
						NewSamplesReadPerStep:   []int64{10, 11, 12},
					},
				},
			},
			expectedError: "number of samples processed steps in encoded form (2) does not match expected (3)",
		},
		"subset has different number of new samples read steps": {
			encoded: &EncodedOperatorEvaluationStats{
				AllSeries: EncodedSubsetStats{
					SamplesProcessedPerStep: []int64{1, 2, 3},
					NewSamplesReadPerStep:   []int64{4, 5, 6},
				},
				Subsets: []EncodedSubsetStats{
					{
						SamplesProcessedPerStep: []int64{7, 8, 9},
						NewSamplesReadPerStep:   []int64{10, 11},
					},
				},
			},
			expectedError: "number of new samples read steps in encoded form (2) does not match expected (3)",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			startT := timestamp.Time(1000)
			timeRange := NewRangeQueryTimeRange(startT, startT.Add(2*time.Second), time.Second) // 3 steps

			_, err := testCase.encoded.Decode(ctx, timeRange, memoryConsumptionTracker)
			require.EqualError(t, err, testCase.expectedError)
		})
	}
}
