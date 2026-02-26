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
