// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/util/stats/query_stats.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package types

import (
	"errors"
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"

	"github.com/grafana/mimir/pkg/util/limiter"
)

// QueryStats tracks statistics about the execution of a single query.
//
// It is not safe to use this type from multiple goroutines simultaneously.
type QueryStats struct {
	// The total number of samples processed during the query.
	//
	// In the case of range vector selectors, each sample is counted once for each time step it appears in.
	// For example, if a query is running with a step of 30s with a range vector selector with range 45s,
	// then samples in the overlapping 15s are counted twice.
	TotalSamples int64
}

func NewQueryStats() *QueryStats {
	return &QueryStats{}
}

// IncrementSamples increments the total samples count.
func (qs *QueryStats) IncrementSamples(samples int64) {
	if qs == nil {
		return
	}
	qs.TotalSamples += samples
}

const timestampFieldSize = int64(unsafe.Sizeof(int64(0)))

func EquivalentFloatSampleCount(h *histogram.FloatHistogram) int64 {
	return (int64(h.Size()) + timestampFieldSize) / int64(FPointSize)
}

type OperatorEvaluationStats struct {
	timeRange                QueryTimeRange
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker

	samplesProcessedPerStep []int64
	newSamplesReadPerStep   []int64
}

func NewOperatorEvaluationStats(timeRange QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*OperatorEvaluationStats, error) {
	samplesProcessedPerStep, err := Int64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	newSamplesReadPerStep, err := Int64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	return &OperatorEvaluationStats{
		timeRange:                timeRange,
		memoryConsumptionTracker: memoryConsumptionTracker,

		samplesProcessedPerStep: samplesProcessedPerStep[:timeRange.StepCount],
		newSamplesReadPerStep:   newSamplesReadPerStep[:timeRange.StepCount],
	}, nil
}

// TrackSampleForInstantVectorSelector records a sample for an instant vector selector at output timestamp stepT.
//
// sampleCount should be 1 for float samples, or the result of EquivalentFloatSampleCount for histogram samples.
//
// TrackSampleForInstantVectorSelector should be called for each output step of an instant vector selector, even if
// the same underlying sample is used for multiple output steps.
func (s *OperatorEvaluationStats) TrackSampleForInstantVectorSelector(stepT int64, sampleCount int64) {
	i := s.timeRange.PointIndex(stepT)

	s.samplesProcessedPerStep[i] += sampleCount
	s.newSamplesReadPerStep[i] += sampleCount
}

// TrackSamplesForRangeVectorSelector records samples for a range vector selector at output timestamp stepT.
//
// TrackSamplesForRangeVectorSelector should be called for each output step of an instant vector selector, even if
// the same underlying sample is used for multiple output steps.
//
// floats and histograms may contain samples beyond rangeEnd, these will be ignored.
// floats and histograms must not contain samples before rangeStart.
func (s *OperatorEvaluationStats) TrackSamplesForRangeVectorSelector(stepT int64, floats *FPointRingBuffer, histograms *HPointRingBuffer, rangeStart int64, rangeEnd int64) {
	i := s.timeRange.PointIndex(stepT)

	s.samplesProcessedPerStep[i] += int64(floats.CountUntil(rangeEnd)) + histograms.EquivalentFloatSampleCountUntil(rangeEnd)

	newSampleRangeStart := rangeEnd - s.timeRange.IntervalMilliseconds
	if s.timeRange.IsInstant {
		newSampleRangeStart = rangeStart
	}

	s.newSamplesReadPerStep[i] += int64(floats.CountBetween(newSampleRangeStart, rangeEnd)) + histograms.EquivalentFloatSampleCountBetween(newSampleRangeStart, rangeEnd)
}

// Add adds the statistics from other to this instance.
//
// This instance is modified in-place.
//
// Both instances must be for the same time range.
func (s *OperatorEvaluationStats) Add(other *OperatorEvaluationStats) error {
	if !s.timeRange.Equal(other.timeRange) {
		return errors.New("cannot add OperatorEvaluationStats with different time ranges")
	}

	for i := range s.samplesProcessedPerStep {
		s.samplesProcessedPerStep[i] += other.samplesProcessedPerStep[i]
		s.newSamplesReadPerStep[i] += other.newSamplesReadPerStep[i]
	}

	return nil
}

// Clone returns a copy of this OperatorEvaluationStats instance.
func (s *OperatorEvaluationStats) Clone() (*OperatorEvaluationStats, error) {
	clone, err := NewOperatorEvaluationStats(s.timeRange, s.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	copy(clone.samplesProcessedPerStep, s.samplesProcessedPerStep)
	copy(clone.newSamplesReadPerStep, s.newSamplesReadPerStep)

	return clone, nil
}

func (s *OperatorEvaluationStats) Close() {
	Int64SlicePool.Put(&s.samplesProcessedPerStep, s.memoryConsumptionTracker)
	Int64SlicePool.Put(&s.newSamplesReadPerStep, s.memoryConsumptionTracker)
}
