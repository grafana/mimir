// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/util/stats/query_stats.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package types

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"

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

	allSeries *statsTracker
	subsets   []*subsetStats
}

// NewOperatorEvaluationStats creates a new OperatorEvaluationStats for the given time range.
//
// subsetMatchers defines zero or more subsets of series to track independently.
// Each subset is defined by a set of label matchers (AND semantics): a series belongs to a subset
// if it matches all matchers in that subset's set.
func NewOperatorEvaluationStats(timeRange QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, subsetMatchers [][]*labels.Matcher) (*OperatorEvaluationStats, error) {
	allSeries, err := newStatsTracker(timeRange, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	subsets := make([]*subsetStats, len(subsetMatchers))
	for i, matchers := range subsetMatchers {
		stats, err := newSubsetsStats(matchers, timeRange, memoryConsumptionTracker)
		if err != nil {
			return nil, err
		}

		subsets[i] = stats
	}

	return &OperatorEvaluationStats{
		timeRange:                timeRange,
		memoryConsumptionTracker: memoryConsumptionTracker,

		allSeries: allSeries,
		subsets:   subsets,
	}, nil
}

// TrackSampleForInstantVectorSelector records a sample for an instant vector selector at output timestamp stepT.
//
// sampleCount should be 1 for float samples, or the result of EquivalentFloatSampleCount for histogram samples.
//
// TrackSampleForInstantVectorSelector should be called for each output step of an instant vector selector, even if
// the same underlying sample is used for multiple output steps.
//
// lbls is the label set of the series the sample belongs to, used to determine which subsets (if any) it belongs to.
func (s *OperatorEvaluationStats) TrackSampleForInstantVectorSelector(stepT int64, sampleCount int64, lbls labels.Labels) {
	i := s.timeRange.PointIndex(stepT)

	s.allSeries.Add(i, sampleCount, sampleCount)

	for _, subset := range s.subsets {
		if subset.matches(lbls) {
			subset.Add(i, sampleCount, sampleCount)
		}
	}
}

// TrackSamplesForRangeVectorSelector records samples for a range vector selector at output timestamp stepT.
//
// TrackSamplesForRangeVectorSelector should be called for each output step of an instant vector selector, even if
// the same underlying sample is used for multiple output steps.
//
// floats and histograms may contain samples beyond rangeEnd, these will be ignored.
// floats and histograms must not contain samples before rangeStart.
//
// lbls is the label set of the series the samples belong to, used to determine which subsets (if any) they belong to.
func (s *OperatorEvaluationStats) TrackSamplesForRangeVectorSelector(stepT int64, floats *FPointRingBuffer, histograms *HPointRingBuffer, rangeStart int64, rangeEnd int64, lbls labels.Labels) {
	i := s.timeRange.PointIndex(stepT)

	samplesProcessed := int64(floats.CountUntil(rangeEnd)) + histograms.EquivalentFloatSampleCountUntil(rangeEnd)

	newSampleRangeStart := rangeEnd - s.timeRange.IntervalMilliseconds
	if s.timeRange.IsInstant {
		newSampleRangeStart = rangeStart
	}
	newSamplesRead := int64(floats.CountBetween(newSampleRangeStart, rangeEnd)) + histograms.EquivalentFloatSampleCountBetween(newSampleRangeStart, rangeEnd)

	s.allSeries.Add(i, samplesProcessed, newSamplesRead)

	for _, subset := range s.subsets {
		if subset.matches(lbls) {
			subset.Add(i, samplesProcessed, newSamplesRead)
		}
	}
}

// Add adds the statistics from other to this instance.
//
// This instance is modified in-place.
//
// Both instances must be for the same time range.
//
// At most one of the two instances may have subsets. If both have subsets, an error is returned.
// When one has subsets and the other does not, the overall statistics from the instance without subsets
// are added to each subset in the instance with subsets.
func (s *OperatorEvaluationStats) Add(other *OperatorEvaluationStats) error {
	if !s.timeRange.Equal(other.timeRange) {
		return errors.New("cannot add OperatorEvaluationStats with different time ranges")
	}

	if len(s.subsets) > 0 && len(other.subsets) > 0 {
		return errors.New("cannot add two OperatorEvaluationStats instances that both have subsets")
	}

	for i := range s.timeRange.StepCount {
		s.allSeries.Add(int64(i), other.allSeries.samplesProcessedPerStep[i], other.allSeries.newSamplesReadPerStep[i])
	}

	for _, subset := range s.subsets {
		for i := range subset.samplesProcessedPerStep {
			subset.Add(int64(i), other.allSeries.samplesProcessedPerStep[i], other.allSeries.newSamplesReadPerStep[i])
		}
	}

	return nil
}

// Clone returns a copy of this OperatorEvaluationStats instance, including any subset definitions and their data.
func (s *OperatorEvaluationStats) Clone() (*OperatorEvaluationStats, error) {
	subsetMatchers := make([][]*labels.Matcher, len(s.subsets))
	for i, subset := range s.subsets {
		subsetMatchers[i] = subset.matchers
	}

	clone, err := NewOperatorEvaluationStats(s.timeRange, s.memoryConsumptionTracker, subsetMatchers)
	if err != nil {
		return nil, err
	}

	clone.allSeries.CopyFrom(s.allSeries)

	for i, subset := range s.subsets {
		clone.subsets[i].CopyFrom(subset.statsTracker)
	}

	return clone, nil
}

// ExtendStepInvariantToFullRange calculates the equivalent statistics for a step invariant
// operation that is used for multiple steps in a range query.
//
// Subset definitions and their data are preserved in the expanded instance.
//
// It is the caller's responsibility to call Close on the original OperatorEvaluationStats instance.
func (s *OperatorEvaluationStats) ExtendStepInvariantToFullRange(timeRange QueryTimeRange) (*OperatorEvaluationStats, error) {
	if !s.timeRange.IsInstant {
		return nil, fmt.Errorf("cannot extend step invariant to full range for non-instant time range %v", s.timeRange)
	}

	subsetMatchers := make([][]*labels.Matcher, len(s.subsets))
	for i, subset := range s.subsets {
		subsetMatchers[i] = subset.matchers
	}

	expanded, err := NewOperatorEvaluationStats(timeRange, s.memoryConsumptionTracker, subsetMatchers)
	if err != nil {
		return nil, err
	}

	expanded.allSeries.SetFromStepInvariant(s.allSeries.samplesProcessedPerStep[0], s.allSeries.newSamplesReadPerStep[0])

	for i, subset := range s.subsets {
		expanded.subsets[i].SetFromStepInvariant(subset.samplesProcessedPerStep[0], subset.newSamplesReadPerStep[0])
	}

	return expanded, nil
}

// ComputeForSubquery calculates the statistics for a subquery's contribution to the parent query,
// based on the statistics for the inner operator of the subquery.
//
// s must be the OperatorEvaluationStats for the inner operator, evaluated over the subquery's time range.
//
// parentTimeRange is the time range of the parent query.
// subqueryRangeMilliseconds is the duration of the subquery range in milliseconds.
// subqueryTimestamp is the fixed evaluation timestamp from the @ modifier, in milliseconds since Unix epoch.
// It should be nil if the @ modifier was not used.
// subqueryOffsetMilliseconds is the subquery offset in milliseconds.
//
// s is not modified and is not closed when ComputeForSubquery returns.
func (s *OperatorEvaluationStats) ComputeForSubquery(
	parentTimeRange QueryTimeRange,
	subqueryRangeMilliseconds int64,
	subqueryTimestamp *int64,
	subqueryOffsetMilliseconds int64,
) (*OperatorEvaluationStats, error) {
	result, err := NewOperatorEvaluationStats(parentTimeRange, s.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	lastNewSamplesIdxUsed := -1

	for outerIdx := range int64(parentTimeRange.StepCount) {
		outerT := parentTimeRange.IndexTime(outerIdx)

		rangeEnd := outerT
		if subqueryTimestamp != nil {
			rangeEnd = *subqueryTimestamp
		}
		rangeEnd -= subqueryOffsetMilliseconds
		rangeStart := rangeEnd - subqueryRangeMilliseconds

		// Find the range of inner steps in (rangeStart, rangeEnd].
		firstIdx := s.timeRange.FirstPointIndexAfter(rangeStart)
		lastIdx := s.timeRange.LastPointIndexAtOrBefore(rangeEnd)

		for innerIdx := firstIdx; innerIdx <= lastIdx; innerIdx++ {
			result.samplesProcessedPerStep[outerIdx] += s.samplesProcessedPerStep[innerIdx]
		}

		firstNewIdx := max(lastNewSamplesIdxUsed, firstIdx)
		for innerIdx := firstNewIdx; innerIdx <= lastIdx; innerIdx++ {
			result.newSamplesReadPerStep[outerIdx] += s.newSamplesReadPerStep[innerIdx]
		}

		lastNewSamplesIdxUsed = lastIdx + 1
	}

	return result, nil
}

func (s *OperatorEvaluationStats) Close() {
	s.allSeries.Close()

	for _, subset := range s.subsets {
		subset.Close()
	}
}

// subsetStats holds per-step tracking data for a single subset of series defined by a set of label matchers.
type subsetStats struct {
	*statsTracker
	matchers []*labels.Matcher
}

func newSubsetsStats(matchers []*labels.Matcher, timeRange QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*subsetStats, error) {
	tracker, err := newStatsTracker(timeRange, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	return &subsetStats{statsTracker: tracker, matchers: matchers}, nil
}

// matches returns true if lbls satisfies all matchers in the subset (AND semantics).
func (s *subsetStats) matches(lbls labels.Labels) bool {
	return MatchersMatch(s.matchers, lbls)
}

type statsTracker struct {
	samplesProcessedPerStep []int64
	newSamplesReadPerStep   []int64

	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func newStatsTracker(timeRange QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*statsTracker, error) {
	samplesProcessed, err := Int64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	newSamplesRead, err := Int64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	return &statsTracker{
		samplesProcessedPerStep:  samplesProcessed[:timeRange.StepCount],
		newSamplesReadPerStep:    newSamplesRead[:timeRange.StepCount],
		memoryConsumptionTracker: memoryConsumptionTracker,
	}, nil
}

func (s *statsTracker) Add(pointIndex int64, samplesProcessed int64, newSamplesRead int64) {
	s.samplesProcessedPerStep[pointIndex] += samplesProcessed
	s.newSamplesReadPerStep[pointIndex] += newSamplesRead
}

func (s *statsTracker) SetFromStepInvariant(samplesProcessed int64, newSamplesRead int64) {
	s.newSamplesReadPerStep[0] = newSamplesRead
	for idx := range s.samplesProcessedPerStep {
		s.samplesProcessedPerStep[idx] = samplesProcessed
	}
}

func (s *statsTracker) CopyFrom(source *statsTracker) {
	copy(s.samplesProcessedPerStep, source.samplesProcessedPerStep)
	copy(s.newSamplesReadPerStep, source.newSamplesReadPerStep)
}

func (s *statsTracker) Close() {
	Int64SlicePool.Put(&s.samplesProcessedPerStep, s.memoryConsumptionTracker)
	Int64SlicePool.Put(&s.newSamplesReadPerStep, s.memoryConsumptionTracker)
}

// CombineStats retrieves and combines query stats from multiple operators.
// The caller is responsible for calling Close() on the returned stats.
func CombineStats[T StatsProvider](ctx context.Context, operators ...T) (*OperatorEvaluationStats, error) {
	var combined *OperatorEvaluationStats

	for _, op := range operators {
		stats, err := op.Stats(ctx)
		if err != nil {
			return nil, err
		}

		if combined == nil {
			combined = stats
			continue
		}

		if err := combined.Add(stats); err != nil {
			return nil, err
		}
		stats.Close()
	}

	return combined, nil
}

type StatsProvider interface {
	Stats(context.Context) (*OperatorEvaluationStats, error)
}
