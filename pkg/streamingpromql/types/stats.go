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
	"github.com/prometheus/prometheus/util/annotations"
	promstats "github.com/prometheus/prometheus/util/stats"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/limiter"
)

const timestampFieldSize = int64(unsafe.Sizeof(int64(0)))

func EquivalentFloatSampleCount(h *histogram.FloatHistogram) int64 {
	return (int64(h.Size()) + timestampFieldSize) / int64(FPointSize)
}

type OperatorEvaluationStats struct {
	timeRange                QueryTimeRange
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	queryStats               *stats.SafeStats

	allSeries *subsetStats
	subsets   []*subsetStats

	// finalizedSamplesRead is the slice returned as the QueryStats.SamplesRead slice by FinalizeAndComputePrometheusStats.
	// It is retained so that it can be returned to a pool in Close.
	finalizedSamplesRead []int64
}

// NewOperatorEvaluationStats creates a new OperatorEvaluationStats for the given time range.
//
// subsetCount is the number of subsets to track. It is the caller's responsibility to track
// which subset is which.
func NewOperatorEvaluationStats(ctx context.Context, timeRange QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, subsetCount int) (*OperatorEvaluationStats, error) {
	return NewOperatorEvaluationStatsWithQueryStats(timeRange, memoryConsumptionTracker, stats.FromContext(ctx), subsetCount)
}

func NewOperatorEvaluationStatsWithQueryStats(timeRange QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, queryStats *stats.SafeStats, subsetCount int) (*OperatorEvaluationStats, error) {
	allSeries, err := newSubsetStats(timeRange, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	stats := &OperatorEvaluationStats{
		timeRange:                timeRange,
		memoryConsumptionTracker: memoryConsumptionTracker,
		queryStats:               queryStats,

		allSeries: allSeries,
	}

	if subsetCount > 0 {
		// Only bother allocating a slice if we actually need it.
		stats.subsets = make([]*subsetStats, 0, subsetCount)

		for range subsetCount {
			subset, err := newSubsetStats(timeRange, memoryConsumptionTracker)
			if err != nil {
				return nil, err
			}

			stats.subsets = append(stats.subsets, subset)
		}
	}

	return stats, nil
}

// TrackSampleForInstantVectorSelector records a sample for an instant vector selector at output timestamp stepT.
//
// sampleCount should be 1 for float samples, or the result of EquivalentFloatSampleCount for histogram samples.
//
// TrackSampleForInstantVectorSelector should be called for each output step of an instant vector selector, even if
// the same underlying sample is used for multiple output steps.
//
// matchesSubsets must be a bitmap with a value for each subset tracked by this instance. true indicates the samples
// should be added to the corresponding subset.
//
// The samples are also recorded in the number of physical samples read in the overall query stats.
func (s *OperatorEvaluationStats) TrackSampleForInstantVectorSelector(stepT int64, sampleCount int64, matchesSubsets []bool) {
	if len(matchesSubsets) != len(s.subsets) {
		panic(fmt.Errorf("expected %d subsets, got %d", len(s.subsets), len(matchesSubsets)))
	}

	pointIdx := s.timeRange.PointIndex(stepT)

	s.allSeries.Add(pointIdx, sampleCount, sampleCount, sampleCount)
	s.queryStats.AddPhysicalSamplesRead(uint64(sampleCount))

	for subsetIdx, subset := range s.subsets {
		if matchesSubsets[subsetIdx] {
			subset.Add(pointIdx, sampleCount, sampleCount, sampleCount)
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
// matchesSubsets must be a bitmap with a value for each subset tracked by this instance. true indicates the samples
// should be added to the corresponding subset.
//
// The samples are also recorded in the number of physical samples read in the overall query stats.
func (s *OperatorEvaluationStats) TrackSamplesForRangeVectorSelector(stepT int64, floats *FPointRingBuffer, histograms *HPointRingBuffer, rangeStart int64, rangeEnd int64, haveTimestamp bool, matchesSubsets []bool) {
	if len(matchesSubsets) != len(s.subsets) {
		panic(fmt.Errorf("expected %d subsets, got %d", len(s.subsets), len(matchesSubsets)))
	}

	pointIdx := s.timeRange.PointIndex(stepT)

	allSamplesInRange := int64(floats.CountUntil(rangeEnd)) + histograms.EquivalentFloatSampleCountUntil(rangeEnd)

	newSampleRangeStart := rangeEnd - s.timeRange.IntervalMilliseconds
	if s.timeRange.IsInstant {
		newSampleRangeStart = rangeStart
	}

	samplesReadIfSubsequentStep := int64(0)

	// If the subquery has a fixed timestamp, then subsequent steps would not read any samples.
	if !haveTimestamp {
		samplesReadIfSubsequentStep = int64(floats.CountBetween(newSampleRangeStart, rangeEnd)) + histograms.EquivalentFloatSampleCountBetween(newSampleRangeStart, rangeEnd)
	}

	s.allSeries.Add(pointIdx, allSamplesInRange, samplesReadIfSubsequentStep, allSamplesInRange)

	if pointIdx == 0 {
		s.queryStats.AddPhysicalSamplesRead(uint64(allSamplesInRange))
	} else {
		s.queryStats.AddPhysicalSamplesRead(uint64(samplesReadIfSubsequentStep))
	}

	for subsetIdx, subset := range s.subsets {
		if matchesSubsets[subsetIdx] {
			subset.Add(pointIdx, allSamplesInRange, samplesReadIfSubsequentStep, allSamplesInRange)
		}
	}
}

// Add adds the statistics from other to this instance.
//
// This instance is modified in-place.
//
// Both instances must be for the same time range.
//
// Only the receiver may have subsets. If the other instance has subsets, an error is returned.
// The samples from the other instance are added to all the receiver's subsets.
func (s *OperatorEvaluationStats) Add(other *OperatorEvaluationStats) error {
	if !s.timeRange.Equal(other.timeRange) {
		return errors.New("cannot add OperatorEvaluationStats with different time ranges")
	}

	if len(other.subsets) > 0 {
		return errors.New("cannot add an OperatorEvaluationStats instance that has subsets")
	}

	for i := range s.timeRange.StepCount {
		s.allSeries.Add(int64(i), other.allSeries.samplesProcessedPerStep[i], other.allSeries.samplesReadIfSubsequentStep[i], other.allSeries.samplesReadIfFirstStep[i])
	}

	for _, subset := range s.subsets {
		for i := range subset.samplesProcessedPerStep {
			subset.Add(int64(i), other.allSeries.samplesProcessedPerStep[i], other.allSeries.samplesReadIfSubsequentStep[i], other.allSeries.samplesReadIfFirstStep[i])
		}
	}

	return nil
}

// AddSingleStep adds the statistics from other to this instance.
//
// Both instances must have a single step, but may have different time ranges.
//
// Both instances must have the same number of subsets.
//
// This instance is modified in place.
func (s *OperatorEvaluationStats) AddSingleStep(other *OperatorEvaluationStats) error {
	if s.timeRange.StepCount != 1 {
		return fmt.Errorf("cannot add a single step to a OperatorEvaluationStats instance with %v steps", s.timeRange.StepCount)
	}

	if other.timeRange.StepCount != 1 {
		return fmt.Errorf("cannot add a single step to a OperatorEvaluationStats instance from another instance with %v steps", other.timeRange.StepCount)
	}

	if len(s.subsets) != len(other.subsets) {
		return fmt.Errorf("cannot add a single step to a OperatorEvaluationStats instance with %v subsets from another instance with %v subsets", len(s.subsets), len(other.subsets))
	}

	s.allSeries.Add(int64(0), other.allSeries.samplesProcessedPerStep[0], other.allSeries.samplesReadIfSubsequentStep[0], other.allSeries.samplesReadIfFirstStep[0])

	for subsetIdx, subset := range s.subsets {
		otherSubset := other.subsets[subsetIdx]
		subset.Add(int64(0), otherSubset.samplesProcessedPerStep[0], otherSubset.samplesReadIfSubsequentStep[0], otherSubset.samplesReadIfFirstStep[0])
	}

	return nil
}

// AddSubRange adds the statistics from other to this instance.
//
// This instance is modified in-place.
//
// An error is returned if the other instance's steps do not align with this instance's steps.
// Steps outside the range of this instance's time range are ignored.
//
// An error is returned if the instances have a different number of subsets.
func (s *OperatorEvaluationStats) AddSubRange(other *OperatorEvaluationStats) error {
	if len(s.subsets) != len(other.subsets) {
		return fmt.Errorf("cannot add a sub-range with %d subset(s) to an OperatorEvaluationStats instance with %d subset(s)", len(other.subsets), len(s.subsets))
	}

	if s.timeRange.StepCount > 1 && other.timeRange.StepCount > 1 && other.timeRange.IntervalMilliseconds != s.timeRange.IntervalMilliseconds {
		return fmt.Errorf("cannot add a sub-range from an OperatorEvaluationStats instance with multiple steps and interval %v ms to another instance with multiple steps and interval %v ms", other.timeRange.IntervalMilliseconds, s.timeRange.IntervalMilliseconds)
	}

	if s.timeRange.IndexTime(s.timeRange.PointIndex(other.timeRange.StartT)) != other.timeRange.StartT || s.timeRange.StartT > other.timeRange.EndT || s.timeRange.EndT < other.timeRange.StartT {
		return fmt.Errorf("cannot add a sub-range from an OperatorEvaluationStats instance with time range [%v, %v] and interval %v ms to another instance with time range [%v, %v] and interval %v ms", other.timeRange.StartT, other.timeRange.EndT, other.timeRange.IntervalMilliseconds, s.timeRange.StartT, s.timeRange.EndT, s.timeRange.IntervalMilliseconds)
	}

	firstOtherIndex := int64(0)
	lastOtherIndex := int64(other.timeRange.StepCount - 1)
	if other.timeRange.StartT < s.timeRange.StartT {
		firstOtherIndex = other.timeRange.PointIndex(s.timeRange.StartT)
	}
	if other.timeRange.EndT > s.timeRange.EndT {
		lastOtherIndex = other.timeRange.PointIndex(s.timeRange.EndT)
	}

	nextIndexInThisInstance := s.timeRange.PointIndex(other.timeRange.IndexTime(firstOtherIndex))

	for otherIndex := firstOtherIndex; otherIndex <= lastOtherIndex; otherIndex++ {
		s.allSeries.Add(nextIndexInThisInstance, other.allSeries.samplesProcessedPerStep[otherIndex], other.allSeries.samplesReadIfSubsequentStep[otherIndex], other.allSeries.samplesReadIfFirstStep[otherIndex])

		for subsetIdx, subset := range s.subsets {
			otherSubset := other.subsets[subsetIdx]
			subset.Add(nextIndexInThisInstance, otherSubset.samplesProcessedPerStep[otherIndex], otherSubset.samplesReadIfSubsequentStep[otherIndex], otherSubset.samplesReadIfFirstStep[otherIndex])
		}

		nextIndexInThisInstance++
	}

	return nil
}

func (s *OperatorEvaluationStats) newEmptyInstanceWithSameSubsets(timeRange QueryTimeRange) (*OperatorEvaluationStats, error) {
	return NewOperatorEvaluationStatsWithQueryStats(timeRange, s.memoryConsumptionTracker, s.queryStats, len(s.subsets))
}

// Clone returns a copy of this OperatorEvaluationStats instance, including any subset definitions and their data.
func (s *OperatorEvaluationStats) Clone() (*OperatorEvaluationStats, error) {
	clone, err := s.newEmptyInstanceWithSameSubsets(s.timeRange)
	if err != nil {
		return nil, err
	}

	clone.allSeries.CopyFrom(s.allSeries)

	for i, subset := range s.subsets {
		clone.subsets[i].CopyFrom(subset)
	}

	return clone, nil
}

// CloneSingleStep returns a new OperatorEvaluationStats instance containing the sample counts for the single step
// in the provided time range.
//
// The returned instance will have the same subset definitions as the original.
func (s *OperatorEvaluationStats) CloneSingleStep(timeRange QueryTimeRange) (*OperatorEvaluationStats, error) {
	if timeRange.StepCount != 1 {
		return nil, fmt.Errorf("cannot clone single step of OperatorEvaluationStats for time range with %v steps", timeRange.StepCount)
	}

	stepIdx := s.timeRange.PointIndex(timeRange.StartT)

	if s.timeRange.IndexTime(stepIdx) != timeRange.StartT {
		return nil, fmt.Errorf("cannot clone single step of OperatorEvaluationStats because the desired time %v is not aligned with the steps of the source (start time %v, step %v)", timeRange.StartT, s.timeRange.StartT, s.timeRange.IntervalMilliseconds)
	}

	if stepIdx < 0 || stepIdx >= int64(s.timeRange.StepCount) {
		return nil, fmt.Errorf("cannot clone single step of OperatorEvaluationStats because the desired time %v is outside the source time range (start time %v, end time %v)", timeRange.StartT, s.timeRange.StartT, s.timeRange.EndT)
	}

	singleStepStats, err := s.newEmptyInstanceWithSameSubsets(timeRange)
	if err != nil {
		return nil, err
	}

	singleStepStats.allSeries.CopySingleStepFrom(s.allSeries, stepIdx)

	for i, subset := range s.subsets {
		singleStepStats.subsets[i].CopySingleStepFrom(subset, stepIdx)
	}

	return singleStepStats, nil
}

// UseSubset replaces the unfiltered statistics on this instance with those from the given subset,
// and removes all other subsets.
func (s *OperatorEvaluationStats) UseSubset(idx int) {
	s.allSeries.Close()

	s.allSeries = s.subsets[idx]
	s.subsets[idx] = nil

	s.RemoveAllSubsets()
}

// RemoveAllSubsets removes all subsets from this instance, leaving only the unfiltered statistics.
func (s *OperatorEvaluationStats) RemoveAllSubsets() {
	for _, subset := range s.subsets {
		if subset == nil {
			// Don't bother trying to close a subset that was just removed by UseSubset.
			continue
		}

		subset.Close()
	}

	s.subsets = nil
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

	expanded, err := s.newEmptyInstanceWithSameSubsets(timeRange)
	if err != nil {
		return nil, err
	}

	expanded.allSeries.SetFromStepInvariant(s.allSeries.samplesProcessedPerStep[0], s.allSeries.samplesReadIfSubsequentStep[0], s.allSeries.samplesReadIfFirstStep[0])

	for i, subset := range s.subsets {
		expanded.subsets[i].SetFromStepInvariant(subset.samplesProcessedPerStep[0], subset.samplesReadIfSubsequentStep[0], subset.samplesReadIfFirstStep[0])
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
	result, err := s.newEmptyInstanceWithSameSubsets(parentTimeRange)
	if err != nil {
		return nil, err
	}

	lastNewSamplesIdxUsed := -1
	haveTimestamp := subqueryTimestamp != nil

	for parentIdx := range parentTimeRange.StepCount {
		parentT := parentTimeRange.IndexTime(int64(parentIdx))

		rangeEnd := parentT
		if haveTimestamp {
			rangeEnd = *subqueryTimestamp
		}
		rangeEnd -= subqueryOffsetMilliseconds
		rangeStart := rangeEnd - subqueryRangeMilliseconds

		// Find the range of inner steps in (rangeStart, rangeEnd].
		firstInnerIdx := s.timeRange.FirstPointIndexAfter(rangeStart)
		lastInnerIdx := s.timeRange.LastPointIndexAtOrBefore(rangeEnd)
		firstNewSamplesInnerIdx := max(lastNewSamplesIdxUsed, firstInnerIdx)

		result.allSeries.SetFromSubquery(s.allSeries, parentIdx, firstInnerIdx, firstNewSamplesInnerIdx, lastInnerIdx, haveTimestamp)

		for i, subset := range result.subsets {
			subset.SetFromSubquery(s.subsets[i], parentIdx, firstInnerIdx, firstNewSamplesInnerIdx, lastInnerIdx, haveTimestamp)
		}

		lastNewSamplesIdxUsed = lastInnerIdx + 1
	}

	return result, nil
}

func (s *OperatorEvaluationStats) HasSubsets() bool {
	return len(s.subsets) > 0
}

func (s *OperatorEvaluationStats) GetSubsetCount() int {
	return len(s.subsets)
}

func (s *OperatorEvaluationStats) GetTotalSamplesProcessed() int64 {
	return sum(s.allSeries.samplesProcessedPerStep)
}

// HalveCounts divides all sample counters in this instance by 2, rounding down.
//
// This is used to correct for double-counting when both the sum and count legs
// of a sharded avg() expression process the same underlying data.
func (s *OperatorEvaluationStats) HalveCounts() {
	s.allSeries.HalveCounts()

	for _, subset := range s.subsets {
		subset.HalveCounts()
	}
}

// FinalizeAndComputePrometheusStats computes an equivalent QuerySamples instance as expected
// by Prometheus' Query.Stats() method.
//
// Mutating the returned QuerySamples instance may result in changes to this OperatorEvaluationStats instance.
//
// The returned QuerySamples instance is only valid until Close is called, as it contains references to slices
// that will be returned to a pool when Close is called.
func (s *OperatorEvaluationStats) FinalizeAndComputePrometheusStats() (*promstats.QuerySamples, error) {
	var err error
	s.finalizedSamplesRead, err = Int64SlicePool.Get(len(s.allSeries.samplesReadIfSubsequentStep), s.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	s.finalizedSamplesRead = s.finalizedSamplesRead[:len(s.allSeries.samplesReadIfSubsequentStep)]
	s.finalizedSamplesRead[0] = s.allSeries.samplesReadIfFirstStep[0]
	copy(s.finalizedSamplesRead[1:], s.allSeries.samplesReadIfSubsequentStep[1:])

	return &promstats.QuerySamples{
		TotalSamples:        sum(s.allSeries.samplesProcessedPerStep),
		TotalSamplesPerStep: s.allSeries.samplesProcessedPerStep,
		SamplesRead:         sum(s.finalizedSamplesRead),
		SamplesReadPerStep:  s.finalizedSamplesRead,
		EnablePerStepStats:  true,
		Interval:            s.timeRange.IntervalMilliseconds,
		StartTimestamp:      s.timeRange.StartT,
	}, nil
}

func sum(s []int64) int64 {
	var sum int64
	for _, v := range s {
		sum += v
	}
	return sum
}

// Encode returns the encoded form of this instance, suitable for serialization.
// The encoded form may share memory with this instance, and so may be modified
// if this instance is modified, and becomes invalid when this instance is closed.
func (s *OperatorEvaluationStats) Encode() EncodedOperatorEvaluationStats {
	encoded := EncodedOperatorEvaluationStats{
		TimeRange: s.timeRange.Encode(),
		AllSeries: s.allSeries.Encode(),
	}

	if len(s.subsets) > 0 {
		// Only bother allocating a slice for subsets if there are any.
		encoded.Subsets = make([]EncodedSubsetStats, 0, len(s.subsets))
		for _, subset := range s.subsets {
			encoded.Subsets = append(encoded.Subsets, subset.Encode())
		}
	}

	return encoded
}

func (e *EncodedOperatorEvaluationStats) Decode(ctx context.Context, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*OperatorEvaluationStats, error) {
	timeRange := e.TimeRange.Decode()
	allSeries, err := e.AllSeries.decode(timeRange, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	decoded := &OperatorEvaluationStats{
		timeRange:                timeRange,
		memoryConsumptionTracker: memoryConsumptionTracker,
		queryStats:               stats.FromContext(ctx),
		allSeries:                allSeries,
	}

	if len(e.Subsets) > 0 {
		// Only bother allocating a slice for subsets if there are any.
		decoded.subsets = make([]*subsetStats, 0, len(e.Subsets))

		for _, encodedSubset := range e.Subsets {
			decodedSubset, err := encodedSubset.decode(timeRange, memoryConsumptionTracker)
			if err != nil {
				return nil, err
			}

			decoded.subsets = append(decoded.subsets, decodedSubset)
		}
	}

	return decoded, nil
}

func (s *OperatorEvaluationStats) Close() {
	s.allSeries.Close()

	for _, subset := range s.subsets {
		subset.Close()
	}

	Int64SlicePool.Put(&s.finalizedSamplesRead, s.memoryConsumptionTracker)
}

type subsetStats struct {
	samplesProcessedPerStep     []int64
	samplesReadIfSubsequentStep []int64
	samplesReadIfFirstStep      []int64

	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func newSubsetStats(timeRange QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*subsetStats, error) {
	samplesProcessed, err := Int64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	samplesReadIfSubsequentStep, err := Int64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	samplesReadIfFirstStep, err := Int64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	return &subsetStats{
		samplesProcessedPerStep:     samplesProcessed[:timeRange.StepCount],
		samplesReadIfSubsequentStep: samplesReadIfSubsequentStep[:timeRange.StepCount],
		samplesReadIfFirstStep:      samplesReadIfFirstStep[:timeRange.StepCount],
		memoryConsumptionTracker:    memoryConsumptionTracker,
	}, nil
}

func (s *subsetStats) Add(pointIndex int64, samplesProcessed int64, samplesReadIfSubsequentStep int64, samplesReadIfFirstStep int64) {
	s.samplesProcessedPerStep[pointIndex] += samplesProcessed
	s.samplesReadIfSubsequentStep[pointIndex] += samplesReadIfSubsequentStep
	s.samplesReadIfFirstStep[pointIndex] += samplesReadIfFirstStep
}

func (s *subsetStats) SetFromSubquery(source *subsetStats, parentIdx, firstInnerIdx, firstNewSamplesInnerIdx, lastIdx int, haveTimestamp bool) {
	for innerIdx := firstInnerIdx; innerIdx <= lastIdx; innerIdx++ {
		s.samplesProcessedPerStep[parentIdx] += source.samplesProcessedPerStep[innerIdx]
	}

	// If the subquery has a fixed timestamp, then subsequent steps would not read any samples.
	if !haveTimestamp {
		for innerIdx := firstNewSamplesInnerIdx; innerIdx <= lastIdx; innerIdx++ {
			s.samplesReadIfSubsequentStep[parentIdx] += source.samplesReadIfSubsequentStep[innerIdx]
		}
	}

	for innerIdx := firstInnerIdx; innerIdx <= lastIdx; innerIdx++ {
		if innerIdx == firstInnerIdx {
			s.samplesReadIfFirstStep[parentIdx] += source.samplesReadIfFirstStep[innerIdx]
		} else {
			s.samplesReadIfFirstStep[parentIdx] += source.samplesReadIfSubsequentStep[innerIdx]
		}
	}
}

func (s *subsetStats) SetFromStepInvariant(samplesProcessed int64, samplesReadIfSubsequentStep int64, samplesReadIfFirstStep int64) {
	for idx := range s.samplesProcessedPerStep {
		s.samplesProcessedPerStep[idx] = samplesProcessed
	}
	for idx := range s.samplesReadIfSubsequentStep {
		s.samplesReadIfSubsequentStep[idx] = samplesReadIfSubsequentStep
	}
	for idx := range s.samplesReadIfFirstStep {
		s.samplesReadIfFirstStep[idx] = samplesReadIfFirstStep
	}
}

func (s *subsetStats) CopyFrom(source *subsetStats) {
	copy(s.samplesProcessedPerStep, source.samplesProcessedPerStep)
	copy(s.samplesReadIfSubsequentStep, source.samplesReadIfSubsequentStep)
	copy(s.samplesReadIfFirstStep, source.samplesReadIfFirstStep)
}

func (s *subsetStats) CopySingleStepFrom(source *subsetStats, stepIdx int64) {
	s.samplesProcessedPerStep[0] = source.samplesProcessedPerStep[stepIdx]
	s.samplesReadIfSubsequentStep[0] = source.samplesReadIfSubsequentStep[stepIdx]
	s.samplesReadIfFirstStep[0] = source.samplesReadIfFirstStep[stepIdx]
}

func (s *subsetStats) HalveCounts() {
	for i := range s.samplesProcessedPerStep {
		s.samplesProcessedPerStep[i] /= 2
	}

	for i := range s.samplesReadIfSubsequentStep {
		s.samplesReadIfSubsequentStep[i] /= 2
	}

	for i := range s.samplesReadIfFirstStep {
		s.samplesReadIfFirstStep[i] /= 2
	}
}

func (s *subsetStats) Encode() EncodedSubsetStats {
	return EncodedSubsetStats{
		SamplesProcessedPerStep:     s.samplesProcessedPerStep,
		SamplesReadIfSubsequentStep: s.samplesReadIfSubsequentStep,
		SamplesReadIfFirstStep:      s.samplesReadIfFirstStep,
	}
}

func (e *EncodedSubsetStats) decode(timeRange QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*subsetStats, error) {
	if len(e.SamplesProcessedPerStep) != timeRange.StepCount {
		return nil, fmt.Errorf("number of 'samples processed' steps in encoded form (%d) does not match expected (%d)", len(e.SamplesProcessedPerStep), timeRange.StepCount)
	}

	if len(e.SamplesReadIfSubsequentStep) != timeRange.StepCount {
		return nil, fmt.Errorf("number of 'samples read if subsequent step' steps in encoded form (%d) does not match expected (%d)", len(e.SamplesReadIfSubsequentStep), timeRange.StepCount)
	}

	if len(e.SamplesReadIfFirstStep) != timeRange.StepCount {
		return nil, fmt.Errorf("number of 'samples read if first step' steps in encoded form (%d) does not match expected (%d)", len(e.SamplesReadIfFirstStep), timeRange.StepCount)
	}

	if err := memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(e.SamplesProcessedPerStep)+cap(e.SamplesReadIfSubsequentStep)+cap(e.SamplesReadIfFirstStep))*Int64Size, limiter.Int64Slices); err != nil {
		return nil, err
	}

	return &subsetStats{
		samplesProcessedPerStep:     e.SamplesProcessedPerStep,
		samplesReadIfSubsequentStep: e.SamplesReadIfSubsequentStep,
		samplesReadIfFirstStep:      e.SamplesReadIfFirstStep,
		memoryConsumptionTracker:    memoryConsumptionTracker,
	}, nil
}

func (s *subsetStats) Close() {
	Int64SlicePool.Put(&s.samplesProcessedPerStep, s.memoryConsumptionTracker)
	Int64SlicePool.Put(&s.samplesReadIfSubsequentStep, s.memoryConsumptionTracker)
	Int64SlicePool.Put(&s.samplesReadIfFirstStep, s.memoryConsumptionTracker)
}

// FinalizeAndCombine retrieves and combines query stats and annotations from multiple operators.
// The caller is responsible for calling Close() on the returned stats.
// The returned annotations may be nil if none of the operators reported any annotations.
func FinalizeAndCombine[T Finalizer](ctx context.Context, operators ...T) (*OperatorEvaluationStats, annotations.Annotations, error) {
	var combinedStats *OperatorEvaluationStats
	var combinedAnnos annotations.Annotations

	for _, op := range operators {
		stats, annos, err := op.Finalize(ctx)
		if err != nil {
			return nil, nil, err
		}

		if combinedStats == nil {
			combinedStats = stats
		} else {
			if err := combinedStats.Add(stats); err != nil {
				return nil, nil, err
			}
			stats.Close()
		}

		if combinedAnnos == nil {
			combinedAnnos = annos
		} else {
			combinedAnnos.Merge(annos)
		}
	}

	return combinedStats, combinedAnnos, nil
}

type Finalizer interface {
	Finalize(context.Context) (*OperatorEvaluationStats, annotations.Annotations, error)
}
