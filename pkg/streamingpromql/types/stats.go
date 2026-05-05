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
	promstats "github.com/prometheus/prometheus/util/stats"

	"github.com/grafana/mimir/pkg/querier/stats"
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
func (s *OperatorEvaluationStats) TrackSamplesForRangeVectorSelector(stepT int64, floats *FPointRingBuffer, histograms *HPointRingBuffer, rangeStart int64, rangeEnd int64, matchesSubsets []bool) {
	if len(matchesSubsets) != len(s.subsets) {
		panic(fmt.Errorf("expected %d subsets, got %d", len(s.subsets), len(matchesSubsets)))
	}

	pointIdx := s.timeRange.PointIndex(stepT)

	allSamplesInRange := int64(floats.CountUntil(rangeEnd)) + histograms.EquivalentFloatSampleCountUntil(rangeEnd)

	newSampleRangeStart := rangeEnd - s.timeRange.IntervalMilliseconds
	if s.timeRange.IsInstant {
		newSampleRangeStart = rangeStart
	}
	samplesReadIfSubsequentStep := int64(floats.CountBetween(newSampleRangeStart, rangeEnd)) + histograms.EquivalentFloatSampleCountBetween(newSampleRangeStart, rangeEnd)

	s.allSeries.Add(pointIdx, allSamplesInRange, samplesReadIfSubsequentStep, allSamplesInRange)
	s.queryStats.AddPhysicalSamplesRead(uint64(samplesReadIfSubsequentStep))

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
		s.allSeries.Add(int64(i), other.allSeries.samplesProcessedPerStep[i], other.allSeries.samplesReadIfSubsequentStep[i], other.allSeries.samplesReadIfFirstStep[i])
	}

	for _, subset := range s.subsets {
		for i := range subset.samplesProcessedPerStep {
			subset.Add(int64(i), other.allSeries.samplesProcessedPerStep[i], other.allSeries.samplesReadIfSubsequentStep[i], other.allSeries.samplesReadIfFirstStep[i])
		}
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

	for parentIdx := range parentTimeRange.StepCount {
		parentT := parentTimeRange.IndexTime(int64(parentIdx))

		rangeEnd := parentT
		if subqueryTimestamp != nil {
			rangeEnd = *subqueryTimestamp
		}
		rangeEnd -= subqueryOffsetMilliseconds
		rangeStart := rangeEnd - subqueryRangeMilliseconds

		// Find the range of inner steps in (rangeStart, rangeEnd].
		firstInnerIdx := s.timeRange.FirstPointIndexAfter(rangeStart)
		lastInnerIdx := s.timeRange.LastPointIndexAtOrBefore(rangeEnd)
		firstNewSamplesInnerIdx := max(lastNewSamplesIdxUsed, firstInnerIdx)

		result.allSeries.SetFromSubquery(s.allSeries, parentIdx, firstInnerIdx, firstNewSamplesInnerIdx, lastInnerIdx)

		for i, subset := range result.subsets {
			subset.SetFromSubquery(s.subsets[i], parentIdx, firstInnerIdx, firstNewSamplesInnerIdx, lastInnerIdx)
		}

		lastNewSamplesIdxUsed = lastInnerIdx + 1
	}

	return result, nil
}

func (s *OperatorEvaluationStats) HasSubsets() bool {
	return len(s.subsets) > 0
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
func (s *OperatorEvaluationStats) Encode() *EncodedOperatorEvaluationStats {
	encoded := &EncodedOperatorEvaluationStats{
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

func (e *EncodedOperatorEvaluationStats) Decode(ctx context.Context, timeRange QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*OperatorEvaluationStats, error) {
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

func (s *subsetStats) SetFromSubquery(source *subsetStats, parentIdx, firstInnerIdx, firstNewSamplesInnerIdx, lastIdx int) {
	for innerIdx := firstInnerIdx; innerIdx <= lastIdx; innerIdx++ {
		s.samplesProcessedPerStep[parentIdx] += source.samplesProcessedPerStep[innerIdx]
	}

	for innerIdx := firstNewSamplesInnerIdx; innerIdx <= lastIdx; innerIdx++ {
		s.samplesReadIfSubsequentStep[parentIdx] += source.samplesReadIfSubsequentStep[innerIdx]
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
	s.samplesReadIfSubsequentStep[0] = samplesReadIfSubsequentStep
	for idx := range s.samplesProcessedPerStep {
		s.samplesProcessedPerStep[idx] = samplesProcessed
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
