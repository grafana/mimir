// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/value.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package types

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/util/limiter"
)

type SeriesMetadata struct {
	Labels   labels.Labels
	DropName bool
}

// AppendSeriesMetadata appends base SeriesMetadataSlice with the provided otherSeriesMetadata.
func AppendSeriesMetadata(tracker *limiter.MemoryConsumptionTracker, base []SeriesMetadata, otherSeriesMetadata ...SeriesMetadata) ([]SeriesMetadata, error) {
	for _, metadata := range otherSeriesMetadata {
		err := tracker.IncreaseMemoryConsumptionForLabels(metadata.Labels)
		if err != nil {
			return nil, err
		}
	}
	return append(base, otherSeriesMetadata...), nil
}

type InstantVectorSeriesData struct {
	// Floats contains floating point samples for this series.
	// Samples must be sorted in timestamp order, earliest timestamps first.
	// Samples must not have duplicate timestamps.
	Floats []promql.FPoint

	// Histograms contains histogram samples for this series.
	// Samples must be sorted in timestamp order, earliest timestamps first.
	// Samples must not have duplicate timestamps.
	// Samples must not share FloatHistogram instances.
	Histograms []promql.HPoint
}

func (d InstantVectorSeriesData) Clone(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (InstantVectorSeriesData, error) {
	clone := InstantVectorSeriesData{}

	var err error
	clone.Floats, err = FPointSlicePool.Get(len(d.Floats), memoryConsumptionTracker)
	if err != nil {
		return InstantVectorSeriesData{}, err
	}

	clone.Floats = clone.Floats[:len(d.Floats)]
	copy(clone.Floats, d.Floats) // We can do a simple copy here, as FPoints don't contain pointers.

	clone.Histograms, err = HPointSlicePool.Get(len(d.Histograms), memoryConsumptionTracker)
	if err != nil {
		return InstantVectorSeriesData{}, err
	}

	clone.Histograms = clone.Histograms[:len(d.Histograms)]

	for i, p := range d.Histograms {
		clone.Histograms[i].T = p.T

		// Reuse existing FloatHistogram instance if we can.
		if clone.Histograms[i].H == nil {
			clone.Histograms[i].H = p.H.Copy()
		} else {
			p.H.CopyTo(clone.Histograms[i].H)
		}
	}

	return clone, nil
}

type InstantVectorSeriesDataIterator struct {
	data   InstantVectorSeriesData
	fIndex int
	hIndex int
}

func (i *InstantVectorSeriesDataIterator) Reset(data InstantVectorSeriesData) {
	i.fIndex = 0
	i.hIndex = 0
	i.data = data
}

// Next returns the value of the next point (either a float or histogram) iterating through both sets of points in timestamp order.
// If the next value is a histogram, ok is true, h is not nil and hIndex is the index into the series' histograms slice.
// If the next value is a float, ok is true and h is nil.
// If there are no more values then ok is false.
func (i *InstantVectorSeriesDataIterator) Next() (t int64, f float64, h *histogram.FloatHistogram, hIndex int, ok bool) {
	if i.fIndex >= len(i.data.Floats) && i.hIndex >= len(i.data.Histograms) {
		return 0, 0, nil, -1, false
	}

	exhaustedFloats := i.fIndex >= len(i.data.Floats)
	exhaustedHistograms := i.hIndex >= len(i.data.Histograms)
	if !exhaustedFloats && (exhaustedHistograms || i.data.Floats[i.fIndex].T < i.data.Histograms[i.hIndex].T) {
		// Return the next float
		point := i.data.Floats[i.fIndex]
		i.fIndex++
		return point.T, point.F, nil, -1, true
	}

	// Return the next histogram
	point := i.data.Histograms[i.hIndex]
	i.hIndex++
	return point.T, 0, point.H, i.hIndex - 1, true
}

// RangeVectorStepData contains the data and timestamps associated with a single time step produced by a
// RangeVectorOperator.
//
// All timestamps are in milliseconds since the Unix epoch.
//
// For example, if the operator represents the selector "some_metric[5m]", and this time step is for
// 2024-05-02T00:00:00Z, then:
//   - StepT is 1714608000000 (2024-05-02T00:00:00Z)
//   - RangeStart is 1714607700000 (2024-05-01T23:55:00Z)
//   - RangeEnd is 1714608000000 (2024-05-02T00:00:00Z)
//
// If the operator represents the selector "some_metric[5m] @ 1712016000", and this time step is for
// 2024-05-02T00:00:00Z, then:
//   - StepT is 1714608000000 (2024-05-02T00:00:00Z)
//   - RangeStart is 1712015700000 (2024-04-01T23:55:00Z)
//   - RangeEnd is 1712016000000 (2024-04-02T00:00:00Z)
type RangeVectorStepData struct {
	// Floats contains the float samples for this time step.
	Floats *FPointRingBufferView

	// Histograms contains the histogram samples for this time step.
	//
	// FloatHistogram instances in the buffer must not be modified as they may be returned for subsequent steps.
	// FloatHistogram instances that are retained after the next call to NextStepSamples must be copied, as they
	// may be modified on subsequent calls to NextStepSamples.
	Histograms *HPointRingBufferView

	// StepT is the timestamp of this time step.
	StepT int64

	// RangeStart is the beginning of the time range selected by this time step.
	// RangeStart is exclusive (ie. points with timestamp > RangeStart are included in the range,
	// and the point with timestamp == RangeStart is excluded).
	RangeStart int64

	// RangeEnd is the end of the time range selected by this time step.
	// RangeEnd is the same as StepT except when the @ modifier or offsets are used, in which case
	// RangeEnd reflects the time of the underlying points, and StepT is the timestamp of the point
	// produced by the query.
	// RangeEnd is inclusive (ie. points with timestamp <= RangeEnd are included in the range).
	RangeEnd int64

	// Anchored is set to true when the anchored modifier has been requested on a range query
	Anchored bool

	// Smoothed is set to true when the smoothed modifier has been requested on a range query
	Smoothed bool
}

// SubStep returns a substep with the same StepT but filtered to range (rangeStart, rangeEnd].
// If previousSubStep is provided, it will be reused to create the new substep. previousSubStep must be a previous
// substep for the same parent step, and the next step is assumed to cover a later range (we only start searching from
// after the samples of the previous subviews).
func (s *RangeVectorStepData) SubStep(rangeStart, rangeEnd int64, previousSubStep *RangeVectorStepData) (*RangeVectorStepData, error) {
	if s.Anchored || s.Smoothed {
		return nil, errors.New("substep not supported for range vectors with anchored or smoothed modifiers")
	}

	if rangeStart < s.RangeStart {
		return nil, fmt.Errorf("substep start (%d) is before parent step's start (%d)", rangeStart, s.RangeStart)
	}
	if rangeEnd > s.RangeEnd {
		return nil, fmt.Errorf("substep end (%d) is after parent step's end (%d)", rangeEnd, s.RangeEnd)
	}
	if rangeStart >= rangeEnd {
		return nil, fmt.Errorf("substep start (%d) must be less than end (%d)", rangeStart, rangeEnd)
	}

	if previousSubStep == nil {
		previousSubStep = &RangeVectorStepData{}
	}

	previousSubStep.StepT = s.StepT
	previousSubStep.RangeStart = rangeStart
	previousSubStep.RangeEnd = rangeEnd

	previousSubStep.Floats = s.Floats.SubView(rangeStart, rangeEnd, previousSubStep.Floats)
	previousSubStep.Histograms = s.Histograms.SubView(rangeStart, rangeEnd, previousSubStep.Histograms)

	return previousSubStep, nil
}

type ScalarData struct {
	// Samples contains floating point samples for this series.
	// Samples must be sorted in timestamp order, earliest timestamps first.
	// Samples must not have duplicate timestamps.
	Samples []promql.FPoint
}

func HasDuplicateSeries(metadata []SeriesMetadata) bool {
	// Note that there's a risk here that we incorrectly flag two series as duplicates when in reality they're different
	// due to hash collisions.
	// However, the hashes are 64-bit integers, so the likelihood of collisions is very small, and this is the same method
	// that Prometheus' engine uses, so we'll at least be consistent with that.

	switch len(metadata) {
	case 0, 1:
		return false
	case 2:
		return metadata[0].Labels.Hash() == metadata[1].Labels.Hash()
	default:
		seen := make(map[uint64]struct{}, len(metadata))

		for _, m := range metadata {
			hash := m.Labels.Hash()

			if _, alreadySeen := seen[hash]; alreadySeen {
				return true
			}

			seen[hash] = struct{}{}
		}

		return false
	}
}

type QueryTimeRange struct {
	StartT               int64 `json:"startT"`               // Start timestamp, in milliseconds since Unix epoch.
	EndT                 int64 `json:"endT"`                 // End timestamp, in milliseconds since Unix epoch.
	IntervalMilliseconds int64 `json:"intervalMilliseconds"` // Range query interval, or 1 for instant queries. Note that this is deliberately different to parser.EvalStmt.Interval for instant queries (where it is 0) to simplify some loop conditions.

	StepCount int  `json:"-"` // 1 for instant queries.
	IsInstant bool `json:"isInstant,omitempty"`
}

func NewInstantQueryTimeRange(t time.Time) QueryTimeRange {
	ts := timestamp.FromTime(t)

	return QueryTimeRange{
		StartT:               ts,
		EndT:                 ts,
		IntervalMilliseconds: 1,
		StepCount:            1,
		IsInstant:            true,
	}
}

func NewRangeQueryTimeRange(start time.Time, end time.Time, interval time.Duration) QueryTimeRange {
	startT := timestamp.FromTime(start)
	endT := timestamp.FromTime(end)
	intervalMilliseconds := interval.Milliseconds()
	stepCount := int((endT-startT)/intervalMilliseconds) + 1

	if startT > endT {
		// It is valid for the timestamps to be around the wrong way: this can happen in the case where a subquery selects no points in the range.
		// However, if this has happened, we need to make sure the step count is not a negative number to prevent trying to allocate a per-step slice
		// with a negative number of elements.
		stepCount = 0
	}

	return QueryTimeRange{
		StartT:               startT,
		EndT:                 endT,
		IntervalMilliseconds: intervalMilliseconds,
		StepCount:            stepCount,
		IsInstant:            false,
	}
}

// PointIndex returns the index in the QueryTimeRange that the timestamp, t, falls on.
// t must be in line with IntervalMs (ie the step).
func (q *QueryTimeRange) PointIndex(t int64) int64 {
	return (t - q.StartT) / q.IntervalMilliseconds
}

// IndexTime returns the timestamp that the point index, p, falls on.
// p must be less than StepCount
func (q *QueryTimeRange) IndexTime(p int64) int64 {
	return q.StartT + p*q.IntervalMilliseconds
}

func (q *QueryTimeRange) Equal(other QueryTimeRange) bool {
	return q.StartT == other.StartT &&
		q.EndT == other.EndT &&
		q.IntervalMilliseconds == other.IntervalMilliseconds &&
		q.StepCount == other.StepCount &&
		q.IsInstant == other.IsInstant
}
