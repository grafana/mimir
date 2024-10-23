// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/value.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package types

import (
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
)

type SeriesMetadata struct {
	Labels labels.Labels
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

// Next returns either a float or histogram iterating through both sets of points.
// It returns the next point with the lowest timestamp.
// If h is not nil, the value is a histogram, otherwise it is a float.
// If no more values exist ok is false.
func (i *InstantVectorSeriesDataIterator) Next() (t int64, f float64, h *histogram.FloatHistogram, ok bool) {
	if i.fIndex >= len(i.data.Floats) && i.hIndex >= len(i.data.Histograms) {
		return 0, 0, nil, false
	}

	exhaustedFloats := i.fIndex >= len(i.data.Floats)
	exhaustedHistograms := i.hIndex >= len(i.data.Histograms)
	if !exhaustedFloats && (exhaustedHistograms || i.data.Floats[i.fIndex].T < i.data.Histograms[i.hIndex].T) {
		// Return the next float
		point := i.data.Floats[i.fIndex]
		i.fIndex++
		return point.T, point.F, nil, true
	}

	// Return the next histogram
	point := i.data.Histograms[i.hIndex]
	i.hIndex++
	return point.T, 0, point.H, true
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
	// RangeStart is inclusive (ie. points with timestamp >= RangeStart are included in the range).
	RangeStart int64

	// RangeEnd is the end of the time range selected by this time step.
	// RangeEnd is the same as StepT except when the @ modifier or offsets are used, in which case
	// RangeEnd reflects the time of the underlying points, and StepT is the timestamp of the point
	// produced by the query.
	// RangeEnd is inclusive (ie. points with timestamp <= RangeEnd are included in the range).
	RangeEnd int64
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
		if metadata[0].Labels.Hash() == metadata[1].Labels.Hash() {
			return true
		}

		return false

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
	StartT               int64 // Start timestamp, in milliseconds since Unix epoch.
	EndT                 int64 // End timestamp, in milliseconds since Unix epoch.
	IntervalMilliseconds int64 // Range query interval, or 1 for instant queries. Note that this is deliberately different to parser.EvalStmt.Interval for instant queries (where it is 0) to simplify some loop conditions.

	StepCount int // 1 for instant queries.
}

func NewInstantQueryTimeRange(t time.Time) QueryTimeRange {
	ts := timestamp.FromTime(t)

	return QueryTimeRange{
		StartT:               ts,
		EndT:                 ts,
		IntervalMilliseconds: 1,
		StepCount:            1,
	}
}

func NewRangeQueryTimeRange(start time.Time, end time.Time, interval time.Duration) QueryTimeRange {
	startT := timestamp.FromTime(start)
	endT := timestamp.FromTime(end)
	IntervalMilliseconds := interval.Milliseconds()

	return QueryTimeRange{
		StartT:               startT,
		EndT:                 endT,
		IntervalMilliseconds: IntervalMilliseconds,
		StepCount:            int((endT-startT)/IntervalMilliseconds) + 1,
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
