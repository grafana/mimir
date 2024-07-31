// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
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
	// HPoint contains a pointer to a histogram, and consecutive HPoints may contain a reference
	// to the same FloatHistogram.
	// It is therefore important to check for references to the same FloatHistogram in
	// subsequent points before mutating it.
	Histograms []promql.HPoint
}

type InstantVectorSeriesDataIterator struct {
	data   *InstantVectorSeriesData
	fIndex int
	hIndex int
}

func NewInstantVectorSeriesDataIterator(data *InstantVectorSeriesData) *InstantVectorSeriesDataIterator {
	return &InstantVectorSeriesDataIterator{
		data: data,
	}
}

// Next returns either a float or histogram iterating through both sets of points.
// It returns the next point with the lowest timestamp.
// If h is not nil, the value is a histogram, otherwise it is a float.
// If no more values exist ok is false.
func (i *InstantVectorSeriesDataIterator) Next() (t int64, f float64, h *histogram.FloatHistogram, ok bool) {
	if i.fIndex >= len(i.data.Floats) && i.hIndex >= len(i.data.Histograms) {
		return 0, 0, nil, false
	}

	if i.fIndex >= len(i.data.Floats) {
		i.hIndex++
		return i.data.Histograms[i.hIndex-1].T, 0, i.data.Histograms[i.hIndex-1].H, true
	}

	if i.hIndex >= len(i.data.Histograms) {
		i.fIndex++
		return i.data.Floats[i.fIndex-1].T, i.data.Floats[i.fIndex-1].F, nil, true
	}

	// NOTE: Floats and Histograms should never exist at the same timestamp, T.
	if i.data.Floats[i.fIndex].T < i.data.Histograms[i.hIndex].T {
		i.fIndex++
		return i.data.Floats[i.fIndex-1].T, i.data.Floats[i.fIndex-1].F, nil, true
	} else {
		i.hIndex++
		return i.data.Histograms[i.hIndex-1].T, 0, i.data.Histograms[i.hIndex-1].H, true
	}
}

// RangeVectorStepData contains the timestamps associated with a single time step produced by a
// RangeVectorOperator.
//
// All values are in milliseconds since the Unix epoch.
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
