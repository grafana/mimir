// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

// Operator represents all operators.
type Operator interface {
	// SeriesMetadata returns a list of all series that will be returned by this operator.
	// The returned []SeriesMetadata can be modified by the caller or returned to a pool.
	// SeriesMetadata may return series in any order, but the same order must be used by both SeriesMetadata and NextSeries.
	// SeriesMetadata should be called no more than once.
	SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error)

	// Close frees all resources associated with this operator.
	// Calling SeriesMetadata or NextSeries after calling Close may result in unpredictable behaviour, corruption or crashes.
	Close()
}

// InstantVectorOperator represents all operators that produce instant vectors.
type InstantVectorOperator interface {
	Operator

	// NextSeries returns the next series from this operator, or EOS if no more series are available.
	// SeriesMetadata must be called exactly once before calling NextSeries.
	// The returned InstantVectorSeriesData can be modified by the caller or returned to a pool.
	// The returned InstantVectorSeriesData can contain no points.
	NextSeries(ctx context.Context) (InstantVectorSeriesData, error)
}

// RangeVectorOperator represents all operators that produce range vectors.
type RangeVectorOperator interface {
	Operator

	// StepCount returns the number of time steps produced for each series by this operator.
	// StepCount must only be called after calling SeriesMetadata.
	StepCount() int

	// Range returns the time range selected by this operator at each time step.
	//
	// For example, if this operator represents the selector "some_metric[5m]", Range returns 5 minutes.
	Range() time.Duration

	// NextSeries advances to the next series produced by this operator, or EOS if no more series are available.
	// SeriesMetadata must be called exactly once before calling NextSeries.
	NextSeries(ctx context.Context) error

	// NextStepSamples populates the provided RingBuffer with the samples for the next time step for the
	// current series and returns the timestamps of the next time step, or returns EOS if no more time
	// steps are available.
	// The provided RingBuffer is expected to only contain points for the current series, and the same
	// RingBuffer should be passed to subsequent NextStepSamples calls for the same series.
	// The provided RingBuffer may be populated with points beyond the end of the expected time range, and
	// callers should compare returned points' timestamps to the returned RangeVectorStepData.RangeEnd.
	// Next must be called at least once before calling NextStepSamples.
	NextStepSamples(floats *RingBuffer) (RangeVectorStepData, error)
}

var EOS = errors.New("operator stream exhausted") //nolint:revive

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
	Histograms []promql.HPoint
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
