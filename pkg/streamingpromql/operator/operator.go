// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"context"
	"errors"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

// Operator represents all operators.
type Operator interface {
	// SeriesMetadata returns a list of all series that will be returned by this operator.
	// The returned []SeriesMetadata can be modified by the caller or returned to a pool.
	// SeriesMetadata may return series in any order, but the same order must be used by both SeriesMetadata and Next.
	// SeriesMetadata should be called no more than once.
	SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error)

	// Close frees all resources associated with this operator.
	// Calling SeriesMetadata or Next after calling Close may result in unpredictable behaviour, corruption or crashes.
	Close()
}

// InstantVectorOperator represents all operators that produce instant vectors.
type InstantVectorOperator interface {
	Operator

	// Next returns the next series from this operator, or EOS if no more series are available.
	// SeriesMetadata must be called exactly once before calling Next.
	// The returned InstantVectorSeriesData can be modified by the caller or returned to a pool.
	// The returned InstantVectorSeriesData can contain no points.
	Next(ctx context.Context) (InstantVectorSeriesData, error)
}

// RangeVectorOperator represents all operators that produce range vectors.
type RangeVectorOperator interface {
	Operator

	// StepCount returns the number of time steps produced for each series by this operator.
	// StepCount must only be called after calling SeriesMetadata.
	StepCount() int

	// Next advances to the next series produced by this operator, or EOS if no more series are available.
	// SeriesMetadata must be called exactly once before calling Next.
	Next(ctx context.Context) error

	// NextStep populates the provided RingBuffer with the samples for the next time step for the
	// current series and returns the timestamps of the next time step, or returns EOS if no more time
	// steps are available.
	// The provided RingBuffer is expected to only contain points for the current series, and the same
	// RingBuffer should be passed to subsequent NextStep calls for the same series.
	// The provided RingBuffer may be populated with points beyond the end of the expected time range, and
	// callers should compare returned points' timestamps to the returned RangeVectorStepData.RangeEnd.
	// Next must be called at least once before calling NextStep.
	NextStep(floats *RingBuffer) (RangeVectorStepData, error)
}

var EOS = errors.New("operator stream exhausted") //nolint:revive

type SeriesMetadata struct {
	Labels labels.Labels
}

type InstantVectorSeriesData struct {
	Floats     []promql.FPoint
	Histograms []promql.HPoint
}

type RangeVectorStepData struct {
	// StepT is the timestamp of this time step.
	StepT int64

	// RangeStart is the beginning of the time range selected by this time step.
	RangeStart int64

	// RangeEnd is the end of the time range selected by this time step.
	// RangeEnd is the same as StepT except when the @ modifier or offsets are used, in which case
	// RangeEnd reflects the time of the underlying points, and StepT is the timestamp of the point
	// produced by the query.
	RangeEnd int64
}
