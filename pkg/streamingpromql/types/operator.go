// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/prometheus/promql/parser/posrange"
)

// Operator represents all operators.
type Operator interface {
	// SeriesMetadata returns a list of all series that will be returned by this operator.
	// The returned []SeriesMetadata can be modified by the caller or returned to a pool.
	// SeriesMetadata may return series in any order, but the same order must be used by both SeriesMetadata and NextSeries.
	// SeriesMetadata should be called no more than once.
	SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error)

	// ExpressionPosition returns the position of the PromQL expression that this operator represents.
	ExpressionPosition() posrange.PositionRange

	// Close frees all resources associated with this operator.
	// Calling SeriesMetadata or NextSeries after calling Close may result in unpredictable behaviour, corruption or crashes.
	// It must be safe to call Close at any time, including if SeriesMetadata or NextSeries have returned an error.
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

	// NextStepSamples populates the provided RingBuffers with the samples for the next time step for the
	// current series and returns the timestamps of the next time step, or returns EOS if no more time
	// steps are available.
	// The provided RingBuffers are expected to only contain points for the current series, and the same
	// RingBuffers should be passed to subsequent NextStepSamples calls for the same series.
	// The provided RingBuffers may be populated with points beyond the end of the expected time range, and
	// callers should compare returned points' timestamps to the returned RangeVectorStepData.RangeEnd.
	// Next must be called at least once before calling NextStepSamples.
	// Keep in mind that HPoint contains a pointer to a histogram, so it is generally not safe to
	// modify directly as the histogram may be used for other HPoint values, such as when lookback has occurred.
	NextStepSamples(floats *FPointRingBuffer, histograms *HPointRingBuffer) (RangeVectorStepData, error)
}

var EOS = errors.New("operator stream exhausted") //nolint:revive
