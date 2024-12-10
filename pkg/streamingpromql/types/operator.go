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
	// ExpressionPosition returns the position of the PromQL expression that this operator represents.
	ExpressionPosition() posrange.PositionRange

	// Close frees all resources associated with this operator.
	// Calling SeriesMetadata or NextSeries after calling Close may result in unpredictable behaviour, corruption or crashes.
	// It must be safe to call Close at any time, including if SeriesMetadata or NextSeries have returned an error.
	Close()
}

// SeriesOperator represents all operators that return one or more series.
type SeriesOperator interface {
	Operator

	// SeriesMetadata returns a list of all series that will be returned by this operator.
	// The returned []SeriesMetadata can be modified by the caller or returned to a pool.
	// SeriesMetadata may return series in any order, but the same order must be used by both SeriesMetadata and NextSeries.
	// SeriesMetadata should be called no more than once.
	SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error)
}

// InstantVectorOperator represents all operators that produce instant vectors.
type InstantVectorOperator interface {
	SeriesOperator

	// NextSeries returns the next series from this operator, or EOS if no more series are available.
	// SeriesMetadata must be called exactly once before calling NextSeries.
	// The returned InstantVectorSeriesData can be modified by the caller or returned to a pool.
	// The returned InstantVectorSeriesData can contain no points.
	NextSeries(ctx context.Context) (InstantVectorSeriesData, error)
}

// RangeVectorOperator represents all operators that produce range vectors.
type RangeVectorOperator interface {
	SeriesOperator

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

	// NextStepSamples returns populated RingBuffers with the samples for the next time step for the
	// current series and the timestamps of the next time step, or returns EOS if no more time
	// steps are available.
	NextStepSamples() (*RangeVectorStepData, error)
}

// ScalarOperator represents all operators that produce scalars.
type ScalarOperator interface {
	Operator

	// GetValues returns the samples for this scalar.
	GetValues(ctx context.Context) (ScalarData, error)
}

// StringOperator represents all operators that produce strings.
type StringOperator interface {
	Operator

	// GetValue returns the string
	GetValue() string
}

var EOS = errors.New("operator stream exhausted") //nolint:revive
