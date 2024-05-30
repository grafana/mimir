// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"context"
	"errors"
	"time"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Operator represents all operators.
type Operator interface {
	// SeriesMetadata returns a list of all series that will be returned by this operator.
	// The returned []SeriesMetadata can be modified by the caller or returned to a pool.
	// SeriesMetadata may return series in any order, but the same order must be used by both SeriesMetadata and NextSeries.
	// SeriesMetadata should be called no more than once.
	SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error)

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
	NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error)
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
	NextStepSamples(floats *RingBuffer) (types.RangeVectorStepData, error)
}

var EOS = errors.New("operator stream exhausted") //nolint:revive
