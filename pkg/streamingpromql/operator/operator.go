// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"context"
	"errors"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

// InstantVectorOperator represents all operators that produce instant vectors.
type InstantVectorOperator interface {
	// SeriesMetadata returns a list of all series that will be returned by this operator.
	// The returned []SeriesMetadata can be modified by the caller or returned to a pool.
	// SeriesMetadata may return series in any order, but the same order must be used by both SeriesMetadata and Next.
	// SeriesMetadata should be called no more than once.
	SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error)

	// Next returns the next series from this operator, or EOS otherwise.
	// SeriesMetadata must be called exactly once before calling Next.
	// The returned InstantVectorSeriesData can be modified by the caller or returned to a pool.
	// The returned InstantVectorSeriesData can contain no points.
	Next(ctx context.Context) (InstantVectorSeriesData, error)

	// Close frees all resources associated with this operator.
	// Calling SeriesMetadata or Next after calling Close may result in unpredictable behaviour, corruption or crashes.
	Close()
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
