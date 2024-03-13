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
	// Series returns a list of all series that will be returned by this operator.
	// The returned []SeriesMetadata can be modified by the caller or returned to a pool.
	// An operator may return series in any order, but the same order must be used by both Series and Next.
	// Series should be called no more than once.
	Series(ctx context.Context) ([]SeriesMetadata, error)

	// Next returns the next series from this operator, or EOS otherwise.
	// Series must be called exactly once before calling Next.
	// The returned InstantVectorSeriesData can be modified by the caller or returned to a pool.
	Next(ctx context.Context) (InstantVectorSeriesData, error)

	// Close frees all resources associated with this operator.
	// Calling Series or Next after calling Close may result in unpredictable behaviour, corruption or crashes.
	Close()
}

var EOS = errors.New("operator stream exhausted")

type SeriesMetadata struct {
	Labels labels.Labels
}

type InstantVectorSeriesData struct {
	Floats     []promql.FPoint
	Histograms []promql.HPoint
}
