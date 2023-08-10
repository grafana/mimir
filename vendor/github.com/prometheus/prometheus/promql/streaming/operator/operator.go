package operator

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

type Operator interface {
	// Series returns a list of all series that will be returned by this operator.
	// The returned []SeriesMetadata can be modified by the caller or returned to a pool.
	Series(ctx context.Context) ([]SeriesMetadata, error)

	// Next returns the next series from this operator, or false otherwise.
	// Series must be called exactly once before calling Next.
	// The returned SeriesData can be modified by the caller or returned to a pool.
	Next(ctx context.Context) (bool, SeriesData, error)

	// Close frees all resources associated with this operator.
	// Calling Series or Next after calling Close may result in unpredictable behaviour, corruption or crashes.
	Close()
}

type SeriesMetadata struct {
	Labels labels.Labels
}

type SeriesData struct {
	Floats     []promql.FPoint
	Histograms []promql.HPoint
}
