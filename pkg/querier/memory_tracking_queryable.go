// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// MemoryTrackingQueryable wraps a storage.Queryable to add memory tracking and
// label deduplication. It creates a MemoryTrackingQuerier for each time range query.
type MemoryTrackingQueryable struct {
	inner storage.Queryable
}

// NewMemoryTrackingQueryable creates a new MemoryTrackingQueryable that wraps the given queryable.
func NewMemoryTrackingQueryable(inner storage.Queryable) *MemoryTrackingQueryable {
	return &MemoryTrackingQueryable{inner: inner}
}

func (w *MemoryTrackingQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	q, err := w.inner.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &memoryTrackingQuerier{inner: q}, nil
}

type memoryTrackingQuerier struct {
	inner storage.Querier
}

func (w *memoryTrackingQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	memoryTracker, err := limiter.MemoryConsumptionTrackerFromContext(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx)

	result := w.inner.Select(ctx, sortSeries, hints, matchers...)
	if hints != nil && hints.Func == "series" {
		return result
	}
	return series.NewMemoryTrackingSeriesSet(result, memoryTracker)
}

func (w *memoryTrackingQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return w.inner.LabelValues(ctx, name, hints, matchers...)
}

func (w *memoryTrackingQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return w.inner.LabelNames(ctx, hints, matchers...)
}

func (w *memoryTrackingQuerier) Close() error {
	return w.inner.Close()
}
