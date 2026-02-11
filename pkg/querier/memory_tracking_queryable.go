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
//
// For each Select() call, this wrapper:
// 1. Retrieves the MemoryConsumptionTracker from context (must be present)
// 2. Creates a new SeriesLabelsDeduplicator in the context
// 3. Calls the inner querier's Select() method
// 4. Wraps the result with MemoryTrackingSeriesSet (unless it's a "series" query)
//
// This centralizes memory tracking logic and is used by multiQuerier to ensure
// consistent memory accounting across different query engines and execution paths.
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

	// For "series" queries, we don't create the deduplicator because we skip MemoryTrackingSeriesSet,
	// which means memory increases from the deduplicator would never be balanced by decreases.
	// The ingester path (distributorQuerier) doesn't use the deduplicator for "series" queries,
	// and the store-gateway path handles the missing deduplicator gracefully.
	if hints == nil || hints.Func != "series" {
		ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx)
	}

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
