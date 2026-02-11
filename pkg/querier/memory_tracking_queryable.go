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

// NewMemoryTrackingQueryable creates a new MemoryTrackingQueryable that wraps the given queryable.
func NewMemoryTrackingQueryable(inner storage.Queryable) storage.Queryable {
	return &MemoryTrackingQueryable{inner: inner}
}

// MemoryTrackingQueryable wraps a storage.Queryable to add memory tracking and
// label deduplication in a MemoryTrackingQuerier.
type MemoryTrackingQueryable struct {
	inner storage.Queryable
}

func (q *MemoryTrackingQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	querier, err := q.inner.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &memoryTrackingQuerier{inner: querier}, nil
}

type memoryTrackingQuerier struct {
	inner storage.Querier
}

func (q *memoryTrackingQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	memoryTracker, err := limiter.MemoryConsumptionTrackerFromContext(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx)

	return series.NewMemoryTrackingSeriesSet(q.inner.Select(ctx, sortSeries, hints, matchers...), memoryTracker)
}

func (q *memoryTrackingQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.inner.LabelValues(ctx, name, hints, matchers...)
}

func (q *memoryTrackingQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.inner.LabelNames(ctx, hints, matchers...)
}

func (q *memoryTrackingQuerier) Close() error {
	return q.inner.Close()
}
