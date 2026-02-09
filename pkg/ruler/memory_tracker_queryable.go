// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/util/limiter"
)

// NewUnlimitedMemoryTrackerQueryable wraps a storage.Queryable to inject an unlimited MemoryConsumptionTracker
// and SeriesLabelsDeduplicator into the context.
func NewUnlimitedMemoryTrackerQueryable(inner storage.Queryable) storage.Queryable {
	return &unlimitedMemoryTrackerQueryable{inner: inner}
}

// unlimitedMemoryTrackerQueryable wraps Queryable that returns unlimitedMemoryTrackerQuerier.
type unlimitedMemoryTrackerQueryable struct {
	inner storage.Queryable
}

func (q *unlimitedMemoryTrackerQueryable) Querier(minT, maxT int64) (storage.Querier, error) {
	querier, err := q.inner.Querier(minT, maxT)
	if err != nil {
		return nil, err
	}
	return &unlimitedMemoryTrackerQuerier{inner: querier}, nil
}

// unlimitedMemoryTrackerQuerier wraps a storage.Querier and ensures all Select calls have an unlimited
// MemoryConsumptionTracker in their context. It does nothing on other Querier methods.
type unlimitedMemoryTrackerQuerier struct {
	inner storage.Querier
}

func (q *unlimitedMemoryTrackerQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
	// Create a per-query deduplicator for operation that require custom Queryable such as ruler queries. Each Select() call gets its own
	// deduplicator to ensure proper label deduplication and memory tracking for rule evaluation.
	ctx = limiter.AddNewSeriesDeduplicatorToContext(ctx)
	return q.inner.Select(ctx, sortSeries, hints, matchers...)
}

func (q *unlimitedMemoryTrackerQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.inner.LabelValues(ctx, name, hints, matchers...)
}

func (q *unlimitedMemoryTrackerQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.inner.LabelNames(ctx, hints, matchers...)
}

func (q *unlimitedMemoryTrackerQuerier) Close() error {
	return q.inner.Close()
}
