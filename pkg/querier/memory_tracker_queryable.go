// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/util/limiter"
)

// memoryTrackerSampleAndChunkQueryable wraps a storage.SampleAndChunkQueryable and ensures all queries have an unlimited
// MemoryConsumptionTracker in their context.
type memoryTrackerSampleAndChunkQueryable struct {
	inner storage.SampleAndChunkQueryable
}

// NewMemoryTrackerSampleAndChunkQueryable wraps a storage.Queryable to inject an unlimited MemoryConsumptionTracker
// into the context for all queriers.
func NewMemoryTrackerSampleAndChunkQueryable(inner storage.Queryable) storage.Queryable {
	// Try to cast to SampleAndChunkQueryable to preserve the interface
	if scq, ok := inner.(storage.SampleAndChunkQueryable); ok {
		return &memoryTrackerSampleAndChunkQueryable{inner: scq}
	}
	// Fall back to wrapping just as Queryable
	return &memoryTrackerQueryable{inner: inner}
}

func (q *memoryTrackerSampleAndChunkQueryable) Querier(minT, maxT int64) (storage.Querier, error) {
	querier, err := q.inner.Querier(minT, maxT)
	if err != nil {
		return nil, err
	}
	return &memoryTrackerQuerier{inner: querier}, nil
}

func (q *memoryTrackerSampleAndChunkQueryable) ChunkQuerier(minT, maxT int64) (storage.ChunkQuerier, error) {
	querier, err := q.inner.ChunkQuerier(minT, maxT)
	if err != nil {
		return nil, err
	}
	return &memoryTrackerChunkQuerier{inner: querier}, nil
}

// memoryTrackerQueryable is for queryables that don't support ChunkQuerier
type memoryTrackerQueryable struct {
	inner storage.Queryable
}

func (q *memoryTrackerQueryable) Querier(minT, maxT int64) (storage.Querier, error) {
	querier, err := q.inner.Querier(minT, maxT)
	if err != nil {
		return nil, err
	}
	return &memoryTrackerQuerier{inner: querier}, nil
}

// memoryTrackerQuerier wraps a storage.Querier and ensures all Select calls have an unlimited
// MemoryConsumptionTracker in their context.
type memoryTrackerQuerier struct {
	inner storage.Querier
}

func (q *memoryTrackerQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if _, err := limiter.MemoryConsumptionTrackerFromContext(ctx); err != nil {
		ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
	}
	return q.inner.Select(ctx, sortSeries, hints, matchers...)
}

func (q *memoryTrackerQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.inner.LabelValues(ctx, name, hints, matchers...)
}

func (q *memoryTrackerQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.inner.LabelNames(ctx, hints, matchers...)
}

func (q *memoryTrackerQuerier) Close() error {
	return q.inner.Close()
}

// memoryTrackerChunkQuerier wraps a storage.ChunkQuerier and ensures all Select calls have an unlimited
// MemoryConsumptionTracker in their context.
type memoryTrackerChunkQuerier struct {
	inner storage.ChunkQuerier
}

func (q *memoryTrackerChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	if _, err := limiter.MemoryConsumptionTrackerFromContext(ctx); err != nil {
		ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
	}
	return q.inner.Select(ctx, sortSeries, hints, matchers...)
}

func (q *memoryTrackerChunkQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.inner.LabelValues(ctx, name, hints, matchers...)
}

func (q *memoryTrackerChunkQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.inner.LabelNames(ctx, hints, matchers...)
}

func (q *memoryTrackerChunkQuerier) Close() error {
	return q.inner.Close()
}
