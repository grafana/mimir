// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// NewMemoryTrackingQueryable creates a new MemoryTrackingQueryable that wraps the given queryable.
func NewMemoryTrackingQueryable(inner storage.Queryable, reg prometheus.Registerer) storage.Queryable {
	return &MemoryTrackingQueryable{
		inner:   inner,
		metrics: limiter.NewSeriesDeduplicatorMetrics(reg),
	}
}

// MemoryTrackingQueryable wraps a storage.Queryable to add memory tracking and
// label deduplication in a MemoryTrackingQuerier.
type MemoryTrackingQueryable struct {
	inner   storage.Queryable
	metrics *limiter.SeriesDeduplicatorMetrics
}

func (q *MemoryTrackingQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	querier, err := q.inner.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &memoryTrackingQuerier{inner: querier, metrics: q.metrics}, nil
}

type memoryTrackingQuerier struct {
	inner   storage.Querier
	metrics *limiter.SeriesDeduplicatorMetrics
}

func (q *memoryTrackingQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	memoryTracker, err := limiter.MemoryConsumptionTrackerFromContext(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, q.metrics)

	return series.NewMemoryTrackingSeriesSet(q.inner.Select(ctx, sortSeries, hints, matchers...), memoryTracker)
}

func (q *memoryTrackingQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.inner.LabelValues(ctx, name, hints, matchers...)
}

func (q *memoryTrackingQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.inner.LabelNames(ctx, hints, matchers...)
}

// SearchLabelNames passes through to the inner querier's mimirSearcher
// implementation. No memory tracking is applied to label search — the
// streaming SearchResultSet caps memory at the per-tenant Limit via the
// merge tree, so wrapping each emitted Value in a tracker would be
// redundant work on the hot path.
func (q *memoryTrackingQuerier) SearchLabelNames(ctx context.Context, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet {
	s, ok := q.inner.(mimirSearcher)
	if !ok {
		return storage.ErrSearchResultSet(fmt.Errorf("inner querier %T does not implement search", q.inner))
	}
	return s.SearchLabelNames(ctx, params, hints, matchers...)
}

// SearchLabelValues mirrors SearchLabelNames; see that docstring for rationale.
func (q *memoryTrackingQuerier) SearchLabelValues(ctx context.Context, name string, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet {
	s, ok := q.inner.(mimirSearcher)
	if !ok {
		return storage.ErrSearchResultSet(fmt.Errorf("inner querier %T does not implement search", q.inner))
	}
	return s.SearchLabelValues(ctx, name, params, hints, matchers...)
}

func (q *memoryTrackingQuerier) Close() error {
	return q.inner.Close()
}
