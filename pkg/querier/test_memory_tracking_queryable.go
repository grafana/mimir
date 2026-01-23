// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/util/limiter"
)

// NewTestMemoryTrackingQueryable wraps a queryable to increase memory for series labels.
// This is needed for tests that use mock queryables (like TSDB or distributor) which don't
// naturally track memory like the production blocks_store_queryable does. The wrapper ensures
// memory is increased for each series before it's decreased by MemoryTrackingSeriesSet.
func NewTestMemoryTrackingQueryable(inner storage.Queryable) storage.Queryable {
	return &testMemoryTrackingQueryable{inner: inner}
}

type testMemoryTrackingQueryable struct {
	inner storage.Queryable
}

func (t *testMemoryTrackingQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	q, err := t.inner.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &testMemoryTrackingQuerier{inner: q}, nil
}

type testMemoryTrackingQuerier struct {
	inner storage.Querier
}

func (t *testMemoryTrackingQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	memoryTracker, err := limiter.MemoryConsumptionTrackerFromContext(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	return &testMemoryIncreasingSeriesSet{
		inner:         t.inner.Select(ctx, sortSeries, hints, matchers...),
		memoryTracker: memoryTracker,
	}
}

func (t *testMemoryTrackingQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return t.inner.LabelValues(ctx, name, hints, matchers...)
}

func (t *testMemoryTrackingQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return t.inner.LabelNames(ctx, hints, matchers...)
}

func (t *testMemoryTrackingQuerier) Close() error {
	return t.inner.Close()
}

type testMemoryIncreasingSeriesSet struct {
	inner         storage.SeriesSet
	memoryTracker *limiter.MemoryConsumptionTracker
}

func (t *testMemoryIncreasingSeriesSet) Next() bool {
	if !t.inner.Next() {
		return false
	}

	// Increase memory for the series labels to balance the decrease done by MemoryTrackingSeriesSet
	s := t.inner.At()
	_ = t.memoryTracker.IncreaseMemoryConsumptionForLabels(s.Labels())

	return true
}

func (t *testMemoryIncreasingSeriesSet) At() storage.Series {
	return t.inner.At()
}

func (t *testMemoryIncreasingSeriesSet) Err() error {
	return t.inner.Err()
}

func (t *testMemoryIncreasingSeriesSet) Warnings() annotations.Annotations {
	return t.inner.Warnings()
}
