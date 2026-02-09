// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"
)

// UnlimitedMemoryTrackingQuery wraps promql.Query so that this can
// pass an unlimited MemoryConsumptionTracker, specifically in the Exec call.
type UnlimitedMemoryTrackingQuery struct {
	inner                    promql.Query
	memoryConsumptionTracker *MemoryConsumptionTracker
}

func (u *UnlimitedMemoryTrackingQuery) Exec(ctx context.Context) *promql.Result {
	ctx = AddMemoryTrackerToContext(ctx, u.memoryConsumptionTracker)
	ctx = ContextWithNewSeriesLabelsDeduplicator(ctx)
	return u.inner.Exec(ctx)
}

func (u *UnlimitedMemoryTrackingQuery) Close() {
	u.inner.Close()
}

func (u *UnlimitedMemoryTrackingQuery) Statement() parser.Statement {
	return u.inner.Statement()
}

func (u *UnlimitedMemoryTrackingQuery) Stats() *stats.Statistics {
	return u.inner.Stats()
}

func (u *UnlimitedMemoryTrackingQuery) Cancel() {
	u.inner.Cancel()
}

func (u *UnlimitedMemoryTrackingQuery) String() string {
	return u.inner.String()
}

func newUnlimitedMemoryTrackingQuery(query promql.Query, memoryConsumptionTracker *MemoryConsumptionTracker) *UnlimitedMemoryTrackingQuery {
	return &UnlimitedMemoryTrackingQuery{
		inner:                    query,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

// UnlimitedMemoryTrackerPromQLEngine wraps promql.Engine so that queries always run
// with a MemoryConsumptionTracker in their context.
//
// The provided MemoryConsumptionTracker will enforce no limit on query memory consumption,
// but is still required as some querier components track memory consumption even
// when running in Prometheus' engine (eg. the ingester and store-gateway Queryable
// implementations).
type UnlimitedMemoryTrackerPromQLEngine struct {
	inner *promql.Engine
}

func NewUnlimitedMemoryTrackerPromQLEngine(inner *promql.Engine) UnlimitedMemoryTrackerPromQLEngine {
	return UnlimitedMemoryTrackerPromQLEngine{inner: inner}
}

func (p UnlimitedMemoryTrackerPromQLEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	qry, err := p.inner.NewInstantQuery(ctx, q, opts, qs, ts)
	if err != nil {
		return nil, err
	}
	memoryTracker := NewUnlimitedMemoryConsumptionTracker(ctx)
	return newUnlimitedMemoryTrackingQuery(qry, memoryTracker), nil
}
func (p UnlimitedMemoryTrackerPromQLEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	qry, err := p.inner.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
	if err != nil {
		return nil, err
	}
	memoryTracker := NewUnlimitedMemoryConsumptionTracker(ctx)
	return newUnlimitedMemoryTrackingQuery(qry, memoryTracker), nil
}
