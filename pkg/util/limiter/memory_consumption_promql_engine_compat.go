// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

type UnlimitedMemoryTrackingQuery struct {
	promql.Query
}

func (u *UnlimitedMemoryTrackingQuery) Exec(ctx context.Context) *promql.Result {
	ctx = ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
	return u.Query.Exec(ctx)
}

func NewUnlimitedMemoryTrackingQuery(query promql.Query) *UnlimitedMemoryTrackingQuery {
	return &UnlimitedMemoryTrackingQuery{query}
}

// UnlimitedMemoryTrackerPromqlEngine wraps promql.Engine so that this can
// pass an unlimited MemoryConsumptionTracker.
//
// We will have MemoryConsumptionTracker when processing response from ingester and store gateway,
// but we only want to track and limit memory consumption for query coming from streamingpromql.Engine.
type UnlimitedMemoryTrackerPromqlEngine struct {
	inner *promql.Engine
}

func NewUnlimitedMemoryTrackerPromqlEngine(inner *promql.Engine) UnlimitedMemoryTrackerPromqlEngine {
	return UnlimitedMemoryTrackerPromqlEngine{inner: inner}
}

func (p UnlimitedMemoryTrackerPromqlEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	qry, err := p.inner.NewInstantQuery(ctx, q, opts, qs, ts)
	if err != nil {
		return nil, err
	}
	return NewUnlimitedMemoryTrackingQuery(qry), nil
}
func (p UnlimitedMemoryTrackerPromqlEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	qry, err := p.inner.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
	if err != nil {
		return nil, err
	}
	return NewUnlimitedMemoryTrackingQuery(qry), nil
}
