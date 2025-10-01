package limiter

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

type noopMemoryConsumptionTracker struct{}

func (n noopMemoryConsumptionTracker) IncreaseMemoryConsumption(_ uint64, _ MemoryConsumptionSource) error {
	return nil
}

func (n noopMemoryConsumptionTracker) DecreaseMemoryConsumption(_ uint64, _ MemoryConsumptionSource) {
}

func (n noopMemoryConsumptionTracker) IncreaseMemoryConsumptionForLabels(_ labels.Labels) error {
	return nil
}

func (n noopMemoryConsumptionTracker) DecreaseMemoryConsumptionForLabels(_ labels.Labels) {}

// NoopMemoryTrackerPromqlEngine wrap promql.Engine so that this can
// pass noopMemoryConsumptionTracker which will do nothing when processing query.
//
// We will have MemoryConsumptionTracker when processing response from ingester and store gateway,
// but we only want to track memory consumption for query coming from streamingpromql.Engine.
type NoopMemoryTrackerPromqlEngine struct {
	inner *promql.Engine
}

func NewNoopMemoryTrackerPromqlEngine(inner *promql.Engine) NoopMemoryTrackerPromqlEngine {
	return NoopMemoryTrackerPromqlEngine{inner: inner}
}

func (p NoopMemoryTrackerPromqlEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	ctx = AddMemoryTrackerToContext(ctx, noopMemoryConsumptionTracker{})
	return p.inner.NewInstantQuery(ctx, q, opts, qs, ts)
}
func (p NoopMemoryTrackerPromqlEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	ctx = AddMemoryTrackerToContext(ctx, noopMemoryConsumptionTracker{})
	return p.inner.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
}
