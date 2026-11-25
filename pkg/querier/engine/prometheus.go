// SPDX-License-Identifier: AGPL-3.0-only

package engine

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions" //lint:ignore faillint requestoptions is fine
	"github.com/grafana/mimir/pkg/util/limiter"                   //lint:ignore faillint limiter is fine
)

type prometheusEngine struct {
	inner              promql.QueryEngine
	useStartTimestamps bool
}

// NewPrometheusEngine configures query execution with memory tracking and start-timestamp collection.
func NewPrometheusEngine(opts promql.EngineOpts) promql.QueryEngine {
	return &prometheusEngine{
		inner:              limiter.NewUnlimitedMemoryTrackerPromQLEngine(promql.NewEngine(opts)),
		useStartTimestamps: opts.UseStartTimestamps,
	}
}

func (e *prometheusEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	query, err := e.inner.NewInstantQuery(ctx, q, opts, qs, ts)
	if err != nil {
		return nil, err
	}
	return e.wrapQuery(query, opts), nil
}

func (e *prometheusEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	query, err := e.inner.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
	if err != nil {
		return nil, err
	}
	return e.wrapQuery(query, opts), nil
}

func (e *prometheusEngine) wrapQuery(query promql.Query, opts promql.QueryOpts) promql.Query {
	option := requestoptions.StartTimestampsUnset
	if opts != nil {
		option = requestoptions.StartTimestampOptionFromBool(opts.UseStartTimestamps())
	}
	return &startTimestampQuery{Query: query, collectStartTimestamps: option.Resolve(e.useStartTimestamps)}
}

type startTimestampQuery struct {
	promql.Query
	collectStartTimestamps bool
}

func (q *startTimestampQuery) Exec(ctx context.Context) *promql.Result {
	// Exec may receive a different context from query construction.
	ctx = requestoptions.ContextWithStartTimestampCollection(ctx, q.collectStartTimestamps)
	return q.Query.Exec(ctx)
}
