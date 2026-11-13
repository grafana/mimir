// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/util/validation"
)

type startTimestampsLimits interface {
	UseStartTimestamps(userID string) bool
}

// startTimestampsEngine wraps a Prometheus engine to apply the per-tenant default for using start
// timestamps in PromQL functions, and to collect start timestamps in the read path when they are used.
type startTimestampsEngine struct {
	next   promql.QueryEngine
	limits startTimestampsLimits
}

func newStartTimestampsEngine(next promql.QueryEngine, limits startTimestampsLimits) promql.QueryEngine {
	return &startTimestampsEngine{next: next, limits: limits}
}

func (e *startTimestampsEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	q, opts, err := e.apply(ctx, q, opts)
	if err != nil {
		return nil, err
	}
	return e.next.NewInstantQuery(ctx, q, opts, qs, ts)
}

func (e *startTimestampsEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	q, opts, err := e.apply(ctx, q, opts)
	if err != nil {
		return nil, err
	}
	return e.next.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
}

// apply resolves whether the query uses start timestamps. A per-query setting takes precedence over the
// tenant limit. For a query spanning multiple tenants, the limit must be enabled for all of them.
func (e *startTimestampsEngine) apply(ctx context.Context, q storage.Queryable, opts promql.QueryOpts) (storage.Queryable, promql.QueryOpts, error) {
	if opts != nil && opts.UseStartTimestamps() != nil {
		if !*opts.UseStartTimestamps() {
			return q, opts, nil
		}
		return startTimestampsQueryable{q}, opts, nil
	}

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, nil, err
	}
	if !validation.AllTrueBooleansPerTenant(tenantIDs, e.limits.UseStartTimestamps) {
		return q, opts, nil
	}
	return startTimestampsQueryable{q}, withUseStartTimestamps(opts), nil
}

// withUseStartTimestamps returns a copy of opts with start timestamps enabled.
func withUseStartTimestamps(opts promql.QueryOpts) promql.QueryOpts {
	use := true
	if opts == nil {
		return promql.NewPrometheusQueryOpts(false, 0, &use)
	}
	if costOpts, ok := opts.(promql.QueryCostOpts); ok {
		return promql.NewPrometheusQueryOptsWithLimits(opts.EnablePerStepStats(), opts.LookbackDelta(), costOpts.MaxSeries(), costOpts.MaxSamplesRead(), costOpts.MaxQueryDuration(), &use)
	}
	return promql.NewPrometheusQueryOpts(opts.EnablePerStepStats(), opts.LookbackDelta(), &use)
}

// startTimestampsQueryable makes its queriers collect start timestamps.
type startTimestampsQueryable struct {
	storage.Queryable
}

func (q startTimestampsQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	querier, err := q.Queryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return startTimestampsQuerier{querier}, nil
}

type startTimestampsQuerier struct {
	storage.Querier
}

func (q startTimestampsQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return q.Querier.Select(contextWithCollectStartTimestamps(ctx), sortSeries, hints, matchers...)
}

type collectStartTimestampsContextKey struct{}

func contextWithCollectStartTimestamps(ctx context.Context) context.Context {
	return context.WithValue(ctx, collectStartTimestampsContextKey{}, true)
}

// collectStartTimestampsFromContext returns whether series iterators should collect the start timestamps of samples.
func collectStartTimestampsFromContext(ctx context.Context) bool {
	collect, _ := ctx.Value(collectStartTimestampsContextKey{}).(bool)
	return collect
}
