// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streaming

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

func NewEngine(opts promql.EngineOpts) promql.QueryEngine {
	lookbackDelta := opts.LookbackDelta
	if lookbackDelta == 0 {
		lookbackDelta = promql.DefaultLookbackDelta
	}

	// TODO: add support for features in EngineOpts (logging, metrics, stats, limits, timeouts, query tracker)
	return &Engine{
		lookbackDelta: lookbackDelta,
	}
}

type Engine struct {
	lookbackDelta time.Duration
}

func (e *Engine) SetQueryLogger(l promql.QueryLogger) {
	panic("implement me")
}

func (e *Engine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	// TODO: check that disabled features (at operator, negative offset) aren't used
	return e.NewRangeQuery(ctx, q, opts, qs, ts, ts, 0)
}

func (e *Engine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	// TODO: don't allow interval == 0
	// TODO: check that expression produces the expected kind of result (scalar or instant vector)
	// TODO: check that disabled features (at operator, negative offset) aren't used
	return NewQuery(q, opts, qs, start, end, interval, e)
}
