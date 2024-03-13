// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streaming

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

const defaultLookbackDelta = 5 * time.Minute // This should be the same value as github.com/prometheus/prometheus/promql.defaultLookbackDelta.

func NewEngine(opts promql.EngineOpts) (promql.QueryEngine, error) {
	lookbackDelta := opts.LookbackDelta
	if lookbackDelta == 0 {
		lookbackDelta = defaultLookbackDelta
	}

	if !opts.EnableAtModifier {
		return nil, errors.New("disabling @ modifier not supported by streaming engine")
	}

	if !opts.EnableNegativeOffset {
		return nil, errors.New("disabling negative offsets not supported by streaming engine")
	}

	if opts.EnablePerStepStats {
		return nil, errors.New("enabling per-step stats not supported by streaming engine")
	}

	return &Engine{
		lookbackDelta: lookbackDelta,
	}, nil
}

type Engine struct {
	lookbackDelta time.Duration
}

func (e *Engine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	// TODO: do we need to do anything more for instant queries?
	return e.NewRangeQuery(ctx, q, opts, qs, ts, ts, 0)
}

func (e *Engine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	// TODO: don't allow interval == 0
	// TODO: check that expression produces the expected kind of result (scalar or instant vector)
	return NewQuery(q, opts, qs, start, end, interval, e)
}
