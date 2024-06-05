// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streamingpromql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

const defaultLookbackDelta = 5 * time.Minute // This should be the same value as github.com/prometheus/prometheus/promql.defaultLookbackDelta.

func NewEngine(opts promql.EngineOpts, limitsProvider QueryLimitsProvider) (promql.QueryEngine, error) {
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
		lookbackDelta:  lookbackDelta,
		timeout:        opts.Timeout,
		limitsProvider: limitsProvider,
	}, nil
}

type Engine struct {
	lookbackDelta  time.Duration
	timeout        time.Duration
	limitsProvider QueryLimitsProvider
}

func (e *Engine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	return newQuery(ctx, q, opts, qs, ts, ts, 0, e)
}

func (e *Engine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("%v is not a valid interval for a range query, must be greater than 0", interval)
	}

	if end.Before(start) {
		return nil, fmt.Errorf("range query time range is invalid: end time %v is before start time %v", end.Format(time.RFC3339), start.Format(time.RFC3339))
	}

	return newQuery(ctx, q, opts, qs, start, end, interval, e)
}

type QueryLimitsProvider interface {
	// GetMaxEstimatedMemoryConsumptionPerQuery returns the maximum estimated memory allowed to be consumed by a query in bytes, or 0 to disable the limit.
	GetMaxEstimatedMemoryConsumptionPerQuery(ctx context.Context) (uint64, error)
}

// NewStaticQueryLimitsProvider returns a QueryLimitsProvider that always returns the provided limits.
//
// This should generally only be used in tests.
func NewStaticQueryLimitsProvider(maxEstimatedMemoryConsumptionPerQuery uint64) QueryLimitsProvider {
	return staticQueryLimitsProvider{
		maxEstimatedMemoryConsumptionPerQuery: maxEstimatedMemoryConsumptionPerQuery,
	}
}

type staticQueryLimitsProvider struct {
	maxEstimatedMemoryConsumptionPerQuery uint64
}

func (p staticQueryLimitsProvider) GetMaxEstimatedMemoryConsumptionPerQuery(_ context.Context) (uint64, error) {
	return p.maxEstimatedMemoryConsumptionPerQuery, nil
}
