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

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/querier/stats"
)

const defaultLookbackDelta = 5 * time.Minute // This should be the same value as github.com/prometheus/prometheus/promql.defaultLookbackDelta.

func NewEngine(opts EngineOpts, limitsProvider QueryLimitsProvider, metrics *stats.QueryMetrics, logger log.Logger) (promql.QueryEngine, error) {
	lookbackDelta := opts.CommonOpts.LookbackDelta
	if lookbackDelta == 0 {
		lookbackDelta = defaultLookbackDelta
	}

	if !opts.CommonOpts.EnableAtModifier {
		return nil, errors.New("disabling @ modifier not supported by Mimir query engine")
	}

	if !opts.CommonOpts.EnableNegativeOffset {
		return nil, errors.New("disabling negative offsets not supported by Mimir query engine")
	}

	if opts.CommonOpts.EnablePerStepStats {
		return nil, errors.New("enabling per-step stats not supported by Mimir query engine")
	}

	if opts.CommonOpts.EnableDelayedNameRemoval {
		return nil, errors.New("enabling delayed name removal not supported by Mimir query engine")
	}

	return &Engine{
		lookbackDelta:            lookbackDelta,
		timeout:                  opts.CommonOpts.Timeout,
		limitsProvider:           limitsProvider,
		activeQueryTracker:       opts.CommonOpts.ActiveQueryTracker,
		mqeOpts:                  opts.MQEOpts,
		noStepSubqueryIntervalFn: opts.CommonOpts.NoStepSubqueryIntervalFn,

		logger: logger,
		estimatedPeakMemoryConsumption: promauto.With(opts.CommonOpts.Reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "cortex_mimir_query_engine_estimated_query_peak_memory_consumption",
			Help:                        "Estimated peak memory consumption of each query (in bytes)",
			NativeHistogramBucketFactor: 1.1,
		}),
		queriesRejectedDueToPeakMemoryConsumption: metrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxEstimatedQueryMemoryConsumption),

		pedantic: opts.Pedantic,
	}, nil
}

type Engine struct {
	lookbackDelta            time.Duration
	timeout                  time.Duration
	limitsProvider           QueryLimitsProvider
	activeQueryTracker       promql.QueryTracker
	mqeOpts                  MQEOpts
	noStepSubqueryIntervalFn func(rangeMillis int64) int64

	logger                                    log.Logger
	estimatedPeakMemoryConsumption            prometheus.Histogram
	queriesRejectedDueToPeakMemoryConsumption prometheus.Counter

	// When operating in pedantic mode, we panic if memory consumption is > 0 after Query.Close()
	// (indicating something was not returned to a pool).
	pedantic bool
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
