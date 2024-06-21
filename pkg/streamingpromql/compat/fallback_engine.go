// SPDX-License-Identifier: AGPL-3.0-only

package compat

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type EngineWithFallback struct {
	preferred promql.QueryEngine
	fallback  promql.QueryEngine

	supportedQueries   prometheus.Counter
	unsupportedQueries *prometheus.CounterVec

	logger log.Logger
}

const fallbackForcedByHTTPHeader = "fallback forced by HTTP header"

func NewEngineWithFallback(preferred, fallback promql.QueryEngine, reg prometheus.Registerer, logger log.Logger) promql.QueryEngine {
	return &EngineWithFallback{
		preferred: preferred,
		fallback:  fallback,

		supportedQueries: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_supported_queries_total",
			Help: "Total number of queries that were supported by the Mimir query engine.",
		}),
		unsupportedQueries: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_unsupported_queries_total",
			Help: "Total number of queries that were not supported by the Mimir query engine and so fell back to Prometheus' engine.",
		}, []string{"reason"}),

		logger: logger,
	}
}

func (e EngineWithFallback) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	reason := ""

	if isForceFallbackEnabled(ctx) {
		reason = fallbackForcedByHTTPHeader
	} else {
		query, err := e.preferred.NewInstantQuery(ctx, q, opts, qs, ts)

		if err == nil {
			e.supportedQueries.Inc()
			return query, nil
		}

		notSupportedErr := NotSupportedError{}
		if !errors.As(err, &notSupportedErr) {
			// Don't bother trying the fallback engine if we failed for a reason other than the expression not being supported.
			return nil, err
		}

		reason = notSupportedErr.reason
	}

	logger := spanlogger.FromContext(ctx, e.logger)
	level.Info(logger).Log("msg", "falling back to Prometheus' PromQL engine", "reason", reason, "expr", qs)
	e.unsupportedQueries.WithLabelValues(reason).Inc()

	return e.fallback.NewInstantQuery(ctx, q, opts, qs, ts)
}

func (e EngineWithFallback) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	reason := ""

	if isForceFallbackEnabled(ctx) {
		reason = fallbackForcedByHTTPHeader
	} else {
		query, err := e.preferred.NewRangeQuery(ctx, q, opts, qs, start, end, interval)

		if err == nil {
			e.supportedQueries.Inc()
			return query, nil
		}

		notSupportedErr := NotSupportedError{}
		if !errors.As(err, &notSupportedErr) {
			// Don't bother trying the fallback engine if we failed for a reason other than the expression not being supported.
			return nil, err
		}

		reason = notSupportedErr.reason
	}

	logger := spanlogger.FromContext(ctx, e.logger)
	level.Info(logger).Log("msg", "falling back to Prometheus' PromQL engine", "reason", reason, "expr", qs)
	e.unsupportedQueries.WithLabelValues(reason).Inc()

	return e.fallback.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
}
