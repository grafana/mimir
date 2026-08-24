// SPDX-License-Identifier: AGPL-3.0-only

package subqueryspinoff

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type Options struct {
	SpinOffSimpleSubqueries            bool `yaml:"subquery_spin_off_simple_subqueries" category:"experimental"`
	SpinOffWithExcessDownstreamQueries bool `yaml:"subquery_spin_off_with_excess_downstream_queries" category:"experimental"`
}

// Map rewrites expr to spin off its subqueries and decides whether doing so is worthwhile, recording the
// outcome in metrics and (on success) in the query stats found in ctx.
//
// It returns the rewritten expression and true when the query should be executed with its subqueries spun
// off.
//
// It returns (nil, false) when the caller should fall back to executing the original query through the
// regular downstream path, which happens when mapping failed, the query had no subqueries to spin off, or
// spinning off was not deemed worthwhile. The original expression is not modified in this case.
func Map(ctx context.Context, expr parser.Expr, wrapper astmapper.SubquerySpinOffWrapper, metrics Metrics, defaultStepFunc func(rangeMillis int64) int64, timeout time.Duration, logger *spanlogger.SpanLogger, opts Options) (parser.Expr, bool) {
	mapperStats := astmapper.NewSubquerySpinOffMapperStats()
	mapperCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	mapper := astmapper.NewSubquerySpinOffMapper(wrapper, defaultStepFunc, logger, mapperStats, opts.SpinOffSimpleSubqueries)

	spinOffQuery, err := mapper.Map(mapperCtx, expr)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			level.Error(logger).Log("msg", "timeout while spinning off subqueries, please fill in a bug report with this query, falling back to try executing without spin-off", "err", err)
		} else {
			level.Error(logger).Log("msg", "failed to map the input query, falling back to try executing without spin-off", "err", err)
		}
		metrics.SpinOffSkipped.WithLabelValues(SkippedReasonMappingFailed).Inc()
		return nil, false
	}

	if mapperStats.SpunOffSubqueries() == 0 {
		// the query has no subqueries, so continue downstream
		logger.DebugLog("msg", "input query resulted in a no operation, falling back to try executing without spinning off subqueries")
		metrics.SpinOffSkipped.WithLabelValues(SkippedReasonNoSubqueries).Inc()
		return nil, false
	}

	if !opts.SpinOffWithExcessDownstreamQueries && mapperStats.DownstreamQueries() > mapperStats.SpunOffSubqueries() {
		// the query has more downstream queries than subqueries, so continue downstream
		// It's probably more efficient to just execute the query as is
		logger.DebugLog("msg", "input query resulted in more downstream queries than subqueries, falling back to try executing without spinning off subqueries")
		metrics.SpinOffSkipped.WithLabelValues(SkippedReasonDownstreamQueries).Inc()
		return nil, false
	}

	logger.DebugLog("msg", "instant query has been rewritten to spin-off subqueries", "rewritten", spinOffQuery, "regular_downstream_queries", mapperStats.DownstreamQueries(), "subqueries_spun_off", mapperStats.SpunOffSubqueries())

	// Update query stats.
	queryStats := stats.FromContext(ctx)
	queryStats.AddSpunOffSubqueries(uint32(mapperStats.SpunOffSubqueries()))

	// Update metrics.
	metrics.SpinOffSuccesses.Inc()
	metrics.SpunOffSubqueries.Add(float64(mapperStats.SpunOffSubqueries()))
	metrics.SpunOffSubqueriesPerQuery.Observe(float64(mapperStats.SpunOffSubqueries()))

	return spinOffQuery, true
}
