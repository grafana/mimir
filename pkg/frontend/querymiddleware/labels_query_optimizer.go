// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type labelsQueryOptimizer struct {
	next   http.RoundTripper
	codec  Codec
	limits Limits
	logger log.Logger

	// Metrics.
	totalQueries     prometheus.Counter
	rewrittenQueries prometheus.Counter
	failedQueries    prometheus.Counter
}

func newLabelsQueryOptimizer(codec Codec, limits Limits, next http.RoundTripper, logger log.Logger, registerer prometheus.Registerer) http.RoundTripper {
	return &labelsQueryOptimizer{
		next:   next,
		codec:  codec,
		limits: limits,
		logger: logger,
		totalQueries: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_query_frontend_labels_optimizer_queries_total",
			Help: "Total number of label API requests processed by the optimizer when enabled.",
		}),
		rewrittenQueries: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_query_frontend_labels_optimizer_queries_rewritten_total",
			Help: "Total number of label API requests that have been optimized.",
		}),
		failedQueries: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_query_frontend_labels_optimizer_queries_failed_total",
			Help: "Total number of label API requests that failed to get optimized.",
		}),
	}
}

func (l *labelsQueryOptimizer) RoundTrip(req *http.Request) (*http.Response, error) {
	spanLog, ctx := spanlogger.New(req.Context(), l.logger, tracer, "labelsQueryOptimizer")
	defer spanLog.Finish()

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Check if the optimization is enabled for this tenant.
	if !validation.AllTrueBooleansPerTenant(tenantIDs, l.limits.LabelsQueryOptimizerEnabled) {
		return l.next.RoundTrip(req)
	}

	// Decode the request. If decoding fails, pass through the original request to ensure the client
	// will get a proper error message.
	parsedReq, err := l.codec.DecodeLabelsSeriesQueryRequest(ctx, req)
	if err != nil {
		return l.next.RoundTrip(req)
	}

	switch parsedReq := parsedReq.(type) {
	case *PrometheusLabelNamesQueryRequest:
		return l.optimizeRequest(ctx, req, parsedReq)
	case *PrometheusLabelValuesQueryRequest:
		return l.optimizeRequest(ctx, req, parsedReq)
	default:
		return l.next.RoundTrip(req)
	}
}

func (l *labelsQueryOptimizer) optimizeRequest(ctx context.Context, req *http.Request, parsedReq LabelsSeriesQueryRequest) (*http.Response, error) {
	spanLog := spanlogger.FromContext(ctx, l.logger)

	// Track the total number of requests processed when optimization is enabled.
	l.totalQueries.Inc()

	// Check if there are any matchers to optimize.
	rawMatchers := parsedReq.GetLabelMatcherSets()
	if len(rawMatchers) == 0 {
		return l.next.RoundTrip(req)
	}

	// Optimize the matchers.
	optimizedMatchers, optimized, err := optimizeLabelsRequestMatchers(rawMatchers)
	if err != nil {
		// Do not log an error because this error is typically caused by a malformed request, which wouldn't be actionable.
		spanLog.DebugLog("msg", "failed to optimize labels request matchers", "err", err)
		return l.next.RoundTrip(req)
	}

	if !optimized {
		return l.next.RoundTrip(req)
	}

	// Create a new request with optimized matchers.
	optimizedParsedReq, err := parsedReq.WithLabelMatcherSets(optimizedMatchers)
	if err != nil {
		level.Error(l.logger).Log("msg", "failed to clone labels request with optimized matchers", "err", err)
		l.failedQueries.Inc()
		return l.next.RoundTrip(req)
	}

	// Encode the optimized request back to HTTP.
	optimizedReq, err := l.codec.EncodeLabelsSeriesQueryRequest(ctx, optimizedParsedReq)
	if err != nil {
		level.Error(l.logger).Log("msg", "failed to encode labels request with optimized matchers", "err", err)
		l.failedQueries.Inc()
		return l.next.RoundTrip(req)
	}

	// Track successful optimization.
	l.rewrittenQueries.Inc()
	spanLog.DebugLog("msg", "optimized labels query", "original_matchers", strings.Join(parsedReq.GetLabelMatcherSets(), " "), "optimized_matchers", strings.Join(optimizedParsedReq.GetLabelMatcherSets(), " "))
	return l.next.RoundTrip(optimizedReq)
}

func optimizeLabelsRequestMatchers(rawMatcherSets []string) (_ []string, optimized bool, _ error) {
	p := astmapper.CreateParser()
	matcherSets, err := p.ParseMetricSelectors(rawMatcherSets)
	if err != nil {
		return nil, false, err
	}

	optimizedRawMatcherSets := make([]string, 0, len(rawMatcherSets))

	for _, matchers := range matcherSets {
		optimizedMatchers := make([]*labels.Matcher, 0, len(matchers))
		hasNonEmptyMatchers := false

		for _, matcher := range matchers {
			// Before filtering out any matcher we should check if the among the original matchers
			// there's anyone not matching the empty string. This will be used later to ensure there's
			// at least one non-empty matcher.
			if !matcher.Matches("") {
				hasNonEmptyMatchers = true
			}

			// Filter out a matcher that matches any string because it matches all series.
			if matcher.Type == labels.MatchRegexp && matcher.Value == ".*" {
				optimized = true
				continue
			}

			// Filter out `__name__!=""` matcher because all series in Mimir have a metric name
			// so this matcher matches all series but very expensive to run.
			if matcher.Name == model.MetricNameLabel && matcher.Type == labels.MatchNotEqual && matcher.Value == "" {
				optimized = true
				continue
			}

			// Keep the matcher as is.
			optimizedMatchers = append(optimizedMatchers, matcher)
		}

		if !hasNonEmptyMatchers {
			// If the matchers set only contain empty matchers then we need to preserve them because it's an invalid case
			// for the labels API, but we want to get the actual error from downstream. In this case we just
			// don't optimize any matchers set.
			return rawMatcherSets, false, nil
		}

		if len(optimizedMatchers) == 0 {
			// It was not an empty matcher (see previous condition), and all the matchers have been removed.
			// It means that we should match all series. Since the matchers in different sets are in a "or" condition
			// it practically means we want to query all series, so we just return no matchers at all.
			return []string{}, true, nil
		}

		optimizedRawMatcherSets = append(optimizedRawMatcherSets, util.LabelMatchersToString(optimizedMatchers))
	}

	return optimizedRawMatcherSets, optimized, nil
}
