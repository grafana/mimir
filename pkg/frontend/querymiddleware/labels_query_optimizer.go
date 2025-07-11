// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type labelsQueryOptimizer struct {
	next   http.RoundTripper
	codec  Codec
	limits Limits
	logger log.Logger
}

func newLabelsQueryOptimizer(codec Codec, limits Limits, next http.RoundTripper, logger log.Logger) http.RoundTripper {
	return &labelsQueryOptimizer{
		next:   next,
		codec:  codec,
		limits: limits,
		logger: logger,
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

	switch parsedReq.(type) {
	case *PrometheusLabelNamesQueryRequest:
		return l.optimizeLabelNamesRequest(ctx, req, parsedReq.(*PrometheusLabelNamesQueryRequest))
	default:
		return l.next.RoundTrip(req)
	}
}

func (l *labelsQueryOptimizer) optimizeLabelNamesRequest(ctx context.Context, req *http.Request, parsedReq *PrometheusLabelNamesQueryRequest) (*http.Response, error) {
	spanLog := spanlogger.FromContext(ctx, l.logger)

	// Check if there are any matchers to optimize.
	rawMatchers := parsedReq.GetLabelMatcherSets()
	if len(rawMatchers) == 0 {
		return l.next.RoundTrip(req)
	}

	// Optimize the matchers.
	optimizedMatchers, optimized, err := optimizeLabelNamesRequestMatchers(rawMatchers)
	if err != nil {
		// Do not log an error because this error is typically caused by a malformed request, which wouldn't be actionable.
		spanLog.DebugLog("msg", "failed to optimize label names matchers", "err", err)
		// TODO track a metric with reason "skipped"
		return l.next.RoundTrip(req)
	}

	if !optimized {
		return l.next.RoundTrip(req)
	}

	// Create a new request with optimized matchers.
	optimizedParsedReq, err := parsedReq.WithLabelMatcherSets(optimizedMatchers)
	if err != nil {
		level.Error(l.logger).Log("msg", "failed to clone label names request with optimized matchers", "err", err)
		// TODO track a metric to count the number of failures
		return l.next.RoundTrip(req)
	}

	// Encode the optimized request back to HTTP.
	optimizedReq, err := l.codec.EncodeLabelsSeriesQueryRequest(ctx, optimizedParsedReq)
	if err != nil {
		level.Error(l.logger).Log("msg", "failed to encode label names request with optimized matchers", "err", err)
		// TODO track a metric to count the number of failures
		return l.next.RoundTrip(req)
	}

	// TODO DEBUG
	level.Info(l.logger).Log("msg", "optimized label names query", "original_matchers", parsedReq.LabelMatcherSets, "optimized_matchers", optimizedParsedReq.GetLabelMatcherSets())

	spanLog.DebugLog("msg", "optimized label names query", "original_matchers", parsedReq.LabelMatcherSets, "optimized_matchers", optimizedParsedReq.GetLabelMatcherSets())
	return l.next.RoundTrip(optimizedReq)
}

func optimizeLabelNamesRequestMatchers(rawMatcherSets []string) (_ []string, optimized bool, _ error) {
	matcherSets, err := parser.ParseMetricSelectors(rawMatcherSets)
	if err != nil {
		return nil, false, err
	}

	optimizedRawMatcherSets := make([]string, 0, len(rawMatcherSets))

	for _, matchers := range matcherSets {
		// If an empty matcher `{}` was provided we need to preserve it because it's an invalid matcher
		// for the labels API, but we want to get the actual error from downstream. In this case we just
		// don't optimize any matchers set.
		if len(matchers) == 0 {
			return nil, false, nil
		}

		optimizedMatchers := make([]*labels.Matcher, 0, len(matchers))

		for _, matcher := range matchers {
			// Filter out `__name__!=""` matcher because all series in Mimir have a metric name
			// so this matcher matches all series but very expensive to run.
			if matcher.Name == labels.MetricName && matcher.Type == labels.MatchNotEqual && matcher.Value == "" {
				optimized = true
				continue
			}

			// Keep the matcher as is.
			optimizedMatchers = append(optimizedMatchers, matcher)
		}

		if len(optimizedMatchers) > 0 {
			optimizedRawMatcherSets = append(optimizedRawMatcherSets, util.LabelMatchersToString(optimizedMatchers))
		} else {
			// It was not an empty matcher (see condition above), and all the matchers have been removed.
			// It means that we should match all series. Since the matchers in different sets are in a "or" condition
			// it practically means we want to query all series, so we just return no matchers at all.
			return []string{}, true, nil
		}
	}

	return optimizedRawMatcherSets, optimized, nil
}
