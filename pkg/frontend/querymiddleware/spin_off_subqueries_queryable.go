// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

// spinOffSubqueriesQueryable is an implementor of the Queryable interface.
type spinOffSubqueriesQueryable struct {
	req                   MetricsQueryRequest
	annotationAccumulator *AnnotationAccumulator
	responseHeaders       *responseHeadersTracker
	handler               MetricsQueryHandler
	upstreamRangeHandler  MetricsQueryHandler
}

func newSpinOffSubqueriesQueryable(req MetricsQueryRequest, annotationAccumulator *AnnotationAccumulator, next MetricsQueryHandler, upstreamRangeHandler MetricsQueryHandler) *spinOffSubqueriesQueryable {
	return &spinOffSubqueriesQueryable{
		req:                   req,
		annotationAccumulator: annotationAccumulator,
		handler:               next,
		responseHeaders:       newResponseHeadersTracker(),
		upstreamRangeHandler:  upstreamRangeHandler,
	}
}

func (q *spinOffSubqueriesQueryable) Querier(_, _ int64) (storage.Querier, error) {
	return &spinOffSubqueriesQuerier{req: q.req, annotationAccumulator: q.annotationAccumulator, handler: q.handler, responseHeaders: q.responseHeaders, upstreamRangeHandler: q.upstreamRangeHandler}, nil
}

// getResponseHeaders returns the merged response headers received by the downstream
// when running the embedded queries.
func (q *spinOffSubqueriesQueryable) getResponseHeaders() []*PrometheusHeader {
	return q.responseHeaders.getHeaders()
}

type spinOffSubqueriesQuerier struct {
	req                   MetricsQueryRequest
	annotationAccumulator *AnnotationAccumulator
	handler               MetricsQueryHandler
	upstreamRangeHandler  MetricsQueryHandler

	// Keep track of response headers received when running embedded queries.
	responseHeaders *responseHeadersTracker
}

func (q *spinOffSubqueriesQuerier) Select(ctx context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	var name string
	values := map[string]string{}
	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName {
			name = matcher.Value
		} else {
			values[matcher.Name] = matcher.Value
		}
	}

	switch name {
	case astmapper.DownstreamQueryMetricName:
		downstreamReq, err := q.req.WithQuery(astmapper.DownstreamQueryLabelName)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		resp, err := q.handler.Do(ctx, downstreamReq)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		promRes, ok := resp.(*PrometheusResponse)
		if !ok {
			return storage.ErrSeriesSet(errors.Errorf("error invalid response type: %T, expected: %T", resp, &PrometheusResponse{}))
		}
		resStreams, err := ResponseToSamples(promRes)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}

		q.responseHeaders.mergeHeaders(promRes.Headers)
		q.annotationAccumulator.addInfos(promRes.Infos)
		q.annotationAccumulator.addWarnings(promRes.Warnings)
		return newSeriesSetFromEmbeddedQueriesResults([][]SampleStream{resStreams}, hints)
	case astmapper.SubqueryMetricName:
		expr := values[astmapper.SubqueryQueryLabelName]
		rangeStr := values[astmapper.SubqueryRangeLabelName]
		stepStr := values[astmapper.SubqueryStepLabelName]
		offsetStr := values[astmapper.SubqueryOffsetLabelName]
		if expr == "" || rangeStr == "" || stepStr == "" {
			return storage.ErrSeriesSet(errors.New("missing required labels for subquery"))
		}

		queryExpr, err := parser.ParseExpr(expr)
		if err != nil {
			return storage.ErrSeriesSet(errors.Wrap(err, "failed to parse subquery"))
		}

		queryRange, err := time.ParseDuration(rangeStr)
		if err != nil {
			return storage.ErrSeriesSet(errors.Wrap(err, "failed to parse subquery range"))
		}
		queryStep, err := time.ParseDuration(stepStr)
		if err != nil {
			return storage.ErrSeriesSet(errors.Wrap(err, "failed to parse subquery step"))
		}
		var queryOffset time.Duration
		if offsetStr == "" {
			queryOffset = 0
		} else if queryOffset, err = time.ParseDuration(offsetStr); err != nil {
			return storage.ErrSeriesSet(errors.Wrap(err, "failed to parse subquery offset"))
		}

		end := q.req.GetEnd() - queryOffset.Milliseconds()
		// Subqueries are aligned on absolute time, while range queries are aligned on relative time.
		// To match the same behavior, we can query slightly into the future.
		// The extra data points aren't used because the subquery is aggregated with an _over_time-style function.
		step := queryStep.Milliseconds()
		if end%step != 0 {
			end += step - (end % step)
		}
		// Calculate the earliest data point we need to query.
		start := end - queryRange.Milliseconds()

		// Split queries into multiple smaller queries if they have more than 11000 datapoints
		rangeStart := start
		var rangeQueries []MetricsQueryRequest
		for {
			var rangeEnd int64
			if remainingPoints := (end - rangeStart) / step; remainingPoints > maxResolutionPoints {
				rangeEnd = rangeStart + maxResolutionPoints*step
			} else {
				rangeEnd = end
			}
			headers := q.req.GetHeaders()
			headers = append(headers,
				&PrometheusHeader{Name: "X-Mimir-Spun-Off-Subquery", Values: []string{"true"}},
				&PrometheusHeader{Name: "Content-Type", Values: []string{"application/x-www-form-urlencoded"}},
			) // Downstream is the querier, which is HTTP req.
			newRangeRequest := NewPrometheusRangeQueryRequest(queryRangePathSuffix, headers, rangeStart, rangeEnd, step, q.req.GetLookbackDelta(), queryExpr, q.req.GetOptions(), q.req.GetHints())
			rangeQueries = append(rangeQueries, newRangeRequest)
			if rangeEnd == end {
				break
			}
			rangeStart = rangeEnd // Move the start to the end of the previous range.
		}

		streams := make([][]SampleStream, len(rangeQueries))
		for idx, req := range rangeQueries {
			resp, err := q.upstreamRangeHandler.Do(ctx, req)
			if err != nil {
				return storage.ErrSeriesSet(err)
			}
			promRes, ok := resp.(*PrometheusResponse)
			if !ok {
				return storage.ErrSeriesSet(errors.Errorf("error invalid response type: %T, expected: %T", resp, &PrometheusResponse{}))
			}
			resStreams, err := ResponseToSamples(promRes)
			if err != nil {
				return storage.ErrSeriesSet(err)
			}
			streams[idx] = resStreams
			q.annotationAccumulator.addInfos(promRes.Infos)
			q.annotationAccumulator.addWarnings(promRes.Warnings)
		}
		return newSeriesSetFromEmbeddedQueriesResults(streams, hints)
	default:
		return storage.ErrSeriesSet(errors.Errorf("invalid metric name for the spin off middleware: %s", name))
	}
}

// LabelValues implements storage.LabelQuerier.
func (q *spinOffSubqueriesQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, errNotImplemented
}

// LabelNames implements storage.LabelQuerier.
func (q *spinOffSubqueriesQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, errNotImplemented
}

// Close implements storage.LabelQuerier.
func (q *spinOffSubqueriesQuerier) Close() error {
	return nil
}
