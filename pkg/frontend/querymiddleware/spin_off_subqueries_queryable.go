// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

// spinOffSubqueriesQueryable is an implementor of the Queryable interface.
type spinOffSubqueriesQueryable struct {
	req                   MetricsQueryRequest
	annotationAccumulator *AnnotationAccumulator
	responseHeaders       *responseHeadersTracker
	handler               MetricsQueryHandler
	rangeHandler          MetricsQueryHandler
}

func newSpinOffSubqueriesQueryable(req MetricsQueryRequest, annotationAccumulator *AnnotationAccumulator, next MetricsQueryHandler, rangeHandler MetricsQueryHandler) *spinOffSubqueriesQueryable {
	return &spinOffSubqueriesQueryable{
		req:                   req,
		annotationAccumulator: annotationAccumulator,
		handler:               next,
		rangeHandler:          rangeHandler,
		responseHeaders:       newResponseHeadersTracker(),
	}
}

func (q *spinOffSubqueriesQueryable) Querier(_, _ int64) (storage.Querier, error) {
	return &spinOffSubqueriesQuerier{
		req:                   q.req,
		annotationAccumulator: q.annotationAccumulator,
		handler:               q.handler,
		rangeHandler:          q.rangeHandler,
		responseHeaders:       q.responseHeaders,
	}, nil
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
	rangeHandler          MetricsQueryHandler

	// Keep track of response headers received when running embedded queries.
	responseHeaders *responseHeadersTracker
}

func (q *spinOffSubqueriesQuerier) Select(ctx context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	var name string
	values := map[string]string{}
	for _, matcher := range matchers {
		if matcher.Name == model.MetricNameLabel {
			name = matcher.Value
		} else {
			values[matcher.Name] = matcher.Value
		}
	}

	switch name {
	case astmapper.DownstreamQueryMetricName:
		query, ok := values[astmapper.DownstreamQueryLabelName]
		if !ok {
			return storage.ErrSeriesSet(errors.New("missing required labels for downstream query"))
		}

		downstreamReq, err := q.req.WithQuery(query)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}

		resp, err := q.handler.Do(ctx, downstreamReq)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}

		// newSeriesSetFromEmbeddedQueriesResults copies the values, so it is safe to close the query after it is done.
		// (It does not copy labels, but MQE does not reuse labels as labels.Labels is immutable so it is still safe).
		defer resp.Close()

		promRes, ok := resp.GetPrometheusResponse()
		if !ok {
			return storage.ErrSeriesSet(errors.Errorf("error invalid response type: %T, expected a Prometheus response", resp))
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

		queryExpr, err := promqlext.NewPromQLParser().ParseExpr(expr)
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

		start := q.req.GetStart()
		end := q.req.GetEnd()
		step := queryStep.Milliseconds()

		// The following code only works for instant queries. Supporting subqueries within range queries would
		// require lots of changes. It hasnt been tested.
		if start != end {
			return storage.ErrSeriesSet(errors.New("subqueries spin-off is not supported in range queries"))
		}

		// Compute the spun-off range query window with the same logic MQE uses natively for subqueries,
		// so the spun-off range query evaluates the same grid and its result matches native evaluation.
		alignedStart, alignedEnd, ok := subquerySpinOffChildTimeRange(start, queryRange, queryStep, queryOffset)
		if !ok {
			// The subquery range selects no steps (e.g. range smaller than the step and misaligned): no series.
			return storage.EmptySeriesSet()
		}

		// Split queries into multiple smaller queries if they have more than 11000 datapoints
		rangeStart := alignedStart
		var rangeQueries []MetricsQueryRequest
		rangePath := strings.Replace(q.req.GetPath(), instantQueryPathSuffix, queryRangePathSuffix, 1)
		for {
			var rangeEnd int64
			if remainingPoints := (alignedEnd - rangeStart) / step; remainingPoints > maxResolutionPoints {
				rangeEnd = rangeStart + maxResolutionPoints*step
			} else {
				rangeEnd = alignedEnd
			}
			newRangeRequest := NewPrometheusRangeQueryRequest(rangePath, q.req.GetHeaders(), rangeStart, rangeEnd, step, q.req.GetLookbackDelta(), queryExpr, q.req.GetOptions(), q.req.GetHints(), q.req.GetStats())
			rangeQueries = append(rangeQueries, newRangeRequest)
			if rangeEnd == alignedEnd {
				break
			}
			rangeStart = rangeEnd // Move the start to the end of the previous range.
		}

		sets := make([]storage.SeriesSet, len(rangeQueries))
		for idx, req := range rangeQueries {
			resp, err := q.rangeHandler.Do(ctx, req)
			if err != nil {
				return storage.ErrSeriesSet(fmt.Errorf("error running subquery: %w", err))
			}
			// newSeriesSetFromEmbeddedQueriesResults copies the values, so it is safe to close the query after it is done.
			// (It does not copy labels, but MQE does not reuse labels as labels.Labels is immutable so it is still safe).
			defer resp.Close()

			promRes, ok := resp.GetPrometheusResponse()
			if !ok {
				return storage.ErrSeriesSet(errors.Errorf("error invalid response type: %T, expected a Prometheus response", resp))
			}
			resStreams, err := ResponseToSamples(promRes)
			if err != nil {
				return storage.ErrSeriesSet(err)
			}
			sets[idx] = newSeriesSetFromEmbeddedQueriesResults([][]SampleStream{resStreams}, hints)
			q.annotationAccumulator.addInfos(promRes.Infos)
			q.annotationAccumulator.addWarnings(promRes.Warnings)
		}

		return storage.NewMergeSeriesSet(sets, 0, storage.ChainedSeriesMerge)
	default:
		return storage.ErrSeriesSet(errors.Errorf("invalid metric name for the spin-off middleware: %s", name))
	}
}

// subquerySpinOffChildTimeRange returns the [start, end] (both inclusive, ms since epoch) of the
// range query that a spun-off subquery evaluated at queryTimeMS should run over. It delegates to the
// same logic MQE uses natively for subqueries so the spun-off result matches the engine's native
// subquery grid: the first step is left-open at queryTime-range and the last step is the largest
// step-aligned timestamp <= queryTime (right-closed). ok is false when the range selects no steps.
func subquerySpinOffChildTimeRange(queryTimeMS int64, queryRange, queryStep, queryOffset time.Duration) (start, end int64, ok bool) {
	tr := core.SubqueryChildrenTimeRange(types.NewInstantQueryTimeRange(time.UnixMilli(queryTimeMS)), queryRange, queryStep, queryOffset, nil)
	if tr.StepCount == 0 {
		return 0, 0, false
	}
	// tr.EndT may not be step-aligned (it is the raw evaluation time for instant queries), so use the
	// last actual step on the grid.
	return tr.StartT, tr.IndexTime(int64(tr.StepCount - 1)), true
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
