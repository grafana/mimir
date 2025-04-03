// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/storage/series"
)

// spinOffSubqueriesQueryable is an implementor of the Queryable interface.
type spinOffSubqueriesQueryable struct {
	req                   MetricsQueryRequest
	annotationAccumulator *AnnotationAccumulator
	responseHeaders       *responseHeadersTracker
	handler               MetricsQueryHandler
	rangeHandler          MetricsQueryHandler

	queryCacheMu sync.RWMutex
	queryCache   map[string]storage.SeriesSet
	// inProgress tracks queries that are currently being executed
	inProgressMu sync.Mutex
	inProgress   map[string]chan struct{}
}

func newSpinOffSubqueriesQueryable(req MetricsQueryRequest, annotationAccumulator *AnnotationAccumulator, next MetricsQueryHandler, rangeHandler MetricsQueryHandler) *spinOffSubqueriesQueryable {
	return &spinOffSubqueriesQueryable{
		req:                   req,
		annotationAccumulator: annotationAccumulator,
		handler:               next,
		rangeHandler:          rangeHandler,
		responseHeaders:       newResponseHeadersTracker(),
		queryCache:            make(map[string]storage.SeriesSet),
		inProgress:            make(map[string]chan struct{}),
	}
}

func (q *spinOffSubqueriesQueryable) Querier(_, _ int64) (storage.Querier, error) {
	return &spinOffSubqueriesQuerier{
		req:                   q.req,
		annotationAccumulator: q.annotationAccumulator,
		handler:               q.handler,
		rangeHandler:          q.rangeHandler,
		responseHeaders:       q.responseHeaders,
		queryCache:            q.queryCache,
		queryCacheMu:          &q.queryCacheMu,
		inProgress:            q.inProgress,
		inProgressMu:          &q.inProgressMu,
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
	queryCache      map[string]storage.SeriesSet
	queryCacheMu    *sync.RWMutex
	inProgress      map[string]chan struct{}
	inProgressMu    *sync.Mutex
}

// loadFromCacheOrWait loads a query result from cache or waits for an in-progress query to complete.
// If the query is not in cache and not in progress, it returns nil.
func (q *spinOffSubqueriesQuerier) loadFromCacheOrWait(ctx context.Context, key string) storage.SeriesSet {
	// First check if the query is in progress
	q.inProgressMu.Lock()
	if waitCh, exists := q.inProgress[key]; exists {
		q.inProgressMu.Unlock()
		// Wait for the query to complete
		select {
		case <-waitCh:
			// Query completed, check cache
		case <-ctx.Done():
			return storage.ErrSeriesSet(ctx.Err())
		}
	} else {
		// Mark this query as in progress
		waitCh := make(chan struct{})
		q.inProgress[key] = waitCh
		q.inProgressMu.Unlock()
		return nil
	}

	q.queryCacheMu.RLock()
	defer q.queryCacheMu.RUnlock()
	if set, ok := q.queryCache[key]; ok {
		concreteSet, ok := set.(*series.ConcreteSeriesSet)
		if !ok {
			return storage.ErrSeriesSet(fmt.Errorf("invalid series set type in cache: %T", set))
		}
		return concreteSet.ShallowClone()
	}
	return nil
}

// storeInCache stores a query result in the cache and closes the wait channel.
func (q *spinOffSubqueriesQuerier) storeInCache(key string, set storage.SeriesSet) {
	q.queryCacheMu.Lock()
	q.queryCache[key] = set
	q.queryCacheMu.Unlock()

	q.inProgressMu.Lock()
	waitCh := q.inProgress[key]
	delete(q.inProgress, key)
	q.inProgressMu.Unlock()
	close(waitCh)
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
		query, ok := values[astmapper.DownstreamQueryLabelName]
		if !ok {
			return storage.ErrSeriesSet(errors.New("missing required labels for downstream query"))
		}

		set := q.loadFromCacheOrWait(ctx, query)
		if set != nil {
			return set
		}

		downstreamReq, err := q.req.WithQuery(query)
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

		set = newSeriesSetFromEmbeddedQueriesResults([][]SampleStream{resStreams}, hints, false)
		q.storeInCache(query, set)
		return set

	case astmapper.SubqueryMetricName:
		expr := values[astmapper.SubqueryQueryLabelName]
		rangeStr := values[astmapper.SubqueryRangeLabelName]
		stepStr := values[astmapper.SubqueryStepLabelName]
		offsetStr := values[astmapper.SubqueryOffsetLabelName]
		if expr == "" || rangeStr == "" || stepStr == "" {
			return storage.ErrSeriesSet(errors.New("missing required labels for subquery"))
		}

		key := fmt.Sprintf("%s:%s:%s:%s", expr, rangeStr, stepStr, offsetStr)

		var set storage.SeriesSet
		set = q.loadFromCacheOrWait(ctx, key)
		if set != nil {
			return set
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

		start := q.req.GetStart()
		end := q.req.GetEnd()
		step := queryStep.Milliseconds()

		// The following code only works for instant queries. Supporting subqueries within range queries would
		// require lots of changes. It hasnt been tested.
		if start != end {
			return storage.ErrSeriesSet(errors.New("subqueries spin-off is not supported in range queries"))
		}

		// Subqueries are always aligned to absolute time in PromQL, so we need to make the same adjustment here for correctness.
		// Find the first timestamp inside the subquery range that is aligned to the step.
		// This is taken from MQE: https://github.com/grafana/mimir/blob/266a393379b2c981a83557c5d66e56c97251ffeb/pkg/streamingpromql/query.go#L384-L398
		alignedStart := step * ((start - queryOffset.Milliseconds() - queryRange.Milliseconds()) / step)
		if alignedStart < start-queryOffset.Milliseconds()-queryRange.Milliseconds() {
			alignedStart += step
		}
		// Align the end too, to allow for caching
		alignedEnd := alignedStart + queryRange.Milliseconds()
		if alignedEnd > end {
			alignedEnd -= step
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
			newRangeRequest := NewPrometheusRangeQueryRequest(rangePath, q.req.GetHeaders(), rangeStart, rangeEnd, step, q.req.GetLookbackDelta(), queryExpr, q.req.GetOptions(), q.req.GetHints())
			rangeQueries = append(rangeQueries, newRangeRequest)
			if rangeEnd == alignedEnd {
				break
			}
			rangeStart = rangeEnd // Move the start to the end of the previous range.
		}

		streams := make([][]SampleStream, len(rangeQueries))
		for idx, req := range rangeQueries {
			resp, err := q.rangeHandler.Do(ctx, req)
			if err != nil {
				return storage.ErrSeriesSet(fmt.Errorf("error running subquery: %w", err))
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

		set = newSeriesSetFromEmbeddedQueriesResults(streams, hints, len(streams) > 1)
		q.storeInCache(key, set)
		return set

	default:
		return storage.ErrSeriesSet(errors.Errorf("invalid metric name for the spin-off middleware: %s", name))
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
