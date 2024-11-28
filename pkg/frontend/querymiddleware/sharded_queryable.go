// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/value.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/queryable.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/grafana/dskit/concurrency"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util"
)

var (
	errMissingEmbeddedQuery = errors.New("missing embedded query")
	errNoEmbeddedQueries    = errors.New("shardedQuerier is expecting embedded queries but didn't find any")
	errNotImplemented       = errors.New("not implemented")
)

type HandleEmbeddedQueryFunc func(ctx context.Context, queryExpr astmapper.EmbeddedQuery, query MetricsQueryRequest, handler MetricsQueryHandler) ([]SampleStream, *PrometheusResponse, error)

// shardedQueryable is an implementor of the Queryable interface.
type shardedQueryable struct {
	req                   MetricsQueryRequest
	annotationAccumulator *AnnotationAccumulator
	handler               MetricsQueryHandler
	upstreamRangeHandler  MetricsQueryHandler
	responseHeaders       *responseHeadersTracker
	handleEmbeddedQuery   HandleEmbeddedQueryFunc
}

// NewShardedQueryable makes a new shardedQueryable. We expect a new queryable is created for each
// query, otherwise the response headers tracker doesn't work as expected, because it merges the
// headers for all queries run through the queryable and never reset them.
func NewShardedQueryable(req MetricsQueryRequest, annotationAccumulator *AnnotationAccumulator, next, upstreamRangeHandler MetricsQueryHandler, handleEmbeddedQuery HandleEmbeddedQueryFunc) *shardedQueryable { //nolint:revive
	if handleEmbeddedQuery == nil {
		handleEmbeddedQuery = defaultHandleEmbeddedQueryFunc
	}
	return &shardedQueryable{
		req:                   req,
		annotationAccumulator: annotationAccumulator,
		handler:               next,
		upstreamRangeHandler:  upstreamRangeHandler,
		responseHeaders:       newResponseHeadersTracker(),
		handleEmbeddedQuery:   handleEmbeddedQuery,
	}
}

// Querier implements storage.Queryable.
func (q *shardedQueryable) Querier(_, _ int64) (storage.Querier, error) {
	return &shardedQuerier{req: q.req, annotationAccumulator: q.annotationAccumulator, handler: q.handler, upstreamRangeHandler: q.upstreamRangeHandler, responseHeaders: q.responseHeaders, handleEmbeddedQuery: q.handleEmbeddedQuery}, nil
}

// getResponseHeaders returns the merged response headers received by the downstream
// when running the embedded queries.
func (q *shardedQueryable) getResponseHeaders() []*PrometheusHeader {
	return q.responseHeaders.getHeaders()
}

// shardedQuerier implements the storage.Querier interface with capabilities to parse the embedded queries
// from the astmapper.EmbeddedQueriesMetricName metric label value and concurrently run embedded queries
// through the downstream handler.
type shardedQuerier struct {
	req                   MetricsQueryRequest
	annotationAccumulator *AnnotationAccumulator
	handler               MetricsQueryHandler
	upstreamRangeHandler  MetricsQueryHandler

	// Keep track of response headers received when running embedded queries.
	responseHeaders *responseHeadersTracker

	handleEmbeddedQuery HandleEmbeddedQueryFunc
}

// Select implements storage.Querier.
// The sorted bool is ignored because the series is always sorted.
func (q *shardedQuerier) Select(ctx context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	var aggregatedSubqueryExpr string
	for _, matcher := range matchers {
		if matcher.Name == astmapper.AggregatedSubqueryMetricName {
			aggregatedSubqueryExpr = matcher.Value
			break
		}
	}

	// Handle subqueries.
	req := q.req
	if aggregatedSubqueryExpr != "" && IsInstantQuery(req.GetPath()) && q.upstreamRangeHandler != nil {
		parsedExpr, err := parser.ParseExpr(aggregatedSubqueryExpr)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		asAgg, ok := parsedExpr.(*parser.Call)
		if !ok {
			return storage.ErrSeriesSet(fmt.Errorf("expected agg expression, got %s (%T)", parsedExpr, parsedExpr))
		}

		subquery, ok := asAgg.Args[0].(*parser.SubqueryExpr)
		if !ok {
			return storage.ErrSeriesSet(fmt.Errorf("expected subquery expression, got %s", asAgg.Args[0]))
		}

		end := req.GetEnd()
		start := end - subquery.Range.Milliseconds()

		newRangeRequest := NewPrometheusRangeQueryRequest(queryRangePathSuffix, req.GetHeaders(), start, end, subquery.Step.Milliseconds(), req.GetLookbackDelta(), subquery.Expr, req.GetOptions(), req.GetHints())

		upstreamRangeHandlerResp, err := q.upstreamRangeHandler.Do(ctx, newRangeRequest)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}

		upstreamRangeHandlerPromRes, ok := upstreamRangeHandlerResp.(*PrometheusResponse)
		if !ok {
			return storage.ErrSeriesSet(errors.Errorf("error invalid response type: %T, expected: %T", upstreamRangeHandlerResp, &PrometheusResponse{}))
		}

		resStreams, err := ResponseToSamples(upstreamRangeHandlerPromRes)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}

		return newSeriesSetFromEmbeddedQueriesResults([][]SampleStream{resStreams}, hints)
	}

	var embeddedQuery string
	var isEmbedded bool
	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName && matcher.Value == astmapper.EmbeddedQueriesMetricName {
			isEmbedded = true
		}

		if matcher.Name == astmapper.EmbeddedQueriesLabelName {
			embeddedQuery = matcher.Value
		}
	}

	if !isEmbedded {
		return storage.ErrSeriesSet(errNoEmbeddedQueries)
	}
	if embeddedQuery == "" {
		return storage.ErrSeriesSet(errMissingEmbeddedQuery)
	}

	// Decode the queries from the label value.
	queries, err := astmapper.JSONCodec.Decode(embeddedQuery)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	return q.handleEmbeddedQueries(ctx, queries, hints)
}

func defaultHandleEmbeddedQueryFunc(ctx context.Context, queryExpr astmapper.EmbeddedQuery, query MetricsQueryRequest, handler MetricsQueryHandler) ([]SampleStream, *PrometheusResponse, error) {
	query, err := query.WithQuery(queryExpr.Expr)
	if err != nil {
		return nil, nil, err
	}

	resp, err := handler.Do(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	promRes, ok := resp.(*PrometheusResponse)
	if !ok {
		return nil, nil, errors.Errorf("error invalid response type: %T, expected: %T", resp, &PrometheusResponse{})
	}
	resStreams, err := ResponseToSamples(promRes)
	if err != nil {
		return nil, nil, err
	}

	return resStreams, promRes, nil
}

// handleEmbeddedQueries concurrently executes the provided queries through the downstream handler.
// The returned storage.SeriesSet contains sorted series.
func (q *shardedQuerier) handleEmbeddedQueries(ctx context.Context, queries []astmapper.EmbeddedQuery, hints *storage.SelectHints) storage.SeriesSet {
	streams := make([][]SampleStream, len(queries))

	// Concurrently run each query. It breaks and cancels each worker context on first error.
	err := concurrency.ForEachJob(ctx, len(queries), len(queries), func(ctx context.Context, idx int) error {
		resStreams, promRes, err := q.handleEmbeddedQuery(ctx, queries[idx], q.req, q.handler)
		if err != nil {
			return err
		}

		streams[idx] = resStreams // No mutex is needed since each job writes its own index. This is like writing separate variables.

		q.responseHeaders.mergeHeaders(promRes.Headers)
		q.annotationAccumulator.addInfos(promRes.Infos)
		q.annotationAccumulator.addWarnings(promRes.Warnings)

		return nil
	})

	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	return newSeriesSetFromEmbeddedQueriesResults(streams, hints)
}

// LabelValues implements storage.LabelQuerier.
func (q *shardedQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, errNotImplemented
}

// LabelNames implements storage.LabelQuerier.
func (q *shardedQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, errNotImplemented
}

// Close implements storage.LabelQuerier.
func (q *shardedQuerier) Close() error {
	return nil
}

type responseHeadersTracker struct {
	headersMx sync.Mutex
	headers   map[string][]string
}

func newResponseHeadersTracker() *responseHeadersTracker {
	return &responseHeadersTracker{
		headers: make(map[string][]string),
	}
}

func (t *responseHeadersTracker) mergeHeaders(headers []*PrometheusHeader) {
	t.headersMx.Lock()
	defer t.headersMx.Unlock()

	for _, header := range headers {
		for _, value := range header.Values {
			// Ensure no duplicates.
			if !util.StringsContain(t.headers[header.Name], value) {
				t.headers[header.Name] = append(t.headers[header.Name], value)
			}
		}
	}
}

func (t *responseHeadersTracker) getHeaders() []*PrometheusHeader {
	t.headersMx.Lock()
	defer t.headersMx.Unlock()

	// Convert the response headers into the right data type.
	out := make([]*PrometheusHeader, 0, len(t.headers))
	for name, values := range t.headers {
		out = append(out, &PrometheusHeader{Name: name, Values: values})
	}

	return out
}

// newSeriesSetFromEmbeddedQueriesResults returns an in memory storage.SeriesSet from embedded queries results.
// The passed hints (if any) is used to inject stale markers at the beginning of each gap in the embedded query
// results.
//
// The returned storage.SeriesSet series is sorted.
func newSeriesSetFromEmbeddedQueriesResults(results [][]SampleStream, hints *storage.SelectHints) storage.SeriesSet {
	totalLen := 0
	for _, r := range results {
		totalLen += len(r)
	}

	var (
		set  = make([]storage.Series, 0, totalLen)
		step int64
	)

	// Get the query step from hints (if they've been passed).
	if hints != nil {
		step = hints.Step
	}

	for _, result := range results {
		for _, stream := range result {
			// We add an extra 10 items to account for some stale markers that could be injected.
			// We're trading a lower chance of reallocation in case stale markers are added for a
			// slightly higher memory utilisation.
			samples := make([]model.SamplePair, 0, len(stream.Samples)+10)

			for idx, sample := range stream.Samples {
				// When an embedded query is executed by PromQL engine, any stale marker in the time-series
				// data is used the engine to stop applying the lookback delta but the stale marker is removed
				// from the query results. The result of embedded queries, which we are processing in this function,
				// is then used as input to run an outer query in the PromQL engine. This data will not contain
				// the stale marker (because has been removed when running the embedded query) but we still need
				// the PromQL engine to not apply the lookback delta when there are gaps in the embedded queries
				// results. For this reason, here we do inject a stale marker at the beginning of each gap in the
				// embedded queries results.
				if step > 0 && idx > 0 && sample.TimestampMs > stream.Samples[idx-1].TimestampMs+step {
					samples = append(samples, model.SamplePair{
						Timestamp: model.Time(stream.Samples[idx-1].TimestampMs + step),
						Value:     model.SampleValue(math.Float64frombits(value.StaleNaN)),
					})
				}

				samples = append(samples, model.SamplePair{
					Timestamp: model.Time(sample.TimestampMs),
					Value:     model.SampleValue(sample.Value),
				})
			}

			// In case the embedded query processed series which all ended before the end of the query time range,
			// we don't want the outer query to apply the lookback at the end of the embedded query results. To keep it
			// simple, it's safe always to add an extra stale marker at the end of the query results.
			//
			// This could result in an extra sample (stale marker) after the end of the query time range, but that's
			// not a problem when running the outer query because it will just be discarded.
			if len(samples) > 0 && step > 0 {
				samples = append(samples, model.SamplePair{
					Timestamp: samples[len(samples)-1].Timestamp + model.Time(step),
					Value:     model.SampleValue(math.Float64frombits(value.StaleNaN)),
				})
			}

			// same logic as samples above
			var histograms []mimirpb.Histogram
			if len(stream.Histograms) > 0 {
				// If there are histograms, which is less likely currently,
				// we add an extra 10 items to account for some stale markers that could be injected.
				// We're trading a lower chance of reallocation in case stale markers are added for a
				// slightly higher memory utilisation.
				histograms = make([]mimirpb.Histogram, 0, len(stream.Histograms)+10)
			} else {
				histograms = make([]mimirpb.Histogram, 0)
			}

			for idx, histogram := range stream.Histograms {
				if step > 0 && idx > 0 && histogram.TimestampMs > stream.Histograms[idx-1].TimestampMs+step {
					histograms = append(histograms, mimirpb.Histogram{
						Timestamp: stream.Histograms[idx-1].TimestampMs + step,
						Sum:       math.Float64frombits(value.StaleNaN),
					})
				}

				histograms = append(histograms, mimirpb.FromFloatHistogramToHistogramProto(histogram.TimestampMs, histogram.Histogram.ToPrometheusModel()))
			}

			if len(histograms) > 0 && step > 0 {
				histograms = append(histograms, mimirpb.Histogram{
					Timestamp: histograms[len(histograms)-1].Timestamp + step,
					Sum:       math.Float64frombits(value.StaleNaN),
				})
			}

			set = append(set, series.NewConcreteSeries(mimirpb.FromLabelAdaptersToLabels(stream.Labels), samples, histograms))
		}
	}
	return series.NewConcreteSeriesSetFromUnsortedSeries(set)
}

// ResponseToSamples is needed to map back from api response to the underlying series data
func ResponseToSamples(resp *PrometheusResponse) ([]SampleStream, error) {
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}

	switch resp.Data.ResultType {
	case string(parser.ValueTypeString),
		string(parser.ValueTypeScalar),
		string(parser.ValueTypeVector),
		string(parser.ValueTypeMatrix):
		return resp.Data.Result, nil
	}

	return nil, errors.Errorf(
		"Invalid promql.Value type: [%s]. Only %s, %s, %s and %s supported",
		resp.Data.ResultType,
		parser.ValueTypeString,
		parser.ValueTypeScalar,
		parser.ValueTypeVector,
		parser.ValueTypeMatrix,
	)
}
