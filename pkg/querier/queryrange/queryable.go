// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/queryable.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/querier/astmapper"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/concurrency"
)

var (
	errMissingEmbeddedQuery = errors.New("missing embedded query")
	errNoEmbeddedQueries    = errors.New("ShardedQuerier is expecting embedded queries but didn't find any")
	errNotImplemented       = errors.New("not implemented")
)

// ShardedQueryable is an implementor of the Queryable interface.
type ShardedQueryable struct {
	req     Request
	handler Handler

	// Keep track of all sharded queriers created by this queryable.
	shardedQueriersMx sync.Mutex
	shardedQueriers   []*ShardedQuerier
}

func NewShardedQueryable(req Request, next Handler) *ShardedQueryable {
	return &ShardedQueryable{
		req:     req,
		handler: next,
	}
}

// Querier implements storage.Queryable.
func (q *ShardedQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	querier := &ShardedQuerier{ctx: ctx, req: q.req, handler: q.handler, responseHeaders: map[string][]string{}}

	// Keep track of the querier.
	q.shardedQueriersMx.Lock()
	q.shardedQueriers = append(q.shardedQueriers, querier)
	q.shardedQueriersMx.Unlock()

	return querier, nil
}

// getResponseHeaders returns the merged response headers received by the downstream
// when running the embedded queries.
func (q *ShardedQueryable) getResponseHeaders() []*PrometheusResponseHeader {
	q.shardedQueriersMx.Lock()
	defer q.shardedQueriersMx.Unlock()

	// Merge response headers from all queriers.
	headers := make(map[string][]string)
	for _, querier := range q.shardedQueriers {
		headers = mergeResponseHeaders(headers, querier.getResponseHeaders())
	}

	// Convert the response headers into the right data type.
	out := make([]*PrometheusResponseHeader, 0, len(headers))
	for name, values := range headers {
		out = append(out, &PrometheusResponseHeader{Name: name, Values: values})
	}

	return out
}

// ShardedQuerier implements the storage.Querier interface with capabilities to parse the embedded queries
// from the astmapper.EmbeddedQueriesMetricName metric label value and concurrently run embedded queries
// through the downstream handler.
type ShardedQuerier struct {
	ctx     context.Context
	req     Request
	handler Handler

	// Keep track of response headers received when running embedded queries.
	responseHeadersMtx sync.Mutex
	responseHeaders    map[string][]string
}

// Select implements storage.Querier.
// The sorted bool is ignored because the series is always sorted.
func (q *ShardedQuerier) Select(_ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	var embeddedQuery string
	var isEmbedded bool
	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName && matcher.Value == astmapper.EmbeddedQueriesMetricName {
			isEmbedded = true
		}

		if matcher.Name == astmapper.QueryLabel {
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

	return q.handleEmbeddedQueries(queries)
}

// handleEmbeddedQueries concurrently executes the provided queries through the downstream handler.
// The returned storage.SeriesSet contains sorted series.
func (q *ShardedQuerier) handleEmbeddedQueries(queries []string) storage.SeriesSet {
	var (
		jobs      = concurrency.CreateJobsFromStrings(queries)
		streamsMx sync.Mutex
		streams   []SampleStream
	)

	// Concurrently run each query. It breaks and cancels each worker context on first error.
	err := concurrency.ForEach(q.ctx, jobs, len(jobs), func(ctx context.Context, job interface{}) error {
		resp, err := q.handler.Do(ctx, q.req.WithQuery(job.(string)))
		if err != nil {
			return err
		}

		resStreams, err := ResponseToSamples(resp)
		if err != nil {
			return err
		}

		q.mergeResponseHeaders(resp.(*PrometheusResponse).Headers)

		streamsMx.Lock()
		streams = append(streams, resStreams...)
		streamsMx.Unlock()

		return nil
	})

	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	return NewSeriesSet(streams)
}

// mergeResponseHeaders merges the input headers with other response headers received
// when running embedded queries.
func (q *ShardedQuerier) mergeResponseHeaders(headers []*PrometheusResponseHeader) {
	q.responseHeadersMtx.Lock()
	defer q.responseHeadersMtx.Unlock()

	for _, header := range headers {
		q.responseHeaders[header.Name] = append(q.responseHeaders[header.Name], header.Values...)
	}
}

// getResponseHeaders returns a copy of the merged response headers received by the downstream
// when running the embedded queries. The returned map can contain duplicated values for a given header.
func (q *ShardedQuerier) getResponseHeaders() map[string][]string {
	q.responseHeadersMtx.Lock()
	defer q.responseHeadersMtx.Unlock()

	out := make(map[string][]string, len(q.responseHeaders))
	for k, v := range q.responseHeaders {
		out[k] = append([]string{}, v...)
	}

	return out
}

// LabelValues implements storage.LabelQuerier.
func (q *ShardedQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, errNotImplemented
}

// LabelNames implements storage.LabelQuerier.
func (q *ShardedQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, errNotImplemented
}

// Close implements storage.LabelQuerier.
func (q *ShardedQuerier) Close() error {
	return nil
}

// mergeResponseHeaders merges "from" headers into "to", removing duplicates. Returns "to" headers.
func mergeResponseHeaders(to, from map[string][]string) map[string][]string {
	for name, values := range from {
		for _, value := range values {
			// Ensure no duplicates.
			if !util.StringsContain(to[name], value) {
				to[name] = append(to[name], value)
			}
		}
	}

	return to
}
