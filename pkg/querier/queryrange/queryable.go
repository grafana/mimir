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
	"github.com/grafana/mimir/pkg/util/concurrency"
)

var (
	errMissingEmbeddedQuery = errors.New("missing embedded query")
	errNoEmbeddedQueries    = errors.New("ShardedQuerier is expecting embedded queries but didn't find any")
	errNotImplemented       = errors.New("not implemented")
)

// ShardedQueryable is an implementor of the Queryable interface.
type ShardedQueryable struct {
	Req     Request
	Handler Handler

	shardedQuerier *ShardedQuerier
}

// Querier implements Queryable
func (q *ShardedQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	q.shardedQuerier = &ShardedQuerier{ctx: ctx, req: q.Req, handler: q.Handler, responseHeaders: map[string][]string{}}
	return q.shardedQuerier, nil
}

func (q *ShardedQueryable) getResponseHeaders() []*PrometheusResponseHeader {
	q.shardedQuerier.responseHeadersMtx.Lock()
	defer q.shardedQuerier.responseHeadersMtx.Unlock()

	// Convert the response headers into the right data type.
	headers := make([]*PrometheusResponseHeader, 0, len(q.shardedQuerier.responseHeaders))
	for name, values := range q.shardedQuerier.responseHeaders {
		headers = append(headers, &PrometheusResponseHeader{Name: name, Values: values})
	}

	return headers
}

// ShardedQuerier is a an implementor of the Querier interface.
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

	streamsMx.Lock()
	defer streamsMx.Unlock()
	return NewSeriesSet(streams)
}

// mergeResponseHeaders merges the input headers with other response headers received
// when running embedded queries.
func (q *ShardedQuerier) mergeResponseHeaders(headers []*PrometheusResponseHeader) {
	q.responseHeadersMtx.Lock()
	defer q.responseHeadersMtx.Unlock()

	for _, header := range headers {
		if _, ok := q.responseHeaders[header.Name]; !ok {
			q.responseHeaders[header.Name] = header.Values
		} else {
			q.responseHeaders[header.Name] = append(q.responseHeaders[header.Name], header.Values...)
		}
	}
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
