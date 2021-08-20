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
	req             Request
	handler         Handler
	responseHeaders *responseHeadersTracker
}

// NewShardedQueryable makes a new ShardedQueryable. We expect a new queryable is created for each
// query, otherwise the response headers tracker doesn't work as expected, because it merges the
// headers for all queries run through the queryable and never reset them.
func NewShardedQueryable(req Request, next Handler) *ShardedQueryable {
	return &ShardedQueryable{
		req:             req,
		handler:         next,
		responseHeaders: newResponseHeadersTracker(),
	}
}

// Querier implements storage.Queryable.
func (q *ShardedQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	querier := &ShardedQuerier{ctx: ctx, req: q.req, handler: q.handler, responseHeaders: q.responseHeaders}
	return querier, nil
}

// getResponseHeaders returns the merged response headers received by the downstream
// when running the embedded queries.
func (q *ShardedQueryable) getResponseHeaders() []*PrometheusResponseHeader {
	return q.responseHeaders.getHeaders()
}

// ShardedQuerier implements the storage.Querier interface with capabilities to parse the embedded queries
// from the astmapper.EmbeddedQueriesMetricName metric label value and concurrently run embedded queries
// through the downstream handler.
type ShardedQuerier struct {
	ctx     context.Context
	req     Request
	handler Handler

	// Keep track of response headers received when running embedded queries.
	responseHeaders *responseHeadersTracker
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

		q.responseHeaders.mergeHeaders(resp.(*PrometheusResponse).Headers)

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

type responseHeadersTracker struct {
	headersMx sync.Mutex
	headers   map[string][]string
}

func newResponseHeadersTracker() *responseHeadersTracker {
	return &responseHeadersTracker{
		headers: make(map[string][]string),
	}
}

func (t *responseHeadersTracker) mergeHeaders(headers []*PrometheusResponseHeader) {
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

func (t *responseHeadersTracker) getHeaders() []*PrometheusResponseHeader {
	t.headersMx.Lock()
	defer t.headersMx.Unlock()

	// Convert the response headers into the right data type.
	out := make([]*PrometheusResponseHeader, 0, len(t.headers))
	for name, values := range t.headers {
		out = append(out, &PrometheusResponseHeader{Name: name, Values: values})
	}

	return out
}
