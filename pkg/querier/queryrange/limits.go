// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/limits.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// Limits allows us to specify per-tenant runtime limits on the behavior of
// the query handling code.
type Limits interface {
	// MaxQueryLookback returns the max lookback period of queries.
	MaxQueryLookback(userID string) time.Duration

	// MaxQueryLength returns the limit of the length (in time) of a query.
	MaxQueryLength(string) time.Duration

	// MaxQueryParallelism returns the limit to the number of split queries the
	// frontend will process in parallel.
	MaxQueryParallelism(string) int

	// MaxCacheFreshness returns the period after which results are cacheable,
	// to prevent caching of very recent results.
	MaxCacheFreshness(string) time.Duration

	// QueryShardingTotalShards returns the number of shards to use for a given tenant.
	QueryShardingTotalShards(string) int
}

type limitsMiddleware struct {
	Limits
	next Handler
}

// NewLimitsMiddleware creates a new Middleware that enforces query limits.
func NewLimitsMiddleware(l Limits) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return limitsMiddleware{
			next:   next,
			Limits: l,
		}
	})
}

func (l limitsMiddleware) Do(ctx context.Context, r Request) (Response, error) {
	log, ctx := spanlogger.New(ctx, "limits")
	defer log.Finish()

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}

	// Clamp the time range based on the max query lookback.

	if maxQueryLookback := validation.SmallestPositiveNonZeroDurationPerTenant(tenantIDs, l.MaxQueryLookback); maxQueryLookback > 0 {
		minStartTime := util.TimeToMillis(time.Now().Add(-maxQueryLookback))

		if r.GetEnd() < minStartTime {
			// The request is fully outside the allowed range, so we can return an
			// empty response.
			level.Debug(log).Log(
				"msg", "skipping the execution of the query because its time range is before the 'max query lookback' setting",
				"reqStart", util.FormatTimeMillis(r.GetStart()),
				"redEnd", util.FormatTimeMillis(r.GetEnd()),
				"maxQueryLookback", maxQueryLookback)

			return NewEmptyPrometheusResponse(), nil
		}

		if r.GetStart() < minStartTime {
			// Replace the start time in the request.
			level.Debug(log).Log(
				"msg", "the start time of the query has been manipulated because of the 'max query lookback' setting",
				"original", util.FormatTimeMillis(r.GetStart()),
				"updated", util.FormatTimeMillis(minStartTime))

			r = r.WithStartEnd(minStartTime, r.GetEnd())
		}
	}

	// Enforce the max query length.
	if maxQueryLength := validation.SmallestPositiveNonZeroDurationPerTenant(tenantIDs, l.MaxQueryLength); maxQueryLength > 0 {
		queryLen := timestamp.Time(r.GetEnd()).Sub(timestamp.Time(r.GetStart()))
		if queryLen > maxQueryLength {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, validation.ErrQueryTooLong, queryLen, maxQueryLength)
		}
	}

	return l.next.Do(ctx, r)
}

type limitedRoundTripper struct {
	downstream Handler
	limits     Limits

	codec      Codec
	middleware Middleware
}

// NewLimitedRoundTripper creates a new roundtripper that enforces MaxQueryParallelism to the `next` roundtripper across `middlewares`.
func NewLimitedRoundTripper(next http.RoundTripper, codec Codec, limits Limits, middlewares ...Middleware) http.RoundTripper {
	transport := limitedRoundTripper{
		downstream: roundTripperHandler{
			next:  next,
			codec: codec,
		},
		codec:      codec,
		limits:     limits,
		middleware: MergeMiddlewares(middlewares...),
	}
	return transport
}

type subRequest struct {
	req    Request
	ctx    context.Context
	result chan result
}

type result struct {
	response Response
	err      error
}

func newSubRequest(ctx context.Context, req Request) subRequest {
	return subRequest{
		req:    req,
		ctx:    ctx,
		result: make(chan result, 1),
	}
}

func (rt limitedRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	var (
		wg           sync.WaitGroup
		intermediate = make(chan subRequest)
		ctx, cancel  = context.WithCancel(r.Context())
	)
	defer func() {
		cancel()
		wg.Wait()
	}()

	request, err := rt.codec.DecodeRequest(ctx, r)
	if err != nil {
		return nil, err
	}

	if span := opentracing.SpanFromContext(ctx); span != nil {
		request.LogToSpan(span)
	}

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}

	// Creates workers that will process the sub-requests in parallel for this query.
	// The amount of workers is limited by the MaxQueryParallelism tenant setting.
	parallelism := validation.SmallestPositiveIntPerTenant(tenantIDs, rt.limits.MaxQueryParallelism)
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case w := <-intermediate:
					resp, err := rt.downstream.Do(w.ctx, w.req)
					w.result <- result{response: resp, err: err}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Wraps middlewares with a final handler, which will receive requests in
	// parallel from upstream handlers. Then each requests gets scheduled to a
	// different worker via the `intermediate` channel, so the maximum
	// parallelism is limited. This worker will then call `Do` on the resulting
	// handler.
	response, err := rt.middleware.Wrap(
		HandlerFunc(func(ctx context.Context, r Request) (Response, error) {
			s := newSubRequest(ctx, r)
			select {
			case intermediate <- s:
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			select {
			case response := <-s.result:
				return response.response, response.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})).Do(ctx, request)
	if err != nil {
		return nil, err
	}

	return rt.codec.EncodeResponse(ctx, response)
}
