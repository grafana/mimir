// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/limits.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/timestamp"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// Limits allows us to specify per-tenant runtime limits on the behavior of
// the query handling code.
type Limits interface {
	// MaxQueryLookback returns the max lookback period of queries.
	MaxQueryLookback(userID string) time.Duration

	// MaxTotalQueryLength returns the limit of the length (in time) of a query.
	MaxTotalQueryLength(userID string) time.Duration

	// MaxQueryParallelism returns the limit to the number of split queries the
	// frontend will process in parallel.
	MaxQueryParallelism(userID string) int

	// MaxQueryExpressionSizeBytes returns the limit of the max number of bytes long a raw
	// query may be. 0 means "unlimited".
	MaxQueryExpressionSizeBytes(userID string) int

	// MaxCacheFreshness returns the period after which results are cacheable,
	// to prevent caching of very recent results.
	MaxCacheFreshness(userID string) time.Duration

	// QueryShardingTotalShards returns the number of shards to use for a given tenant.
	QueryShardingTotalShards(userID string) int

	// QueryShardingMaxShardedQueries returns the max number of sharded queries that can
	// be run for a given received query. 0 to disable limit.
	QueryShardingMaxShardedQueries(userID string) int

	// QueryShardingMaxRegexpSizeBytes returns the limit to the max number of bytes allowed
	// for a regexp matcher in a shardable query. If a query contains a regexp matcher longer
	// than this limit, the query will not be sharded. 0 to disable limit.
	QueryShardingMaxRegexpSizeBytes(userID string) int

	// SplitInstantQueriesByInterval returns the time interval to split instant queries for a given tenant.
	SplitInstantQueriesByInterval(userID string) time.Duration

	// CompactorSplitAndMergeShards returns the number of shards to use when splitting blocks
	// This method is copied from compactor.ConfigProvider.
	CompactorSplitAndMergeShards(userID string) int

	// CompactorBlocksRetentionPeriod returns the retention period for a given user.
	CompactorBlocksRetentionPeriod(userID string) time.Duration

	// OutOfOrderTimeWindow returns the out-of-order time window for the user.
	OutOfOrderTimeWindow(userID string) time.Duration

	// CreationGracePeriod returns the time interval to control how far into the future
	// incoming samples are accepted compared to the wall clock.
	CreationGracePeriod(userID string) time.Duration

	// NativeHistogramsIngestionEnabled returns whether to ingest native histograms in the ingester
	NativeHistogramsIngestionEnabled(userID string) bool

	// ResultsCacheTTL returns TTL for cached results for query that doesn't fall into out of order window, or
	// if out of order ingestion is disabled.
	ResultsCacheTTL(userID string) time.Duration

	// ResultsCacheTTLForOutOfOrderTimeWindow returns TTL for cached results for query that falls into out-of-order ingestion window.
	ResultsCacheTTLForOutOfOrderTimeWindow(userID string) time.Duration

	// ResultsCacheTTLForCardinalityQuery returns TTL for cached results for cardinality queries.
	ResultsCacheTTLForCardinalityQuery(userID string) time.Duration

	// ResultsCacheTTLForLabelsQuery returns TTL for cached results for label names and values queries.
	ResultsCacheTTLForLabelsQuery(userID string) time.Duration

	// ResultsCacheForUnalignedQueryEnabled returns whether to cache results for queries that are not step-aligned
	ResultsCacheForUnalignedQueryEnabled(userID string) bool

	// BlockedQueries returns the blocked queries.
	BlockedQueries(userID string) []*validation.BlockedQuery
}

type limitsMiddleware struct {
	Limits
	next   Handler
	logger log.Logger
}

// newLimitsMiddleware creates a new Middleware that enforces query limits.
func newLimitsMiddleware(l Limits, logger log.Logger) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return limitsMiddleware{
			next:   next,
			Limits: l,
			logger: logger,
		}
	})
}

func (l limitsMiddleware) Do(ctx context.Context, r Request) (Response, error) {
	log, ctx := spanlogger.NewWithLogger(ctx, l.logger, "limits")
	defer log.End()

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Clamp the time range based on the max query lookback and block retention period.
	blocksRetentionPeriod := validation.SmallestPositiveNonZeroDurationPerTenant(tenantIDs, l.CompactorBlocksRetentionPeriod)
	maxQueryLookback := validation.SmallestPositiveNonZeroDurationPerTenant(tenantIDs, l.MaxQueryLookback)
	maxLookback := util_math.Min(blocksRetentionPeriod, maxQueryLookback)
	if maxLookback > 0 {
		minStartTime := util.TimeToMillis(time.Now().Add(-maxLookback))

		if r.GetEnd() < minStartTime {
			// The request is fully outside the allowed range, so we can return an
			// empty response.
			level.Debug(log).Log(
				"msg", "skipping the execution of the query because its time range is before the 'max query lookback' or 'blocks retention period' setting",
				"reqStart", util.FormatTimeMillis(r.GetStart()),
				"redEnd", util.FormatTimeMillis(r.GetEnd()),
				"maxQueryLookback", maxQueryLookback,
				"blocksRetentionPeriod", blocksRetentionPeriod)

			return newEmptyPrometheusResponse(), nil
		}

		if r.GetStart() < minStartTime {
			// Replace the start time in the request.
			level.Debug(log).Log(
				"msg", "the start time of the query has been manipulated because of the 'max query lookback' or 'blocks retention period' setting",
				"original", util.FormatTimeMillis(r.GetStart()),
				"updated", util.FormatTimeMillis(minStartTime),
				"maxQueryLookback", maxQueryLookback,
				"blocksRetentionPeriod", blocksRetentionPeriod)

			r = r.WithStartEnd(minStartTime, r.GetEnd())
		}
	}

	// Enforce the max end time.
	creationGracePeriod := validation.LargestPositiveNonZeroDurationPerTenant(tenantIDs, l.CreationGracePeriod)
	maxEndTime := util.TimeToMillis(time.Now().Add(creationGracePeriod))
	if r.GetEnd() > maxEndTime {
		// Replace the end time in the request.
		level.Debug(log).Log(
			"msg", "the end time of the query has been manipulated because of the 'creation grace period' setting",
			"original", util.FormatTimeMillis(r.GetEnd()),
			"updated", util.FormatTimeMillis(maxEndTime),
			"creationGracePeriod", creationGracePeriod)

		r = r.WithStartEnd(r.GetStart(), maxEndTime)
	}

	// Enforce max query size, in bytes.
	if maxQuerySize := validation.SmallestPositiveNonZeroIntPerTenant(tenantIDs, l.MaxQueryExpressionSizeBytes); maxQuerySize > 0 {
		querySize := len(r.GetQuery())
		if querySize > maxQuerySize {
			return nil, apierror.New(apierror.TypeBadData, validation.NewMaxQueryExpressionSizeBytesError(querySize, maxQuerySize).Error())
		}
	}

	// Enforce the max query length.
	if maxQueryLength := validation.SmallestPositiveNonZeroDurationPerTenant(tenantIDs, l.MaxTotalQueryLength); maxQueryLength > 0 {
		queryLen := timestamp.Time(r.GetEnd()).Sub(timestamp.Time(r.GetStart()))
		if queryLen > maxQueryLength {
			return nil, apierror.New(apierror.TypeBadData, validation.NewMaxTotalQueryLengthError(queryLen, maxQueryLength).Error())
		}
	}

	return l.next.Do(ctx, r)
}

type limitedParallelismRoundTripper struct {
	downstream Handler
	limits     Limits

	codec      Codec
	middleware Middleware
}

// newLimitedParallelismRoundTripper creates a new roundtripper that enforces MaxQueryParallelism to the `next` roundtripper across `middlewares`.
func newLimitedParallelismRoundTripper(next http.RoundTripper, codec Codec, limits Limits, middlewares ...Middleware) http.RoundTripper {
	return limitedParallelismRoundTripper{
		downstream: roundTripperHandler{
			next:  next,
			codec: codec,
		},
		codec:      codec,
		limits:     limits,
		middleware: MergeMiddlewares(middlewares...),
	}
}

func (rt limitedParallelismRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	request, err := rt.codec.DecodeRequest(ctx, r)
	if err != nil {
		return nil, err
	}

	span := trace.SpanFromContext(ctx)
	request.LogToSpan(span)

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Limit the amount of parallel sub-requests according to the MaxQueryParallelism tenant setting.
	parallelism := validation.SmallestPositiveIntPerTenant(tenantIDs, rt.limits.MaxQueryParallelism)
	sem := semaphore.NewWeighted(int64(parallelism))

	// Wraps middlewares with a final handler, which will receive sub-requests in
	// parallel from upstream handlers and ensure that no more than MaxQueryParallelism
	// sub-requests run in parallel.
	response, err := rt.middleware.Wrap(
		HandlerFunc(func(ctx context.Context, r Request) (Response, error) {
			if err := sem.Acquire(ctx, 1); err != nil {
				return nil, fmt.Errorf("could not acquire work: %w", err)
			}
			defer sem.Release(1)

			return rt.downstream.Do(ctx, r)
		})).Do(ctx, request)
	if err != nil {
		return nil, err
	}

	return rt.codec.EncodeResponse(ctx, r, response)
}

// roundTripperHandler is an adapter that implements the Handler interface using a http.RoundTripper to perform
// the requests and a Codec to translate between http Request/Response model and this package's Request/Response model.
// It basically encodes a Request from Handler.Do and decodes response from next roundtripper.
type roundTripperHandler struct {
	logger log.Logger
	next   http.RoundTripper
	codec  Codec
}

func (rth roundTripperHandler) Do(ctx context.Context, r Request) (Response, error) {
	request, err := rth.codec.EncodeRequest(ctx, r)
	if err != nil {
		return nil, err
	}

	if err := user.InjectOrgIDIntoHTTPRequest(ctx, request); err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	response, err := rth.next.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()

	return rth.codec.DecodeResponse(ctx, response, r, rth.logger)
}
