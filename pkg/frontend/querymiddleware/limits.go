// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/limits.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/propagation"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

var errExecutingParallelQueriesFinished = cancellation.NewErrorf("executing parallel queries finished")

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

	// CompactorSplitAndMergeShards returns the number of shards to use when splitting blocks
	// This method is copied from compactor.ConfigProvider.
	CompactorSplitAndMergeShards(userID string) int

	// CompactorBlocksRetentionPeriod returns the retention period for a given user.
	CompactorBlocksRetentionPeriod(userID string) time.Duration

	// OutOfOrderTimeWindow returns the out-of-order time window for the user.
	OutOfOrderTimeWindow(userID string) time.Duration

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

	// ResultsCacheTTLForErrors returns TTL for cached non-transient errors.
	ResultsCacheTTLForErrors(userID string) time.Duration

	// ResultsCacheForUnalignedQueryEnabled returns whether to cache results for queries that are not step-aligned
	ResultsCacheForUnalignedQueryEnabled(userID string) bool

	// EnabledPromQLExperimentalFunctions returns the names of PromQL experimental functions allowed for the tenant.
	EnabledPromQLExperimentalFunctions(userID string) []string

	// Prom2RangeCompat returns if Prometheus 2/3 range compatibility fixes are enabled for the tenant.
	Prom2RangeCompat(userID string) bool

	// BlockedQueries returns the blocked queries.
	BlockedQueries(userID string) []validation.BlockedQuery

	// BlockedRequests returns the blocked http requests.
	BlockedRequests(userID string) []validation.BlockedRequest

	// LimitedQueries returns the limited queries.
	LimitedQueries(userID string) []validation.LimitedQuery

	// AlignQueriesWithStep returns if queries should be adjusted to be step-aligned
	AlignQueriesWithStep(userID string) bool

	// QueryIngestersWithin returns the maximum lookback beyond which queries are not sent to ingester.
	QueryIngestersWithin(userID string) time.Duration

	// IngestStorageReadConsistency returns the default read consistency for the tenant.
	IngestStorageReadConsistency(userID string) string

	// SubquerySpinOffEnabled returns if the feature of spinning off subqueries from instant queries as range queries is enabled.
	SubquerySpinOffEnabled(userID string) bool

	// LabelsQueryOptimizerEnabled returns whether labels query optimizations are enabled.
	LabelsQueryOptimizerEnabled(userID string) bool
}

type limitsMiddleware struct {
	Limits
	next   MetricsQueryHandler
	logger log.Logger
}

// newLimitsMiddleware creates a new MetricsQueryMiddleware that enforces query limits.
func newLimitsMiddleware(l Limits, logger log.Logger) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return limitsMiddleware{
			next:   next,
			Limits: l,
			logger: logger,
		}
	})
}

func (l limitsMiddleware) Do(ctx context.Context, r MetricsQueryRequest) (Response, error) {
	log, ctx := spanlogger.New(ctx, l.logger, tracer, "limits")
	defer log.Finish()

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Clamp the time range based on the max query lookback and block retention period.
	blocksRetentionPeriod := validation.LargestPositiveNonZeroDurationPerTenant(tenantIDs, l.CompactorBlocksRetentionPeriod)
	maxQueryLookback := validation.SmallestPositiveNonZeroDurationPerTenant(tenantIDs, l.MaxQueryLookback)
	maxLookback := smallestPositiveNonZeroDuration(blocksRetentionPeriod, maxQueryLookback)
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

			return NewEmptyPrometheusResponse(), nil
		}

		if r.GetStart() < minStartTime {
			// Replace the start time in the request.
			level.Debug(log).Log(
				"msg", "the start time of the query has been manipulated because of the 'max query lookback' or 'blocks retention period' setting",
				"original", util.FormatTimeMillis(r.GetStart()),
				"updated", util.FormatTimeMillis(minStartTime),
				"maxQueryLookback", maxQueryLookback,
				"blocksRetentionPeriod", blocksRetentionPeriod)

			r, err = r.WithStartEnd(minStartTime, r.GetEnd())
			if err != nil {
				return nil, apierror.New(apierror.TypeInternal, err.Error())
			}
		}
	}

	// Enforce max query size, in bytes.
	if maxQuerySize := validation.SmallestPositiveNonZeroIntPerTenant(tenantIDs, l.MaxQueryExpressionSizeBytes); maxQuerySize > 0 {
		querySize := len(r.GetQuery())
		if querySize > maxQuerySize {
			return nil, newMaxQueryExpressionSizeBytesError(querySize, maxQuerySize)
		}
	}

	// Enforce the max query length.
	if maxQueryLength := validation.SmallestPositiveNonZeroDurationPerTenant(tenantIDs, l.MaxTotalQueryLength); maxQueryLength > 0 {
		queryLen := timestamp.Time(r.GetEnd()).Sub(timestamp.Time(r.GetStart()))
		if queryLen > maxQueryLength {
			return nil, newMaxTotalQueryLengthError(queryLen, maxQueryLength)
		}
	}
	return l.next.Do(ctx, r)
}

type limitedParallelismRoundTripper struct {
	downstream             MetricsQueryHandler
	limits                 LimitedParallelismLimits
	remoteExecutionEnabled bool

	codec      Codec
	middleware MetricsQueryMiddleware
}

type LimitedParallelismLimits interface {
	MaxQueryParallelism(userID string) int
}

// NewLimitedParallelismRoundTripper creates a new roundtripper that enforces MaxQueryParallelism to the `next` roundtripper across `middlewares`.
func NewLimitedParallelismRoundTripper(next MetricsQueryHandler, codec Codec, limits LimitedParallelismLimits, remoteExecutionEnabled bool, middlewares ...MetricsQueryMiddleware) http.RoundTripper {
	return limitedParallelismRoundTripper{
		downstream:             next,
		codec:                  codec,
		limits:                 limits,
		remoteExecutionEnabled: remoteExecutionEnabled,
		middleware:             MergeMetricsQueryMiddlewares(middlewares...),
	}
}

func (rt limitedParallelismRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancelCause(r.Context())
	defer cancel(errExecutingParallelQueriesFinished)

	request, err := rt.codec.DecodeMetricsQueryRequest(ctx, r)
	if err != nil {
		return nil, err
	}

	request.AddSpanTags(trace.SpanFromContext(ctx))
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Limit the number of parallel sub-requests according to the MaxQueryParallelism tenant setting.
	maxParallelism := validation.SmallestPositiveIntPerTenant(tenantIDs, rt.limits.MaxQueryParallelism)
	limiter := NewParallelismLimiter(maxParallelism)
	ctx = ContextWithParallelismLimiter(ctx, limiter)

	// Wraps middlewares with a final handler, which will receive sub-requests in
	// parallel from upstream handlers and ensure that no more than MaxQueryParallelism
	// sub-requests run in parallel.
	response, err := rt.middleware.Wrap(
		HandlerFunc(func(ctx context.Context, r MetricsQueryRequest) (Response, error) {
			// If remote execution is disabled, apply the parallelism limit here.
			// If remote execution is enabled, we'll enforce the limit in Frontend.DoProtobufRequest instead.
			// We need to enforce the limit in DoProtobufRequest because the requests we see here are before sharding is applied if sharding inside MQE is enabled.
			// If we were to enforce the limit here with remote execution enabled, then we'll count requests twice: once here, and again in Frontend.DoProtobufRequest.
			if !rt.remoteExecutionEnabled {
				if err := limiter.BeginRequest(ctx); err != nil {
					return nil, err
				}
				defer limiter.RequestFinished()
			}

			return rt.downstream.Do(ctx, r)
		})).Do(ctx, request)
	if err != nil {
		return nil, err
	}

	// EncodeMetricsQueryResponse handles closing the response
	return rt.codec.EncodeMetricsQueryResponse(ctx, r, response)
}

type ParallelismLimiter struct {
	sem *semaphore.Weighted
}

func NewParallelismLimiter(maxParallelism int) *ParallelismLimiter {
	return &ParallelismLimiter{
		sem: semaphore.NewWeighted(int64(maxParallelism)),
	}
}

func (l *ParallelismLimiter) BeginRequest(ctx context.Context) error {
	if err := l.sem.Acquire(ctx, 1); err != nil {
		// Acquire returns ctx.Err(), rather than context.Cause(ctx).
		// We want to return the cause if there is one.
		if errors.Is(err, ctx.Err()) {
			err = context.Cause(ctx)
		}
		return fmt.Errorf("could not acquire work: %w", err)
	}

	return nil
}

func (l *ParallelismLimiter) RequestFinished() {
	l.sem.Release(1)
}

func NewHTTPQueryRequestRoundTripperHandler(next http.RoundTripper, codec Codec, logger log.Logger) MetricsQueryHandler {
	return httpQueryRequestRoundTripperHandler{
		next:   next,
		codec:  codec,
		logger: logger,
	}
}

// httpQueryRequestRoundTripperHandler is an adapter that implements the MetricsQueryHandler interface using a http.RoundTripper to perform
// the requests and a Codec to translate between http Request/Response model and this package's Request/Response model.
// It basically encodes a MetricsQueryRequest from MetricsQueryHandler.Do and decodes response from next roundtripper.
type httpQueryRequestRoundTripperHandler struct {
	logger log.Logger
	next   http.RoundTripper
	codec  Codec
}

func (rth httpQueryRequestRoundTripperHandler) Do(ctx context.Context, r MetricsQueryRequest) (resp Response, err error) {
	spanLogger, ctx := spanlogger.New(ctx, rth.logger, tracer, "httpQueryRequestRoundTripperHandler.Do")
	defer func() {
		if err != nil {
			spanLogger.Error(err)
		}
		spanLogger.Finish()
	}()

	request, err := rth.codec.EncodeMetricsQueryRequest(ctx, r)
	if err != nil {
		return nil, err
	}

	response, err := rth.next.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()

	return rth.codec.DecodeMetricsQueryResponse(ctx, response, r, rth.logger)
}

type engineQueryRequestRoundTripperHandler struct {
	engine  *streamingpromql.Engine
	storage storage.Queryable
	codec   Codec
	logger  log.Logger
}

func NewEngineQueryRequestRoundTripperHandler(engine *streamingpromql.Engine, codec Codec, logger log.Logger) MetricsQueryHandler {
	return &engineQueryRequestRoundTripperHandler{
		engine:  engine,
		storage: unqueryableQueryable{},
		codec:   codec,
		logger:  logger,
	}
}

func (rth *engineQueryRequestRoundTripperHandler) Do(ctx context.Context, r MetricsQueryRequest) (resp Response, err error) {
	spanLogger, ctx := spanlogger.New(ctx, rth.logger, tracer, "engineQueryRequestRoundTripperHandler.Do")
	defer func() {
		if err != nil {
			// TypeForError handles both apierror.APIError instances as well as context.Canceled instances, so we don't need to check for both below.
			if apierror.TypeForError(err, apierror.TypeNone) == apierror.TypeCanceled {
				spanLogger.DebugLog("msg", "request returned cancellation error", "err", err)
			} else {
				spanLogger.Error(err)
			}
		}
		spanLogger.Finish()
	}()

	headers := map[string][]string{}
	if err := rth.codec.AddHeadersForMetricQueryRequest(ctx, r, propagation.MapCarrier(headers)); err != nil {
		return nil, err
	}

	ctx = ContextWithHeadersToPropagate(ctx, headers)
	ctx = ContextWithRequestHintsAndOptions(ctx, r.GetHints(), r.GetOptions())
	opts := promql.NewPrometheusQueryOpts(r.GetStats() == "all", 0)

	var q promql.Query

	switch r := r.(type) {
	case *PrometheusRangeQueryRequest:
		q, err = rth.engine.NewRangeQuery(ctx, rth.storage, opts, r.GetQuery(), timestamp.Time(r.GetStart()), timestamp.Time(r.GetEnd()), time.Duration(r.GetStep())*time.Millisecond)
	case *PrometheusInstantQueryRequest:
		q, err = rth.engine.NewInstantQuery(ctx, rth.storage, opts, r.GetQuery(), timestamp.Time(r.GetTime()))
	default:
		return nil, fmt.Errorf("unknown metrics query request type: %T", r)
	}

	if err != nil {
		err = convertToAPIError(err, apierror.TypeInternal)
		return nil, err
	}

	res := q.Exec(ctx)
	if res.Err != nil {
		err := convertToAPIError(res.Err, apierror.TypeExec)
		return nil, err
	}

	data, err := promqlResultToSamples(res)
	if err != nil {
		err = convertToAPIError(err, apierror.TypeInternal)
		return nil, err
	}

	warnings, infos := res.Warnings.AsStrings(r.GetQuery(), 0, 0)

	if localStats := stats.FromContext(ctx); localStats != nil {
		engineStats := q.Stats()
		localStats.AddSamplesProcessed(uint64(engineStats.Samples.TotalSamples))

		stepStats := make([]stats.StepStat, 0, len(engineStats.Samples.TotalSamplesPerStep))
		for i, count := range engineStats.Samples.TotalSamplesPerStep {
			stepStats = append(stepStats, stats.StepStat{
				Timestamp: r.GetStart() + int64(i)*r.GetStep(),
				Value:     count,
			})
		}

		localStats.AddSamplesProcessedPerStep(stepStats)
	}

	resp = &PrometheusResponseWithFinalizer{
		PrometheusResponse: &PrometheusResponse{
			Status: statusSuccess,
			Data: &PrometheusData{
				ResultType: string(res.Value.Type()),
				Result:     data,
			},
			Warnings: warnings,
			Infos:    infos,
		},
		finalizer: q.Close,
	}

	return resp, nil
}

func convertToAPIError(err error, fallbackErrorType apierror.Type) error {
	var apiError *apierror.APIError
	if errors.As(err, &apiError) {
		return apiError
	}

	t := apierror.TypeForError(err, fallbackErrorType)
	return apierror.New(t, err.Error())
}

type unqueryableQueryable struct{}

var errShouldNeverBeQueried = errors.New("this Queryable should never be queried as all selectors should be evaluated remotely: if you are seeing this, this is a bug")

func (u unqueryableQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return nil, errShouldNeverBeQueried
}

// smallestPositiveNonZeroDuration returns the smallest positive and non-zero value
// in the input values. If the input values slice is empty or they only contain
// non-positive numbers, then this function will return 0.
func smallestPositiveNonZeroDuration(values ...time.Duration) time.Duration {
	smallest := time.Duration(0)

	for _, value := range values {
		if value > 0 && (smallest == 0 || smallest > value) {
			smallest = value
		}
	}

	return smallest

}

type requestContextKeyType int

const (
	requestHintsKey requestContextKeyType = iota
	requestOptionsKey
	parallelismLimiterKey
)

func ContextWithRequestHintsAndOptions(ctx context.Context, hints *Hints, options Options) context.Context {
	ctx = context.WithValue(ctx, requestHintsKey, hints)
	ctx = context.WithValue(ctx, requestOptionsKey, options)
	return ctx
}

func RequestHintsFromContext(ctx context.Context) *Hints {
	if v := ctx.Value(requestHintsKey); v != nil {
		return v.(*Hints)
	}

	return nil
}

func RequestOptionsFromContext(ctx context.Context) Options {
	if v := ctx.Value(requestOptionsKey); v != nil {
		return v.(Options)
	}

	return Options{}
}

func ContextWithParallelismLimiter(ctx context.Context, limiter *ParallelismLimiter) context.Context {
	return context.WithValue(ctx, parallelismLimiterKey, limiter)
}

func ParallelismLimiterFromContext(ctx context.Context) *ParallelismLimiter {
	if v := ctx.Value(parallelismLimiterKey); v != nil {
		return v.(*ParallelismLimiter)
	}

	return nil
}
