// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	reasonNotAPIError       = "not-api-error"
	reasonNotCacheableError = "not-cacheable-api-error"
)

func newErrorCachingMiddleware(cache cache.Cache, limits Limits, shouldCacheReq shouldCacheFn, keyGen CacheKeyGenerator, logger log.Logger, reg prometheus.Registerer) MetricsQueryMiddleware {
	cacheLoadAttempted := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_frontend_query_error_cache_requests_total",
		Help: "Number of requests that check the results cache for an error.",
	})
	cacheLoadHits := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_frontend_query_error_cache_hits_total",
		Help: "Number of hits for the errors in the results cache.",
	})
	cacheStoreAttempted := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_frontend_query_error_cache_store_requests_total",
		Help: "Number of requests that resulted in an error.",
	})
	cacheStoreSkipped := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_frontend_query_error_cache_store_skipped_total",
		Help: "Number of requests that resulted in an error that was not stored in the results cache.",
	}, []string{"reason"})

	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &errorCachingHandler{
			next:                next,
			cache:               cache,
			limits:              limits,
			shouldCacheReq:      shouldCacheReq,
			keyGen:              keyGen,
			logger:              logger,
			cacheLoadAttempted:  cacheLoadAttempted,
			cacheLoadHits:       cacheLoadHits,
			cacheStoreAttempted: cacheStoreAttempted,
			cacheStoreSkipped:   cacheStoreSkipped,
		}
	})
}

type errorCachingHandler struct {
	next           MetricsQueryHandler
	cache          cache.Cache
	limits         Limits
	shouldCacheReq shouldCacheFn
	keyGen         CacheKeyGenerator
	logger         log.Logger

	cacheLoadAttempted  prometheus.Counter
	cacheLoadHits       prometheus.Counter
	cacheStoreAttempted prometheus.Counter
	cacheStoreSkipped   *prometheus.CounterVec
}

func (e *errorCachingHandler) Do(ctx context.Context, request MetricsQueryRequest) (Response, error) {
	spanLog := spanlogger.FromContext(ctx, e.logger)
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return e.next.Do(ctx, request)
	}

	// Check if caching has disabled via an option on the request
	if !e.shouldCacheReq(request) {
		return e.next.Do(ctx, request)
	}

	addWithExemplar(ctx, e.cacheLoadAttempted, 1)
	key := e.keyGen.QueryRequestError(ctx, tenant.JoinTenantIDs(tenantIDs), request)
	hashedKey := cacheHashKey(key)

	if cachedErr := e.loadErrorFromCache(ctx, key, hashedKey, spanLog); cachedErr != nil {
		e.cacheLoadHits.Inc()
		spanLog.DebugLog(
			"msg", "returned cached API error",
			"error_type", cachedErr.Type,
			"key", key,
			"hashed_key", hashedKey,
		)

		return nil, cachedErr
	}

	res, err := e.next.Do(ctx, request)
	if err != nil {
		addWithExemplar(ctx, e.cacheStoreAttempted, 1)

		var apiErr *apierror.APIError
		if !errors.As(err, &apiErr) {
			e.cacheStoreSkipped.WithLabelValues(reasonNotAPIError).Inc()
			return res, err
		}

		if cacheable, reason := e.isCacheable(apiErr); !cacheable {
			e.cacheStoreSkipped.WithLabelValues(reason).Inc()
			spanLog.DebugLog(
				"msg", "error result from request is not cacheable",
				"error_type", apiErr.Type,
				"reason", reason,
			)
			return res, err
		}

		ttl := validation.MinDurationPerTenant(tenantIDs, e.limits.ResultsCacheTTLForErrors)
		e.storeErrorToCache(key, hashedKey, ttl, apiErr, spanLog)

		spanLog.DebugLog(
			"msg", "stored API error to cache",
			"key", key,
			"hashed_key", hashedKey,
			"error_type", apiErr.Type,
			"error_message", apiErr.Message,
		)
	}

	return res, err
}

func (e *errorCachingHandler) loadErrorFromCache(ctx context.Context, key, hashedKey string, spanLog *spanlogger.SpanLogger) *apierror.APIError {
	res := e.cache.GetMulti(ctx, []string{hashedKey})
	cached, ok := res[hashedKey]
	if !ok {
		return nil
	}

	var cachedError CachedError
	if err := proto.Unmarshal(cached, &cachedError); err != nil {
		level.Warn(spanLog).Log("msg", "unable to unmarshall cached error", "err", err)
		return nil
	}

	if cachedError.GetKey() != key {
		spanLog.DebugLog(
			"msg", "cached error key does not match",
			"expected_key", key,
			"actual_key", cachedError.GetKey(),
			"hashed_key", hashedKey,
		)
		return nil
	}

	return apierror.New(apierror.Type(cachedError.ErrorType), cachedError.ErrorMessage)

}

func (e *errorCachingHandler) storeErrorToCache(key, hashedKey string, ttl time.Duration, apiErr *apierror.APIError, spanLog *spanlogger.SpanLogger) {
	bytes, err := proto.Marshal(&CachedError{
		Key:          key,
		ErrorType:    string(apiErr.Type),
		ErrorMessage: apiErr.Message,
	})

	if err != nil {
		level.Warn(spanLog).Log(
			"msg", "unable to marshal cached error",
			"key", key,
			"hashed_key", hashedKey,
			"error_type", apiErr.Type,
			"error_message", apiErr.Message,
			"err", err,
		)
		return
	}

	e.cache.SetAsync(hashedKey, bytes, ttl)
}

func (e *errorCachingHandler) isCacheable(apiErr *apierror.APIError) (bool, string) {
	if apiErr.Type != apierror.TypeBadData && apiErr.Type != apierror.TypeExec && apiErr.Type != apierror.TypeTooLargeEntry {
		return false, reasonNotCacheableError
	}

	return true, ""
}

func addWithExemplar(ctx context.Context, counter prometheus.Counter, val float64) {
	if traceID, traceOK := tracing.ExtractSampledTraceID(ctx); traceOK {
		counter.(prometheus.ExemplarAdder).AddWithExemplar(val, prometheus.Labels{"trace_id": traceID, "traceID": traceID})
	} else {
		// If there is no trace ID, just add to the counter.
		counter.Add(val)
	}
}
