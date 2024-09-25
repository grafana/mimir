// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/split_by_interval.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/sync/errgroup"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	notCachableReasonUnalignedTimeRange   = "unaligned-time-range"
	notCachableReasonTooNew               = "too-new"
	notCachableReasonModifiersNotCachable = "has-modifiers"
)

var (
	// defaultMinCacheExtent is the minimum time range of a query response to
	// be eligible for caching.
	defaultMinCacheExtent = (5 * time.Minute).Milliseconds()
)

type splitAndCacheMiddlewareMetrics struct {
	*resultsCacheMetrics

	splitQueriesCount              prometheus.Counter
	queryResultCacheAttemptedCount prometheus.Counter
	queryResultCacheSkippedCount   *prometheus.CounterVec
}

func newSplitAndCacheMiddlewareMetrics(reg prometheus.Registerer) *splitAndCacheMiddlewareMetrics {
	m := &splitAndCacheMiddlewareMetrics{
		resultsCacheMetrics: newResultsCacheMetrics("query_range", reg),
		splitQueriesCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_split_queries_total",
			Help: "Total number of underlying query requests after the split by interval is applied.",
		}),
		queryResultCacheAttemptedCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_query_result_cache_attempted_total",
			Help: "Total number of queries that were attempted to be fetched from cache.",
		}),
		queryResultCacheSkippedCount: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_frontend_query_result_cache_skipped_total",
			Help: "Total number of times a query was not cacheable because of a reason. This metric is tracked for each partial query when time-splitting is enabled.",
		}, []string{"reason"}),
	}

	// Initialize known label values.
	for _, reason := range []string{notCachableReasonUnalignedTimeRange, notCachableReasonTooNew,
		notCachableReasonModifiersNotCachable} {
		m.queryResultCacheSkippedCount.WithLabelValues(reason)
	}

	return m
}

// splitAndCacheMiddleware is a MetricsQueryMiddleware that can (optionally) split the query by interval
// and run split queries through the results cache.
type splitAndCacheMiddleware struct {
	next    MetricsQueryHandler
	limits  Limits
	merger  Merger
	logger  log.Logger
	metrics *splitAndCacheMiddlewareMetrics

	// Split by interval.
	splitEnabled  bool
	splitInterval time.Duration

	// Results caching.
	cacheEnabled   bool
	cache          cache.Cache
	splitter       CacheKeyGenerator
	extractor      Extractor
	shouldCacheReq shouldCacheFn

	// Can be set from tests
	currentTime func() time.Time
}

// newSplitAndCacheMiddleware makes a new splitAndCacheMiddleware.
func newSplitAndCacheMiddleware(
	splitEnabled bool,
	cacheEnabled bool,
	splitInterval time.Duration,
	limits Limits,
	merger Merger,
	cache cache.Cache,
	splitter CacheKeyGenerator,
	extractor Extractor,
	shouldCacheReq shouldCacheFn,
	logger log.Logger,
	reg prometheus.Registerer) MetricsQueryMiddleware {
	metrics := newSplitAndCacheMiddlewareMetrics(reg)

	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &splitAndCacheMiddleware{
			splitEnabled:   splitEnabled,
			cacheEnabled:   cacheEnabled,
			next:           next,
			limits:         limits,
			merger:         merger,
			splitInterval:  splitInterval,
			metrics:        metrics,
			cache:          cache,
			splitter:       splitter,
			extractor:      extractor,
			shouldCacheReq: shouldCacheReq,
			logger:         logger,
			currentTime:    time.Now,
		}
	})
}

func (s *splitAndCacheMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	spanLog := spanlogger.FromContext(ctx, s.logger)
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Split the input requests by the configured interval (eg. day).
	// Returns the input request if splitting is disabled.
	splitReqs, err := s.splitRequestByInterval(req)
	if err != nil {
		return nil, err
	}

	isCacheEnabled := s.cacheEnabled && (s.shouldCacheReq == nil || s.shouldCacheReq(req))
	maxCacheFreshness := validation.MaxDurationPerTenant(tenantIDs, s.limits.MaxCacheFreshness)
	maxCacheTime := int64(model.Now().Add(-maxCacheFreshness))
	cacheUnalignedRequests := validation.AllTrueBooleansPerTenant(tenantIDs, s.limits.ResultsCacheForUnalignedQueryEnabled)

	// Lookup the results cache.
	if isCacheEnabled {
		s.metrics.queryResultCacheAttemptedCount.Add(float64(len(splitReqs)))

		// Build the cache keys for all requests to try to fetch from cache.
		lookupReqs := make([]*splitRequest, 0, len(splitReqs))
		lookupKeys := make([]string, 0, len(splitReqs))

		for _, splitReq := range splitReqs {
			// Do not try to pick response from cache at all if the request is not cachable.
			if cachable, reason := isRequestCachable(splitReq.orig, maxCacheTime, cacheUnalignedRequests, s.logger); !cachable {
				level.Debug(spanLog).Log("msg", "skipping response cache as query is not cacheable", "query", splitReq.orig.GetQuery(), "reason", reason, "tenants", tenant.JoinTenantIDs(tenantIDs))
				splitReq.downstreamRequests = []MetricsQueryRequest{splitReq.orig}
				s.metrics.queryResultCacheSkippedCount.WithLabelValues(reason).Inc()
				continue
			}

			splitReq.cacheKey = s.splitter.QueryRequest(ctx, tenant.JoinTenantIDs(tenantIDs), splitReq.orig)
			lookupKeys = append(lookupKeys, splitReq.cacheKey)
			lookupReqs = append(lookupReqs, splitReq)
		}

		// Lookup all keys from cache.
		fetchedExtents := s.fetchCacheExtents(ctx, s.currentTime(), tenantIDs, lookupKeys)

		for lookupIdx, extents := range fetchedExtents {
			if len(extents) == 0 {
				// We just need to run the request as is because no part of it has been cached yet.
				lookupReqs[lookupIdx].downstreamRequests = []MetricsQueryRequest{lookupReqs[lookupIdx].orig}
				continue
			}

			// We have some extents. This means some parts of the response has been cached and we need
			// to generate the queries for the missing parts.
			requests, responses, err := partitionCacheExtents(lookupReqs[lookupIdx].orig, extents, defaultMinCacheExtent, s.extractor)
			if err != nil {
				return nil, err
			}

			if len(requests) == 0 {
				// The full response has been picked up from the cache so we can merge it and store it.
				response, err := s.merger.MergeResponse(responses...)
				if err != nil {
					return nil, err
				}

				lookupReqs[lookupIdx].cachedResponses = []Response{response}
				continue
			}

			lookupReqs[lookupIdx].downstreamRequests = requests
			lookupReqs[lookupIdx].cachedResponses = responses
			lookupReqs[lookupIdx].cachedExtents = extents
		}
	} else {
		// Cache is disabled. We've just to execute the original request.
		for _, splitReq := range splitReqs {
			splitReq.downstreamRequests = []MetricsQueryRequest{splitReq.orig}
		}
	}

	// Prepare and execute the downstream requests.
	execReqs, err := splitReqs.prepareDownstreamRequests()
	if err != nil {
		return nil, err
	}

	// Update query stats.
	// Only consider the actual number of downstream requests, not the cache hits.
	queryStats := stats.FromContext(ctx)
	queryStats.AddSplitQueries(uint32(len(execReqs)))

	queryTime := s.currentTime()

	if len(execReqs) > 0 {
		execResps, err := doRequests(ctx, s.next, execReqs)
		if err != nil {
			return nil, err
		}

		// Store the downstream responses in our internal data structure.
		if err := splitReqs.storeDownstreamResponses(execResps); err != nil {
			return nil, err
		}

		if details := QueryDetailsFromContext(ctx); details != nil {
			details.ResultsCacheMissBytes = splitReqs.countDownstreamResponseBytes()
		}
	}

	// Store the updated response in the results cache.
	if isCacheEnabled && len(execReqs) > 0 {
		for _, splitReq := range splitReqs {
			// If there are no downstream requests it means the response was entirely picked up from the cache
			// so there's no need to store it again in the cache (because nothing has changed).
			if len(splitReq.downstreamRequests) == 0 {
				continue
			}

			// Skip caching if the request is not cachable.
			if cachable, _ := isRequestCachable(splitReq.orig, maxCacheTime, cacheUnalignedRequests, s.logger); !cachable {
				continue
			}

			// Update extents with the new ones from downstream responses.
			updatedExtents := splitReq.cachedExtents

			for downstreamIdx, downstreamReq := range splitReq.downstreamRequests {
				downstreamRes := splitReq.downstreamResponses[downstreamIdx]
				if !isResponseCachable(downstreamRes, s.logger) {
					continue
				}

				extent, err := toExtent(ctx, downstreamReq, s.extractor.ResponseWithoutHeaders(downstreamRes), queryTime)
				if err != nil {
					return nil, err
				}

				updatedExtents = append(updatedExtents, extent)
			}

			// If extents haven't been updated, we can skip storing it in the cache again.
			if len(splitReq.cachedExtents) == len(updatedExtents) {
				continue
			}

			mergedExtents, err := mergeCacheExtentsForRequest(ctx, splitReq.orig, s.merger, updatedExtents)
			if err != nil {
				return nil, err
			}

			// Filter out recent extents from merged ones.
			// TODO(codesome): make filterRecentCacheExtents break it into 2 sets, one to cache with lower TTL and one with the usual TTL.
			filteredExtents, err := filterRecentCacheExtents(splitReq.orig, maxCacheFreshness, s.extractor, mergedExtents)
			if err != nil {
				return nil, err
			}

			// Put back into the cache the filtered ones.
			s.storeCacheExtents(splitReq.cacheKey, tenantIDs, filteredExtents)
		}
	}

	// We can finally build the response, which is the merge of all downstream responses and the responses
	// we've got from the cache (if any).
	responses := make([]Response, 0, splitReqs.countDownstreamRequests()+splitReqs.countCachedResponses())
	for _, splitReq := range splitReqs {
		responses = append(responses, splitReq.cachedResponses...)
		responses = append(responses, splitReq.downstreamResponses...)
	}

	return s.merger.MergeResponse(responses...)
}

// splitRequestByInterval splits the given MetricsQueryRequest by configured interval. Returns the input request if splitting is disabled.
func (s *splitAndCacheMiddleware) splitRequestByInterval(req MetricsQueryRequest) (splitRequests, error) {
	if !s.splitEnabled {
		return splitRequests{{orig: req}}, nil
	}

	splitReqs, err := splitQueryByInterval(req, s.splitInterval)
	if err != nil {
		return nil, err
	}

	s.metrics.splitQueriesCount.Add(float64(len(splitReqs)))

	// Wrap the split requests into our internal data structure.
	out := make(splitRequests, 0, len(splitReqs))
	for _, splitReq := range splitReqs {
		out = append(out, &splitRequest{orig: splitReq})
	}
	return out, nil
}

// fetchCacheExtents fetches the extents for the given key from the cache. The returned slice
// is guaranteed to have the same length of the input keys. For each input key, the fetched
// extents are stored in the returned slice at the same position. In case of error or cache miss,
// the returned extents are empty.
// Extents created from queries that outlived current configured TTL are filtered out.
func (s *splitAndCacheMiddleware) fetchCacheExtents(ctx context.Context, now time.Time, tenantIDs []string, keys []string) [][]Extent {
	spanLog, ctx := spanlogger.NewWithLogger(ctx, s.logger, "fetchCacheExtents")
	defer spanLog.Finish()

	// Fast path.
	if len(keys) == 0 {
		return nil
	}

	// Hash all the input cache keys.
	hashedKeys := make([]string, 0, len(keys))
	hashedKeysIdx := make(map[string]int, len(keys))
	for idx, key := range keys {
		hashed := cacheHashKey(key)
		hashedKeys = append(hashedKeys, hashed)
		hashedKeysIdx[hashed] = idx

		spanLog.LogKV("msg", "looking up", "key", key, "hashedKey", hashed)
	}

	// Lookup the cache.
	s.metrics.cacheRequests.Add(float64(len(keys)))
	founds := s.cache.GetMulti(ctx, hashedKeys)
	s.metrics.cacheHits.Add(float64(len(founds)))

	// Decode all cached responses.
	extents := make([][]Extent, len(keys))
	fetchedBytes := 0
	usedBytes := 0
	extentsOutOfTTL := 0

	ttl, ttlForExtentsInOOOWindow, oooWindow := s.getCacheOptions(tenantIDs)

	for foundKey, foundData := range founds {
		fetchedBytes += len(foundData)
		// Find the index of this cache key.
		keyIdx, ok := hashedKeysIdx[foundKey]
		if !ok {
			err := errors.Errorf("the cache lookup returned entries for a key which has not been requested (returned key: %v)", foundKey)
			level.Error(spanLog).Log("msg", err.Error())
			spanLog.Error(err)
			continue
		}

		var resp CachedResponse
		if err := proto.Unmarshal(foundData, &resp); err != nil {
			level.Error(spanLog).Log("msg", "error unmarshalling cached response", "err", err)
			spanLog.Error(err)
			continue
		}

		// Ensure there's no hashed key collision.
		if resp.Key != keys[keyIdx] {
			continue
		}

		extents[keyIdx] = make([]Extent, 0, len(resp.Extents))

		// Filter out extents that are outside TTL.
		for _, cachedExtent := range resp.Extents {
			// If we don't know the query timestamp, we use the cached result.
			// This is temporary ... after max 7 days (previous hardcoded TTL) all cached results will have query timestamp recorded.
			usedTTL := getTTLForExtent(now, ttl, ttlForExtentsInOOOWindow, oooWindow, cachedExtent)
			if cachedExtent.QueryTimestampMs > 0 && cachedExtent.QueryTimestampMs < now.UnixMilli()-usedTTL.Milliseconds() {
				extentsOutOfTTL++
				continue
			}

			extents[keyIdx] = append(extents[keyIdx], cachedExtent)
			// log only hashed key so that we keep the logs briefer
			spanLog.LogKV(
				"msg", "fetched",
				"hashedKey", foundKey,
				"traceID", cachedExtent.TraceId,
				"start", time.UnixMilli(cachedExtent.Start),
				"end", time.UnixMilli(cachedExtent.End),
			)
			usedBytes += cachedExtent.Response.Size()
		}

		if len(extents[keyIdx]) == 0 {
			extents[keyIdx] = nil
		}
	}

	spanLog.LogKV(
		"requested keys", len(hashedKeys),
		"found keys", len(founds),
		"fetched bytes", fetchedBytes,
		"used bytes", usedBytes,
		"extents filtered out due to ttl", extentsOutOfTTL,
	)

	if details := QueryDetailsFromContext(ctx); details != nil {
		details.ResultsCacheHitBytes = usedBytes
	}

	return extents
}

func (s *splitAndCacheMiddleware) getCacheOptions(tenantIDs []string) (ttl, ttlInOOO, oooWindow time.Duration) {
	ttl = validation.SmallestPositiveNonZeroDurationPerTenant(tenantIDs, s.limits.ResultsCacheTTL)
	ttlInOOO = validation.SmallestPositiveNonZeroDurationPerTenant(tenantIDs, s.limits.ResultsCacheTTLForOutOfOrderTimeWindow)
	oooWindow = validation.MaxDurationPerTenant(tenantIDs, s.limits.OutOfOrderTimeWindow)
	return
}

// storeCacheExtents stores the extents for given key in the cache.
func (s *splitAndCacheMiddleware) storeCacheExtents(key string, tenantIDs []string, extents []Extent) {
	if len(extents) == 0 {
		return
	}

	ttl, ttlInOOO, oooWindow := s.getCacheOptions(tenantIDs)
	usedTTL := getTTLForExtent(time.Now(), ttl, ttlInOOO, oooWindow, extents[len(extents)-1])

	buf, err := proto.Marshal(&CachedResponse{
		Key:     key,
		Extents: extents,
	})
	if err != nil {
		level.Error(s.logger).Log("msg", "error marshalling cached extent", "err", err)
		return
	}

	s.cache.SetMultiAsync(map[string][]byte{cacheHashKey(key): buf}, usedTTL)
}

func getTTLForExtent(now time.Time, ttl, ttlInOOOWindow, oooWindow time.Duration, e Extent) time.Duration {
	if oooWindow > 0 && e.End >= now.Add(-oooWindow).UnixMilli() {
		return ttlInOOOWindow
	}
	return ttl
}

// splitRequest holds information about a split request.
type splitRequest struct {
	// The original split query.
	orig MetricsQueryRequest

	// The cache key for the request.
	cacheKey string

	// The extents picked up from the cache.
	cachedExtents []Extent

	// The responses picked up from the cache.
	cachedResponses []Response

	// The requests/responses we send/receive to/from downstream. For a given request, its
	// response is stored at the same index.
	downstreamRequests  []MetricsQueryRequest
	downstreamResponses []Response
}

// splitRequests holds a list of splitRequest.
type splitRequests []*splitRequest

// countCachedResponses returns the total number of cached responses.
func (s *splitRequests) countCachedResponses() int {
	count := 0
	for _, req := range *s {
		count += len(req.cachedResponses)
	}
	return count
}

// countDownstreamRequests returns the total number of downstream requests.
func (s *splitRequests) countDownstreamRequests() int {
	count := 0
	for _, req := range *s {
		count += len(req.downstreamRequests)
	}
	return count
}

// countDownstreamRequests returns the total number of bytes returned from downstream requests.
func (s *splitRequests) countDownstreamResponseBytes() int {
	bytes := 0
	for _, req := range *s {
		for _, resp := range req.downstreamResponses {
			bytes += proto.Size(resp)
		}
	}
	return bytes
}

// prepareDownstreamRequests injects a unique ID and hints to all downstream requests and
// initialize downstream responses slice to have the same length of requests.
func (s *splitRequests) prepareDownstreamRequests() ([]MetricsQueryRequest, error) {
	// Count the total number of downstream requests to run and build the hints we're going
	// to attach to each request.
	numDownstreamRequests := s.countDownstreamRequests()
	if numDownstreamRequests == 0 {
		return nil, nil
	}

	// Build the whole list of requests to execute. For each downstream request,
	// inject hints and a unique ID used to correlate responses once executed.
	// ID intentionally start at 1 to detect any bug in case the default zero value is used.
	nextReqID := int64(1)

	execReqs := make([]MetricsQueryRequest, 0, numDownstreamRequests)
	for _, splitReq := range *s {
		for i := 0; i < len(splitReq.downstreamRequests); i++ {
			newRequest, err := splitReq.downstreamRequests[i].WithID(nextReqID)
			if err != nil {
				return nil, err
			}
			newRequest, err = newRequest.WithTotalQueriesHint(int32(numDownstreamRequests))
			if err != nil {
				return nil, err
			}
			splitReq.downstreamRequests[i] = newRequest
			nextReqID++
		}

		execReqs = append(execReqs, splitReq.downstreamRequests...)
		splitReq.downstreamResponses = make([]Response, len(splitReq.downstreamRequests))
	}

	return execReqs, nil
}

// storeDownstreamResponses associates the given executed requestResponse with the downstream requests
// and stores the associated downstream responses for each request. If returns no error, then it's guaranteed
// that any downstream request got its response associated.
func (s *splitRequests) storeDownstreamResponses(responses []requestResponse) error {
	execRespsByID := make(map[int64]Response, len(responses))

	// Map responses by (unique) request IDs.
	for _, resp := range responses {
		// Ensure doesn't exist (otherwise it's a bug).
		if _, ok := execRespsByID[resp.Request.GetID()]; ok {
			// Should never happen unless a bug.
			return errors.New("consistency check failed: conflicting downstream request ID")
		}

		execRespsByID[resp.Request.GetID()] = resp.Response
	}

	mappedDownstreamRequests := 0

	for _, splitReq := range *s {
		for downstreamIdx, downstreamReq := range splitReq.downstreamRequests {
			downstreamRes, ok := execRespsByID[downstreamReq.GetID()]
			if !ok {
				// Should never happen unless a bug.
				return errors.New("consistency check failed: missing downstream response")
			}

			splitReq.downstreamResponses[downstreamIdx] = downstreamRes
			mappedDownstreamRequests++
		}
	}

	// Finally, we have to make sure all provided responses have been associated. Should never happen unless a bug.
	if mappedDownstreamRequests != len(responses) {
		return errors.Errorf("consistency check failed: received more responses than expected (expected: %d, got: %d)", mappedDownstreamRequests, len(responses))
	}

	return nil
}

// requestResponse contains a request response and the respective request that was used.
type requestResponse struct {
	Request  MetricsQueryRequest
	Response Response
}

// doRequests executes a list of requests in parallel.
func doRequests(ctx context.Context, downstream MetricsQueryHandler, reqs []MetricsQueryRequest) ([]requestResponse, error) {
	g, ctx := errgroup.WithContext(ctx)
	mtx := sync.Mutex{}
	resps := make([]requestResponse, 0, len(reqs))
	queryStatistics := stats.FromContext(ctx)
	for i := 0; i < len(reqs); i++ {
		req := reqs[i]
		g.Go(func() error {
			// partialStats are the statistics for this partial query, which we'll need to
			// get correct aggregation of statistics for partial queries.
			partialStats, childCtx := stats.ContextWithEmptyStats(ctx)
			var span opentracing.Span
			span, childCtx = opentracing.StartSpanFromContext(childCtx, "doRequests")
			req.AddSpanTags(span)
			defer span.Finish()

			resp, err := downstream.Do(childCtx, req)
			queryStatistics.Merge(partialStats)
			if err != nil {
				return err
			}

			mtx.Lock()
			resps = append(resps, requestResponse{req, resp})
			mtx.Unlock()

			return nil
		})
	}

	return resps, g.Wait()
}

func splitQueryByInterval(req MetricsQueryRequest, interval time.Duration) ([]MetricsQueryRequest, error) {
	// Replace @ modifier function to their respective constant values in the query.
	// This way subqueries will be evaluated at the same time as the parent query.
	query, err := evaluateAtModifierFunction(req.GetQuery(), req.GetStart(), req.GetEnd())
	if err != nil {
		return nil, err
	}
	var reqs []MetricsQueryRequest
	for start := req.GetStart(); start <= req.GetEnd(); {
		end := nextIntervalBoundary(start, req.GetStep(), interval)
		if end > req.GetEnd() {
			end = req.GetEnd()
		}

		// If step isn't too big, and adding another step saves us one extra request,
		// then extend the current request to cover the extra step too.
		if end+req.GetStep() == req.GetEnd() && req.GetStep() <= 5*time.Minute.Milliseconds() {
			end = req.GetEnd()
		}

		splitReq, err := req.WithQuery(query)
		if err != nil {
			return nil, err
		}
		splitReq, err = splitReq.WithStartEnd(start, end)
		if err != nil {
			return nil, err
		}
		reqs = append(reqs, splitReq)

		start = end + splitReq.GetStep()
	}
	return reqs, nil
}

// evaluateAtModifierFunction parse the query and evaluates the `start()` and `end()` at modifier functions into actual constant timestamps.
// For example given the start of the query is 10.00, `http_requests_total[1h] @ start()` query will be replaced with `http_requests_total[1h] @ 10.00`
// If the modifier is already a constant, it will be returned as is.
func evaluateAtModifierFunction(query string, start, end int64) (string, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", apierror.New(apierror.TypeBadData, decorateWithParamName(err, "query").Error())
	}
	parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
		switch exprAt := n.(type) {
		case *parser.VectorSelector:
			switch exprAt.StartOrEnd {
			case parser.START:
				exprAt.Timestamp = &start
			case parser.END:
				exprAt.Timestamp = &end
			}
			exprAt.StartOrEnd = 0
		case *parser.SubqueryExpr:
			switch exprAt.StartOrEnd {
			case parser.START:
				exprAt.Timestamp = &start
			case parser.END:
				exprAt.Timestamp = &end
			}
			exprAt.StartOrEnd = 0
		}
		return nil
	})
	return expr.String(), nil
}

// Round up to the step before the next interval boundary.
func nextIntervalBoundary(t, step int64, interval time.Duration) int64 {
	intervalMillis := interval.Milliseconds()
	startOfNextInterval := ((t / intervalMillis) + 1) * intervalMillis
	// ensure that target is a multiple of steps away from the start time
	target := startOfNextInterval - ((startOfNextInterval - t) % step)
	if target == startOfNextInterval {
		target -= step
	}
	return target
}
