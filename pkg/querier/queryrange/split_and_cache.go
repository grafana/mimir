// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/split_by_interval.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/chunk/cache"
	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	// defaultMinCacheExtent is the minimum time range of a query response to
	// be eligible for caching.
	defaultMinCacheExtent = (5 * time.Minute).Milliseconds()
)

type splitAndCacheMiddlewareMetrics struct {
	splitQueriesCount prometheus.Counter
}

func newSplitAndCacheMiddlewareMetrics(reg prometheus.Registerer) *splitAndCacheMiddlewareMetrics {
	return &splitAndCacheMiddlewareMetrics{
		splitQueriesCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_split_queries_total",
			Help: "Total number of underlying query requests after the split by interval is applied",
		}),
	}
}

// splitAndCacheMiddleware is a Middleware that can (optionally) split the query by interval
// and run split queries through the results cache.
type splitAndCacheMiddleware struct {
	next    Handler
	limits  Limits
	merger  Merger
	logger  log.Logger
	metrics *splitAndCacheMiddlewareMetrics

	// Split by interval.
	splitEnabled  bool
	splitInterval time.Duration

	// Results caching.
	cacheEnabled           bool
	cacheUnalignedRequests bool
	cache                  cache.Cache
	splitter               CacheSplitter
	extractor              Extractor
	cacheGenNumberLoader   CacheGenNumberLoader
	shouldCacheReq         shouldCacheFn
}

// newSplitAndCacheMiddleware makes a new splitAndCacheMiddleware.
func newSplitAndCacheMiddleware(
	splitEnabled bool,
	cacheEnabled bool,
	splitInterval time.Duration,
	cacheUnalignedRequests bool,
	limits Limits,
	merger Merger,
	cache cache.Cache,
	splitter CacheSplitter,
	extractor Extractor,
	cacheGenNumberLoader CacheGenNumberLoader,
	shouldCacheReq shouldCacheFn,
	logger log.Logger,
	reg prometheus.Registerer) Middleware {
	metrics := newSplitAndCacheMiddlewareMetrics(reg)

	return MiddlewareFunc(func(next Handler) Handler {
		return &splitAndCacheMiddleware{
			splitEnabled:           splitEnabled,
			cacheEnabled:           cacheEnabled,
			cacheUnalignedRequests: cacheUnalignedRequests,
			next:                   next,
			limits:                 limits,
			merger:                 merger,
			splitInterval:          splitInterval,
			metrics:                metrics,
			cache:                  cache,
			splitter:               splitter,
			extractor:              extractor,
			cacheGenNumberLoader:   cacheGenNumberLoader,
			shouldCacheReq:         shouldCacheReq,
			logger:                 logger,
		}
	})
}

func (s *splitAndCacheMiddleware) Do(ctx context.Context, req Request) (Response, error) {
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

	// Lookup the results cache.
	if isCacheEnabled {
		// Inject the cache generation number into the context (if provided).
		if s.cacheGenNumberLoader != nil {
			ctx = cache.InjectCacheGenNumber(ctx, s.cacheGenNumberLoader.GetResultsCacheGenNumber(tenantIDs))
		}

		// Build the cache keys for all requests to try to fetch from cache.
		lookupReqs := make([]*splitRequest, 0, len(splitReqs))
		lookupKeys := make([]string, 0, len(splitReqs))

		for _, splitReq := range splitReqs {
			// Do not try to pick response from cache at all if the request is not cachable.
			if !isRequestCachable(splitReq.orig, maxCacheTime, s.cacheUnalignedRequests, s.logger) {
				splitReq.downstreamRequests = []Request{splitReq.orig}
				continue
			}

			splitReq.cacheKey = s.splitter.GenerateCacheKey(tenant.JoinTenantIDs(tenantIDs), splitReq.orig)
			lookupKeys = append(lookupKeys, splitReq.cacheKey)
			lookupReqs = append(lookupReqs, splitReq)
		}

		// Lookup all keys from cache.
		fetchedExtents := s.fetchCacheExtents(ctx, lookupKeys)

		for lookupIdx, extents := range fetchedExtents {
			if len(extents) == 0 {
				// We just need to run the request as is because no part of it has been cached yet.
				lookupReqs[lookupIdx].downstreamRequests = []Request{lookupReqs[lookupIdx].orig}
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
			splitReq.downstreamRequests = []Request{splitReq.orig}
		}
	}

	// Prepare and execute the downstream requests.
	execReqs := splitReqs.prepareDownstreamRequests()

	if len(execReqs) > 0 {
		execResps, err := doRequests(ctx, s.next, execReqs, true)
		if err != nil {
			return nil, err
		}

		// Store the downstream responses in our internal data structure.
		if err := splitReqs.storeDownstreamResponses(execResps); err != nil {
			return nil, err
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
			if !isRequestCachable(splitReq.orig, maxCacheTime, s.cacheUnalignedRequests, s.logger) {
				continue
			}

			// Update extents with the new ones from downstream responses.
			updatedExtents := splitReq.cachedExtents

			for downstreamIdx, downstreamReq := range splitReq.downstreamRequests {
				downstreamRes := splitReq.downstreamResponses[downstreamIdx]
				if !isResponseCachable(ctx, downstreamRes, s.cacheGenNumberLoader, s.logger) {
					continue
				}

				extent, err := toExtent(ctx, downstreamReq, s.extractor.ResponseWithoutHeaders(downstreamRes))
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
			filteredExtents, err := filterRecentCacheExtents(splitReq.orig, maxCacheFreshness, s.extractor, mergedExtents)
			if err != nil {
				return nil, err
			}

			// Put back into the cache the filtered ones.
			s.storeCacheExtents(ctx, splitReq.cacheKey, filteredExtents)
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

// splitRequestByInterval splits the given Request by configured interval. Returns the input request if splitting is disabled.
func (s *splitAndCacheMiddleware) splitRequestByInterval(req Request) (splitRequests, error) {
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
func (s *splitAndCacheMiddleware) fetchCacheExtents(ctx context.Context, keys []string) [][]Extent {
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
		hashed := cache.HashKey(key)
		hashedKeys = append(hashedKeys, hashed)
		hashedKeysIdx[hashed] = idx

		spanLog.LogKV("key", key, "hashedKey", hashed)
	}

	founds, bufs, _ := s.cache.Fetch(ctx, hashedKeys)

	// Decode all cached responses.
	extents := make([][]Extent, len(keys))
	returnedBytes := 0

	for foundIdx, foundKey := range founds {
		// Find the index of this cache key.
		keyIdx, ok := hashedKeysIdx[foundKey]
		if !ok {
			err := errors.Errorf("the cache lookup returned entries for a key which has not been requested (returned key: %v)", foundKey)
			level.Error(spanLog).Log("msg", err.Error())
			spanLog.Error(err)
			continue
		}

		var resp CachedResponse
		if err := proto.Unmarshal(bufs[foundIdx], &resp); err != nil {
			level.Error(spanLog).Log("msg", "error unmarshalling cached response", "err", err)
			spanLog.Error(err)
			continue
		}

		// Ensure there's no hashed key collision.
		if resp.Key != keys[keyIdx] {
			continue
		}

		extents[keyIdx] = resp.Extents
		returnedBytes += len(bufs[foundIdx])
	}

	spanLog.LogKV("requested keys", len(hashedKeys))
	spanLog.LogKV("found keys", len(founds))
	spanLog.LogKV("returned bytes", returnedBytes)

	return extents
}

// storeCacheExtents stores the extents for given key in the cache.
func (s *splitAndCacheMiddleware) storeCacheExtents(ctx context.Context, key string, extents []Extent) {
	buf, err := proto.Marshal(&CachedResponse{
		Key:     key,
		Extents: extents,
	})
	if err != nil {
		level.Error(s.logger).Log("msg", "error marshalling cached extent", "err", err)
		return
	}

	s.cache.Store(ctx, []string{cache.HashKey(key)}, [][]byte{buf})
}

// splitRequest holds information about a split request.
type splitRequest struct {
	// The original split query.
	orig Request

	// The cache key for the request.
	cacheKey string

	// The extents picked up from the cache.
	cachedExtents []Extent

	// The responses picked up from the cache.
	cachedResponses []Response

	// The requests/responses we send/receive to/from downstream. For a given request, its
	// response is stored at the same index.
	downstreamRequests  []Request
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

// prepareDownstreamRequests injects a unique ID and hints to all downstream requests and
// initialize downstream responses slice to have the same length of requests.
func (s *splitRequests) prepareDownstreamRequests() []Request {
	// Count the total number of downstream requests to run and build the hints we're going
	// to attach to each request.
	numDownstreamRequests := s.countDownstreamRequests()
	if numDownstreamRequests == 0 {
		return nil
	}

	hints := &Hints{TotalQueries: int32(numDownstreamRequests)}

	// Build the whole list of requests to execute. For each downstream request,
	// inject hints and a unique ID used to correlate responses once executed.
	// ID intentionally start at 1 to detect any bug in case the default zero value is used.
	nextReqID := int64(1)

	execReqs := make([]Request, 0, numDownstreamRequests)
	for _, splitReq := range *s {
		for i := 0; i < len(splitReq.downstreamRequests); i++ {
			splitReq.downstreamRequests[i] = splitReq.downstreamRequests[i].WithID(nextReqID).WithHints(hints)
			nextReqID++
		}

		execReqs = append(execReqs, splitReq.downstreamRequests...)
		splitReq.downstreamResponses = make([]Response, len(splitReq.downstreamRequests))
	}

	return execReqs
}

// storeDownstreamResponses associates the given executed requestResponse with the downstream requests
// and stores the associated downstream responses for each request. If returns no error, then it's guaranteed
// that any downstream request got its response associated.
func (s *splitRequests) storeDownstreamResponses(responses []requestResponse) error {
	execRespsByID := make(map[int64]Response, len(responses))

	// Map responses by (unique) request IDs.
	for _, resp := range responses {
		// Ensure doesn't exist (otherwise it's a bug).
		if _, ok := execRespsByID[resp.Request.GetId()]; ok {
			// Should never happen unless a bug.
			return errors.New("consistency check failed: conflicting downstream request ID")
		}

		execRespsByID[resp.Request.GetId()] = resp.Response
	}

	mappedDownstreamRequests := 0

	for _, splitReq := range *s {
		for downstreamIdx, downstreamReq := range splitReq.downstreamRequests {
			downstreamRes, ok := execRespsByID[downstreamReq.GetId()]
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
	Request  Request
	Response Response
}

// doRequests executes a list of requests in parallel.
func doRequests(ctx context.Context, downstream Handler, reqs []Request, recordSpan bool) ([]requestResponse, error) {
	g, ctx := errgroup.WithContext(ctx)
	mtx := sync.Mutex{}
	resps := make([]requestResponse, 0, len(reqs))
	for i := 0; i < len(reqs); i++ {
		req := reqs[i]
		g.Go(func() error {
			var childCtx = ctx
			if recordSpan {
				var span opentracing.Span
				span, childCtx = opentracing.StartSpanFromContext(ctx, "doRequests")
				req.LogToSpan(span)
				defer span.Finish()
			}

			resp, err := downstream.Do(childCtx, req)
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

func splitQueryByInterval(r Request, interval time.Duration) ([]Request, error) {
	// Replace @ modifier function to their respective constant values in the query.
	// This way subqueries will be evaluated at the same time as the parent query.
	query, err := evaluateAtModifierFunction(r.GetQuery(), r.GetStart(), r.GetEnd())
	if err != nil {
		return nil, err
	}
	var reqs []Request
	for start := r.GetStart(); start <= r.GetEnd(); {
		end := nextIntervalBoundary(start, r.GetStep(), interval)
		if end > r.GetEnd() {
			end = r.GetEnd()
		}

		// If step isn't too big, and adding another step saves us one extra request,
		// then extend the current request to cover the extra step too.
		if end+r.GetStep() == r.GetEnd() && r.GetStep() <= 5*time.Minute.Milliseconds() {
			end = r.GetEnd()
		}

		reqs = append(reqs, r.WithQuery(query).WithStartEnd(start, end))

		start = end + r.GetStep()
	}
	return reqs, nil
}

// evaluateAtModifierFunction parse the query and evaluates the `start()` and `end()` at modifier functions into actual constant timestamps.
// For example given the start of the query is 10.00, `http_requests_total[1h] @ start()` query will be replaced with `http_requests_total[1h] @ 10.00`
// If the modifier is already a constant, it will be returned as is.
func evaluateAtModifierFunction(query string, start, end int64) (string, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", apierror.New(apierror.TypeBadData, err.Error())
	}
	parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
		if selector, ok := n.(*parser.VectorSelector); ok {
			switch selector.StartOrEnd {
			case parser.START:
				selector.Timestamp = &start
			case parser.END:
				selector.Timestamp = &end
			}
			selector.StartOrEnd = 0
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
