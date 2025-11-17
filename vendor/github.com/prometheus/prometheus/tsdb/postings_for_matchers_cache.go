// Copyright 2021 Grafana Labs
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DmitriyVTitov/size"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	promContext "github.com/prometheus/prometheus/util/context"
)

const (
	// NOTE: keep them exported to reference them in Mimir.

	DefaultPostingsForMatchersCacheTTL          = 10 * time.Second
	DefaultPostingsForMatchersCacheMaxItems     = 100
	DefaultPostingsForMatchersCacheMaxBytes     = 10 * 1024 * 1024 // DefaultPostingsForMatchersCacheMaxBytes is based on the default max items, 10MB / 100 = 100KB per cached entry on average.
	DefaultPostingsForMatchersCacheForce        = false
	DefaultPostingsForMatchersCacheInvalidation = false
	DefaultPostingsForMatchersCacheVersions     = 2 * 1024 * 1024 // The number of metric versions to store across all metric names
	DefaultPostingsForMatchersCacheShared       = false
)

const (
	evictionReasonTTL = iota
	evictionReasonMaxBytes
	evictionReasonMaxItems
	evictionReasonUnknown
	evictionReasonsLength
)

// CacheKeyFunc provides a cache key for a Context. The cache key can be used to better distinguish cache entries, along
// with the metric name from queries, such as for different users.
type CacheKeyFunc func(ctx context.Context) (string, error)

var DefaultPostingsForMatchersCacheKeyFunc = func(_ context.Context) (string, error) { return "", nil }

// PostingsForMatchersCacheFactory gets or creates PostingsForMatchersCache instances.
// `tracingKV` specifies attributes for tracing purposes, which are appended to the default attributes.
type PostingsForMatchersCacheFactory interface {
	NewPostingsForMatchersCache(tracingKV []attribute.KeyValue) *PostingsForMatchersCache
}

type sharedPostingsForMatchersCacheFactory struct {
	instance *PostingsForMatchersCache
}

func (f *sharedPostingsForMatchersCacheFactory) NewPostingsForMatchersCache(_ []attribute.KeyValue) *PostingsForMatchersCache {
	// Additional attributes are not propagated for shared caches
	return f.instance
}

type nonSharedPostingsForMatchersCacheFactory struct {
	factoryFunc func(tracingKV []attribute.KeyValue) *PostingsForMatchersCache
}

func (f *nonSharedPostingsForMatchersCacheFactory) NewPostingsForMatchersCache(tracingKV []attribute.KeyValue) *PostingsForMatchersCache {
	return f.factoryFunc(tracingKV)
}

var DefaultPostingsForMatchersCacheConfig = PostingsForMatchersCacheConfig{
	Shared:                DefaultPostingsForMatchersCacheShared,
	KeyFunc:               DefaultPostingsForMatchersCacheKeyFunc,
	Invalidation:          DefaultPostingsForMatchersCacheInvalidation,
	CacheVersions:         DefaultPostingsForMatchersCacheVersions,
	TTL:                   DefaultPostingsForMatchersCacheTTL,
	MaxItems:              DefaultPostingsForMatchersCacheMaxItems,
	MaxBytes:              DefaultPostingsForMatchersCacheMaxBytes,
	Force:                 DefaultPostingsForMatchersCacheForce,
	Metrics:               NewPostingsForMatchersCacheMetrics(nil),
	PostingsClonerFactory: DefaultPostingsClonerFactory{},
}

var DefaultPostingsForMatchersCacheFactory = NewPostingsForMatchersCacheFactory(DefaultPostingsForMatchersCacheConfig)

type PostingsClonerFactory interface {
	PostingsCloner(id ulid.ULID, postings index.Postings) PostingsCloner
}

type PostingsCloner interface {
	Clone(context.Context) index.Postings
	NumPostings() int
}

// DefaultPostingsClonerFactory is the default implementation of PostingsClonerFactory.
type DefaultPostingsClonerFactory struct{}

func (DefaultPostingsClonerFactory) PostingsCloner(_ ulid.ULID, postings index.Postings) PostingsCloner {
	return index.NewPostingsCloner(postings)
}

type PostingsForMatchersCacheConfig struct {
	// Shared indicates whether the factory will return a cache that is shared across all invocations.
	Shared bool
	// KeyFunc allows `shared` caches to provide a key that better distinguishes entries.
	KeyFunc CacheKeyFunc
	// Invalidation can be enabled for `shared` caches to invalidate head block cache entries when postings change for a metric.
	Invalidation bool
	// CacheVersions determines the size of the metric versions cache when `invalidation` is enabled.
	CacheVersions int
	// TTL indicates the time-to-live for cache entries, else when 0, only in-flight requests are de-duplicated.
	TTL      time.Duration
	MaxItems int
	MaxBytes int64
	// Force indicates that all requests should go through cache, regardless of the `concurrent` param provided to the PostingsForMatchers method.
	Force bool
	// Metrics the metrics that should be used by the produced caches.
	Metrics *PostingsForMatchersCacheMetrics
	// PostingsClonerFactory is used to create PostingsCloner instances for cloning postings when returning cached results.
	PostingsClonerFactory PostingsClonerFactory
}

// NewPostingsForMatchersCacheFactory creates a factory for building PostingsForMatchersCache instances. A few
// configurations are possible for the resulting cache:
//   - Not shared - A cache is created for each call, and entries are not invalidated when postings change.
//   - Shared without invalidation - A single cache is shared for all factory callers, and entries are not invalidated when postings change.
//   - Shared with invalidation - A cache is shared for all factory callers, and entries are invalidated when postings change.
func NewPostingsForMatchersCacheFactory(config PostingsForMatchersCacheConfig) PostingsForMatchersCacheFactory {
	factoryFunc := func(tracingKV []attribute.KeyValue) *PostingsForMatchersCache {
		var mv *metricVersions
		if config.Shared && config.Invalidation {
			mv = &metricVersions{
				versions: make([]atomic.Int64, config.CacheVersions),
			}
		}
		return &PostingsForMatchersCache{
			calls:            &sync.Map{},
			cached:           list.New(),
			expireInProgress: atomic.NewBool(false),

			shared:                config.Shared,
			keyFunc:               config.KeyFunc,
			ttl:                   config.TTL,
			maxItems:              config.MaxItems,
			maxBytes:              config.MaxBytes,
			force:                 config.Force,
			postingsClonerFactory: config.PostingsClonerFactory,
			metricVersions:        mv,
			metrics:               config.Metrics,

			timeNow: func() time.Time {
				// Ensure it is UTC, so that it's faster to compute the cache entry size.
				return time.Now().UTC()
			},
			postingsForMatchers: PostingsForMatchers,
			// Clone the slice so we don't end up sharding it with the parent.
			additionalAttributes: append(slices.Clone(tracingKV), attribute.Bool("shared", config.Shared), attribute.Stringer("ttl", config.TTL), attribute.Bool("force", config.Force)),
		}
	}
	if config.Shared {
		return &sharedPostingsForMatchersCacheFactory{instance: factoryFunc([]attribute.KeyValue{})}
	}
	return &nonSharedPostingsForMatchersCacheFactory{factoryFunc: factoryFunc}
}

// IndexPostingsReader is a subset of IndexReader methods, the minimum required to evaluate PostingsForMatchers.
type IndexPostingsReader interface {
	// LabelValues returns possible label values which may not be sorted.
	LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error)

	// Postings returns the postings list iterator for the label pairs.
	// The Postings here contain the offsets to the series inside the index.
	// Found IDs are not strictly required to point to a valid Series, e.g.
	// during background garbage collections. Input values must be sorted.
	Postings(ctx context.Context, name string, values ...string) (index.Postings, error)

	// PostingsForLabelMatching returns a sorted iterator over postings having a label with the given name and a value for which match returns true.
	// If no postings are found having at least one matching label, an empty iterator is returned.
	PostingsForLabelMatching(ctx context.Context, name string, match func(value string) bool) index.Postings

	// PostingsForAllLabelValues returns a sorted iterator over all postings having a label with the given name.
	// If no postings are found with the label in question, an empty iterator is returned.
	PostingsForAllLabelValues(ctx context.Context, name string) index.Postings
}

// PostingsForMatchersCache caches PostingsForMatchers call results when the concurrent hint is passed in or force is true.
//
// Invalidation of cache entries is supported for head blocks. Cache entries are partially keyed by a version per metric
// name, and the version is incremented via InvalidateMetric, effectively invalidating all previous entries for the metric
// name. Invalidation is lazy, where expired series become orphaned until they're later evicted.
type PostingsForMatchersCache struct {
	// Config
	shared                bool
	keyFunc               CacheKeyFunc
	ttl                   time.Duration
	maxItems              int
	maxBytes              int64
	force                 bool
	postingsClonerFactory PostingsClonerFactory

	// Mutable state
	calls                *sync.Map // Tracks calls that are in progress so they can be coalesced
	cachedMtx            sync.RWMutex
	cached               *list.List      // Guarded by cachedMtx. Tracks postingsForMatcherPromise entries.
	cachedBytes          int64           // Guarded by cachedMtx
	metricVersions       *metricVersions // Tracks versions for metric names, enabling per-metric invalidation
	metrics              *PostingsForMatchersCacheMetrics
	additionalAttributes []attribute.KeyValue // Preallocated for performance

	// Signal whether there's already a call to expire() in progress, in order to avoid multiple goroutines
	// cleaning up expired entries at the same time (1 at a time is enough).
	expireInProgress *atomic.Bool

	// Test config that can be replaced for testing purposes
	timeNow                          func() time.Time
	postingsForMatchers              func(ctx context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error)
	onPromiseExecutionDoneBeforeHook func() // allows to hook at the beginning of onPromiseExecutionDone() execution.
	evictHeadBeforeHook              func() // allows to hook before calls to evictHead().
}

func (c *PostingsForMatchersCache) PostingsForMatchers(ctx context.Context, ix IndexPostingsReader, concurrent bool, blockID ulid.ULID, ms ...*labels.Matcher) (index.Postings, error) {
	c.metrics.requests.Inc()

	span := trace.SpanFromContext(ctx)
	defer func(startTime time.Time) {
		span.AddEvent(
			"PostingsForMatchers returned",
			trace.WithAttributes(c.additionalAttributes...),
			trace.WithAttributes(attribute.Bool("concurrent", concurrent), attribute.Stringer("duration", time.Since(startTime))),
		)
	}(time.Now())

	metricVersion, cacheKey, sharedKeyOk := c.sharedKeyForMatchers(ctx, blockID, ms...)

	// Skip caching of non-concurrent non-forced calls, or where a shared cache key is not ok
	skipCache := !concurrent && !c.force || !sharedKeyOk

	if skipCache {
		c.metrics.skipsBecauseIneligible.Inc()
		span.AddEvent("cache not used", trace.WithAttributes(c.additionalAttributes...))

		p, err := c.postingsForMatchers(ctx, ix, ms...)
		if err != nil {
			span.SetStatus(codes.Error, "getting postings for matchers without cache failed")
			span.RecordError(err)
		}
		return p, err
	}

	span.AddEvent("using cache", trace.WithAttributes(c.additionalAttributes...))
	c.expire()
	p, err := c.postingsForMatchersPromise(ctx, metricVersion, cacheKey, blockID, ix, ms)(ctx)
	if err != nil {
		span.SetStatus(codes.Error, "getting postings for matchers with cache failed")
		span.RecordError(err)
	}
	return p, err
}

// sharedKeyForMatchers returns a metric version and cache key for the blockID and matchers. For non-shared caches,
// returns ok. For shared caches, returns ok when a cache key is computed and a metric name is retrievable from a head
// block.
func (c *PostingsForMatchersCache) sharedKeyForMatchers(ctx context.Context, blockID ulid.ULID, ms ...*labels.Matcher) (metricVersion int, cacheKey string, ok bool) {
	ok = true
	if c.shared {
		var cacheKeyError error
		if cacheKey, cacheKeyError = c.keyFunc(ctx); cacheKeyError != nil {
			// Skip caching when using a shared cache where the keyFunc returns an error, since cache keys are important in shared caches
			ok = false
		} else if c.metricVersions != nil && blockID == headULID {
			// Get metric version for invalidation
			// This is only necessary for head blocks since postings can change for them. For compacted blocks, postings are considered immutable.
			if metricName, mok := metricNameFromMatchers(ms); !mok {
				// Skip caching when using invalidation with head blocks where a metric name matcher is missing, since this is needed for invalidation
				ok = false
			} else {
				metricVersion = c.metricVersions.getVersion(cacheKey, metricName)
			}
		}
	}
	return metricVersion, cacheKey, ok
}

// InvalidateMetric invalidates cache entries for a key and metric name contained in the labels by incrementing its
// version. Entries are not immediately removed from the cache since we don't track metric to postings mappings.
func (c *PostingsForMatchersCache) InvalidateMetric(key, metricName string) {
	if c.metricVersions == nil {
		return
	}
	c.metrics.invalidations.Inc()

	// Increment metric version so it can't be used for future cache lookups
	c.metricVersions.incrementVersion(key, metricName)
}

// metricNameFromMatchers extracts the metric name from matchers if there's an equal matcher for __name__.
func metricNameFromMatchers(ms []*labels.Matcher) (string, bool) {
	for _, m := range ms {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual {
			return m.Value, true
		}
	}
	return "", false
}

// metricVersions stores versions for metric names.
// This type is concurrency safe.
type metricVersions struct {
	versions []atomic.Int64
}

// getVersion gets the version for the key and metricName.
func (mv *metricVersions) getVersion(key, metricName string) int {
	return int(mv.versions[mv.getIndex(key, metricName)].Load())
}

// incrementVersion increments the version for the key and metricName.
func (mv *metricVersions) incrementVersion(key, metricName string) {
	mv.versions[mv.getIndex(key, metricName)].Inc()
}

// getIndex returns the index of the versions entry for the key and metricName.
func (mv *metricVersions) getIndex(key, metricName string) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	h.Write([]byte(metricName))
	return int(h.Sum64() % uint64(len(mv.versions)))
}

type postingsForMatcherPromise struct {
	// Keep track of all callers contexts in order to cancel the execution context if all
	// callers contexts get canceled.
	callersCtxTracker *promContext.ContextsTracker

	// The result of the promise is stored either in cloner or err (only of the two is valued).
	// Do not access these fields until the done channel is closed.
	done   chan struct{}
	cloner PostingsCloner
	err    error

	// Keep track of the time this promise completed evaluation.
	// Do not access this field until the done channel is closed.
	evaluationCompletedAt time.Time
}

func (p *postingsForMatcherPromise) result(ctx context.Context) (index.Postings, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("interrupting wait on postingsForMatchers promise due to context error: %w", ctx.Err())
	case <-p.done:
		// Checking context error is necessary for deterministic tests,
		// as channel selection order is random
		if ctx.Err() != nil {
			return nil, fmt.Errorf("completed postingsForMatchers promise, but context has error: %w", ctx.Err())
		}
		if p.err != nil {
			return nil, fmt.Errorf("postingsForMatchers promise completed with error: %w", p.err)
		}

		trace.SpanFromContext(ctx).AddEvent("completed postingsForMatchers promise", trace.WithAttributes(
			// Do not format the timestamp to introduce a performance regression.
			attribute.Int64("evaluation completed at (epoch seconds)", p.evaluationCompletedAt.Unix()),
			// We don't include the block ID because propagating it from the caller would increase the size of the promise.
			// With a bigger promise we can fit fewer promises in the cache, so the cache will be less effective.
			attribute.String("block", "unknown"),
		))

		return p.cloner.Clone(ctx), nil
	}
}

func (c *PostingsForMatchersCache) postingsForMatchersPromise(ctx context.Context, metricVersion int, cacheKey string, blockID ulid.ULID, ix IndexPostingsReader, ms []*labels.Matcher) func(context.Context) (index.Postings, error) {
	span := trace.SpanFromContext(ctx)

	promiseCallersCtxTracker, promiseExecCtx := promContext.NewContextsTracker()
	promise := &postingsForMatcherPromise{
		done:              make(chan struct{}),
		callersCtxTracker: promiseCallersCtxTracker,
	}

	// Add the caller context to the ones tracked by the new promise.
	//
	// It's important to do it here so that if the promise will be stored, then the caller's context is already
	// tracked. Otherwise, if the promise will not be stored (because there's another in-flight promise for the
	// same label matchers) then it's not a problem, and resources will be released.
	//
	// Skipping the error checking because it can't happen here.
	_ = promise.callersCtxTracker.Add(ctx)

	var key string
	if !c.shared {
		key = cacheKeyForMatchers(ms)
	} else {
		key = sharedCacheKey(metricVersion, cacheKey, blockID, ms)
	}

	if oldPromiseValue, loaded := c.calls.LoadOrStore(key, promise); loaded {
		// The new promise hasn't been stored because there's already an in-flight promise
		// for the same label matchers. We should just wait for it.

		// Release the resources created by the new promise, that will not be used.
		close(promise.done)
		promise.callersCtxTracker.Close()

		oldPromise := oldPromiseValue.(*postingsForMatcherPromise)

		// Check if the promise already completed the execution and its TTL has not expired yet.
		// If the TTL has expired, we don't want to return it, so we just recompute the postings
		// on-the-fly, bypassing the cache logic. It's less performant, but more accurate, because
		// avoids returning stale data.
		if c.ttl > 0 {
			select {
			case <-oldPromise.done:
				if c.timeNow().Sub(oldPromise.evaluationCompletedAt) >= c.ttl {
					// The cached promise already expired, but it has not been evicted.
					span.AddEvent("skipping cached postingsForMatchers promise because its TTL already expired",
						trace.WithAttributes(
							attribute.Stringer("cached promise evaluation completed at", oldPromise.evaluationCompletedAt),
							attribute.String("cache_key", key),
						),
						trace.WithAttributes(c.additionalAttributes...),
					)
					c.metrics.skipsBecauseStale.Inc()

					return func(ctx context.Context) (index.Postings, error) {
						return c.postingsForMatchers(ctx, ix, ms...)
					}
				}

			default:
				// The evaluation is still in-flight. We wait for it.
			}
		}

		// Add the caller context to the ones tracked by the old promise (currently in-flight).
		if err := oldPromise.callersCtxTracker.Add(ctx); err != nil && errors.Is(err, promContext.ErrContextsTrackerCanceled{}) {
			// We've hit a race condition happening when the "loaded" promise execution was just canceled,
			// but it hasn't been removed from map of calls yet, so the old promise was loaded anyway.
			//
			// We expect this race condition to be infrequent. In this case we simply skip the cache and
			// pass through the execution to the underlying postingsForMatchers().
			c.metrics.skipsBecauseCanceled.Inc()
			span.AddEvent(
				"looked up in-flight postingsForMatchers promise, but the promise was just canceled due to a race condition: skipping the cache",
				trace.WithAttributes(attribute.String("cache_key", key)),
				trace.WithAttributes(c.additionalAttributes...),
			)

			return func(ctx context.Context) (index.Postings, error) {
				return c.postingsForMatchers(ctx, ix, ms...)
			}
		}

		c.metrics.hits.Inc()
		span.AddEvent("waiting cached postingsForMatchers promise",
			trace.WithAttributes(attribute.String("cache_key", key)),
			trace.WithAttributes(c.additionalAttributes...),
		)

		return oldPromise.result
	}

	c.metrics.misses.Inc()
	span.AddEvent("no postingsForMatchers promise in cache, executing query", trace.WithAttributes(attribute.String("cache_key", key)), trace.WithAttributes(c.additionalAttributes...))

	// promise was stored, close its channel after fulfilment
	defer close(promise.done)

	// The execution context will be canceled only once all callers contexts will be canceled. This way we:
	// 1. Do not cancel postingsForMatchers() the input ctx is cancelled, but another goroutine is waiting
	//    for the promise result.
	// 2. Cancel postingsForMatchers() once all callers contexts have been canceled, so that we don't waist
	//    resources computing postingsForMatchers() is all requests have been canceled (this is particularly
	//    important if the postingsForMatchers() is very slow due to expensive regexp matchers).
	if postings, err := c.postingsForMatchers(promiseExecCtx, ix, ms...); err != nil {
		promise.err = err
	} else {
		promise.cloner = c.postingsClonerFactory.PostingsCloner(blockID, postings)
	}

	// Keep track of when the evaluation completed.
	promise.evaluationCompletedAt = c.timeNow()

	// The execution terminated (or has been canceled). We have to close the tracker to release resources.
	// It's important to close it before computing the promise size, so that the actual size is smaller.
	promise.callersCtxTracker.Close()

	sizeBytes := int64(len(key) + size.Of(promise))
	numPostings := 0
	if promise.cloner != nil {
		numPostings = promise.cloner.NumPostings()
	}

	c.onPromiseExecutionDone(ctx, key, promise.evaluationCompletedAt, sizeBytes, numPostings, promise.err)
	return promise.result
}

type postingsForMatchersCachedCall struct {
	key string
	ts  time.Time

	// Size of the cached entry, in bytes.
	sizeBytes int64
}

func (c *PostingsForMatchersCache) expire() {
	if c.ttl <= 0 {
		return
	}

	// Ensure there's no other cleanup in progress. It's not a technical issue if there are two ongoing cleanups,
	// but it's a waste of resources and it adds extra pressure to the mutex. One cleanup at a time is enough.
	if !c.expireInProgress.CompareAndSwap(false, true) {
		return
	}

	defer c.expireInProgress.Store(false)

	c.cachedMtx.RLock()
	if !c.shouldEvictHead(c.timeNow()) {
		c.cachedMtx.RUnlock()
		return
	}
	c.cachedMtx.RUnlock()

	// Call the registered hook, if any. It's used only for testing purposes.
	if c.evictHeadBeforeHook != nil {
		c.evictHeadBeforeHook()
	}

	var evictionReasons [evictionReasonsLength]int

	// Evict the head taking an exclusive lock.
	c.cachedMtx.Lock()
	now := c.timeNow()
	for c.shouldEvictHead(now) {
		reason := c.evictHead(now)
		evictionReasons[reason]++
	}
	c.cachedMtx.Unlock()

	// Keep track of the reason why items where evicted.
	c.metrics.evictionsBecauseTTL.Add(float64(evictionReasons[evictionReasonTTL]))
	c.metrics.evictionsBecauseMaxBytes.Add(float64(evictionReasons[evictionReasonMaxBytes]))
	c.metrics.evictionsBecauseMaxItems.Add(float64(evictionReasons[evictionReasonMaxItems]))
	c.metrics.evictionsBecauseUnknown.Add(float64(evictionReasons[evictionReasonUnknown]))
}

// shouldEvictHead returns true if cache head should be evicted, either because it's too old,
// or because the cache has too many elements
// Must be called with a read lock held on cachedMtx.
func (c *PostingsForMatchersCache) shouldEvictHead(now time.Time) bool {
	// The cache should be evicted for sure if the max size (either items or bytes) is reached.
	if c.cached.Len() > c.maxItems || c.cachedBytes > c.maxBytes {
		return true
	}

	h := c.cached.Front()
	if h == nil {
		return false
	}
	ts := h.Value.(*postingsForMatchersCachedCall).ts
	return now.Sub(ts) >= c.ttl
}

// Must be called with a write lock held on cachedMtx.
func (c *PostingsForMatchersCache) evictHead(now time.Time) (reason int) {
	front := c.cached.Front()
	oldest := front.Value.(*postingsForMatchersCachedCall)

	// Detect the reason why we're evicting it.
	//
	// The checks order is:
	// 1. TTL: if an item is expired, it should be tracked as such even if the cache was full.
	// 2. Max bytes: "max items" is deprecated, and we expect to set it to a high value because
	//    we want to primarily limit by bytes size.
	// 3. Max items: the last one.
	switch {
	case now.Sub(oldest.ts) >= c.ttl:
		reason = evictionReasonTTL
	case c.cachedBytes > c.maxBytes:
		reason = evictionReasonMaxBytes
	case c.cached.Len() > c.maxItems:
		reason = evictionReasonMaxItems
	default:
		// This should never happen, but we track it to detect unexpected behaviours.
		reason = evictionReasonUnknown
	}

	c.calls.Delete(oldest.key)
	c.cached.Remove(front)
	c.cachedBytes -= oldest.sizeBytes
	c.metrics.entries.Dec()
	c.metrics.bytes.Sub(float64(oldest.sizeBytes))

	return reason
}

// onPromiseExecutionDone must be called once the execution of PostingsForMatchers promise has done.
// The input err contains details about any error that could have occurred when executing it.
// The input ts is the function call time.
func (c *PostingsForMatchersCache) onPromiseExecutionDone(ctx context.Context, key string, completedAt time.Time, sizeBytes int64, numPostings int, err error) {
	span := trace.SpanFromContext(ctx)

	// Call the registered hook, if any. It's used only for testing purposes.
	if c.onPromiseExecutionDoneBeforeHook != nil {
		c.onPromiseExecutionDoneBeforeHook()
	}

	// Do not cache if cache is disabled.
	if c.ttl <= 0 {
		span.AddEvent("not caching promise result because configured TTL is <= 0", trace.WithAttributes(c.additionalAttributes...))
		c.calls.Delete(key)
		return
	}

	// Do not cache if the promise execution was canceled (it gets cancelled once all the callers contexts have
	// been canceled).
	if errors.Is(err, context.Canceled) {
		span.AddEvent("not caching promise result because execution has been canceled", trace.WithAttributes(c.additionalAttributes...))
		c.calls.Delete(key)
		return
	}

	// Cache the promise.
	var lastCachedBytes int64
	{
		c.cachedMtx.Lock()

		c.cached.PushBack(&postingsForMatchersCachedCall{
			key:       key,
			ts:        completedAt,
			sizeBytes: sizeBytes,
		})
		c.cachedBytes += sizeBytes
		c.metrics.entries.Inc()
		c.metrics.bytes.Add(float64(sizeBytes))
		lastCachedBytes = c.cachedBytes

		c.cachedMtx.Unlock()
	}

	span.AddEvent("added cached value to expiry queue",
		trace.WithAttributes(
			attribute.Stringer("evaluation completed at", completedAt),
			attribute.Int64("size in bytes", sizeBytes),
			attribute.Int("num postings", numPostings),
			attribute.Int64("cached bytes", lastCachedBytes),
		),
		trace.WithAttributes(c.additionalAttributes...),
	)
}

// cacheKeyForMatchers provides a unique string key for the given matchers slice.
// NOTE: different orders of matchers will produce different keys,
// but it's unlikely that we'll receive same matchers in different orders at the same time.
func cacheKeyForMatchers(ms []*labels.Matcher) string {
	const (
		typeLen = 2
		sepLen  = 1
	)
	var size int
	for _, m := range ms {
		size += len(m.Name) + typeLen + len(m.Value) + sepLen
	}
	sb := strings.Builder{}
	sb.Grow(size)
	for _, m := range ms {
		sb.WriteString(m.Name)
		sb.WriteString(m.Type.String())
		sb.WriteString(m.Value)
		sb.WriteByte(0)
	}
	key := sb.String()
	return key
}

// sharedCacheKey provides a key for an entry in shared cache, where entries are distinguished by all of the provided
// fields. The version param may be 0 when invalidation is not enabled.
func sharedCacheKey(version int, cacheKey string, blockID ulid.ULID, ms []*labels.Matcher) string {
	// Sort matchers for consistent cache keys since shared cache entries may be long-lived
	slices.SortFunc(ms, func(i, j *labels.Matcher) int {
		if i.Type != j.Type {
			return int(i.Type - j.Type)
		}
		if i.Name != j.Name {
			return strings.Compare(i.Name, j.Name)
		}
		if i.Value != j.Value {
			return strings.Compare(i.Value, j.Value)
		}
		return 0
	})

	const (
		typeLen = 2
		sepLen  = 1
	)
	versionStr := strconv.Itoa(version)
	size := len(versionStr) + len(cacheKey) + len(blockID.String()) + 2*sepLen
	for _, m := range ms {
		size += sepLen + len(m.Name) + typeLen + len(m.Value)
	}
	sb := strings.Builder{}
	sb.Grow(size)
	sb.WriteString(versionStr)
	sb.WriteByte(0)
	sb.WriteString(cacheKey)
	sb.WriteByte(0)
	sb.WriteString(blockID.String())
	for _, m := range ms {
		sb.WriteByte(0)
		sb.WriteString(m.Name)
		sb.WriteString(m.Type.String())
		sb.WriteString(m.Value)
	}
	return sb.String()
}

// indexReaderWithPostingsForMatchers adapts an index.Reader to be an IndexReader by adding the PostingsForMatchers method.
type indexReaderWithPostingsForMatchers struct {
	blockID ulid.ULID
	*index.Reader
	pfmc *PostingsForMatchersCache
}

func (ir indexReaderWithPostingsForMatchers) PostingsForMatchers(ctx context.Context, concurrent bool, ms ...*labels.Matcher) (index.Postings, error) {
	return ir.pfmc.PostingsForMatchers(ctx, ir, concurrent, ir.blockID, ms...)
}

var _ IndexReader = indexReaderWithPostingsForMatchers{}

type PostingsForMatchersCacheMetrics struct {
	entries  prometheus.Gauge
	bytes    prometheus.Gauge
	requests prometheus.Counter
	hits     prometheus.Counter
	misses   prometheus.Counter

	skipsBecauseIneligible prometheus.Counter
	skipsBecauseStale      prometheus.Counter
	skipsBecauseCanceled   prometheus.Counter

	invalidations prometheus.Counter

	evictionsBecauseTTL      prometheus.Counter
	evictionsBecauseMaxBytes prometheus.Counter
	evictionsBecauseMaxItems prometheus.Counter
	evictionsBecauseUnknown  prometheus.Counter
}

func NewPostingsForMatchersCacheMetrics(reg prometheus.Registerer) *PostingsForMatchersCacheMetrics {
	const (
		skipsMetric = "postings_for_matchers_cache_skips_total"
		skipsHelp   = "Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached."

		evictionsMetric = "postings_for_matchers_cache_evictions_total"
		evictionsHelp   = "Total number of evictions from the PostingsForMatchers cache."
	)

	return &PostingsForMatchersCacheMetrics{
		entries: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "postings_for_matchers_cache_entries_total",
			Help: "Total number of entries in the PostingsForMatchers cache.",
		}),
		bytes: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "postings_for_matchers_cache_bytes_total",
			Help: "Total number of bytes in the PostingsForMatchers cache.",
		}),
		requests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "postings_for_matchers_cache_requests_total",
			Help: "Total number of requests to the PostingsForMatchers cache.",
		}),
		hits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "postings_for_matchers_cache_hits_total",
			Help: "Total number of postings lists returned from the PostingsForMatchers cache.",
		}),
		misses: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "postings_for_matchers_cache_misses_total",
			Help: "Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.",
		}),
		skipsBecauseIneligible: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        skipsMetric,
			Help:        skipsHelp,
			ConstLabels: map[string]string{"reason": "ineligible"},
		}),
		skipsBecauseStale: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        skipsMetric,
			Help:        skipsHelp,
			ConstLabels: map[string]string{"reason": "stale-cached-entry"},
		}),
		skipsBecauseCanceled: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        skipsMetric,
			Help:        skipsHelp,
			ConstLabels: map[string]string{"reason": "canceled-cached-entry"},
		}),
		invalidations: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "postings_for_matchers_cache_invalidations_total",
			Help: "Total number of cache entries that were invalidated.",
		}),
		evictionsBecauseTTL: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        evictionsMetric,
			Help:        evictionsHelp,
			ConstLabels: map[string]string{"reason": "ttl-expired"},
		}),
		evictionsBecauseMaxBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        evictionsMetric,
			Help:        evictionsHelp,
			ConstLabels: map[string]string{"reason": "max-bytes-reached"},
		}),
		evictionsBecauseMaxItems: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        evictionsMetric,
			Help:        evictionsHelp,
			ConstLabels: map[string]string{"reason": "max-items-reached"},
		}),
		evictionsBecauseUnknown: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        evictionsMetric,
			Help:        evictionsHelp,
			ConstLabels: map[string]string{"reason": "unknown"},
		}),
	}
}
