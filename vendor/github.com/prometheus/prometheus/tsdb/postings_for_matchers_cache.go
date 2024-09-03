package tsdb

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/DmitriyVTitov/size"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

const (
	// NOTE: keep them exported to reference them in Mimir.

	DefaultPostingsForMatchersCacheTTL      = 10 * time.Second
	DefaultPostingsForMatchersCacheMaxItems = 100
	DefaultPostingsForMatchersCacheMaxBytes = 10 * 1024 * 1024 // DefaultPostingsForMatchersCacheMaxBytes is based on the default max items, 10MB / 100 = 100KB per cached entry on average.
	DefaultPostingsForMatchersCacheForce    = false
)

// IndexPostingsReader is a subset of IndexReader methods, the minimum required to evaluate PostingsForMatchers.
type IndexPostingsReader interface {
	// LabelValues returns possible label values which may not be sorted.
	LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, error)

	// Postings returns the postings list iterator for the label pairs.
	// The Postings here contain the offsets to the series inside the index.
	// Found IDs are not strictly required to point to a valid Series, e.g.
	// during background garbage collections. Input values must be sorted.
	Postings(ctx context.Context, name string, values ...string) (index.Postings, error)

	// PostingsForLabelMatching returns a sorted iterator over postings having a label with the given name and a value for which match returns true.
	// If no postings are found having at least one matching label, an empty iterator is returned.
	PostingsForLabelMatching(ctx context.Context, name string, match func(value string) bool) index.Postings
}

// NewPostingsForMatchersCache creates a new PostingsForMatchersCache.
// If `ttl` is 0, then it only deduplicates in-flight requests.
// If `force` is true, then all requests go through cache, regardless of the `concurrent` param provided to the PostingsForMatchers method.
func NewPostingsForMatchersCache(ttl time.Duration, maxItems int, maxBytes int64, force bool) *PostingsForMatchersCache {
	b := &PostingsForMatchersCache{
		calls:  &sync.Map{},
		cached: list.New(),

		ttl:      ttl,
		maxItems: maxItems,
		maxBytes: maxBytes,
		force:    force,

		timeNow:             time.Now,
		postingsForMatchers: PostingsForMatchers,

		tracer:      otel.Tracer(""),
		ttlAttrib:   attribute.Stringer("ttl", ttl),
		forceAttrib: attribute.Bool("force", force),
	}

	return b
}

// PostingsForMatchersCache caches PostingsForMatchers call results when the concurrent hint is passed in or force is true.
type PostingsForMatchersCache struct {
	calls *sync.Map

	cachedMtx   sync.RWMutex
	cached      *list.List
	cachedBytes int64

	ttl      time.Duration
	maxItems int
	maxBytes int64
	force    bool

	// timeNow is the time.Now that can be replaced for testing purposes
	timeNow func() time.Time

	// postingsForMatchers can be replaced for testing purposes
	postingsForMatchers func(ctx context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error)

	// onPromiseExecutionDoneBeforeHook is used for testing purposes. It allows to hook at the
	// beginning of onPromiseExecutionDone() execution.
	onPromiseExecutionDoneBeforeHook func()

	tracer trace.Tracer
	// Preallocated for performance
	ttlAttrib   attribute.KeyValue
	forceAttrib attribute.KeyValue
}

func (c *PostingsForMatchersCache) PostingsForMatchers(ctx context.Context, ix IndexPostingsReader, concurrent bool, ms ...*labels.Matcher) (index.Postings, error) {
	span := trace.SpanFromContext(ctx)
	defer func(startTime time.Time) {
		span.AddEvent(
			"PostingsForMatchers returned",
			trace.WithAttributes(attribute.Bool("concurrent", concurrent), c.ttlAttrib, c.forceAttrib, attribute.Stringer("duration", time.Since(startTime))),
		)
	}(time.Now())

	if !concurrent && !c.force {
		span.AddEvent("cache not used")
		p, err := c.postingsForMatchers(ctx, ix, ms...)
		if err != nil {
			span.SetStatus(codes.Error, "getting postings for matchers without cache failed")
			span.RecordError(err)
		}
		return p, err
	}

	span.AddEvent("using cache")
	c.expire()
	p, err := c.postingsForMatchersPromise(ctx, ix, ms)(ctx)
	if err != nil {
		span.SetStatus(codes.Error, "getting postings for matchers with cache failed")
		span.RecordError(err)
	}
	return p, err
}

type postingsForMatcherPromise struct {
	// Keep track of all callers contexts in order to cancel the execution context if all
	// callers contexts get canceled.
	callersCtxTracker *contextsTracker

	// The result of the promise is stored either in cloner or err (only of the two is valued).
	// Do not access these fields until the done channel is closed.
	done   chan struct{}
	cloner *index.PostingsCloner
	err    error
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
		return p.cloner.Clone(), nil
	}
}

func (c *PostingsForMatchersCache) postingsForMatchersPromise(ctx context.Context, ix IndexPostingsReader, ms []*labels.Matcher) func(context.Context) (index.Postings, error) {
	span := trace.SpanFromContext(ctx)

	promiseCallersCtxTracker, promiseExecCtx := newContextsTracker()
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
	_ = promise.callersCtxTracker.add(ctx)

	key := matchersKey(ms)

	if oldPromiseValue, loaded := c.calls.LoadOrStore(key, promise); loaded {
		// The new promise hasn't been stored because there's already an in-flight promise
		// for the same label matchers. We should just wait for it.

		// Release the resources created by the new promise, that will not be used.
		close(promise.done)
		promise.callersCtxTracker.close()

		oldPromise := oldPromiseValue.(*postingsForMatcherPromise)

		// Add the caller context to the ones tracked by the old promise (currently in-flight).
		if err := oldPromise.callersCtxTracker.add(ctx); err != nil && errors.Is(err, errContextsTrackerCanceled{}) {
			// We've hit a race condition happening when the "loaded" promise execution was just canceled,
			// but it hasn't been removed from map of calls yet, so the old promise was loaded anyway.
			//
			// We expect this race condition to be infrequent. In this case we simply skip the cache and
			// pass through the execution to the underlying postingsForMatchers().
			span.AddEvent("looked up in-flight postingsForMatchers promise, but the promise was just canceled due to a race condition: skipping the cache", trace.WithAttributes(
				attribute.String("cache_key", key),
			))

			return func(ctx context.Context) (index.Postings, error) {
				return c.postingsForMatchers(ctx, ix, ms...)
			}
		}

		span.AddEvent("using cached postingsForMatchers promise", trace.WithAttributes(
			attribute.String("cache_key", key),
		))

		return oldPromise.result
	}

	span.AddEvent("no postingsForMatchers promise in cache, executing query", trace.WithAttributes(attribute.String("cache_key", key)))

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
		promise.cloner = index.NewPostingsCloner(postings)
	}

	// The execution terminated (or has been canceled). We have to close the tracker to release resources.
	// It's important to close it before computing the promise size, so that the actual size is smaller.
	promise.callersCtxTracker.close()

	sizeBytes := int64(len(key) + size.Of(promise))

	c.onPromiseExecutionDone(ctx, key, c.timeNow(), sizeBytes, promise.err)
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

	c.cachedMtx.RLock()
	if !c.shouldEvictHead() {
		c.cachedMtx.RUnlock()
		return
	}
	c.cachedMtx.RUnlock()

	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()

	for c.shouldEvictHead() {
		c.evictHead()
	}
}

// shouldEvictHead returns true if cache head should be evicted, either because it's too old,
// or because the cache has too many elements
// should be called while read lock is held on cachedMtx.
func (c *PostingsForMatchersCache) shouldEvictHead() bool {
	// The cache should be evicted for sure if the max size (either items or bytes) is reached.
	if c.cached.Len() > c.maxItems || c.cachedBytes > c.maxBytes {
		return true
	}

	h := c.cached.Front()
	if h == nil {
		return false
	}
	ts := h.Value.(*postingsForMatchersCachedCall).ts
	return c.timeNow().Sub(ts) >= c.ttl
}

func (c *PostingsForMatchersCache) evictHead() {
	front := c.cached.Front()
	oldest := front.Value.(*postingsForMatchersCachedCall)
	c.calls.Delete(oldest.key)
	c.cached.Remove(front)
	c.cachedBytes -= oldest.sizeBytes
}

// onPromiseExecutionDone must be called once the execution of PostingsForMatchers promise has done.
// The input err contains details about any error that could have occurred when executing it.
// The input ts is the function call time.
func (c *PostingsForMatchersCache) onPromiseExecutionDone(ctx context.Context, key string, ts time.Time, sizeBytes int64, err error) {
	span := trace.SpanFromContext(ctx)

	// Call the registered hook, if any. It's used only for testing purposes.
	if c.onPromiseExecutionDoneBeforeHook != nil {
		c.onPromiseExecutionDoneBeforeHook()
	}

	// Do not cache if cache is disabled.
	if c.ttl <= 0 {
		span.AddEvent("not caching promise result because configured TTL is <= 0")
		c.calls.Delete(key)
		return
	}

	// Do not cache if the promise execution was canceled (it gets cancelled once all the callers contexts have
	// been canceled).
	if errors.Is(err, context.Canceled) {
		span.AddEvent("not caching promise result because execution has been canceled")
		c.calls.Delete(key)
		return
	}

	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()

	c.cached.PushBack(&postingsForMatchersCachedCall{
		key:       key,
		ts:        ts,
		sizeBytes: sizeBytes,
	})
	c.cachedBytes += sizeBytes
	span.AddEvent("added cached value to expiry queue", trace.WithAttributes(
		attribute.Stringer("timestamp", ts),
		attribute.Int64("size in bytes", sizeBytes),
		attribute.Int64("cached bytes", c.cachedBytes),
	))
}

// matchersKey provides a unique string key for the given matchers slice.
// NOTE: different orders of matchers will produce different keys,
// but it's unlikely that we'll receive same matchers in different orders at the same time.
func matchersKey(ms []*labels.Matcher) string {
	const (
		typeLen = 2
		sepLen  = 1
	)
	var size int
	for _, m := range ms {
		size += len(m.Name) + len(m.Value) + typeLen + sepLen
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

// indexReaderWithPostingsForMatchers adapts an index.Reader to be an IndexReader by adding the PostingsForMatchers method.
type indexReaderWithPostingsForMatchers struct {
	*index.Reader
	pfmc *PostingsForMatchersCache
}

func (ir indexReaderWithPostingsForMatchers) PostingsForMatchers(ctx context.Context, concurrent bool, ms ...*labels.Matcher) (index.Postings, error) {
	return ir.pfmc.PostingsForMatchers(ctx, ir, concurrent, ms...)
}

var _ IndexReader = indexReaderWithPostingsForMatchers{}

// errContextsTrackerClosed is the reason to identify contextsTracker has been explicitly closed by calling close().
//
// This error is a struct instead of a globally generic error so that postingsForMatcherPromise computed size is smaller
// (this error is referenced by contextsTracker, which is referenced by postingsForMatcherPromise).
type errContextsTrackerClosed struct{}

func (e errContextsTrackerClosed) Error() string {
	return "contexts tracker is closed"
}

// errContextsTrackerCanceled is the reason to identify contextsTracker has been automatically closed because
// all tracked contexts have been canceled.
//
// This error is a struct instead of a globally generic error so that postingsForMatcherPromise computed size is smaller
// (this error is referenced by contextsTracker, which is referenced by postingsForMatcherPromise).
type errContextsTrackerCanceled struct{}

func (e errContextsTrackerCanceled) Error() string {
	return "contexts tracker has been canceled"
}

// contextsTracker is responsible to monitor multiple context.Context and provides an execution
// that gets canceled once all monitored context.Context have done.
type contextsTracker struct {
	cancelExecCtx context.CancelFunc

	mx               sync.Mutex
	closedWithReason error         // Track whether the tracker is closed and why. The tracker is not closed if this is nil.
	trackedCount     int           // Number of tracked contexts.
	trackedStopFuncs []func() bool // The stop watching functions for all tracked contexts.
}

func newContextsTracker() (*contextsTracker, context.Context) {
	t := &contextsTracker{}

	// Create a new execution context that will be canceled only once all tracked contexts have done.
	var execCtx context.Context
	execCtx, t.cancelExecCtx = context.WithCancel(context.Background())

	return t, execCtx
}

// add the input ctx to the group of monitored context.Context.
// Returns false if the input context couldn't be added to the tracker because the tracker is already closed.
func (t *contextsTracker) add(ctx context.Context) error {
	t.mx.Lock()
	defer t.mx.Unlock()

	// Check if we've already done.
	if t.closedWithReason != nil {
		return t.closedWithReason
	}

	// Register a function that will be called once the tracked context has done.
	t.trackedCount++
	t.trackedStopFuncs = append(t.trackedStopFuncs, context.AfterFunc(ctx, t.onTrackedContextDone))

	return nil
}

// close the tracker. When the tracker is closed, the execution context is canceled
// and resources releases.
//
// This function must be called once done to not leak resources.
func (t *contextsTracker) close() {
	t.mx.Lock()
	defer t.mx.Unlock()

	t.unsafeClose(errContextsTrackerClosed{})
}

// unsafeClose must be called with the t.mx lock hold.
func (t *contextsTracker) unsafeClose(reason error) {
	if t.closedWithReason != nil {
		return
	}

	t.cancelExecCtx()

	// Stop watching the tracked contexts. It's safe to call the stop function on a context
	// for which was already done.
	for _, fn := range t.trackedStopFuncs {
		fn()
	}

	t.trackedCount = 0
	t.trackedStopFuncs = nil
	t.closedWithReason = reason
}

func (t *contextsTracker) onTrackedContextDone() {
	t.mx.Lock()
	defer t.mx.Unlock()

	t.trackedCount--

	// If this was the last context to be tracked, we can close the tracker and cancel the execution context.
	if t.trackedCount == 0 {
		t.unsafeClose(errContextsTrackerCanceled{})
	}
}

func (t *contextsTracker) trackedContextsCount() int {
	t.mx.Lock()
	defer t.mx.Unlock()

	return t.trackedCount
}
