// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/caching_bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package bucketcache

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/util/pool"
)

type contextKey int

const (
	originCache  = "cache"
	originBucket = "bucket"

	memoryPoolContextKey         contextKey = 0
	cacheLookupEnabledContextKey contextKey = 1
)

var errObjNotFound = errors.Errorf("object not found")

// WithMemoryPool returns a new context with a slab pool to be used as a cache.Allocator
// implementation by the underlying cache client. Slabs are released back to p when the
// io.ReadCloser associated with the Get or GetRange call is closed.
func WithMemoryPool(ctx context.Context, p pool.Interface, slabSize int) context.Context {
	return context.WithValue(ctx, memoryPoolContextKey, pool.NewSafeSlabPool[byte](p, slabSize))
}

// WithCacheLookupEnabled returns a new context which will explicitly enable/disable the cache lookup but keep
// storing the result to the cache. The cache lookup is enabled by default.
func WithCacheLookupEnabled(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, cacheLookupEnabledContextKey, enabled)
}

func getMemoryPool(ctx context.Context) *pool.SafeSlabPool[byte] {
	val := ctx.Value(memoryPoolContextKey)
	if val == nil {
		return nil
	}

	slabs, ok := val.(*pool.SafeSlabPool[byte])
	if !ok {
		return nil
	}

	return slabs
}

func isCacheLookupEnabled(ctx context.Context) bool {
	val := ctx.Value(cacheLookupEnabledContextKey)
	if val == nil {
		return true
	}

	return val.(bool)
}

func getCacheOptions(slabs *pool.SafeSlabPool[byte]) []cache.Option {
	var opts []cache.Option

	if slabs != nil {
		opts = append(opts, cache.WithAllocator(pool.NewSafeSlabPoolAllocator(slabs)))
	}

	return opts
}

// CachingBucket implementation that provides some caching features, based on passed configuration.
type CachingBucket struct {
	objstore.Bucket

	bucketID string
	cfg      *CachingBucketConfig
	logger   log.Logger

	requestedGetRangeBytes *prometheus.CounterVec
	fetchedGetRangeBytes   *prometheus.CounterVec
	refetchedGetRangeBytes *prometheus.CounterVec

	operationConfigs  map[string][]*operationConfig
	operationRequests *prometheus.CounterVec
	operationHits     *prometheus.CounterVec
}

// NewCachingBucket creates new caching bucket with provided configuration. Configuration should not be
// changed after creating caching bucket.
func NewCachingBucket(bucketID string, bucketClient objstore.Bucket, cfg *CachingBucketConfig, logger log.Logger, reg prometheus.Registerer) (*CachingBucket, error) {
	if bucketClient == nil {
		return nil, errors.New("bucket is nil")
	}

	cb := &CachingBucket{
		Bucket:   bucketClient,
		bucketID: bucketID,
		cfg:      cfg,
		logger:   logger,

		operationConfigs: map[string][]*operationConfig{},

		requestedGetRangeBytes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_getrange_requested_bytes_total",
			Help: "Total number of bytes requested via GetRange.",
		}, []string{"config"}),
		fetchedGetRangeBytes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_getrange_fetched_bytes_total",
			Help: "Total number of bytes fetched because of GetRange operation. Data from bucket is then stored to cache.",
		}, []string{"origin", "config"}),
		refetchedGetRangeBytes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_getrange_refetched_bytes_total",
			Help: "Total number of bytes re-fetched from storage because of GetRange operation, despite being in cache already.",
		}, []string{"origin", "config"}),

		operationRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_operation_requests_total",
			Help: "Number of requested operations matching given config which triggered a cache lookup.",
		}, []string{"operation", "config"}),
		operationHits: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_operation_hits_total",
			Help: "Number of operations served from cache for given config.",
		}, []string{"operation", "config"}),
	}

	for op, names := range cfg.allConfigNames() {
		for _, n := range names {
			cb.operationRequests.WithLabelValues(op, n)
			cb.operationHits.WithLabelValues(op, n)

			if op == objstore.OpGetRange {
				cb.requestedGetRangeBytes.WithLabelValues(n)
				cb.fetchedGetRangeBytes.WithLabelValues(originCache, n)
				cb.fetchedGetRangeBytes.WithLabelValues(originBucket, n)
				cb.refetchedGetRangeBytes.WithLabelValues(originCache, n)
			}
		}
	}

	return cb, nil
}

func (cb *CachingBucket) Name() string {
	return "caching: " + cb.Bucket.Name()
}

func (cb *CachingBucket) WithExpectedErrs(expectedFunc objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	if ib, ok := cb.Bucket.(objstore.InstrumentedBucket); ok {
		// Make a copy, but replace bucket with instrumented one.
		res := &CachingBucket{}
		*res = *cb
		res.Bucket = ib.WithExpectedErrs(expectedFunc)
		return res
	}

	return cb
}

func (cb *CachingBucket) ReaderWithExpectedErrs(expectedFunc objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return cb.WithExpectedErrs(expectedFunc)
}

func (cb *CachingBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	cfgName, cfg := cb.cfg.findIterConfig(dir)
	if cfg == nil {
		return cb.Bucket.Iter(ctx, dir, f, options...)
	}

	key := cachingKeyIter(cb.bucketID, dir, options...)

	// Lookup the cache.
	if isCacheLookupEnabled(ctx) {
		cb.operationRequests.WithLabelValues(objstore.OpIter, cfgName).Inc()

		data := cfg.cache.Fetch(ctx, []string{key})
		if data[key] != nil {
			list, err := cfg.codec.Decode(data[key])
			if err == nil {
				cb.operationHits.WithLabelValues(objstore.OpIter, cfgName).Inc()
				for _, n := range list {
					if err := f(n); err != nil {
						return err
					}
				}
				return nil
			}
			level.Warn(cb.logger).Log("msg", "failed to decode cached Iter result", "key", key, "err", err)
		}
	}

	// Iteration can take a while (esp. since it calls function), and iterTTL is generally low.
	// We will compute TTL based on time when iteration started.
	iterTime := time.Now()
	var list []string
	err := cb.Bucket.Iter(ctx, dir, func(s string) error {
		list = append(list, s)
		return f(s)
	}, options...)

	remainingTTL := cfg.ttl - time.Since(iterTime)
	if err == nil && remainingTTL > 0 {
		data, encErr := cfg.codec.Encode(list)
		if encErr == nil {
			cfg.cache.StoreAsync(map[string][]byte{key: data}, remainingTTL)
			return nil
		}
		level.Warn(cb.logger).Log("msg", "failed to encode Iter result", "key", key, "err", encErr)
	}
	return err
}

func (cb *CachingBucket) Exists(ctx context.Context, name string) (bool, error) {
	cfgName, cfg := cb.cfg.findExistConfig(name)
	if cfg == nil {
		return cb.Bucket.Exists(ctx, name)
	}

	key := cachingKeyExists(cb.bucketID, name)

	// Lookup the cache.
	if isCacheLookupEnabled(ctx) {
		cb.operationRequests.WithLabelValues(objstore.OpExists, cfgName).Inc()

		hits := cfg.cache.Fetch(ctx, []string{key})

		if ex := hits[key]; ex != nil {
			exists, err := strconv.ParseBool(string(ex))
			if err == nil {
				cb.operationHits.WithLabelValues(objstore.OpExists, cfgName).Inc()
				return exists, nil
			}
			level.Warn(cb.logger).Log("msg", "unexpected cached 'exists' value", "key", key, "val", string(ex))
		}
	}

	existsTime := time.Now()
	ok, err := cb.Bucket.Exists(ctx, name)
	if err == nil {
		storeExistsCacheEntry(ctx, key, ok, existsTime, cfg.cache, cfg.existsTTL, cfg.doesntExistTTL)
	}

	return ok, err
}

func storeExistsCacheEntry(ctx context.Context, cachingKey string, exists bool, ts time.Time, cache cache.Cache, existsTTL, doesntExistTTL time.Duration) {
	var ttl time.Duration
	if exists {
		ttl = existsTTL - time.Since(ts)
	} else {
		ttl = doesntExistTTL - time.Since(ts)
	}

	if ttl > 0 {
		cache.StoreAsync(map[string][]byte{cachingKey: []byte(strconv.FormatBool(exists))}, ttl)
	}
}

func (cb *CachingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	cfgName, cfg := cb.cfg.findGetConfig(name)
	if cfg == nil {
		return cb.Bucket.Get(ctx, name)
	}

	contentKey := cachingKeyContent(cb.bucketID, name)
	existsKey := cachingKeyExists(cb.bucketID, name)

	// Lookup the cache.
	if isCacheLookupEnabled(ctx) {
		slabs := getMemoryPool(ctx)
		cacheOpts := getCacheOptions(slabs)
		releaseSlabs := true

		// On cache hit, the returned reader is responsible for freeing any allocated
		// slabs. However, if the content key isn't a hit the client still might have
		// allocated memory for the exists key, and we need to free it in that case.
		if slabs != nil {
			defer func() {
				if releaseSlabs {
					slabs.Release()
				}
			}()
		}

		cb.operationRequests.WithLabelValues(objstore.OpGet, cfgName).Inc()

		hits := cfg.cache.Fetch(ctx, []string{contentKey, existsKey}, cacheOpts...)

		// If we know that file doesn't exist, we can return that. Useful for deletion marks.
		//
		// Non-existence is updated in the cache on each Get() going through the object storage
		// and not finding the object, while the object content is not updated, so it's safer
		// to check this before the actual cached content (if any).
		if ex := hits[existsKey]; ex != nil {
			if exists, err := strconv.ParseBool(string(ex)); err == nil && !exists {
				cb.operationHits.WithLabelValues(objstore.OpGet, cfgName).Inc()
				return nil, errObjNotFound
			}
		}

		// Check if the content was cached.
		if hits[contentKey] != nil {
			cb.operationHits.WithLabelValues(objstore.OpGet, cfgName).Inc()

			releaseSlabs = false
			return &sizedSlabGetReader{bytes.NewBuffer(hits[contentKey]), slabs}, nil
		}
	}

	getTime := time.Now()
	reader, err := cb.Bucket.Get(ctx, name)
	if err != nil {
		if cb.Bucket.IsObjNotFoundErr(err) {
			// Cache that object doesn't exist.
			storeExistsCacheEntry(ctx, existsKey, false, getTime, cfg.cache, cfg.existsTTL, cfg.doesntExistTTL)
		}

		return nil, err
	}

	storeExistsCacheEntry(ctx, existsKey, true, getTime, cfg.cache, cfg.existsTTL, cfg.doesntExistTTL)
	return &getReader{
		c:         cfg.cache,
		r:         reader,
		buf:       new(bytes.Buffer),
		startTime: getTime,
		ttl:       cfg.contentTTL,
		cacheKey:  contentKey,
		maxSize:   cfg.maxCacheableSize,
	}, nil
}

func (cb *CachingBucket) IsObjNotFoundErr(err error) bool {
	return errors.Is(err, errObjNotFound) || cb.Bucket.IsObjNotFoundErr(err)
}

func (cb *CachingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if off < 0 || length <= 0 {
		return cb.Bucket.GetRange(ctx, name, off, length)
	}

	cfgName, cfg := cb.cfg.findGetRangeConfig(name)
	if cfg == nil {
		return cb.Bucket.GetRange(ctx, name, off, length)
	}

	return cb.cachedGetRange(ctx, name, off, length, cfgName, cfg)
}

func (cb *CachingBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	cfgName, cfg := cb.cfg.findAttributesConfig(name)
	if cfg == nil {
		return cb.Bucket.Attributes(ctx, name)
	}

	return cb.cachedAttributes(ctx, name, cfgName, cfg.cache, cfg.ttl)
}

func (cb *CachingBucket) cachedAttributes(ctx context.Context, name, cfgName string, cache cache.Cache, ttl time.Duration) (objstore.ObjectAttributes, error) {
	key := cachingKeyAttributes(cb.bucketID, name)

	// Lookup the cache.
	if isCacheLookupEnabled(ctx) {
		cb.operationRequests.WithLabelValues(objstore.OpAttributes, cfgName).Inc()

		hits := cache.Fetch(ctx, []string{key})
		if raw, ok := hits[key]; ok {
			var attrs objstore.ObjectAttributes
			err := json.Unmarshal(raw, &attrs)
			if err == nil {
				cb.operationHits.WithLabelValues(objstore.OpAttributes, cfgName).Inc()
				return attrs, nil
			}

			level.Warn(cb.logger).Log("msg", "failed to decode cached Attributes result", "key", key, "err", err)
		}
	}

	attrs, err := cb.Bucket.Attributes(ctx, name)
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}

	if raw, err := json.Marshal(attrs); err == nil {
		cache.StoreAsync(map[string][]byte{key: raw}, ttl)
	} else {
		level.Warn(cb.logger).Log("msg", "failed to encode cached Attributes result", "key", key, "err", err)
	}

	return attrs, nil
}

func (cb *CachingBucket) cachedGetRange(ctx context.Context, name string, offset, length int64, cfgName string, cfg *getRangeConfig) (io.ReadCloser, error) {
	attrs, err := cb.cachedAttributes(ctx, name, cfgName, cfg.attributes.cache, cfg.attributes.ttl)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get object attributes: %s", name)
	}

	// If length goes over object size, adjust length. We use it later to limit number of read bytes.
	if offset+length > attrs.Size {
		length = attrs.Size - offset
	}

	// Start and end range are subrange-aligned offsets into object, that we're going to read.
	startRange := (offset / cfg.subrangeSize) * cfg.subrangeSize
	endRange := ((offset + length) / cfg.subrangeSize) * cfg.subrangeSize
	if (offset+length)%cfg.subrangeSize > 0 {
		endRange += cfg.subrangeSize
	}

	// The very last subrange in the object may have length that is not divisible by subrange size.
	lastSubrangeOffset := endRange - cfg.subrangeSize
	lastSubrangeLength := int(cfg.subrangeSize)
	if endRange > attrs.Size {
		lastSubrangeOffset = (attrs.Size / cfg.subrangeSize) * cfg.subrangeSize
		lastSubrangeLength = int(attrs.Size - lastSubrangeOffset)
	}

	numSubranges := (endRange - startRange) / cfg.subrangeSize

	offsetKeys := make(map[int64]string, numSubranges)
	keys := make([]string, 0, numSubranges)

	totalRequestedBytes := int64(0)
	for off := startRange; off < endRange; off += cfg.subrangeSize {
		end := off + cfg.subrangeSize
		if end > attrs.Size {
			end = attrs.Size
		}
		totalRequestedBytes += (end - off)

		k := cachingKeyObjectSubrange(cb.bucketID, name, off, end)
		keys = append(keys, k)
		offsetKeys[off] = k
	}

	var (
		hits         map[string][]byte
		slabs        = getMemoryPool(ctx)
		releaseSlabs = true
	)

	cb.requestedGetRangeBytes.WithLabelValues(cfgName).Add(float64(length))

	// Lookup the cache.
	if isCacheLookupEnabled(ctx) {
		cacheOpts := getCacheOptions(slabs)

		// If there's an error after fetching things from cache but before we return the subrange
		// reader we're responsible for releasing any memory used by the slab pool.
		if slabs != nil {
			defer func() {
				if releaseSlabs {
					slabs.Release()
				}
			}()
		}

		// Try to get all subranges from the cache.
		totalCachedBytes := int64(0)
		hits = cfg.cache.Fetch(ctx, keys, cacheOpts...)
		for _, b := range hits {
			totalCachedBytes += int64(len(b))
		}

		cb.fetchedGetRangeBytes.WithLabelValues(originCache, cfgName).Add(float64(totalCachedBytes))
		cb.operationRequests.WithLabelValues(objstore.OpGetRange, cfgName).Inc()
		cb.operationHits.WithLabelValues(objstore.OpGetRange, cfgName).Add(float64(len(hits)) / float64(len(keys)))
	}

	if len(hits) < len(keys) {
		if hits == nil {
			hits = map[string][]byte{}
		}

		err := cb.fetchMissingSubranges(ctx, name, startRange, endRange, offsetKeys, hits, lastSubrangeOffset, lastSubrangeLength, cfgName, cfg)
		if err != nil {
			return nil, err
		}
	}

	releaseSlabs = false
	return newSubrangesReader(cfg.subrangeSize, offsetKeys, hits, offset, length, slabs), nil
}

type rng struct {
	start, end int64
}

// fetchMissingSubranges fetches missing subranges, stores them into "hits" map
// and into cache as well (using provided cacheKeys).
func (cb *CachingBucket) fetchMissingSubranges(ctx context.Context, name string, startRange, endRange int64, cacheKeys map[int64]string, hits map[string][]byte, lastSubrangeOffset int64, lastSubrangeLength int, cfgName string, cfg *getRangeConfig) error {
	// Ordered list of missing sub-ranges.
	var missing []rng

	for off := startRange; off < endRange; off += cfg.subrangeSize {
		if hits[cacheKeys[off]] == nil {
			missing = append(missing, rng{start: off, end: off + cfg.subrangeSize})
		}
	}

	missing = mergeRanges(missing, 0) // Merge adjacent ranges.
	// Keep merging until we have only max number of ranges (= requests).
	for limit := cfg.subrangeSize; cfg.maxSubRequests > 0 && len(missing) > cfg.maxSubRequests; limit = limit * 2 {
		missing = mergeRanges(missing, limit)
	}

	var hitsMutex sync.Mutex

	// Run parallel queries for each missing range. Fetched data is stored into 'hits' map, protected by hitsMutex.
	g, gctx := errgroup.WithContext(ctx)
	for _, m := range missing {
		m := m
		g.Go(func() error {
			r, err := cb.Bucket.GetRange(gctx, name, m.start, m.end-m.start)
			if err != nil {
				return errors.Wrapf(err, "fetching range [%d, %d]", m.start, m.end)
			}
			defer runutil.CloseWithLogOnErr(cb.logger, r, "fetching range [%d, %d]", m.start, m.end)

			for off := m.start; off < m.end && gctx.Err() == nil; off += cfg.subrangeSize {
				key := cacheKeys[off]
				if key == "" {
					return errors.Errorf("fetching range [%d, %d]: caching key for offset %d not found", m.start, m.end, off)
				}

				// We need a new buffer for each subrange, both for storing into hits, and also for caching.
				var subrangeData []byte
				if off == lastSubrangeOffset {
					// The very last subrange in the object may have different length,
					// if object length isn't divisible by subrange size.
					subrangeData = make([]byte, lastSubrangeLength)
				} else {
					subrangeData = make([]byte, cfg.subrangeSize)
				}
				_, err := io.ReadFull(r, subrangeData)
				if err != nil {
					return errors.Wrapf(err, "fetching range [%d, %d]", m.start, m.end)
				}

				storeToCache := false
				hitsMutex.Lock()
				if _, ok := hits[key]; !ok {
					storeToCache = true
					hits[key] = subrangeData
				}
				hitsMutex.Unlock()

				if storeToCache {
					cb.fetchedGetRangeBytes.WithLabelValues(originBucket, cfgName).Add(float64(len(subrangeData)))
					cfg.cache.StoreAsync(map[string][]byte{key: subrangeData}, cfg.subrangeTTL)
				} else {
					cb.refetchedGetRangeBytes.WithLabelValues(originCache, cfgName).Add(float64(len(subrangeData)))
				}
			}

			return gctx.Err()
		})
	}

	return g.Wait()
}

// Merges ranges that are close to each other. Modifies input.
func mergeRanges(input []rng, limit int64) []rng {
	if len(input) == 0 {
		return input
	}

	last := 0
	for ix := 1; ix < len(input); ix++ {
		if (input[ix].start - input[last].end) <= limit {
			input[last].end = input[ix].end
		} else {
			last++
			input[last] = input[ix]
		}
	}
	return input[:last+1]
}

func cachingKeyAttributes(bucketID, name string) string {
	return composeCachingKey("attrs", bucketID, name)
}

func cachingKeyObjectSubrange(bucketID, name string, start, end int64) string {
	return composeCachingKey("subrange", bucketID, name, strconv.FormatInt(start, 10), strconv.FormatInt(end, 10))
}

func cachingKeyIter(bucketID, name string, options ...objstore.IterOption) string {
	// Ensure the caching key is different for the same request but different
	// recursive config.
	if params := objstore.ApplyIterOptions(options...); params.Recursive {
		return composeCachingKey("iter", bucketID, name, "recursive")
	}

	return composeCachingKey("iter", bucketID, name)
}

func cachingKeyExists(bucketID, name string) string {
	return composeCachingKey("exists", bucketID, name)
}

func cachingKeyContent(bucketID, name string) string {
	return composeCachingKey("content", bucketID, name)
}

func composeCachingKey(op, bucketID string, values ...string) string {
	// Estimate size.
	estimatedSize := len(op) + len(bucketID) + (2 + len(values))
	for _, value := range values {
		estimatedSize += len(value)
	}

	b := strings.Builder{}
	b.Grow(estimatedSize)

	if bucketID != "" {
		b.WriteString(bucketID)
		b.WriteRune(':')
	}

	b.WriteString(op)

	for _, value := range values {
		b.WriteRune(':')
		b.WriteString(value)
	}

	return b.String()
}

// Reader implementation that uses in-memory subranges.
type subrangesReader struct {
	subrangeSize int64

	// Mapping of subrangeSize-aligned offsets to keys in hits.
	offsetsKeys map[int64]string
	subranges   map[string][]byte

	// Offset for next read, used to find correct subrange to return data from.
	readOffset int64

	// Remaining data to return from this reader. Once zero, this reader reports EOF.
	remaining int64

	// Pool of bytes used for cache results
	slabs *pool.SafeSlabPool[byte]
}

func newSubrangesReader(subrangeSize int64, offsetsKeys map[int64]string, subranges map[string][]byte, readOffset, remaining int64, slabs *pool.SafeSlabPool[byte]) *subrangesReader {
	return &subrangesReader{
		subrangeSize: subrangeSize,
		offsetsKeys:  offsetsKeys,
		subranges:    subranges,

		readOffset: readOffset,
		remaining:  remaining,

		slabs: slabs,
	}
}

func (c *subrangesReader) Close() error {
	if c.slabs != nil {
		c.slabs.Release()
	}

	return nil
}

func (c *subrangesReader) Read(p []byte) (n int, err error) {
	if c.remaining <= 0 {
		return 0, io.EOF
	}

	currentSubrangeOffset := (c.readOffset / c.subrangeSize) * c.subrangeSize
	currentSubrange, err := c.subrangeAt(currentSubrangeOffset)
	if err != nil {
		return 0, errors.Wrapf(err, "read position: %d", c.readOffset)
	}

	offsetInSubrange := int(c.readOffset - currentSubrangeOffset)
	toCopy := len(currentSubrange) - offsetInSubrange
	if toCopy <= 0 {
		// This can only happen if subrange's length is not subrangeSize, and reader is told to read more data.
		return 0, errors.Errorf("no more data left in subrange at position %d, subrange length %d, reading position %d", currentSubrangeOffset, len(currentSubrange), c.readOffset)
	}

	if len(p) < toCopy {
		toCopy = len(p)
	}
	if c.remaining < int64(toCopy) {
		toCopy = int(c.remaining) // Conversion is safe, c.remaining is small enough.
	}

	copy(p, currentSubrange[offsetInSubrange:offsetInSubrange+toCopy])
	c.readOffset += int64(toCopy)
	c.remaining -= int64(toCopy)

	return toCopy, nil
}

func (c *subrangesReader) subrangeAt(offset int64) ([]byte, error) {
	b := c.subranges[c.offsetsKeys[offset]]
	if b == nil {
		return nil, errors.Errorf("subrange for offset %d not found", offset)
	}
	return b, nil
}

type getReader struct {
	c         cache.Cache
	r         io.ReadCloser
	buf       *bytes.Buffer
	startTime time.Time
	ttl       time.Duration
	cacheKey  string
	maxSize   int
}

func (g *getReader) Close() error {
	// We don't know if entire object was read, don't store it here.
	g.buf = nil
	return g.r.Close()
}

func (g *getReader) Read(p []byte) (n int, err error) {
	n, err = g.r.Read(p)
	if n > 0 && g.buf != nil {
		if g.buf.Len()+n <= g.maxSize {
			g.buf.Write(p[:n])
		} else {
			// Object is larger than max size, stop caching.
			g.buf = nil
		}
	}

	if errors.Is(err, io.EOF) && g.buf != nil {
		remainingTTL := g.ttl - time.Since(g.startTime)
		if remainingTTL > 0 {
			g.c.StoreAsync(map[string][]byte{g.cacheKey: g.buf.Bytes()}, remainingTTL)
		}
		// Clear reference, to avoid doing another Store on next read.
		g.buf = nil
	}

	return n, err
}

// sizedSlabGetReader wraps an existing io.Reader with a cleanup method to release
// any allocated slabs back to a pool.SafeSlabPool when closed.
type sizedSlabGetReader struct {
	io.Reader
	slabs *pool.SafeSlabPool[byte]
}

func (s *sizedSlabGetReader) Close() error {
	if s.slabs != nil {
		s.slabs.Release()
	}

	return nil
}

func (s *sizedSlabGetReader) ObjectSize() (int64, error) {
	return objstore.TryToGetSize(s.Reader)
}

// JSONIterCodec encodes iter results into JSON. Suitable for root dir.
type JSONIterCodec struct{}

func (jic JSONIterCodec) Encode(files []string) ([]byte, error) {
	return json.Marshal(files)
}

func (jic JSONIterCodec) Decode(data []byte) ([]string, error) {
	var list []string
	err := json.Unmarshal(data, &list)
	return list, err
}

type SnappyIterCodec struct {
	IterCodec
}

func (i SnappyIterCodec) Encode(files []string) ([]byte, error) {
	b, err := i.IterCodec.Encode(files)
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, b), nil
}

func (i SnappyIterCodec) Decode(cachedData []byte) ([]string, error) {
	b, err := snappy.Decode(nil, cachedData)
	if err != nil {
		return nil, errors.Wrap(err, "SnappyIterCodec")
	}
	return i.IterCodec.Decode(b)
}
