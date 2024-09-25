// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/cache_bucket_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

//nolint:unparam
package bucketcache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/runutil"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"golang.org/x/exp/slices"
)

func TestChunksCaching(t *testing.T) {
	const bucketID = "test"

	length := int64(1024 * 1024)
	subrangeSize := int64(16000) // All tests are based on this value.

	data := make([]byte, length)
	for ix := 0; ix < len(data); ix++ {
		data[ix] = byte(ix)
	}

	name := "/test/chunks/000001"

	inmem := objstore.NewInMemBucket()
	assert.NoError(t, inmem.Upload(context.Background(), name, bytes.NewReader(data)))

	// We reuse cache between tests (!)
	cache := cache.NewMockCache()

	// Warning, these tests must be run in order, they depend cache state from previous test.
	for _, tc := range []struct {
		name                   string
		init                   func()
		offset                 int64
		length                 int64
		maxGetRangeRequests    int
		cacheLookupDisabled    bool
		expectedLength         int64
		expectedFetchedBytes   int64
		expectedCachedBytes    int64
		expectedRefetchedBytes int64
		expectedCacheRequests  int64
		expectedCacheHits      float64
	}{
		{
			name:                  "basic test",
			offset:                555555,
			length:                55555,
			expectedLength:        55555,
			expectedFetchedBytes:  5 * subrangeSize,
			expectedCacheRequests: 1,
			expectedCacheHits:     0,
		},

		{
			name:                  "same request will hit all subranges in the cache",
			offset:                555555,
			length:                55555,
			expectedLength:        55555,
			expectedCachedBytes:   5 * subrangeSize,
			expectedCacheRequests: 1,
			expectedCacheHits:     1,
		},

		{
			name:                  "request data close to the end of object",
			offset:                length - 10,
			length:                3000,
			expectedLength:        10,
			expectedFetchedBytes:  8576, // Last (incomplete) subrange is fetched.
			expectedCacheRequests: 1,
			expectedCacheHits:     0,
		},

		{
			name:                  "another request data close to the end of object, cached by previous test",
			offset:                1040100,
			length:                subrangeSize,
			expectedLength:        8476,
			expectedCachedBytes:   8576,
			expectedCacheRequests: 1,
			expectedCacheHits:     1,
		},

		{
			name:                  "entire object, combination of cached and uncached subranges",
			offset:                0,
			length:                length,
			expectedLength:        length,
			expectedCachedBytes:   5*subrangeSize + 8576, // 5 subrange cached from first test, plus last incomplete subrange.
			expectedFetchedBytes:  60 * subrangeSize,
			expectedCacheRequests: 1,
			expectedCacheHits:     0.09,
		},

		{
			name:                  "entire object again, everything is cached",
			offset:                0,
			length:                length,
			expectedLength:        length,
			expectedCachedBytes:   length, // Entire file is now cached.
			expectedCacheRequests: 1,
			expectedCacheHits:     1,
		},

		{
			name:                  "entire object again, nothing is cached",
			offset:                0,
			length:                length,
			expectedLength:        length,
			expectedFetchedBytes:  length,
			expectedCachedBytes:   0, // Cache is flushed.
			expectedCacheRequests: 1,
			expectedCacheHits:     0,
			init: func() {
				cache.Flush()
			},
		},

		{
			name:                  "missing first subranges",
			offset:                0,
			length:                10 * subrangeSize,
			expectedLength:        10 * subrangeSize,
			expectedFetchedBytes:  3 * subrangeSize,
			expectedCachedBytes:   7 * subrangeSize,
			expectedCacheRequests: 1,
			expectedCacheHits:     7.0 / 10.0,
			init: func() {
				ctx := context.Background()
				// Delete first 3 subranges.
				require.NoError(t, cache.Delete(ctx, cachingKeyObjectSubrange(bucketID, name, 0*subrangeSize, 1*subrangeSize)))
				require.NoError(t, cache.Delete(ctx, cachingKeyObjectSubrange(bucketID, name, 1*subrangeSize, 2*subrangeSize)))
				require.NoError(t, cache.Delete(ctx, cachingKeyObjectSubrange(bucketID, name, 2*subrangeSize, 3*subrangeSize)))
			},
		},

		{
			name:                  "missing last subranges",
			offset:                0,
			length:                10 * subrangeSize,
			expectedLength:        10 * subrangeSize,
			expectedFetchedBytes:  3 * subrangeSize,
			expectedCachedBytes:   7 * subrangeSize,
			expectedCacheRequests: 1,
			expectedCacheHits:     7.0 / 10.0,
			init: func() {
				ctx := context.Background()
				// Delete last 3 subranges.
				require.NoError(t, cache.Delete(ctx, cachingKeyObjectSubrange(bucketID, name, 7*subrangeSize, 8*subrangeSize)))
				require.NoError(t, cache.Delete(ctx, cachingKeyObjectSubrange(bucketID, name, 8*subrangeSize, 9*subrangeSize)))
				require.NoError(t, cache.Delete(ctx, cachingKeyObjectSubrange(bucketID, name, 9*subrangeSize, 10*subrangeSize)))
			},
		},

		{
			name:                  "missing middle subranges",
			offset:                0,
			length:                10 * subrangeSize,
			expectedLength:        10 * subrangeSize,
			expectedFetchedBytes:  3 * subrangeSize,
			expectedCachedBytes:   7 * subrangeSize,
			expectedCacheRequests: 1,
			expectedCacheHits:     7.0 / 10.0,
			init: func() {
				ctx := context.Background()
				// Delete 3 subranges in the middle.
				require.NoError(t, cache.Delete(ctx, cachingKeyObjectSubrange(bucketID, name, 3*subrangeSize, 4*subrangeSize)))
				require.NoError(t, cache.Delete(ctx, cachingKeyObjectSubrange(bucketID, name, 4*subrangeSize, 5*subrangeSize)))
				require.NoError(t, cache.Delete(ctx, cachingKeyObjectSubrange(bucketID, name, 5*subrangeSize, 6*subrangeSize)))
			},
		},

		{
			name:                  "missing everything except middle subranges",
			offset:                0,
			length:                10 * subrangeSize,
			expectedLength:        10 * subrangeSize,
			expectedFetchedBytes:  7 * subrangeSize,
			expectedCachedBytes:   3 * subrangeSize,
			expectedCacheRequests: 1,
			expectedCacheHits:     3.0 / 10.0,
			init: func() {
				// Delete all but 3 subranges in the middle, and keep unlimited number of ranged subrequests.
				for i := int64(0); i < 10; i++ {
					if i > 0 && i%3 == 0 {
						continue
					}
					require.NoError(t, cache.Delete(context.Background(), cachingKeyObjectSubrange(bucketID, name, i*subrangeSize, (i+1)*subrangeSize)))
				}
			},
		},

		{
			name:                   "missing everything except middle subranges, one subrequest only",
			offset:                 0,
			length:                 10 * subrangeSize,
			expectedLength:         10 * subrangeSize,
			expectedFetchedBytes:   7 * subrangeSize,
			expectedCachedBytes:    3 * subrangeSize,
			expectedRefetchedBytes: 3 * subrangeSize, // Entire object fetched, 3 subranges are "refetched".
			maxGetRangeRequests:    1,
			expectedCacheRequests:  1,
			expectedCacheHits:      3.0 / 10.0,
			init: func() {
				// Delete all but 3 subranges in the middle, but only allow 1 subrequest.
				for i := int64(0); i < 10; i++ {
					if i == 3 || i == 5 || i == 7 {
						continue
					}
					require.NoError(t, cache.Delete(context.Background(), cachingKeyObjectSubrange(bucketID, name, i*subrangeSize, (i+1)*subrangeSize)))
				}
			},
		},

		{
			name:                  "missing everything except middle subranges, two subrequests",
			offset:                0,
			length:                10 * subrangeSize,
			expectedLength:        10 * subrangeSize,
			expectedFetchedBytes:  7 * subrangeSize,
			expectedCachedBytes:   3 * subrangeSize,
			maxGetRangeRequests:   2,
			expectedCacheRequests: 1,
			expectedCacheHits:     3.0 / 10.0,
			init: func() {
				// Delete all but one subranges in the middle, and allow 2 subrequests. They will be: 0-80000, 128000-160000.
				for i := int64(0); i < 10; i++ {
					if i == 5 || i == 6 || i == 7 {
						continue
					}
					require.NoError(t, cache.Delete(context.Background(), cachingKeyObjectSubrange(bucketID, name, i*subrangeSize, (i+1)*subrangeSize)))
				}
			},
		},

		{
			name:                  "should skip cache look but store the subranges to cache when cache lookup is disabled",
			offset:                0,
			length:                length,
			cacheLookupDisabled:   true,
			expectedLength:        length,
			expectedFetchedBytes:  length,
			expectedCachedBytes:   0,
			expectedCacheRequests: 0, // No cache lookup.
			expectedCacheHits:     0,
			init: func() {
				// Cleanup the cache.
				cache.Flush()
			},
		},

		{
			name:                  "should have cached subranges in the previous call even if cache lookup was disabled",
			offset:                0,
			length:                length,
			cacheLookupDisabled:   false,
			expectedLength:        length,
			expectedFetchedBytes:  0,
			expectedCachedBytes:   length,
			expectedCacheRequests: 1,
			expectedCacheHits:     1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.init != nil {
				tc.init()
			}

			const cfgName = "chunks"
			cfg := NewCachingBucketConfig()
			cfg.CacheGetRange(cfgName, cache, isTSDBChunkFile, subrangeSize, cache, time.Hour, time.Hour, tc.maxGetRangeRequests)

			cachingBucket, err := NewCachingBucket(bucketID, inmem, cfg, nil, nil)
			assert.NoError(t, err)

			ctx := context.Background()
			if tc.cacheLookupDisabled {
				ctx = WithCacheLookupEnabled(ctx, false)
			}

			verifyGetRange(ctx, t, cachingBucket, name, tc.offset, tc.length, tc.expectedLength)

			assert.Equal(t, tc.expectedCacheRequests, int64(promtest.ToFloat64(cachingBucket.operationRequests.WithLabelValues(objstore.OpGetRange, cfgName))))
			assert.InDelta(t, tc.expectedCacheHits, promtest.ToFloat64(cachingBucket.operationHits.WithLabelValues(objstore.OpGetRange, cfgName)), 0.01)
			assert.Equal(t, tc.expectedCachedBytes, int64(promtest.ToFloat64(cachingBucket.fetchedGetRangeBytes.WithLabelValues(originCache, cfgName))))
			assert.Equal(t, tc.expectedFetchedBytes, int64(promtest.ToFloat64(cachingBucket.fetchedGetRangeBytes.WithLabelValues(originBucket, cfgName))))
			assert.Equal(t, tc.expectedRefetchedBytes, int64(promtest.ToFloat64(cachingBucket.refetchedGetRangeBytes.WithLabelValues(originCache, cfgName))))
		})
	}
}

func verifyGetRange(ctx context.Context, t *testing.T, cachingBucket *CachingBucket, name string, offset, length, expectedLength int64) {
	r, err := cachingBucket.GetRange(ctx, name, offset, length)
	assert.NoError(t, err)

	read, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, expectedLength, int64(len(read)))

	for ix := 0; ix < len(read); ix++ {
		if byte(ix)+byte(offset) != read[ix] {
			t.Fatalf("bytes differ at position %d", ix)
		}
	}
}

func TestMergeRanges(t *testing.T) {
	for ix, tc := range []struct {
		input    []rng
		limit    int64
		expected []rng
	}{
		{
			input:    nil,
			limit:    0,
			expected: nil,
		},

		{
			input:    []rng{{start: 0, end: 100}, {start: 100, end: 200}, {start: 500, end: 1000}},
			limit:    0,
			expected: []rng{{start: 0, end: 200}, {start: 500, end: 1000}},
		},

		{
			input:    []rng{{start: 0, end: 100}, {start: 500, end: 1000}},
			limit:    300,
			expected: []rng{{start: 0, end: 100}, {start: 500, end: 1000}},
		},
		{
			input:    []rng{{start: 0, end: 100}, {start: 500, end: 1000}},
			limit:    400,
			expected: []rng{{start: 0, end: 1000}},
		},
	} {
		t.Run(fmt.Sprintf("%d", ix), func(t *testing.T) {
			assert.Equal(t, tc.expected, mergeRanges(tc.input, tc.limit))
		})
	}
}

func TestInvalidOffsetAndLength(t *testing.T) {
	b := &testBucket{objstore.NewInMemBucket()}

	cache := cache.NewMockCache()
	cfg := NewCachingBucketConfig()
	cfg.CacheGetRange("chunks", cache, func(string) bool { return true }, 10000, cache, time.Hour, time.Hour, 3)

	c, err := NewCachingBucket("test", b, cfg, nil, nil)
	assert.NoError(t, err)

	r, err := c.GetRange(context.Background(), "test", -1, 1000)
	assert.Equal(t, nil, r)
	assert.Error(t, err)

	r, err = c.GetRange(context.Background(), "test", 100, -1)
	assert.Equal(t, nil, r)
	assert.Error(t, err)
}

type testBucket struct {
	*objstore.InMemBucket
}

func (b *testBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if off < 0 {
		return nil, errors.Errorf("invalid offset: %d", off)
	}

	if length <= 0 {
		return nil, errors.Errorf("invalid length: %d", length)
	}

	return b.InMemBucket.GetRange(ctx, name, off, length)
}

func TestCachedIter(t *testing.T) {
	inmem := objstore.NewInMemBucket()
	assert.NoError(t, inmem.Upload(context.Background(), "/file-1", strings.NewReader("hej")))
	assert.NoError(t, inmem.Upload(context.Background(), "/file-2", strings.NewReader("ahoj")))
	assert.NoError(t, inmem.Upload(context.Background(), "/file-3", strings.NewReader("hello")))
	assert.NoError(t, inmem.Upload(context.Background(), "/file-4", strings.NewReader("ciao")))

	allFiles := []string{"/file-1", "/file-2", "/file-3", "/file-4"}

	// We reuse cache between tests (!)
	cache := cache.NewMockCache()
	ctx := context.Background()

	const cfgName = "dirs"
	cfg := NewCachingBucketConfig()
	cfg.CacheIter(cfgName, cache, func(string) bool { return true }, 5*time.Minute, JSONIterCodec{})

	cb, err := NewCachingBucket("test", inmem, cfg, nil, nil)
	assert.NoError(t, err)

	t.Run("Iter() should return objects list from the cache on cache hit", func(t *testing.T) {
		// Pre-condition: populate the cache.
		cache.Flush()
		verifyIter(ctx, t, cb, allFiles, true, false, cfgName)

		assert.NoError(t, inmem.Upload(context.Background(), "/file-5", strings.NewReader("nazdar")))
		verifyIter(ctx, t, cb, allFiles, true, true, cfgName) // Iter returns old response.

		cache.Flush()
		allFiles = append(allFiles, "/file-5")
		verifyIter(ctx, t, cb, allFiles, true, false, cfgName)
	})

	t.Run("Iter() should skip the cache lookup but cache the result on cache lookup disabled", func(t *testing.T) {
		// Pre-condition: populate the cache.
		cache.Flush()
		verifyIter(ctx, t, cb, allFiles, true, false, cfgName)

		// Pre-condition: add a new object and make sure it's NOT returned by the Iter()
		// (because result is picked up from the cache).
		assert.NoError(t, inmem.Upload(context.Background(), "/file-6", strings.NewReader("world")))
		verifyIter(ctx, t, cb, allFiles, true, true, cfgName)

		// Calling Iter() with cache lookup disabled should return the new object and also update the cached list.
		allFiles = append(allFiles, "/file-6")
		verifyIter(WithCacheLookupEnabled(ctx, false), t, cb, allFiles, false, false, cfgName)
		verifyIter(ctx, t, cb, allFiles, true, true, cfgName)
	})

	t.Run("Iter() should not cache objects list on error while iterating the bucket", func(t *testing.T) {
		cache.Flush()

		// This iteration returns false. Result will not be cached.
		e := errors.Errorf("test error")
		assert.Equal(t, e, cb.Iter(context.Background(), "/", func(_ string) error {
			return e
		}))

		// Nothing cached now.
		verifyIter(ctx, t, cb, allFiles, true, false, cfgName)
	})
}

func verifyIter(ctx context.Context, t *testing.T, cb *CachingBucket, expectedFiles []string, expectedCacheLookup, expectedFromCache bool, cfgName string) {
	requestsBefore := int(promtest.ToFloat64(cb.operationRequests.WithLabelValues(objstore.OpIter, cfgName)))
	hitsBefore := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpIter, cfgName)))

	col := iterCollector{}
	assert.NoError(t, cb.Iter(ctx, "/", col.collect))

	requestsAfter := int(promtest.ToFloat64(cb.operationRequests.WithLabelValues(objstore.OpIter, cfgName)))
	hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpIter, cfgName)))

	slices.Sort(col.items)
	assert.Equal(t, expectedFiles, col.items)

	expectedRequestsDiff := 0
	expectedHitsDiff := 0
	if expectedCacheLookup {
		expectedRequestsDiff = 1
	}
	if expectedFromCache {
		expectedHitsDiff = 1
	}

	assert.Equal(t, expectedRequestsDiff, requestsAfter-requestsBefore)
	assert.Equal(t, expectedHitsDiff, hitsAfter-hitsBefore)
}

type iterCollector struct {
	items []string
}

func (it *iterCollector) collect(s string) error {
	it.items = append(it.items, s)
	return nil
}

func TestExists(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := cache.NewMockCache()
	ctx := context.Background()

	cfg := NewCachingBucketConfig()
	const cfgName = "test"
	cfg.CacheExists(cfgName, cache, matchAll, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket("test", inmem, cfg, nil, nil)
	assert.NoError(t, err)

	t.Run("Exists() should return cached value on cache hit", func(t *testing.T) {
		const filename = "/object-1"

		// Pre-condition: the object should not exist.
		verifyExists(ctx, t, cb, filename, false, true, false, cfgName)

		// Upload the file then ensure the value is picked up from the cache.
		assert.NoError(t, inmem.Upload(context.Background(), filename, strings.NewReader("hej")))
		verifyExists(ctx, t, cb, filename, false, true, true, cfgName) // Reused cache result.

		cache.Flush()
		verifyExists(ctx, t, cb, filename, true, true, false, cfgName)

		assert.NoError(t, inmem.Delete(context.Background(), filename))
		verifyExists(ctx, t, cb, filename, true, true, true, cfgName) // Reused cache result.

		cache.Flush()
		verifyExists(ctx, t, cb, filename, false, true, false, cfgName)
	})

	t.Run("Exists() should skip the cache lookup but cache the result on cache lookup disabled", func(t *testing.T) {
		const filename = "/object-2"

		// Pre-condition: the object should not exist.
		verifyExists(ctx, t, cb, filename, false, true, false, cfgName)

		// Pre-condition: upload the file then ensure the value is picked up from the cache if cache lookup is enabled.
		assert.NoError(t, inmem.Upload(context.Background(), filename, strings.NewReader("hej")))
		verifyExists(ctx, t, cb, filename, false, true, true, cfgName) // Reused cache result.

		// Calling Exists() with cache lookup disabled should lookup the object storage and also update the cached value.
		verifyExists(WithCacheLookupEnabled(ctx, false), t, cb, filename, true, false, false, cfgName)
		verifyExists(ctx, t, cb, filename, true, true, true, cfgName)
	})
}

func TestExistsCachingDisabled(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := cache.NewMockCache()
	ctx := context.Background()

	cfg := NewCachingBucketConfig()
	const cfgName = "test"
	cfg.CacheExists(cfgName, cache, func(string) bool { return false }, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket("test", inmem, cfg, nil, nil)
	assert.NoError(t, err)

	t.Run("Exists() should not use the cache when caching is disabled for the given object", func(t *testing.T) {
		const filename = "/object-1"

		verifyExists(ctx, t, cb, filename, false, false, false, cfgName)

		assert.NoError(t, inmem.Upload(context.Background(), filename, strings.NewReader("hej")))
		verifyExists(ctx, t, cb, filename, true, false, false, cfgName)
		verifyExists(WithCacheLookupEnabled(ctx, false), t, cb, filename, true, false, false, cfgName)

		assert.NoError(t, inmem.Delete(context.Background(), filename))
		verifyExists(ctx, t, cb, filename, false, false, false, cfgName)
		verifyExists(WithCacheLookupEnabled(ctx, false), t, cb, filename, false, false, false, cfgName)
	})
}

func verifyExists(ctx context.Context, t *testing.T, cb *CachingBucket, file string, expectedExists, expectedCacheLookup, expectedFromCache bool, cfgName string) {
	requestsBefore := int(promtest.ToFloat64(cb.operationRequests.WithLabelValues(objstore.OpExists, cfgName)))
	hitsBefore := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpExists, cfgName)))

	ok, err := cb.Exists(ctx, file)
	assert.NoError(t, err)
	assert.Equal(t, expectedExists, ok)

	requestsAfter := int(promtest.ToFloat64(cb.operationRequests.WithLabelValues(objstore.OpExists, cfgName)))
	hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpExists, cfgName)))

	if expectedCacheLookup {
		assert.Equal(t, 1, requestsAfter-requestsBefore)
	} else {
		assert.Equal(t, 0, requestsAfter-requestsBefore)
	}

	if expectedFromCache {
		assert.Equal(t, 1, hitsAfter-hitsBefore)
	} else {
		assert.Equal(t, 0, hitsAfter-hitsBefore)
	}
}

func TestGet(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := cache.NewMockCache()
	ctx := context.Background()

	cfg := NewCachingBucketConfig()
	const cfgName = "metafile"
	cfg.CacheGet(cfgName, cache, matchAll, 1024, 10*time.Minute, 10*time.Minute, 2*time.Minute, false)
	cfg.CacheExists(cfgName, cache, matchAll, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket("test", inmem, cfg, nil, nil)
	assert.NoError(t, err)

	t.Run("Get() should cache non-existence of the requested object if it doesn't exist", func(t *testing.T) {
		const filename = "/object-1"

		// Pre-condition: issue a Get() request when the object does not exist, and ensure non-existence has been cached.
		verifyGet(ctx, t, cb, filename, nil, true, false, cfgName)
		verifyExists(ctx, t, cb, filename, false, true, true, cfgName)

		// Upload the object.
		data := []byte("content-1")
		assert.NoError(t, inmem.Upload(ctx, filename, bytes.NewBuffer(data)))

		// Even if object is now uploaded, old data is served from cache.
		verifyGet(ctx, t, cb, filename, nil, true, true, cfgName)
		verifyExists(ctx, t, cb, filename, false, true, true, cfgName)
	})

	t.Run("Get() should cache both the content and existence of the requested object if it does exist", func(t *testing.T) {
		const filename = "/object-2"

		// Pre-condition: ensure the object exists and is cached.
		data := []byte("content-2")
		assert.NoError(t, inmem.Upload(ctx, filename, bytes.NewBuffer(data)))
		verifyGet(ctx, t, cb, filename, data, true, false, cfgName)

		// Issue another Get(). This time we expect the content to be served from cache.
		verifyGet(ctx, t, cb, filename, data, true, true, cfgName)
		verifyExists(ctx, t, cb, filename, true, true, true, cfgName)
	})

	t.Run("Get() should skip the cache lookup but store the content and existence to the cache if cache lookup is disabled", func(t *testing.T) {
		const filename = "/object-3"

		// Pre-condition: issue a Get() request when the object does not exist, and ensure non-existence has been cached.
		verifyGet(ctx, t, cb, filename, nil, true, false, cfgName)
		verifyExists(ctx, t, cb, filename, false, true, true, cfgName)

		// Upload the object.
		data := []byte("content-3")
		assert.NoError(t, inmem.Upload(ctx, filename, bytes.NewBuffer(data)))

		// Calling Get() with cache lookup disabled should lookup the object storage and also update the cached value.
		verifyGet(WithCacheLookupEnabled(ctx, false), t, cb, filename, data, false, false, cfgName)

		verifyGet(ctx, t, cb, filename, data, true, true, cfgName)
		verifyExists(ctx, t, cb, filename, true, true, true, cfgName)

		// Delete the object.
		require.NoError(t, inmem.Delete(ctx, filename))

		// Pre-condition: the content and existence is looked up from the cache by default.
		verifyGet(ctx, t, cb, filename, data, true, true, cfgName)
		verifyExists(ctx, t, cb, filename, true, true, true, cfgName)

		// Calling Get() with cache lookup disabled should lookup the object storage and also update the cached value.
		verifyGet(WithCacheLookupEnabled(ctx, false), t, cb, filename, nil, false, false, cfgName)

		verifyGet(ctx, t, cb, filename, nil, true, true, cfgName)
		verifyExists(ctx, t, cb, filename, false, true, true, cfgName)
	})
}

func TestGetTooBigObject(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := cache.NewMockCache()
	ctx := context.Background()

	cfg := NewCachingBucketConfig()
	const filename = "/object-1"
	const cfgName = "metafile"
	// Only allow 5 bytes to be cached.
	cfg.CacheGet(cfgName, cache, matchAll, 5, 10*time.Minute, 10*time.Minute, 2*time.Minute, false)
	cfg.CacheExists(cfgName, cache, matchAll, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket("test", inmem, cfg, nil, nil)
	assert.NoError(t, err)

	data := []byte("hello world")
	assert.NoError(t, inmem.Upload(context.Background(), filename, bytes.NewBuffer(data)))

	// Object is too big, so it will not be stored to cache on first read.
	verifyGet(ctx, t, cb, filename, data, true, false, cfgName)
	verifyGet(ctx, t, cb, filename, data, true, false, cfgName)
	verifyExists(ctx, t, cb, filename, true, true, true, cfgName)
}

func TestGetPartialRead(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	cache := cache.NewMockCache()
	ctx := context.Background()

	cfg := NewCachingBucketConfig()
	const filename = "/object-1"
	const cfgName = "metafile"
	cfg.CacheGet(cfgName, cache, matchAll, 1024, 10*time.Minute, 10*time.Minute, 2*time.Minute, false)
	cfg.CacheExists(cfgName, cache, matchAll, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket("test", inmem, cfg, nil, nil)
	assert.NoError(t, err)

	data := []byte("hello world")
	assert.NoError(t, inmem.Upload(context.Background(), filename, bytes.NewBuffer(data)))

	// Read only few bytes from data.
	r, err := cb.Get(context.Background(), filename)
	assert.NoError(t, err)
	_, err = r.Read(make([]byte, 1))
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	// Object wasn't cached as it wasn't fully read.
	verifyGet(ctx, t, cb, filename, data, true, false, cfgName)
	// VerifyGet read object, so now it's cached.
	verifyGet(ctx, t, cb, filename, data, true, true, cfgName)
}

func verifyGet(ctx context.Context, t *testing.T, cb *CachingBucket, file string, expectedData []byte, expectedCacheLookup, expectedFromCache bool, cfgName string) {
	requestsBefore := int(promtest.ToFloat64(cb.operationRequests.WithLabelValues(objstore.OpGet, cfgName)))
	hitsBefore := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpGet, cfgName)))

	r, err := cb.Get(ctx, file)

	if expectedData == nil {
		assert.Error(t, err)
		assert.True(t, cb.IsObjNotFoundErr(err))
	} else {
		assert.NoError(t, err)
		defer runutil.CloseWithLogOnErr(log.NewNopLogger(), r, "verifyGet")
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, expectedData, data)
	}

	requestsAfter := int(promtest.ToFloat64(cb.operationRequests.WithLabelValues(objstore.OpGet, cfgName)))
	hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpGet, cfgName)))

	if expectedCacheLookup {
		assert.Equal(t, 1, requestsAfter-requestsBefore)
	} else {
		assert.Equal(t, 0, requestsAfter-requestsBefore)
	}

	if expectedFromCache {
		assert.Equal(t, 1, hitsAfter-hitsBefore)
	} else {
		assert.Equal(t, 0, hitsAfter-hitsBefore)
	}
}

func TestAttributes(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := cache.NewMockCache()
	ctx := context.Background()

	cfg := NewCachingBucketConfig()
	const cfgName = "test"
	cfg.CacheAttributes(cfgName, cache, matchAll, time.Minute, false)

	cb, err := NewCachingBucket("test", inmem, cfg, nil, nil)
	assert.NoError(t, err)

	t.Run("Attributes() should not cache non existing objects", func(t *testing.T) {
		const filename = "/object-1"

		// Run twice and make sure it's never looked up from cache.
		verifyObjectAttrs(ctx, t, cb, filename, -1, true, false, cfgName)
		verifyObjectAttrs(ctx, t, cb, filename, -1, true, false, cfgName)
	})

	t.Run("Attributes() should return value from cache on cache hit", func(t *testing.T) {
		const filename = "/object-2"

		// Pre-condition: the object should not exist.
		verifyExists(ctx, t, cb, filename, false, false, false, cfgName)

		// Upload the object.
		data := []byte("hello world")
		assert.NoError(t, inmem.Upload(ctx, filename, bytes.NewBuffer(data)))

		// The first call to Attributes() should cache it, while the second one should return the value from the cache.
		verifyObjectAttrs(ctx, t, cb, filename, len(data), true, false, cfgName)
		verifyObjectAttrs(ctx, t, cb, filename, len(data), true, true, cfgName)
	})

	t.Run("Attributes() should skip the cache lookup but store the value to the cache if cache lookup is disabled", func(t *testing.T) {
		const filename = "/object-3"

		// Pre-condition: the object should exist.
		firstData := []byte("hello world")
		assert.NoError(t, inmem.Upload(ctx, filename, bytes.NewBuffer(firstData)))
		verifyExists(ctx, t, cb, filename, true, false, false, cfgName)

		// A call to Attributes() should cache the object attributes.
		verifyObjectAttrs(ctx, t, cb, filename, len(firstData), true, false, cfgName)
		verifyObjectAttrs(ctx, t, cb, filename, len(firstData), true, true, cfgName)

		// Modify the object.
		secondData := append(firstData, []byte("with additional data")...)
		require.NotEqual(t, len(firstData), len(secondData))
		assert.NoError(t, inmem.Upload(ctx, filename, bytes.NewBuffer(secondData)))

		// A call to Attributes() with cache lookup disabled should go to the object storage, but cache the updated value.
		verifyObjectAttrs(WithCacheLookupEnabled(ctx, false), t, cb, filename, len(secondData), false, false, cfgName)
		verifyObjectAttrs(ctx, t, cb, filename, len(secondData), true, true, cfgName)
	})
}

func TestCachingKeyAttributes(t *testing.T) {
	assert.Equal(t, "attrs:/object", cachingKeyAttributes("", "/object"))
	assert.Equal(t, "test:attrs:/object", cachingKeyAttributes("test", "/object"))
}

func TestCachingKeyObjectSubrange(t *testing.T) {
	assert.Equal(t, "subrange:/object:10:20", cachingKeyObjectSubrange("", "/object", 10, 20))
	assert.Equal(t, "test:subrange:/object:10:20", cachingKeyObjectSubrange("test", "/object", 10, 20))
}

func TestCachingKeyIter(t *testing.T) {
	assert.Equal(t, "iter:/object", cachingKeyIter("", "/object"))
	assert.Equal(t, "iter:/object:recursive", cachingKeyIter("", "/object", objstore.WithRecursiveIter))
	assert.Equal(t, "test:iter:/object", cachingKeyIter("test", "/object"))
	assert.Equal(t, "test:iter:/object:recursive", cachingKeyIter("test", "/object", objstore.WithRecursiveIter))
}

func TestCachingKeyExists(t *testing.T) {
	assert.Equal(t, "exists:/object", cachingKeyExists("", "/object"))
	assert.Equal(t, "test:exists:/object", cachingKeyExists("test", "/object"))
}

func TestCachingKeyContent(t *testing.T) {
	assert.Equal(t, "content:/object", cachingKeyContent("", "/object"))
	assert.Equal(t, "test:content:/object", cachingKeyContent("test", "/object"))
}

func TestCachingKey_ShouldKeepAllocationsToMinimum(t *testing.T) {
	tests := map[string]struct {
		run            func(bucketID string)
		expectedAllocs float64
	}{
		"cachingKeyAttributes()": {
			run: func(bucketID string) {
				cachingKeyAttributes(bucketID, "/object")
			},
			expectedAllocs: 1.0,
		},
		"cachingKeyObjectSubrange()": {
			run: func(bucketID string) {
				cachingKeyObjectSubrange(bucketID, "/object", 10, 20)
			},
			expectedAllocs: 1.0,
		},
		"cachingKeyIter()": {
			run: func(bucketID string) {
				cachingKeyIter(bucketID, "/dir")
			},
			expectedAllocs: 2.0,
		},
		"cachingKeyIter() recursive": {
			run: func(bucketID string) {
				cachingKeyIter(bucketID, "/dir", objstore.WithRecursiveIter)
			},
			expectedAllocs: 2.0,
		},
		"cachingKeyExists()": {
			run: func(bucketID string) {
				cachingKeyExists(bucketID, "/object")
			},
			expectedAllocs: 1.0,
		},
		"cachingKeyContent()": {
			run: func(bucketID string) {
				cachingKeyContent(bucketID, "/object")
			},
			expectedAllocs: 1.0,
		},
	}

	for testName, testData := range tests {
		for _, withBucketID := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s with bucket ID: %t", testName, withBucketID), func(t *testing.T) {
				bucketID := ""
				if withBucketID {
					bucketID = "test"
				}

				allocs := testing.AllocsPerRun(100, func() {
					testData.run(bucketID)
				})

				require.Equal(t, testData.expectedAllocs, allocs)
			})
		}
	}
}

func TestMutationInvalidatesCache(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	c := cache.NewMockCache()
	ctx := context.Background()

	const cfgName = "test"
	cfg := NewCachingBucketConfig()
	cfg.CacheGet(cfgName, c, matchAll, 1024, time.Minute, time.Minute, time.Minute, true)
	cfg.CacheExists(cfgName, c, matchAll, time.Minute, time.Minute)
	cfg.CacheAttributes(cfgName, c, matchAll, time.Minute, true)

	cb, err := NewCachingBucket("test", inmem, cfg, nil, nil)
	require.NoError(t, err)

	t.Run("invalidated on upload", func(t *testing.T) {
		c.Flush()

		// Initial upload bypassing the CachingBucket but read the object back to ensure it is in cache.
		require.NoError(t, inmem.Upload(ctx, "/object-1", strings.NewReader("test content 1")))
		verifyGet(ctx, t, cb, "/object-1", []byte("test content 1"), true, false, cfgName)
		verifyExists(ctx, t, cb, "/object-1", true, true, true, cfgName)
		verifyObjectAttrs(ctx, t, cb, "/object-1", 14, true, false, cfgName)

		// Do an upload via the CachingBucket and ensure the first read after does not come from cache.
		require.NoError(t, cb.Upload(ctx, "/object-1", strings.NewReader("test content 12")))
		verifyGet(ctx, t, cb, "/object-1", []byte("test content 12"), true, false, cfgName)
		verifyExists(ctx, t, cb, "/object-1", true, true, true, cfgName)
		verifyObjectAttrs(ctx, t, cb, "/object-1", 15, true, false, cfgName)
	})

	t.Run("invalidated on delete", func(t *testing.T) {
		c.Flush()

		// Initial upload bypassing the CachingBucket but read the object back to ensure it is in cache.
		require.NoError(t, inmem.Upload(ctx, "/object-1", strings.NewReader("test content 1")))
		verifyGet(ctx, t, cb, "/object-1", []byte("test content 1"), true, false, cfgName)
		verifyExists(ctx, t, cb, "/object-1", true, true, true, cfgName)
		verifyObjectAttrs(ctx, t, cb, "/object-1", 14, true, false, cfgName)

		// Delete via the CachingBucket and ensure the first read after does not come from cache but non-existence is cached.
		require.NoError(t, cb.Delete(ctx, "/object-1"))
		verifyGet(ctx, t, cb, "/object-1", nil, true, false, cfgName)
		verifyExists(ctx, t, cb, "/object-1", false, true, true, cfgName)
		verifyObjectAttrs(ctx, t, cb, "/object-1", -1, true, false, cfgName)
	})
}

func BenchmarkCachingKey(b *testing.B) {
	tests := map[string]struct {
		run func(bucketID string)
	}{
		"cachingKeyAttributes()": {
			run: func(bucketID string) {
				cachingKeyAttributes(bucketID, "/object")
			},
		},
		"cachingKeyObjectSubrange()": {
			run: func(bucketID string) {
				cachingKeyObjectSubrange(bucketID, "/object", 10, 20)
			},
		},
		"cachingKeyIter()": {
			run: func(bucketID string) {
				cachingKeyIter(bucketID, "/dir")
			},
		},
		"cachingKeyIter() recursive": {
			run: func(bucketID string) {
				cachingKeyIter(bucketID, "/dir", objstore.WithRecursiveIter)
			},
		},
		"cachingKeyExists()": {
			run: func(bucketID string) {
				cachingKeyExists(bucketID, "/object")
			},
		},
		"cachingKeyContent()": {
			run: func(bucketID string) {
				cachingKeyContent(bucketID, "/object")
			},
		},
	}

	for testName, testData := range tests {
		for _, withBucketID := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s with bucket ID: %t", testName, withBucketID), func(b *testing.B) {
				bucketID := ""
				if withBucketID {
					bucketID = "test"
				}

				b.ResetTimer()

				for n := 0; n < b.N; n++ {
					testData.run(bucketID)
				}
			})
		}
	}
}

func verifyObjectAttrs(ctx context.Context, t *testing.T, cb *CachingBucket, file string, expectedLength int, expectedCacheLookup, expectedFromCache bool, cfgName string) {
	requestsBefore := int(promtest.ToFloat64(cb.operationRequests.WithLabelValues(objstore.OpAttributes, cfgName)))
	hitsBefore := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpAttributes, cfgName)))

	attrs, err := cb.Attributes(ctx, file)
	if expectedLength < 0 {
		assert.True(t, cb.IsObjNotFoundErr(err))
	} else {
		assert.NoError(t, err)
		assert.Equal(t, int64(expectedLength), attrs.Size)
	}

	requestsAfter := int(promtest.ToFloat64(cb.operationRequests.WithLabelValues(objstore.OpAttributes, cfgName)))
	hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpAttributes, cfgName)))

	if expectedCacheLookup {
		assert.Equal(t, 1, requestsAfter-requestsBefore)
	} else {
		assert.Equal(t, 0, requestsAfter-requestsBefore)
	}

	if expectedFromCache {
		assert.Equal(t, 1, hitsAfter-hitsBefore)
	} else {
		assert.Equal(t, 0, hitsAfter-hitsBefore)
	}
}

func matchAll(string) bool { return true }

var chunksMatcher = regexp.MustCompile(`^.*/chunks/\d+$`)

func isTSDBChunkFile(name string) bool { return chunksMatcher.MatchString(name) }
