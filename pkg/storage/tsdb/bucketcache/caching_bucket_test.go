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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/runutil"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/cache"
)

const testFilename = "/random_object"

func TestChunksCaching(t *testing.T) {
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
		expectedLength         int64
		expectedFetchedBytes   int64
		expectedCachedBytes    int64
		expectedRefetchedBytes int64
	}{
		{
			name:                 "basic test",
			offset:               555555,
			length:               55555,
			expectedLength:       55555,
			expectedFetchedBytes: 5 * subrangeSize,
		},

		{
			name:                "same request will hit all subranges in the cache",
			offset:              555555,
			length:              55555,
			expectedLength:      55555,
			expectedCachedBytes: 5 * subrangeSize,
		},

		{
			name:                 "request data close to the end of object",
			offset:               length - 10,
			length:               3000,
			expectedLength:       10,
			expectedFetchedBytes: 8576, // Last (incomplete) subrange is fetched.
		},

		{
			name:                "another request data close to the end of object, cached by previous test",
			offset:              1040100,
			length:              subrangeSize,
			expectedLength:      8476,
			expectedCachedBytes: 8576,
		},

		{
			name:                 "entire object, combination of cached and uncached subranges",
			offset:               0,
			length:               length,
			expectedLength:       length,
			expectedCachedBytes:  5*subrangeSize + 8576, // 5 subrange cached from first test, plus last incomplete subrange.
			expectedFetchedBytes: 60 * subrangeSize,
		},

		{
			name:                "entire object again, everything is cached",
			offset:              0,
			length:              length,
			expectedLength:      length,
			expectedCachedBytes: length, // Entire file is now cached.
		},

		{
			name:                 "entire object again, nothing is cached",
			offset:               0,
			length:               length,
			expectedLength:       length,
			expectedFetchedBytes: length,
			expectedCachedBytes:  0, // Cache is flushed.
			init: func() {
				cache.Flush()
			},
		},

		{
			name:                 "missing first subranges",
			offset:               0,
			length:               10 * subrangeSize,
			expectedLength:       10 * subrangeSize,
			expectedFetchedBytes: 3 * subrangeSize,
			expectedCachedBytes:  7 * subrangeSize,
			init: func() {
				// Delete first 3 subranges.
				cache.Delete(cachingKeyObjectSubrange(name, 0*subrangeSize, 1*subrangeSize))
				cache.Delete(cachingKeyObjectSubrange(name, 1*subrangeSize, 2*subrangeSize))
				cache.Delete(cachingKeyObjectSubrange(name, 2*subrangeSize, 3*subrangeSize))
			},
		},

		{
			name:                 "missing last subranges",
			offset:               0,
			length:               10 * subrangeSize,
			expectedLength:       10 * subrangeSize,
			expectedFetchedBytes: 3 * subrangeSize,
			expectedCachedBytes:  7 * subrangeSize,
			init: func() {
				// Delete last 3 subranges.
				cache.Delete(cachingKeyObjectSubrange(name, 7*subrangeSize, 8*subrangeSize))
				cache.Delete(cachingKeyObjectSubrange(name, 8*subrangeSize, 9*subrangeSize))
				cache.Delete(cachingKeyObjectSubrange(name, 9*subrangeSize, 10*subrangeSize))
			},
		},

		{
			name:                 "missing middle subranges",
			offset:               0,
			length:               10 * subrangeSize,
			expectedLength:       10 * subrangeSize,
			expectedFetchedBytes: 3 * subrangeSize,
			expectedCachedBytes:  7 * subrangeSize,
			init: func() {
				// Delete 3 subranges in the middle.
				cache.Delete(cachingKeyObjectSubrange(name, 3*subrangeSize, 4*subrangeSize))
				cache.Delete(cachingKeyObjectSubrange(name, 4*subrangeSize, 5*subrangeSize))
				cache.Delete(cachingKeyObjectSubrange(name, 5*subrangeSize, 6*subrangeSize))
			},
		},

		{
			name:                 "missing everything except middle subranges",
			offset:               0,
			length:               10 * subrangeSize,
			expectedLength:       10 * subrangeSize,
			expectedFetchedBytes: 7 * subrangeSize,
			expectedCachedBytes:  3 * subrangeSize,
			init: func() {
				// Delete all but 3 subranges in the middle, and keep unlimited number of ranged subrequests.
				for i := int64(0); i < 10; i++ {
					if i > 0 && i%3 == 0 {
						continue
					}
					cache.Delete(cachingKeyObjectSubrange(name, i*subrangeSize, (i+1)*subrangeSize))
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
			init: func() {
				// Delete all but 3 subranges in the middle, but only allow 1 subrequest.
				for i := int64(0); i < 10; i++ {
					if i == 3 || i == 5 || i == 7 {
						continue
					}
					cache.Delete(cachingKeyObjectSubrange(name, i*subrangeSize, (i+1)*subrangeSize))
				}
			},
		},

		{
			name:                 "missing everything except middle subranges, two subrequests",
			offset:               0,
			length:               10 * subrangeSize,
			expectedLength:       10 * subrangeSize,
			expectedFetchedBytes: 7 * subrangeSize,
			expectedCachedBytes:  3 * subrangeSize,
			maxGetRangeRequests:  2,
			init: func() {
				// Delete all but one subranges in the middle, and allow 2 subrequests. They will be: 0-80000, 128000-160000.
				for i := int64(0); i < 10; i++ {
					if i == 5 || i == 6 || i == 7 {
						continue
					}
					cache.Delete(cachingKeyObjectSubrange(name, i*subrangeSize, (i+1)*subrangeSize))
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.init != nil {
				tc.init()
			}

			cfg := NewCachingBucketConfig()
			cfg.CacheGetRange("chunks", cache, isTSDBChunkFile, subrangeSize, cache, time.Hour, time.Hour, tc.maxGetRangeRequests)

			cachingBucket, err := NewCachingBucket(inmem, cfg, nil, nil)
			assert.NoError(t, err)

			verifyGetRange(t, cachingBucket, name, tc.offset, tc.length, tc.expectedLength)
			assert.Equal(t, tc.expectedCachedBytes, int64(promtest.ToFloat64(cachingBucket.fetchedGetRangeBytes.WithLabelValues(originCache, "chunks"))))
			assert.Equal(t, tc.expectedFetchedBytes, int64(promtest.ToFloat64(cachingBucket.fetchedGetRangeBytes.WithLabelValues(originBucket, "chunks"))))
			assert.Equal(t, tc.expectedRefetchedBytes, int64(promtest.ToFloat64(cachingBucket.refetchedGetRangeBytes.WithLabelValues(originCache, "chunks"))))
		})
	}
}

func verifyGetRange(t *testing.T, cachingBucket *CachingBucket, name string, offset, length, expectedLength int64) {
	r, err := cachingBucket.GetRange(context.Background(), name, offset, length)
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

	c, err := NewCachingBucket(b, cfg, nil, nil)
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

	const cfgName = "dirs"
	cfg := NewCachingBucketConfig()
	cfg.CacheIter(cfgName, cache, func(string) bool { return true }, 5*time.Minute, JSONIterCodec{})

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	assert.NoError(t, err)

	verifyIter(t, cb, allFiles, false, cfgName)

	assert.NoError(t, inmem.Upload(context.Background(), "/file-5", strings.NewReader("nazdar")))
	verifyIter(t, cb, allFiles, true, cfgName) // Iter returns old response.

	cache.Flush()
	allFiles = append(allFiles, "/file-5")
	verifyIter(t, cb, allFiles, false, cfgName)

	cache.Flush()

	e := errors.Errorf("test error")

	// This iteration returns false. Result will not be cached.
	assert.Equal(t, e, cb.Iter(context.Background(), "/", func(_ string) error {
		return e
	}))

	// Nothing cached now.
	verifyIter(t, cb, allFiles, false, cfgName)
}

func verifyIter(t *testing.T, cb *CachingBucket, expectedFiles []string, expectedCache bool, cfgName string) {
	hitsBefore := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpIter, cfgName)))

	col := iterCollector{}
	assert.NoError(t, cb.Iter(context.Background(), "/", col.collect))

	hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpIter, cfgName)))

	sort.Strings(col.items)
	assert.Equal(t, expectedFiles, col.items)

	expectedHitsDiff := 0
	if expectedCache {
		expectedHitsDiff = 1
	}

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

	cfg := NewCachingBucketConfig()
	const cfgName = "test"
	cfg.CacheExists(cfgName, cache, matchAll, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	assert.NoError(t, err)

	verifyExists(t, cb, testFilename, false, false, cfgName)

	assert.NoError(t, inmem.Upload(context.Background(), testFilename, strings.NewReader("hej")))
	verifyExists(t, cb, testFilename, false, true, cfgName) // Reused cache result.
	cache.Flush()
	verifyExists(t, cb, testFilename, true, false, cfgName)

	assert.NoError(t, inmem.Delete(context.Background(), testFilename))
	verifyExists(t, cb, testFilename, true, true, cfgName) // Reused cache result.
	cache.Flush()
	verifyExists(t, cb, testFilename, false, false, cfgName)
}

func TestExistsCachingDisabled(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := cache.NewMockCache()

	cfg := NewCachingBucketConfig()
	const cfgName = "test"
	cfg.CacheExists(cfgName, cache, func(string) bool { return false }, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	assert.NoError(t, err)

	verifyExists(t, cb, testFilename, false, false, cfgName)

	assert.NoError(t, inmem.Upload(context.Background(), testFilename, strings.NewReader("hej")))
	verifyExists(t, cb, testFilename, true, false, cfgName)

	assert.NoError(t, inmem.Delete(context.Background(), testFilename))
	verifyExists(t, cb, testFilename, false, false, cfgName)
}

func verifyExists(t *testing.T, cb *CachingBucket, file string, exists, fromCache bool, cfgName string) {
	t.Helper()
	hitsBefore := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpExists, cfgName)))
	ok, err := cb.Exists(context.Background(), file)
	assert.NoError(t, err)
	assert.Equal(t, exists, ok)
	hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpExists, cfgName)))

	if fromCache {
		assert.Equal(t, 1, hitsAfter-hitsBefore)
	} else {
		assert.Equal(t, 0, hitsAfter-hitsBefore)
	}
}

func TestGet(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := cache.NewMockCache()

	cfg := NewCachingBucketConfig()
	const cfgName = "metafile"
	cfg.CacheGet(cfgName, cache, matchAll, 1024, 10*time.Minute, 10*time.Minute, 2*time.Minute)
	cfg.CacheExists(cfgName, cache, matchAll, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	assert.NoError(t, err)

	verifyGet(t, cb, testFilename, nil, false, cfgName)
	verifyExists(t, cb, testFilename, false, true, cfgName)

	data := []byte("hello world")
	assert.NoError(t, inmem.Upload(context.Background(), testFilename, bytes.NewBuffer(data)))

	// Even if file is now uploaded, old data is served from cache.
	verifyGet(t, cb, testFilename, nil, true, cfgName)
	verifyExists(t, cb, testFilename, false, true, cfgName)

	cache.Flush()

	verifyGet(t, cb, testFilename, data, false, cfgName)
	verifyGet(t, cb, testFilename, data, true, cfgName)
	verifyExists(t, cb, testFilename, true, true, cfgName)
}

func TestGetTooBigObject(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := cache.NewMockCache()

	cfg := NewCachingBucketConfig()
	const cfgName = "metafile"
	// Only allow 5 bytes to be cached.
	cfg.CacheGet(cfgName, cache, matchAll, 5, 10*time.Minute, 10*time.Minute, 2*time.Minute)
	cfg.CacheExists(cfgName, cache, matchAll, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	assert.NoError(t, err)

	data := []byte("hello world")
	assert.NoError(t, inmem.Upload(context.Background(), testFilename, bytes.NewBuffer(data)))

	// Object is too big, so it will not be stored to cache on first read.
	verifyGet(t, cb, testFilename, data, false, cfgName)
	verifyGet(t, cb, testFilename, data, false, cfgName)
	verifyExists(t, cb, testFilename, true, true, cfgName)
}

func TestGetPartialRead(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	cache := cache.NewMockCache()

	cfg := NewCachingBucketConfig()
	const cfgName = "metafile"
	cfg.CacheGet(cfgName, cache, matchAll, 1024, 10*time.Minute, 10*time.Minute, 2*time.Minute)
	cfg.CacheExists(cfgName, cache, matchAll, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	assert.NoError(t, err)

	data := []byte("hello world")
	assert.NoError(t, inmem.Upload(context.Background(), testFilename, bytes.NewBuffer(data)))

	// Read only few bytes from data.
	r, err := cb.Get(context.Background(), testFilename)
	assert.NoError(t, err)
	_, err = r.Read(make([]byte, 1))
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	// Object wasn't cached as it wasn't fully read.
	verifyGet(t, cb, testFilename, data, false, cfgName)
	// VerifyGet read object, so now it's cached.
	verifyGet(t, cb, testFilename, data, true, cfgName)
}

func verifyGet(t *testing.T, cb *CachingBucket, file string, expectedData []byte, cacheUsed bool, cfgName string) {
	hitsBefore := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpGet, cfgName)))

	r, err := cb.Get(context.Background(), file)
	if expectedData == nil {
		assert.True(t, cb.IsObjNotFoundErr(err))

		hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpGet, cfgName)))
		if cacheUsed {
			assert.Equal(t, 1, hitsAfter-hitsBefore)
		} else {
			assert.Equal(t, 0, hitsAfter-hitsBefore)
		}
	} else {
		assert.NoError(t, err)
		defer runutil.CloseWithLogOnErr(log.NewNopLogger(), r, "verifyGet")
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, expectedData, data)

		hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpGet, cfgName)))
		if cacheUsed {
			assert.Equal(t, 1, hitsAfter-hitsBefore)
		} else {
			assert.Equal(t, 0, hitsAfter-hitsBefore)
		}
	}
}

func TestAttributes(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := cache.NewMockCache()

	cfg := NewCachingBucketConfig()
	const cfgName = "test"
	cfg.CacheAttributes(cfgName, cache, matchAll, time.Minute)

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	assert.NoError(t, err)

	verifyObjectAttrs(t, cb, testFilename, -1, false, cfgName)
	verifyObjectAttrs(t, cb, testFilename, -1, false, cfgName) // Attributes doesn't cache non-existent files.

	data := []byte("hello world")
	assert.NoError(t, inmem.Upload(context.Background(), testFilename, bytes.NewBuffer(data)))

	verifyObjectAttrs(t, cb, testFilename, len(data), false, cfgName)
	verifyObjectAttrs(t, cb, testFilename, len(data), true, cfgName)
}

func verifyObjectAttrs(t *testing.T, cb *CachingBucket, file string, expectedLength int, cacheUsed bool, cfgName string) {
	t.Helper()
	hitsBefore := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpAttributes, cfgName)))

	attrs, err := cb.Attributes(context.Background(), file)
	if expectedLength < 0 {
		assert.True(t, cb.IsObjNotFoundErr(err))
	} else {
		assert.NoError(t, err)
		assert.Equal(t, int64(expectedLength), attrs.Size)

		hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpAttributes, cfgName)))
		if cacheUsed {
			assert.Equal(t, 1, hitsAfter-hitsBefore)
		} else {
			assert.Equal(t, 0, hitsAfter-hitsBefore)
		}
	}
}

func matchAll(string) bool { return true }

var chunksMatcher = regexp.MustCompile(`^.*/chunks/\d+$`)

func isTSDBChunkFile(name string) bool { return chunksMatcher.MatchString(name) }
