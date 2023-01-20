// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/memcached_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package chunkscache

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

// TODO add tests with some fuzzing
func TestMemcachedIndexCache_FetchMultiPostings(t *testing.T) {
	// Init some data to conveniently define test cases later one.
	user1 := "tenant1"
	user2 := "tenant2"
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	key1 := Key{BlockID: block1, FirstChunk: chunks.ChunkRef(100)}
	key2 := Key{BlockID: block1, FirstChunk: chunks.ChunkRef(200)}
	key3 := Key{BlockID: block2, FirstChunk: chunks.ChunkRef(100)}
	value1 := []storepb.AggrChunk{{MinTime: 11, MaxTime: 22, Raw: &storepb.Chunk{Data: []byte{1, 2, 3, 4, 5}}}}
	value2 := []storepb.AggrChunk{{MinTime: 33, MaxTime: 44, Raw: &storepb.Chunk{Data: []byte{6, 7, 8, 9, 0}}}}
	value3 := []storepb.AggrChunk{{MinTime: 55, MaxTime: 66, Raw: &storepb.Chunk{Data: []byte{1, 9, 2, 8, 3}}}}

	tests := map[string]struct {
		setup        []mockedChunks
		mockedErr    error
		fetchUserID  string
		fetchKeys    []Key
		expectedHits map[Key][]storepb.AggrChunk
	}{
		"no hits on empty cache": {
			setup:        []mockedChunks{},
			fetchUserID:  user1,
			fetchKeys:    []Key{key1, key2},
			expectedHits: nil,
		},
		"100% hits": {
			setup: []mockedChunks{
				{userID: user1, r: key1, value: value1},
				{userID: user2, r: key2, value: value2},
				{userID: user1, r: key3, value: value3},
			},
			fetchUserID: user1,
			fetchKeys:   []Key{key1, key3},
			expectedHits: map[Key][]storepb.AggrChunk{
				key1: value1,
				key3: value3,
			},
		},
		"partial hits": {
			setup: []mockedChunks{
				{userID: user1, r: key1, value: value1},
				{userID: user1, r: key2, value: value2},
			},
			fetchUserID:  user1,
			fetchKeys:    []Key{key1, key3},
			expectedHits: map[Key][]storepb.AggrChunk{key1: value1},
		},
		"no hits on memcached error": {
			setup: []mockedChunks{
				{userID: user1, r: key1, value: value1},
				{userID: user1, r: key1, value: value2},
				{userID: user1, r: key1, value: value3},
			},
			mockedErr:    errors.New("mocked error"),
			fetchUserID:  user1,
			fetchKeys:    []Key{key1, key2},
			expectedHits: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			memcached := newMockedMemcachedClient(testData.mockedErr)
			c, err := NewMemcachedChunksCache(log.NewNopLogger(), memcached, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreChunks(ctx, p.userID, p.r, p.value)
			}

			// Fetch postings from cached and assert on it.
			bytesPool := pool.NewSafeSlabPool[byte](&pool.TrackedPool{Parent: &sync.Pool{}}, 10000)
			chunksPool := pool.NewSafeSlabPool[storepb.AggrChunk](&pool.TrackedPool{Parent: &sync.Pool{}}, 10000)
			hits := c.FetchMultiChunks(ctx, testData.fetchUserID, testData.fetchKeys, chunksPool, bytesPool)
			assert.Equal(t, testData.expectedHits, hits)

			// Assert on metrics.
			assert.Equal(t, float64(len(testData.fetchKeys)), prom_testutil.ToFloat64(c.requests))
			assert.Equal(t, float64(len(testData.expectedHits)), prom_testutil.ToFloat64(c.hits))

		})
	}
}

func BenchmarkStringCacheKeys(b *testing.B) {
	userID := "tenant"
	rng := Key{BlockID: ulid.MustNew(1, nil), FirstChunk: chunks.ChunkRef(200)}

	b.Run("chunks", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			chunksKey(userID, rng)
		}
	})
}

type mockedChunks struct {
	userID string
	r      Key
	value  []storepb.AggrChunk
}

type mockedMemcachedClient struct {
	cache             map[string][]byte
	mockedGetMultiErr error
}

func newMockedMemcachedClient(mockedGetMultiErr error) *mockedMemcachedClient {
	return &mockedMemcachedClient{
		cache:             map[string][]byte{},
		mockedGetMultiErr: mockedGetMultiErr,
	}
}

func (c *mockedMemcachedClient) GetMulti(_ context.Context, keys []string, _ ...cache.Option) map[string][]byte {
	if c.mockedGetMultiErr != nil {
		return nil
	}

	hits := map[string][]byte{}

	for _, key := range keys {
		if value, ok := c.cache[key]; ok {
			hits[key] = value
		}
	}

	return hits
}

func (c *mockedMemcachedClient) SetAsync(_ context.Context, key string, value []byte, _ time.Duration) error {
	c.cache[key] = value

	return nil
}

func (c *mockedMemcachedClient) Delete(_ context.Context, key string) error {
	delete(c.cache, key)

	return nil
}

func (c *mockedMemcachedClient) Stop() {
	// Nothing to do.
}
