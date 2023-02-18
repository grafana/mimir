// SPDX-License-Identifier: AGPL-3.0-only

package chunkscache

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
)

func TestDskitChunksCache_FetchMultiChunks(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	user1 := "tenant1"
	user2 := "tenant2"
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	range1 := Range{BlockID: block1, Start: chunks.ChunkRef(100), NumChunks: 10}
	range2 := Range{BlockID: block1, Start: chunks.ChunkRef(200), NumChunks: 20}
	range3 := Range{BlockID: block2, Start: chunks.ChunkRef(100), NumChunks: 10}
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup        []mockedChunks
		mockedErr    error
		fetchUserID  string
		fetchRanges  []Range
		expectedHits map[Range][]byte
	}{
		"should return no hits on empty cache": {
			setup:        []mockedChunks{},
			fetchUserID:  user1,
			fetchRanges:  []Range{range1, range2},
			expectedHits: nil,
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedChunks{
				{userID: user1, r: range1, value: value1},
				{userID: user2, r: range2, value: value2},
				{userID: user1, r: range3, value: value3},
			},
			fetchUserID: user1,
			fetchRanges: []Range{range1, range3},
			expectedHits: map[Range][]byte{
				range1: value1,
				range3: value3,
			},
		},
		"should return hits and misses on partial hits": {
			setup: []mockedChunks{
				{userID: user1, r: range1, value: value1},
				{userID: user1, r: range2, value: value2},
			},
			fetchUserID:  user1,
			fetchRanges:  []Range{range1, range3},
			expectedHits: map[Range][]byte{range1: value1},
		},
		"should return no hits on cache error": {
			setup: []mockedChunks{
				{userID: user1, r: range1, value: value1},
				{userID: user1, r: range1, value: value2},
				{userID: user1, r: range1, value: value3},
			},
			mockedErr:    errors.New("mocked error"),
			fetchUserID:  user1,
			fetchRanges:  []Range{range1, range2},
			expectedHits: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cacheClient := newMockedCacheClient(testData.mockedErr)
			c, err := NewChunksCache(log.NewNopLogger(), cacheClient, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			toStore := make(map[string]map[Range][]byte)
			for _, p := range testData.setup {
				if toStore[p.userID] == nil {
					toStore[p.userID] = make(map[Range][]byte)
				}
				toStore[p.userID][p.r] = p.value
			}
			for userID, userRanges := range toStore {
				c.StoreChunks(ctx, userID, userRanges)
			}

			// Fetch postings from cached and assert on it.
			hits := c.FetchMultiChunks(ctx, testData.fetchUserID, testData.fetchRanges)
			assert.Equal(t, testData.expectedHits, hits)

			// Assert on metrics.
			assert.Equal(t, float64(len(testData.fetchRanges)), prom_testutil.ToFloat64(c.requests))
			assert.Equal(t, float64(len(testData.expectedHits)), prom_testutil.ToFloat64(c.hits))

		})
	}
}

func BenchmarkStringCacheKeys(b *testing.B) {
	userID := "tenant"
	rng := Range{BlockID: ulid.MustNew(1, nil), Start: chunks.ChunkRef(200), NumChunks: 20}

	b.Run("chunks", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			chunksKey(userID, rng)
		}
	})

}

type mockedChunks struct {
	userID string
	r      Range
	value  []byte
}

type mockedCacheClient struct {
	cache             map[string][]byte
	mockedGetMultiErr error
}

func newMockedCacheClient(mockedGetMultiErr error) *mockedCacheClient {
	return &mockedCacheClient{
		cache:             map[string][]byte{},
		mockedGetMultiErr: mockedGetMultiErr,
	}
}

func (c *mockedCacheClient) Fetch(_ context.Context, keys []string, _ ...cache.Option) map[string][]byte {
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

func (c *mockedCacheClient) Store(_ context.Context, data map[string][]byte, _ time.Duration) {
	for key, value := range data {
		c.cache[key] = value
	}
}

func (c *mockedCacheClient) Delete(_ context.Context, key string) error {
	delete(c.cache, key)

	return nil
}

func (c *mockedCacheClient) Name() string {
	return "mockedCacheClient"
}
