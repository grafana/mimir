package storegateway

import (
	"context"
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

// TODO test: addLoad() error
// TODO test: load() error
func TestCachedChunkReader_Load(t *testing.T) {
	var (
		ctx     = context.Background()
		userID  = "user-1"
		blockID = ulid.MustNew(0, nil)
	)

	expectedSeries := []seriesEntry{
		{
			refs: []chunks.ChunkRef{1},
			chks: []storepb.AggrChunk{{Raw: &storepb.Chunk{Data: []byte{1}}}},
		}, {
			refs: []chunks.ChunkRef{2, 3},
			chks: []storepb.AggrChunk{{Raw: &storepb.Chunk{Data: []byte{2}}}, {Raw: &storepb.Chunk{Data: []byte{3}}}},
		}, {
			refs: []chunks.ChunkRef{4},
			chks: []storepb.AggrChunk{{Raw: &storepb.Chunk{Data: []byte{4}}}},
		},
	}

	tests := map[string]struct {
		setup               func(t *testing.T, cache *inMemoryChunksCache)
		expectedBackendRefs []chunks.ChunkRef
	}{
		"should load all series from backend on 100% cache miss": {
			setup:               func(t *testing.T, cache *inMemoryChunksCache) {},
			expectedBackendRefs: []chunks.ChunkRef{1, 2, 3, 4},
		},
		"should load no series from backend on 100% cache hit": {
			setup: func(t *testing.T, cache *inMemoryChunksCache) {
				cache.StoreChunk(ctx, userID, blockID, expectedSeries[0].refs[0], expectedSeries[0].chks[0].Raw.Data)
				cache.StoreChunk(ctx, userID, blockID, expectedSeries[1].refs[0], expectedSeries[1].chks[0].Raw.Data)
				cache.StoreChunk(ctx, userID, blockID, expectedSeries[1].refs[1], expectedSeries[1].chks[1].Raw.Data)
				cache.StoreChunk(ctx, userID, blockID, expectedSeries[2].refs[0], expectedSeries[2].chks[0].Raw.Data)
			},
			expectedBackendRefs: []chunks.ChunkRef{},
		},
		"should load from backend only series missed from cache": {
			setup: func(t *testing.T, cache *inMemoryChunksCache) {
				cache.StoreChunk(ctx, userID, blockID, expectedSeries[0].refs[0], expectedSeries[0].chks[0].Raw.Data)
				cache.StoreChunk(ctx, userID, blockID, expectedSeries[1].refs[0], expectedSeries[1].chks[0].Raw.Data)
			},
			expectedBackendRefs: []chunks.ChunkRef{3, 4},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualSeries := []seriesEntry{
				{chks: []storepb.AggrChunk{{}}},
				{chks: []storepb.AggrChunk{{}, {}}},
				{chks: []storepb.AggrChunk{{}}},
			}

			cache := newInmemoryChunksCache()
			testData.setup(t, cache)

			backendReader := newChunkReaderMockWithSeries(expectedSeries, nil, nil)
			cachedReader := newCachedChunkReader(context.Background(), userID, blockID, backendReader, cache)
			require.NoError(t, cachedReader.addLoad(chunks.ChunkRef(1), 0, 0))
			require.NoError(t, cachedReader.addLoad(chunks.ChunkRef(2), 1, 0))
			require.NoError(t, cachedReader.addLoad(chunks.ChunkRef(3), 1, 1))
			require.NoError(t, cachedReader.addLoad(chunks.ChunkRef(4), 2, 0))
			require.NoError(t, cachedReader.load(actualSeries, pool.NewSafeSlabPool[byte](chunkBytesSlicePool, chunkBytesSlabSize), newSafeQueryStats()))

			// Ensure all chunks have been loaded.
			for idx, expected := range expectedSeries {
				assert.Equal(t, expected.chks, actualSeries[idx].chks, "idx: %d", idx)
			}

			// Ensure only cache misses have been fetched from the backend.
			actualBackendRefs := make([]chunks.ChunkRef, 0, len(backendReader.toLoad))
			for ref := range backendReader.toLoad {
				actualBackendRefs = append(actualBackendRefs, ref)
			}

			assert.ElementsMatch(t, testData.expectedBackendRefs, actualBackendRefs)
		})
	}
}

type inMemoryChunksCache struct {
	cached map[ulid.ULID]map[chunks.ChunkRef][]byte
}

func newInmemoryChunksCache() *inMemoryChunksCache {
	return &inMemoryChunksCache{
		cached: map[ulid.ULID]map[chunks.ChunkRef][]byte{},
	}
}

func (c *inMemoryChunksCache) FetchMultiChunks(ctx context.Context, userID string, blockID ulid.ULID, refs []chunks.ChunkRef, bytesPool *pool.SafeSlabPool[byte]) (hits map[chunks.ChunkRef][]byte) {
	hits = make(map[chunks.ChunkRef][]byte, len(refs))

	for _, ref := range refs {
		if cached, ok := c.cached[blockID][ref]; ok {
			pooled := bytesPool.Get(len(cached))
			copy(pooled, cached)
			hits[ref] = pooled
		}
	}
	return
}

func (c *inMemoryChunksCache) StoreChunk(ctx context.Context, userID string, blockID ulid.ULID, ref chunks.ChunkRef, data []byte) {
	if c.cached[blockID] == nil {
		c.cached[blockID] = make(map[chunks.ChunkRef][]byte)
	}
	c.cached[blockID][ref] = data
}
