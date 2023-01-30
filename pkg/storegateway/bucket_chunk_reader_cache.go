package storegateway

import (
	"context"
	"fmt"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/storegateway/chunkscache"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

type cacheLoadEntry struct {
	seriesEntry int
	chunk       int
}

type cachedChunkReader struct {
	ctx     context.Context
	userID  string
	blockID ulid.ULID
	wrapped chunkReader
	cache   chunkscache.ChunksCache

	toLoad map[chunks.ChunkRef]cacheLoadEntry
}

func newCachedChunkReader(ctx context.Context, userID string, blockID ulid.ULID, wrapped chunkReader, cache chunkscache.ChunksCache) *cachedChunkReader {
	return &cachedChunkReader{
		ctx:     ctx,
		userID:  userID,
		blockID: blockID,
		wrapped: wrapped,
		cache:   cache,
		toLoad:  map[chunks.ChunkRef]cacheLoadEntry{},
	}
}

func (r *cachedChunkReader) addLoad(ref chunks.ChunkRef, seriesEntry, chunk int) error {
	r.toLoad[ref] = cacheLoadEntry{
		seriesEntry: seriesEntry,
		chunk:       chunk,
	}

	return nil
}

func (r *cachedChunkReader) load(result []seriesEntry, chunksPool *pool.SafeSlabPool[byte], stats *safeQueryStats) error {
	// Build the list of chunk IDs to fetch.
	refs := make([]chunks.ChunkRef, 0, len(r.toLoad))
	for ref := range r.toLoad {
		refs = append(refs, ref)
	}

	// Lookup the cache.
	hits := r.cache.FetchMultiChunks(r.ctx, r.userID, r.blockID, refs, chunksPool)

	for chunkRef, chunkData := range hits {
		entry, ok := r.toLoad[chunkRef]
		if !ok {
			return fmt.Errorf("chunks cache returned the chunk ref %d but it was not requested", chunkRef)
		}

		result[entry.seriesEntry].chks[entry.chunk].Raw = &storepb.Chunk{
			Type: storepb.Chunk_XOR, // TODO support multiple types
			Data: chunkData,
		}

		// Remove it from the list of chunks to load.
		delete(r.toLoad, chunkRef)
	}

	// No more to do if we load all chunks from cache.
	if len(r.toLoad) == 0 {
		return nil
	}

	// Load all remaining chunks from the wrapped reader.
	for chunkRef, entry := range r.toLoad {
		if err := r.wrapped.addLoad(chunkRef, entry.seriesEntry, entry.chunk); err != nil {
			return err
		}
	}

	if err := r.wrapped.load(result, chunksPool, stats); err != nil {
		return err
	}

	// Store back the loaded chunks to cache.
	for chunkRef, entry := range r.toLoad {
		r.cache.StoreChunk(r.ctx, r.userID, r.blockID, chunkRef, result[entry.seriesEntry].chks[entry.chunk].Raw.Data)
	}

	return nil
}

func (r *cachedChunkReader) reset() {
	for ref := range r.toLoad {
		delete(r.toLoad, ref)
	}

	r.wrapped.reset()
}

func (r *cachedChunkReader) Close() error {
	return r.wrapped.Close()
}
