// SPDX-License-Identifier: AGPL-3.0-only

package chunkscache

import (
	"context"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

type Key struct {
	BlockID    ulid.ULID
	FirstChunk chunks.ChunkRef // It's easier to rely on the id of the first chunk of a series, because we don't have the series ID at the time of loading the chunks
}

type ChunksCache interface {
	FetchMultiChunks(ctx context.Context, userID string, keys []Key, chunksPool *pool.SafeSlabPool[storepb.AggrChunk], bytesPool *pool.SafeSlabPool[byte]) (hits map[Key][]storepb.AggrChunk)
	StoreChunks(ctx context.Context, userID string, r Key, v []storepb.AggrChunk)
}
