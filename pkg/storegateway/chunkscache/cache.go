// SPDX-License-Identifier: AGPL-3.0-only

package chunkscache

import (
	"context"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/util/pool"
)

type ChunksCache interface {
	StoreChunk(ctx context.Context, userID string, blockID ulid.ULID, ref chunks.ChunkRef, data []byte)

	FetchMultiChunks(ctx context.Context, userID string, blockID ulid.ULID, refs []chunks.ChunkRef, bytesPool *pool.SafeSlabPool[byte]) (hits map[chunks.ChunkRef][]byte)
}
