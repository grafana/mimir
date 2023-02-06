// SPDX-License-Identifier: AGPL-3.0-only

package chunkscache

import (
	"context"

	gotkit_log "github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type Range struct {
	BlockID   ulid.ULID
	Start     chunks.ChunkRef
	NumChunks int
}

type ChunksCache interface {
	FetchMultiChunks(ctx context.Context, userID string, ranges []Range) (hits map[Range][]byte)
	StoreChunks(ctx context.Context, userID string, r Range, v []byte)
}

type TracingCache struct {
	C ChunksCache
}

func (c TracingCache) FetchMultiChunks(ctx context.Context, userID string, ranges []Range) (hits map[Range][]byte) {
	l := spanlogger.FromContext(ctx, gotkit_log.NewNopLogger())
	hits = c.C.FetchMultiChunks(ctx, userID, ranges)

	l.LogFields(
		log.String("name", "ChunksCache.FetchMultiChunks"),
		log.Int("requested", len(ranges)),
		log.Int("hits", len(hits)),
		log.Int("hits_bytes", hitsSize(hits)),
		log.Int("misses", len(ranges)-len(hits)),
	)
	return
}

func hitsSize(hits map[Range][]byte) (size int) {
	for _, b := range hits {
		size += len(b)
	}
	return
}

func (c TracingCache) StoreChunks(ctx context.Context, userID string, r Range, v []byte) {
	c.C.StoreChunks(ctx, userID, r, v)
}
