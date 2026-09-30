// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/chunks"
)

// Every stored series keeps its chunk list, so it takes exactly its encoded size.
func TestChunkListsTakeExactlyTheirEncodedSize(t *testing.T) {
	for _, count := range []int{1, 2, 26, 300} {
		metas := make([]ChunkMeta, 0, count)
		for index := range count {
			metas = append(metas, ChunkMeta{
				Ref:      chunks.Ref(uint64(index)<<32 | uint64(index*977)),
				MinTime:  int64(index)*1_800_000 - math.MaxInt32,
				MaxTime:  int64(index)*1_800_000 + int64(index*index),
				Len:      uint32(index * 131),
				Encoding: uint8(chunks.EncodingXOR),
			})
		}
		list := chunkListFromMetas(metas)
		require.Equal(t, len(list), cap(list), "%d chunks", count)
		require.Equal(t, metas, list.toSlice(), "%d chunks", count)
	}
}
