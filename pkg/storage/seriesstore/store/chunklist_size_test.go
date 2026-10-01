// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
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

// Pushing chunks one at a time encodes the list exactly as building it from all of them does.
func TestChunkListPushesEncodeLikeWholeLists(t *testing.T) {
	var metas []ChunkMeta
	var pushed chunkList
	for index := range 500 {
		meta := ChunkMeta{
			Ref:        chunks.Ref(uint64(index/7)<<32 | uint64((index*7919)%100_000)),
			MinTime:    int64(index)*15_000 - int64(index%3)*40_000,
			MaxTime:    int64(index)*15_000 + int64(index%11)*1_000,
			Len:        uint32(index * 37 % 2048),
			Encoding:   uint8(int(chunks.EncodingXOR) + index%3),
			OutOfOrder: index%5 == 0,
		}
		metas = append(metas, meta)
		pushed.push(meta)
		require.Equal(t, chunkListFromMetas(metas), pushed, "after %d chunks", index+1)
		require.Equal(t, len(pushed), cap(pushed))
	}
	require.Equal(t, metas, pushed.toSlice())
}

func BenchmarkChunkListPush(b *testing.B) {
	for _, chunksPerSeries := range []int{10, 1_000} {
		b.Run(fmt.Sprintf("chunks=%d", chunksPerSeries), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				var list chunkList
				for index := range chunksPerSeries {
					list.push(ChunkMeta{Ref: chunks.Ref(index * 1_000), MinTime: int64(index) * 30_000, MaxTime: int64(index)*30_000 + 29_000, Len: 900})
				}
			}
		})
	}
}
