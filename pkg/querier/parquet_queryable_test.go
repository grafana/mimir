// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/parquet_queryable_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

// TestParquetFieldFunctionality tests that the logic to detect if a block has Parquet files or not works.
func TestParquetFieldFunctionality(t *testing.T) {
	t.Run("Block with Parquet metadata", func(t *testing.T) {
		block := &bucketindex.Block{
			ID:      ulid.MustNew(1, nil),
			Parquet: &bucketindex.ConverterMarkMeta{Version: 1},
		}

		require.NotNil(t, block.Parquet)
		require.Equal(t, 1, block.Parquet.Version)
	})

	t.Run("Block without Parquet metadata", func(t *testing.T) {
		block := &bucketindex.Block{
			ID: ulid.MustNew(2, nil),
		}

		require.Nil(t, block.Parquet)
	})

	t.Run("Index ParquetBlocks method", func(t *testing.T) {
		idx := &bucketindex.Index{
			Blocks: bucketindex.Blocks{
				&bucketindex.Block{ID: ulid.MustNew(1, nil), Parquet: &bucketindex.ConverterMarkMeta{Version: 1}},
				&bucketindex.Block{ID: ulid.MustNew(2, nil)},
				&bucketindex.Block{ID: ulid.MustNew(3, nil), Parquet: &bucketindex.ConverterMarkMeta{Version: 1}},
			},
		}

		parquetBlocks := idx.ParquetBlocks()
		require.Len(t, parquetBlocks, 2)
		require.NotNil(t, parquetBlocks[0].Parquet)
		require.NotNil(t, parquetBlocks[1].Parquet)

		nonParquetBlocks := idx.NonParquetBlocks()
		require.Len(t, nonParquetBlocks, 1)
		require.Nil(t, nonParquetBlocks[0].Parquet)
	})
}

// TestParquetQueryableContextFunctions tests the context functions to add and get storage type from
// context work properly
func TestParquetQueryableContextFunctions(t *testing.T) {
	block1 := &bucketindex.Block{ID: ulid.MustNew(1, nil)}
	block2 := &bucketindex.Block{ID: ulid.MustNew(2, nil)}

	t.Run("InjectBlocksIntoContext and ExtractBlocksFromContext", func(t *testing.T) {
		ctx := context.Background()

		// Test extraction from empty context
		blocks, ok := ExtractBlocksFromContext(ctx)
		require.False(t, ok)
		require.Nil(t, blocks)

		// Test injection and extraction
		ctxWithBlocks := InjectBlocksIntoContext(ctx, block1, block2)
		blocks, ok = ExtractBlocksFromContext(ctxWithBlocks)
		require.True(t, ok)
		require.Len(t, blocks, 2)
		require.Equal(t, block1.ID, blocks[0].ID)
		require.Equal(t, block2.ID, blocks[1].ID)
	})

	t.Run("AddBlockStoreTypeToContext", func(t *testing.T) {
		ctx := context.Background()

		// Test with parquet block store type
		ctxWithParquet := AddBlockStoreTypeToContext(ctx, string(parquetBlockStore))
		storeType := getBlockStoreType(ctxWithParquet, tsdbBlockStore)
		require.Equal(t, parquetBlockStore, storeType)

		// Test with tsdb block store type  
		ctxWithTSDB := AddBlockStoreTypeToContext(ctx, string(tsdbBlockStore))
		storeType = getBlockStoreType(ctxWithTSDB, parquetBlockStore)
		require.Equal(t, tsdbBlockStore, storeType)

		// Test with invalid type - should return default
		ctxWithInvalid := AddBlockStoreTypeToContext(ctx, "invalid")
		storeType = getBlockStoreType(ctxWithInvalid, parquetBlockStore)
		require.Equal(t, parquetBlockStore, storeType)
	})
}
