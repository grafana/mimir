// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/caching_bucket_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
)

func TestIsTenantDir(t *testing.T) {
	assert.False(t, isTenantBlocksDir(""))
	assert.True(t, isTenantBlocksDir("test"))
	assert.True(t, isTenantBlocksDir("test/"))
	assert.False(t, isTenantBlocksDir("test/block"))
	assert.False(t, isTenantBlocksDir("test/block/chunks"))
}

func TestIsBucketIndexFile(t *testing.T) {
	assert.False(t, isBucketIndexFile(""))
	assert.False(t, isBucketIndexFile("test"))
	assert.False(t, isBucketIndexFile("test/block"))
	assert.False(t, isBucketIndexFile("test/block/chunks"))
	assert.True(t, isBucketIndexFile("test/bucket-index.json.gz"))
}

func TestIsBlockIndexFile(t *testing.T) {
	blockID := ulid.MustNew(1, nil)

	assert.False(t, isBlockIndexFile(""))
	assert.False(t, isBlockIndexFile("/index"))
	assert.False(t, isBlockIndexFile("test/index"))
	assert.False(t, isBlockIndexFile("/test/index"))
	assert.True(t, isBlockIndexFile(fmt.Sprintf("%s/index", blockID.String())))
	assert.True(t, isBlockIndexFile(fmt.Sprintf("/%s/index", blockID.String())))
}

func TestIsParquetLabelsFile(t *testing.T) {
	blockID := ulid.MustNew(1, nil)

	assert.True(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/%s/0.labels.parquet", blockID.String())))
	assert.True(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/%s/1.labels.parquet", blockID.String())))

	assert.False(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/%s/labels.parquet", blockID.String())))
	assert.False(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/%s/0.labels.parquet/something", blockID.String())))
	assert.False(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/0.labels.parquet/%s/index", blockID.String())))
	assert.False(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/%s/0.labels.parquet.tmp", blockID.String())))
	assert.False(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/%s/something.0.labels.parquet", blockID.String())))
	assert.False(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/%s/0.labels.parquet.gz", blockID.String())))
}

func TestIsParquetChunksFile(t *testing.T) {
	blockID := ulid.MustNew(1, nil)

	assert.True(t, isParquetChunksFile(fmt.Sprintf("tenant1/%s/0.chunks.parquet", blockID.String())))
	assert.True(t, isParquetChunksFile(fmt.Sprintf("tenant1/%s/1.chunks.parquet", blockID.String())))

	assert.False(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/%s/chunks.parquet", blockID.String())))
	assert.False(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/%s/0.chunks.parquet/something", blockID.String())))
	assert.False(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/0.chunks.parquet/%s/index", blockID.String())))
	assert.False(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/%s/0.chunks.parquet.tmp", blockID.String())))
	assert.False(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/%s/something.0.chunks.parquet", blockID.String())))
	assert.False(t, IsParquetLabelsFile(fmt.Sprintf("tenant1/%s/0.chunks.parquet.gz", blockID.String())))
}
