// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/objtools"
)

// newFilesystemCopyConfig builds a config that copies between two local filesystem
// buckets, leaving all other settings at their defaults.
func newFilesystemCopyConfig(t *testing.T, sourceDir, destDir string) config {
	t.Helper()
	cfg := config{}
	cfg.copyConfig = objtools.NewFilesystemCopyBucketConfig(sourceDir, destDir)
	cfg.tenantConcurrency = 5
	cfg.blockConcurrency = 5
	require.NoError(t, cfg.validate())
	return cfg
}

func blockObj(tenant string, id ulid.ULID, file string) string {
	return tenant + objtools.Delim + id.String() + objtools.Delim + file
}

func markerPath(tenant, marker string) string {
	return tenant + objtools.Delim + marker
}

func metaPath(tenant string, id ulid.ULID) string {
	return blockObj(tenant, id, block.MetaFilename)
}

// newTestSetup creates a pair of temp-dir filesystem buckets and a ready-to-use copy config.
func newTestSetup(t *testing.T) (cfg config, source, dest objtools.Bucket) {
	t.Helper()
	sourceDir, destDir := t.TempDir(), t.TempDir()
	return newFilesystemCopyConfig(t, sourceDir, destDir), fsBucket(t, sourceDir), fsBucket(t, destDir)
}

func fsBucket(t *testing.T, dir string) objtools.Bucket {
	t.Helper()
	bkt, err := (&objtools.FilesystemClientConfig{Directory: dir}).ToBucket()
	require.NoError(t, err)
	return bkt
}

func writeBlock(t *testing.T, bkt objtools.Bucket, tenant string, minT, maxT int64) ulid.ULID {
	t.Helper()
	id := ulid.MustNew(uint64(maxT), rand.Reader)
	meta := block.Meta{
		BlockMeta: tsdb.BlockMeta{
			Version: 1,
			ULID:    id,
			MinTime: minT,
			MaxTime: maxT,
		},
	}
	var buf bytes.Buffer
	require.NoError(t, meta.Write(&buf))

	upload(t, bkt, metaPath(tenant, id), buf.String())
	upload(t, bkt, blockObj(tenant, id, block.IndexFilename), "foo")
	upload(t, bkt, blockObj(tenant, id, block.ChunksDirname+objtools.Delim+"000001"), "bar")
	return id
}

func upload(t *testing.T, bkt objtools.Bucket, name, content string) {
	t.Helper()
	require.NoError(t, bkt.Upload(context.Background(), name, bytes.NewReader([]byte(content)), int64(len(content))))
}

func exists(t *testing.T, bkt objtools.Bucket, name string) bool {
	t.Helper()
	ok, err := bkt.Exists(context.Background(), name, objtools.ExistsOptions{})
	require.NoError(t, err)
	return ok
}

// runOnce performs a single copy cycle and returns the metrics it recorded.
func runOnce(t *testing.T, cfg config) *metrics {
	t.Helper()
	userMapping, err := cfg.parseUserMapping()
	require.NoError(t, err)
	m := newMetrics(prometheus.NewRegistry())
	require.NoError(t, copyBlocks(context.Background(), cfg, userMapping, log.NewNopLogger(), m))
	return m
}

func TestCopyBlocks(t *testing.T) {
	cfg, source, dest := newTestSetup(t)

	id := writeBlock(t, source, "tenant", 1, 2)

	m := runOnce(t, cfg)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.blocksCopied))

	// The block's files were all copied to the destination.
	assert.True(t, exists(t, dest, metaPath("tenant", id)))
	assert.True(t, exists(t, dest, blockObj("tenant", id, block.IndexFilename)))
	assert.True(t, exists(t, dest, blockObj("tenant", id, block.ChunksDirname+objtools.Delim+"000001")))

	// A copy marker was written to the source so the block isn't copied again.
	assert.True(t, exists(t, source, markerPath("tenant", CopiedToBucketMarkFilename(id, dest.Name()))))

	// Re-running is a no-op: the copy marker causes the block to be skipped.
	m = runOnce(t, cfg)
	assert.Equal(t, 0.0, testutil.ToFloat64(m.blocksCopied))
}

func TestCopyBlocksDryRun(t *testing.T) {
	cfg, source, dest := newTestSetup(t)
	cfg.dryRun = true

	id := writeBlock(t, source, "tenant", 1, 2)

	m := runOnce(t, cfg)
	assert.Equal(t, 0.0, testutil.ToFloat64(m.blocksCopied))

	// Nothing was copied and no marker was written.
	assert.False(t, exists(t, dest, metaPath("tenant", id)))
	assert.False(t, exists(t, source, markerPath("tenant", CopiedToBucketMarkFilename(id, dest.Name()))))
}

func TestCopyBlocksTimeFilters(t *testing.T) {
	t.Run("min-time", func(t *testing.T) {
		cfg, source, dest := newTestSetup(t)
		cfg.minTime = flagext.Time(time.UnixMilli(5).UTC())

		oldBlock := writeBlock(t, source, "tenant", 1, 2)   // entirely before the filter
		newBlock := writeBlock(t, source, "tenant", 10, 20) // after the filter

		m := runOnce(t, cfg)
		assert.Equal(t, 1.0, testutil.ToFloat64(m.blocksCopied))
		assert.False(t, exists(t, dest, metaPath("tenant", oldBlock)))
		assert.True(t, exists(t, dest, metaPath("tenant", newBlock)))
	})

	t.Run("max-time", func(t *testing.T) {
		cfg, source, dest := newTestSetup(t)
		cfg.maxTime = flagext.Time(time.UnixMilli(15).UTC())

		earlyBlock := writeBlock(t, source, "tenant", 1, 2)  // before the filter
		lateBlock := writeBlock(t, source, "tenant", 10, 20) // after the filter

		m := runOnce(t, cfg)
		assert.Equal(t, 1.0, testutil.ToFloat64(m.blocksCopied))
		assert.True(t, exists(t, dest, metaPath("tenant", earlyBlock)))
		assert.False(t, exists(t, dest, metaPath("tenant", lateBlock)))
	})
}

func TestCopyBlocksSkipsDeletionMarkedBlocks(t *testing.T) {
	cfg, source, dest := newTestSetup(t)

	id := writeBlock(t, source, "tenant", 1, 2)
	// Write a global deletion marker for the block.
	upload(t, source, markerPath("tenant", block.DeletionMarkFilepath(id)), "{}")

	m := runOnce(t, cfg)
	assert.Equal(t, 0.0, testutil.ToFloat64(m.blocksCopied))
	assert.False(t, exists(t, dest, metaPath("tenant", id)))
}

func TestCopyBlocksAlreadyExistsOnDestination(t *testing.T) {
	cfg, source, dest := newTestSetup(t)

	id := writeBlock(t, source, "tenant", 1, 2)
	// The block's meta.json already exists on the destination.
	upload(t, dest, metaPath("tenant", id), "{}")

	m := runOnce(t, cfg)
	assert.Equal(t, 0.0, testutil.ToFloat64(m.blocksCopied))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.blocksAlreadyExist))

	// A copy marker is still written so the block is skipped next time.
	assert.True(t, exists(t, source, markerPath("tenant", CopiedToBucketMarkFilename(id, dest.Name()))))
}

func TestCopyBlocksUserMapping(t *testing.T) {
	cfg, source, dest := newTestSetup(t)
	cfg.userMapping = flagext.StringSliceCSV{"source-tenant:dest-tenant"}

	id := writeBlock(t, source, "source-tenant", 1, 2)

	m := runOnce(t, cfg)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.blocksCopied))

	// The block is copied under the mapped destination tenant.
	assert.True(t, exists(t, dest, metaPath("dest-tenant", id)))
	assert.False(t, exists(t, dest, metaPath("source-tenant", id)))

	// The copy marker is written under the source tenant.
	assert.True(t, exists(t, source, markerPath("source-tenant", CopiedToBucketMarkFilename(id, dest.Name()))))
}
