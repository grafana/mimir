// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// TestUploadPopulatesMeta verifies that block.Upload properly populates the meta parameter
// with file stats and metadata, eliminating the need for separate ReadMetaFromDir and
// GatherFileStats calls after upload.
func TestUploadPopulatesMeta(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	bkt := objstore.NewInMemBucket()

	// Create a test block
	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("b", "1"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "val1"))
	require.NoError(t, err)

	blockDir := filepath.Join(tmpDir, blockID.String())

	// First, read the basic meta information that we would have before calling Upload
	meta, err := block.ReadMetaFromDir(blockDir)
	require.NoError(t, err)

	// Save original file info to compare later
	originalFiles := meta.Thanos.Files

	// Test: Upload should populate meta.Thanos.Files with current file stats
	err = block.Upload(ctx, log.NewNopLogger(), bkt, blockDir, meta)
	require.NoError(t, err)

	// Verify meta was populated with basic metadata (should remain the same)
	require.Equal(t, blockID, meta.ULID)
	require.NotZero(t, meta.Stats.NumSeries, "NumSeries should be populated")
	require.NotZero(t, meta.Stats.NumSamples, "NumSamples should be populated")
	require.NotZero(t, meta.Compaction.Level, "Compaction level should be populated")

	// Verify meta.Thanos.Files was populated/updated by Upload function
	require.NotEmpty(t, meta.Thanos.Files, "Thanos.Files should be populated by Upload")

	// The Upload function should have updated the file stats (they might be the same, but it should have run GatherFileStats)
	require.NotEqual(t, originalFiles, meta.Thanos.Files, "Upload should have refreshed Thanos.Files")

	// Calculate total block size from meta.Thanos.Files
	var totalSize int64
	for _, f := range meta.Thanos.Files {
		if f.SizeBytes > 0 {
			totalSize += f.SizeBytes
		}
	}
	require.Greater(t, totalSize, int64(0), "Total block size should be greater than 0")

	// Verify we have expected files (chunks and index at minimum)
	var hasChunks, hasIndex bool
	for _, f := range meta.Thanos.Files {
		if f.RelPath == "chunks/000001" {
			hasChunks = true
			require.Greater(t, f.SizeBytes, int64(0), "Chunks file should have size > 0")
		}
		if f.RelPath == "index" {
			hasIndex = true
			require.Greater(t, f.SizeBytes, int64(0), "Index file should have size > 0")
		}
	}
	require.True(t, hasChunks, "Should have chunks file in meta.Thanos.Files")
	require.True(t, hasIndex, "Should have index file in meta.Thanos.Files")

	t.Logf("Upload successfully populated meta with %d series, %d samples, %d bytes across %d files",
		meta.Stats.NumSeries, meta.Stats.NumSamples, totalSize, len(meta.Thanos.Files))
}

// TestUploadMetaVersusDirectRead compares the meta populated by Upload
// against directly reading meta and gathering file stats to ensure consistency.
func TestUploadMetaVersusDirectRead(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	bkt := objstore.NewInMemBucket()

	// Create a test block
	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("b", "1"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "val1"))
	require.NoError(t, err)

	blockDir := filepath.Join(tmpDir, blockID.String())

	// Method 1: Upload with meta pointer (our optimized approach)
	uploadedMeta, err := block.ReadMetaFromDir(blockDir)
	require.NoError(t, err)
	err = block.Upload(ctx, log.NewNopLogger(), bkt, blockDir, uploadedMeta)
	require.NoError(t, err)

	// Method 2: Direct read approach (what the original PR was doing)
	directMeta, err := block.ReadMetaFromDir(blockDir)
	require.NoError(t, err)

	directFiles, err := block.GatherFileStats(blockDir)
	require.NoError(t, err)

	// Compare basic metadata
	require.Equal(t, directMeta.ULID, uploadedMeta.ULID)
	require.Equal(t, directMeta.Stats.NumSeries, uploadedMeta.Stats.NumSeries)
	require.Equal(t, directMeta.Stats.NumSamples, uploadedMeta.Stats.NumSamples)
	require.Equal(t, directMeta.Compaction.Level, uploadedMeta.Compaction.Level)

	// Compare file stats
	require.Equal(t, len(directFiles), len(uploadedMeta.Thanos.Files), "Should have same number of files")

	// Calculate sizes from both methods
	var directTotalSize, uploadTotalSize int64
	for _, f := range directFiles {
		directTotalSize += f.SizeBytes
	}
	for _, f := range uploadedMeta.Thanos.Files {
		uploadTotalSize += f.SizeBytes
	}

	require.Equal(t, directTotalSize, uploadTotalSize, "Total sizes should match between methods")

	t.Logf("Both methods report identical stats: %d series, %d samples, %d bytes",
		uploadedMeta.Stats.NumSeries, uploadedMeta.Stats.NumSamples, uploadTotalSize)
}

// TestUploadWithEmptyMeta verifies that passing an empty block.Meta struct
// to Upload works correctly and gets properly populated, which is exactly
// how our optimized compactor code works.
func TestUploadWithEmptyMeta(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	bkt := objstore.NewInMemBucket()

	// Create a test block
	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("b", "1"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "val1"))
	require.NoError(t, err)

	blockDir := filepath.Join(tmpDir, blockID.String())

	// Test: Pass empty Meta struct (like our compactor code does with `var blockMeta block.Meta`)
	var blockMeta block.Meta
	require.Equal(t, block.Meta{}, blockMeta, "blockMeta should start empty")

	err = block.Upload(ctx, log.NewNopLogger(), bkt, blockDir, &blockMeta)
	require.NoError(t, err)

	// Verify that Upload properly populated the empty meta struct
	require.NotEqual(t, block.Meta{}, blockMeta, "blockMeta should no longer be empty after Upload")
	require.Equal(t, blockID, blockMeta.ULID, "ULID should be populated from directory name")
	require.NotZero(t, blockMeta.Stats.NumSeries, "NumSeries should be populated")
	require.NotZero(t, blockMeta.Stats.NumSamples, "NumSamples should be populated")
	require.NotZero(t, blockMeta.Compaction.Level, "Compaction level should be populated")

	// Most importantly: verify Thanos.Files is populated (this is what we use for size calculation)
	require.NotEmpty(t, blockMeta.Thanos.Files, "Thanos.Files should be populated by Upload")

	// Calculate total block size from meta.Thanos.Files (this is what our compactor code does)
	var totalSize int64
	for _, f := range blockMeta.Thanos.Files {
		if f.SizeBytes > 0 {
			totalSize += f.SizeBytes
		}
	}
	require.Greater(t, totalSize, int64(0), "Total block size should be greater than 0")

	// Verify we have expected files
	var hasChunks, hasIndex bool
	for _, f := range blockMeta.Thanos.Files {
		if f.RelPath == "chunks/000001" {
			hasChunks = true
		}
		if f.RelPath == "index" {
			hasIndex = true
		}
	}
	require.True(t, hasChunks, "Should have chunks file")
	require.True(t, hasIndex, "Should have index file")

	t.Logf("Empty Meta successfully populated by Upload: %d series, %d samples, %d bytes across %d files",
		blockMeta.Stats.NumSeries, blockMeta.Stats.NumSamples, totalSize, len(blockMeta.Thanos.Files))
}
