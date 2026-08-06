// SPDX-License-Identifier: AGPL-3.0-only

package blockvalidation

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	testutil "github.com/grafana/mimir/pkg/util/test"
)

// generateBlockOnDisk writes a real TSDB block under parent, populates
// meta.Thanos.Files from the on-disk contents (block.GenerateBlockFromSpec
// leaves Files empty), and returns the block directory along with its meta.
func generateBlockOnDisk(t *testing.T, parent string) (string, *block.Meta) {
	t.Helper()
	samples := []chunks.Sample{
		testutil.Sample{TS: 1_000, Val: 1.0},
		testutil.Sample{TS: 2_000, Val: 2.0},
		testutil.Sample{TS: 3_000, Val: 3.0},
	}
	makeChunk := func() chunks.Meta {
		c, err := chunks.ChunkFromSamples(samples)
		require.NoError(t, err)
		return c
	}
	specs := []*block.SeriesSpec{
		{Labels: labels.FromStrings("__name__", "metric_a"), Chunks: []chunks.Meta{makeChunk()}},
		{Labels: labels.FromStrings("__name__", "metric_b"), Chunks: []chunks.Meta{makeChunk()}},
	}
	meta, err := block.GenerateBlockFromSpec(parent, specs)
	require.NoError(t, err)
	dir := filepath.Join(parent, meta.ULID.String())
	meta.Thanos.Files = walkBlockFiles(t, dir)
	return dir, meta
}

// walkBlockFiles enumerates the on-disk block files, returning entries
// suitable for meta.Thanos.Files: the meta.json entry carries no size, every
// other entry records the file size.
func walkBlockFiles(t *testing.T, blockDir string) []block.File {
	t.Helper()
	var out []block.File
	err := filepath.WalkDir(blockDir, func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(blockDir, path)
		require.NoError(t, err)
		rel = filepath.ToSlash(rel)
		if rel == block.MetaFilename {
			out = append(out, block.File{RelPath: rel})
			return nil
		}
		info, err := d.Info()
		require.NoError(t, err)
		out = append(out, block.File{RelPath: rel, SizeBytes: info.Size()})
		return nil
	})
	require.NoError(t, err)
	return out
}

func TestCheckBlockOnDisk_NilMeta(t *testing.T) {
	err := CheckBlockOnDisk(context.Background(), log.NewNopLogger(), t.TempDir(), nil, CheckBlockOnDiskOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing block metadata")
}

func TestCheckBlockOnDisk_Valid(t *testing.T) {
	dir, meta := generateBlockOnDisk(t, t.TempDir())
	ctx := context.Background()

	require.NoError(t, CheckBlockOnDisk(ctx, log.NewNopLogger(), dir, meta, CheckBlockOnDiskOptions{CheckChunks: false}))
	require.NoError(t, CheckBlockOnDisk(ctx, log.NewNopLogger(), dir, meta, CheckBlockOnDiskOptions{CheckChunks: true}))
}

func TestCheckBlockOnDisk_MissingFile(t *testing.T) {
	dir, meta := generateBlockOnDisk(t, t.TempDir())
	// Delete the index file declared in meta.
	require.NoError(t, os.Remove(filepath.Join(dir, "index")))

	err := CheckBlockOnDisk(context.Background(), log.NewNopLogger(), dir, meta, CheckBlockOnDiskOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to stat index")
}

func TestCheckBlockOnDisk_SizeMismatch(t *testing.T) {
	dir, meta := generateBlockOnDisk(t, t.TempDir())
	// Truncate the first chunks segment so its on-disk size diverges from the declared size.
	require.NoError(t, os.Truncate(filepath.Join(dir, "chunks", "000001"), 10))

	err := CheckBlockOnDisk(context.Background(), log.NewNopLogger(), dir, meta, CheckBlockOnDiskOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "file size mismatch for chunks/000001")
}

func TestCheckBlockOnDisk_NotARegularFile(t *testing.T) {
	dir, meta := generateBlockOnDisk(t, t.TempDir())
	// Replace the index file with a directory of the same name.
	indexPath := filepath.Join(dir, "index")
	require.NoError(t, os.Remove(indexPath))
	require.NoError(t, os.Mkdir(indexPath, 0o755))

	err := CheckBlockOnDisk(context.Background(), log.NewNopLogger(), dir, meta, CheckBlockOnDiskOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a file: index")
}

func TestCheckBlockOnDisk_MaxBlockSizeEnforced(t *testing.T) {
	dir, meta := generateBlockOnDisk(t, t.TempDir())

	// Sum declared file sizes; cap one byte below that to force a rejection.
	var declared int64
	for _, f := range meta.Thanos.Files {
		declared += f.SizeBytes
	}
	require.Greater(t, declared, int64(0), "expected non-zero declared block size")

	err := CheckBlockOnDisk(context.Background(), log.NewNopLogger(), dir, meta, CheckBlockOnDiskOptions{MaxBlockSizeBytes: declared - 1})
	require.Error(t, err)
	assert.Equal(t, fmt.Sprintf(MaxBlockSizeBytesFormat, declared-1), err.Error())
}

func TestCheckBlockOnDisk_VerifyBlockCatchesMangledIndex(t *testing.T) {
	dir, meta := generateBlockOnDisk(t, t.TempDir())
	// Zero out the first 100 bytes of the index file while preserving its size,
	// so the file-size precheck passes and block.VerifyBlock has to catch the
	// structural damage.
	f, err := os.OpenFile(filepath.Join(dir, "index"), os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(make([]byte, 100), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	err = CheckBlockOnDisk(context.Background(), log.NewNopLogger(), dir, meta, CheckBlockOnDiskOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error validating block")
}
