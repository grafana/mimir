// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// TODO: fix tests and add config flag test

// TestStreamBinaryReader_ShouldBuildSparseHeadersFromFile tests if StreamBinaryReader constructs
// and writes sparse index headers on first build and reads from disk on the second build.
func TestStreamBinaryReader_ShouldBuildSparseHeadersFromFileSimple(t *testing.T) {
	ctx := context.Background()

	tmpDir := filepath.Join(t.TempDir(), "test-sparse index headers")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	// Create block.
	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"))
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil))

	// Write sparse index headers to disk on first build.
	r, err := NewStreamBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, true, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)

	// Read sparse index headers to disk on second build.
	sparseHeadersPath := filepath.Join(tmpDir, blockID.String(), block.SparseIndexHeaderFilename)
	sparseData, err := os.ReadFile(sparseHeadersPath)
	require.NoError(t, err)

	err = r.loadFromSparseIndexHeader(ctx, log.NewNopLogger(), blockID, sparseHeadersPath, sparseData, 3)
	require.NoError(t, err)
}

// TestStreamBinaryReader_ShouldNotBuildSparseHeadersFromFile tests if StreamBinaryReader
// follows the config and disables sparse index header persistence.
func TestStreamBinaryReader_ShouldBuildNotSparseHeadersFromFileSimple(t *testing.T) {
	ctx := context.Background()

	tmpDir := filepath.Join(t.TempDir(), "test-sparse index headers")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	// Create block.
	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"))
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil))

	// Write sparse index headers to disk on first build.
	_, err = NewStreamBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, false, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)

	// Read sparse index headers to disk on second build.
	sparseHeadersPath := filepath.Join(tmpDir, blockID.String(), block.SparseIndexHeaderFilename)
	_, err = os.ReadFile(sparseHeadersPath)
	require.Error(t, err)
}

// TestStreamBinaryReader_CheckSparseHeadersCorrectnessExtensive tests if StreamBinaryReader
// reads and writes sparse index headers accurately for a variety of index-headers.
func TestStreamBinaryReader_CheckSparseHeadersCorrectnessExtensive(t *testing.T) {
	ctx := context.Background()

	tmpDir := filepath.Join(t.TempDir(), "test-sparse index headers")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	for _, nameCount := range []int{3, 20, 50} {
		for _, valueCount := range []int{3, 10, 100, 500} {

			nameSymbols := generateSymbols("name", nameCount)
			valueSymbols := generateSymbols("value", valueCount)

			t.Run(fmt.Sprintf("%vNames%vValues", nameCount, valueCount), func(t *testing.T) {
				t.Parallel()

				blockID, err := block.CreateBlock(ctx, tmpDir, generateLabels(nameSymbols, valueSymbols), 100, 0, 1000, labels.FromStrings("ext1", "1"))
				require.NoError(t, err)
				require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil))

				indexFile, err := fileutil.OpenMmapFile(filepath.Join(tmpDir, blockID.String(), block.IndexFilename))
				require.NoError(t, err)
				requireCleanup(t, indexFile.Close)

				b := realByteSlice(indexFile.Bytes())

				// Write sparse index headers to disk on first build.
				_, err = NewStreamBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, true, 3, NewStreamBinaryReaderMetrics(nil), Config{})
				require.NoError(t, err)
				// Read sparse index headers to disk on second build.
				r2, err := NewStreamBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, true, 3, NewStreamBinaryReaderMetrics(nil), Config{})
				require.NoError(t, err)

				// Check correctness of sparse index headers.
				compareIndexToHeader(t, b, r2)
			})
		}
	}
}
