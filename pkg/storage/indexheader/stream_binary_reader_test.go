// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	promtestutil "github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

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
	r, err := NewStreamBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)

	// Read sparse index headers to disk on second build.
	sparseHeadersPath := filepath.Join(tmpDir, blockID.String(), block.SparseIndexHeaderFilename)
	sparseData, err := os.ReadFile(sparseHeadersPath)
	require.NoError(t, err)

	logger := spanlogger.FromContext(context.Background(), log.NewNopLogger())
	err = r.loadFromSparseIndexHeader(logger, sparseData, 3)
	require.NoError(t, err)
}

// TestStreamBinaryReader_CheckSparseHeadersCorrectnessExtensive tests if StreamBinaryReader
// reads and writes sparse index headers accurately for a variety of index-headers.
func TestStreamBinaryReader_CheckSparseHeadersCorrectnessExtensive(t *testing.T) {
	ctx := context.Background()

	for _, nameCount := range []int{3, 20, 50} {
		for _, valueCount := range []int{3, 10, 100, 500} {

			nameSymbols := generateSymbols("name", nameCount)
			valueSymbols := generateSymbols("value", valueCount)

			t.Run(fmt.Sprintf("%vNames%vValues", nameCount, valueCount), func(t *testing.T) {
				t.Parallel()
				tmpDir := t.TempDir()
				bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, bkt.Close()) })

				blockID, err := block.CreateBlock(ctx, tmpDir, generateLabels(nameSymbols, valueSymbols), 100, 0, 1000, labels.FromStrings("ext1", "1"))
				require.NoError(t, err)
				require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil))

				indexFile, err := fileutil.OpenMmapFile(filepath.Join(tmpDir, blockID.String(), block.IndexFilename))
				require.NoError(t, err)
				requireCleanup(t, indexFile.Close)

				b := realByteSlice(indexFile.Bytes())

				// Write sparse index headers to disk on first build.
				_, err = NewStreamBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
				require.NoError(t, err)
				// Read sparse index headers to disk on second build.
				r2, err := NewStreamBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
				require.NoError(t, err)

				// Check correctness of sparse index headers.
				compareIndexToHeader(t, b, r2)
				compareIndexToHeaderPostings(t, b, r2)
			})
		}
	}
}

func TestStreamBinaryReader_LabelValuesOffsetsHonorsContextCancel(t *testing.T) {
	ctx := context.Background()

	tmpDir := filepath.Join(t.TempDir(), "test-stream-binary-reader-cancel")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	seriesCount := streamindex.CheckContextEveryNIterations * 10
	// Create block.
	lbls := make([]labels.Labels, 0, seriesCount)
	for i := 0; i < seriesCount; i++ {
		lbls = append(lbls, labels.FromStrings("a", fmt.Sprintf("%d", i)))
	}
	blockID, err := block.CreateBlock(ctx, tmpDir, lbls, 1, 0, 10, labels.FromStrings("ext1", "1"))
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil))

	// Write sparse index headers to disk on first build.
	r, err := NewStreamBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)

	// LabelValuesOffsets will read all series and check for cancelation every CheckContextEveryNIterations,
	// we set ctx to fail after half of the series are read.
	failAfter := uint64(seriesCount / 2 / streamindex.CheckContextEveryNIterations)
	ctx = &promtestutil.MockContextErrAfter{FailAfter: failAfter}
	_, err = r.LabelValuesOffsets(ctx, "a", "", func(string) bool { return true })
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

// TestStreamBinaryReader_UsesSparseHeaderFromObjectStore tests if StreamBinaryReader uses
// a sparse index header that's already present in the object store instead of recreating it.
func TestStreamBinaryReader_UsesSparseHeaderFromObjectStore(t *testing.T) {
	const samplingRate = 32
	ctx := context.Background()
	logger := log.NewNopLogger()

	tmpDir := filepath.Join(t.TempDir(), "test-sparse-headers-from-objstore")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	// Create block with sample data
	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("b", "3"),
	}, 100, 0, 1000, labels.EmptyLabels())
	require.NoError(t, err)

	// Upload block to bucket
	require.NoError(t, block.Upload(ctx, logger, bkt, filepath.Join(tmpDir, blockID.String()), nil))

	// First, create a StreamBinaryReader to generate the sparse header file
	origReader, err := NewStreamBinaryReader(ctx, logger, bkt, tmpDir, blockID, samplingRate, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)
	require.NoError(t, origReader.Close())

	// Get the generated sparse header file path
	sparseHeadersPath := filepath.Join(tmpDir, blockID.String(), block.SparseIndexHeaderFilename)

	// Read the sparse header file content and save its size
	originalSparseData, err := os.ReadFile(sparseHeadersPath)
	require.NoError(t, err)
	originalSparseHeader, err := decodeSparseData(logger, originalSparseData)
	require.NoError(t, err)

	// Delete the local sparse header file to ensure we'll need to get it from the object store
	require.NoError(t, os.Remove(sparseHeadersPath))

	// Delete the local block directory to ensure nothing is read from local disk
	require.NoError(t, os.RemoveAll(filepath.Join(tmpDir, blockID.String())))

	// Upload the sparse header directly to the object store
	sparseHeaderObjPath := filepath.Join(blockID.String(), block.SparseIndexHeaderFilename)
	require.NoError(t, bkt.Upload(ctx, sparseHeaderObjPath, bytes.NewReader(originalSparseData)))

	// Create a bucket that can track downloads and verify content
	trackedBkt := &trackedBucket{
		BucketReader: bkt,
		expectedPath: sparseHeaderObjPath,
	}

	// Create a new StreamBinaryReader - it should use the sparse header from the object store
	newReader, err := NewStreamBinaryReader(ctx, logger, trackedBkt, tmpDir, blockID, samplingRate, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)
	defer newReader.Close()

	// The sparse header file should have been downloaded from object store
	require.True(t, trackedBkt.getWasCalled, "The sparse header file should have been requested from the bucket")
	require.Equal(t, sparseHeaderObjPath, trackedBkt.downloadedPath, "The correct path should have been downloaded")

	// Verify that the sparse header file exists locally
	newSparseData, err := os.ReadFile(sparseHeadersPath)
	require.NoError(t, err)
	newSparseHeader, err := decodeSparseData(logger, newSparseData)
	require.NoError(t, err)
	require.Equal(t, originalSparseHeader, newSparseHeader, "Downloaded file should have the same size as the original")

	// Check that the reader is functional by performing a label names query
	labelNames, err := newReader.LabelNames(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b"}, labelNames)
}

// trackedBucket wraps a BucketReader and tracks details about downloaded files
type trackedBucket struct {
	objstore.BucketReader
	expectedPath   string
	getWasCalled   bool
	downloadedPath string
}

func (b *trackedBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.getWasCalled = true
	b.downloadedPath = name
	return b.BucketReader.Get(ctx, name)
}
