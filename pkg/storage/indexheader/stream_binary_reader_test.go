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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	promtestutil "github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/assert"
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

	ubkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	bkt := objstore.WithNoopInstr(ubkt)

	t.Cleanup(func() {
		require.NoError(t, ubkt.Close())
		require.NoError(t, bkt.Close())
	})

	// Create block.
	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"))
	require.NoError(t, err)
	_, err = block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil)
	require.NoError(t, err)

	// Write sparse index headers to disk on first build.
	r, err := NewStreamBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)

	// Read sparse index headers to disk on second build.
	sparseHeadersPath := filepath.Join(tmpDir, blockID.String(), block.SparseIndexHeaderFilename)
	sparseData, err := os.Open(sparseHeadersPath)
	require.NoError(t, err)

	logger := spanlogger.FromContext(context.Background(), log.NewNopLogger())
	err = r.loadFromSparseIndexHeader(context.Background(), logger, sparseData, 3)
	require.NoError(t, err)
	require.NoError(t, sparseData.Close())
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
				ubkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
				require.NoError(t, err)
				bkt := objstore.WithNoopInstr(ubkt)

				t.Cleanup(func() {
					require.NoError(t, bkt.Close())
					require.NoError(t, ubkt.Close())
				})

				blockID, err := block.CreateBlock(ctx, tmpDir, generateLabels(nameSymbols, valueSymbols), 100, 0, 1000, labels.FromStrings("ext1", "1"))
				require.NoError(t, err)
				_, err = block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil)
				require.NoError(t, err)

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
	ubkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	bkt := objstore.WithNoopInstr(ubkt)

	t.Cleanup(func() {
		require.NoError(t, ubkt.Close())
		require.NoError(t, bkt.Close())
	})

	seriesCount := streamindex.CheckContextEveryNIterations * 10
	// Create block.
	lbls := make([]labels.Labels, 0, seriesCount)
	for i := 0; i < seriesCount; i++ {
		lbls = append(lbls, labels.FromStrings("a", fmt.Sprintf("%d", i)))
	}
	blockID, err := block.CreateBlock(ctx, tmpDir, lbls, 1, 0, 10, labels.FromStrings("ext1", "1"))
	require.NoError(t, err)
	_, err = block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil)
	require.NoError(t, err)

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

func TestStreamBinaryReader_FailedSparseHeaderGetOpsAreNotTracked(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	ctx := context.Background()
	logger := log.NewNopLogger()

	tmpDir := filepath.Join(t.TempDir(), "test-sparse-headers-from-objstore")

	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("b", "3"),
	}, 100, 0, 1000, labels.EmptyLabels())
	require.NoError(t, err)

	ubkt, err := filesystem.NewBucket(tmpDir)
	require.NoError(t, err)

	bkt := objstore.WrapWithMetrics(ubkt, prometheus.WrapRegistererWithPrefix("thanos_", reg), "")
	t.Cleanup(func() {
		require.NoError(t, ubkt.Close())
		require.NoError(t, bkt.Close())
	})

	// Create a new StreamBinaryReader - no sparse index header in object storage to use, will return 4XX on GET.
	newReader, err := NewStreamBinaryReader(ctx, logger, bkt, tmpDir, blockID, 32, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)
	defer newReader.Close()

	// Should not count the failure to get sparse index header in thanos_objstore_bucket_operation_failures_total
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP thanos_objstore_bucket_operation_failures_total Total number of operations against a bucket that failed, but were not expected to fail in certain way from caller perspective. Those errors have to be investigated.
		# TYPE thanos_objstore_bucket_operation_failures_total counter
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="attributes"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="delete"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="exists"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="get"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="get_range"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="iter"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="upload"} 0
		# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
		# TYPE thanos_objstore_bucket_operations_total counter
		thanos_objstore_bucket_operations_total{bucket="",operation="attributes"} 1
		thanos_objstore_bucket_operations_total{bucket="",operation="delete"} 0
		thanos_objstore_bucket_operations_total{bucket="",operation="exists"} 0
		thanos_objstore_bucket_operations_total{bucket="",operation="get"} 1
		thanos_objstore_bucket_operations_total{bucket="",operation="get_range"} 4
		thanos_objstore_bucket_operations_total{bucket="",operation="iter"} 0
		thanos_objstore_bucket_operations_total{bucket="",operation="upload"} 0
	`),
		"thanos_objstore_bucket_operations_total",
		"thanos_objstore_bucket_operation_failures_total",
	))
}

// TestStreamBinaryReader_UsesSparseHeaderFromObjectStore tests if StreamBinaryReader uses
// a sparse index header that's already present in the object store instead of recreating it.
func TestStreamBinaryReader_UsesSparseHeaderFromObjectStore(t *testing.T) {
	const samplingRate = 32
	ctx := context.Background()
	logger := log.NewNopLogger()

	tmpDir := filepath.Join(t.TempDir(), "test-sparse-headers-from-objstore")
	ubkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	bkt := objstore.WithNoopInstr(ubkt)

	t.Cleanup(func() {
		require.NoError(t, bkt.Close())
		require.NoError(t, ubkt.Close())
	})

	// Create block with sample data
	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("b", "3"),
	}, 100, 0, 1000, labels.EmptyLabels())
	require.NoError(t, err)

	// Upload block to bucket
	_, err = block.Upload(ctx, logger, bkt, filepath.Join(tmpDir, blockID.String()), nil)
	require.NoError(t, err)

	// First, create a StreamBinaryReader to generate the sparse header file
	origReader, err := NewStreamBinaryReader(ctx, logger, bkt, tmpDir, blockID, samplingRate, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)
	require.NoError(t, origReader.Close())

	// Get the generated sparse header file path
	sparseHeadersPath := filepath.Join(tmpDir, blockID.String(), block.SparseIndexHeaderFilename)

	// Read the sparse header file content and save its size
	originalSparseData, err := os.ReadFile(sparseHeadersPath)
	require.NoError(t, err)
	originalSparseHeader, err := decodeSparseData(ctx, bytes.NewReader(originalSparseData))
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
		InstrumentedBucketReader: bkt,
	}

	// Create a new StreamBinaryReader - it should use the sparse header from the object store
	newReader, err := NewStreamBinaryReader(ctx, logger, trackedBkt, tmpDir, blockID, samplingRate, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)
	defer newReader.Close()

	// The sparse header file should have been downloaded from object store
	require.True(t, trackedBkt.getWasCalled, "The sparse header file should have been requested from the bucket")
	require.Equal(t, sparseHeaderObjPath, trackedBkt.downloadedPath, "The correct path should have been downloaded")

	// Verify that the sparse header file exists locally
	newSparseData, err := os.Open(sparseHeadersPath)
	require.NoError(t, err)
	newSparseHeader, err := decodeSparseData(ctx, newSparseData)
	require.NoError(t, err)
	require.Equal(t, originalSparseHeader, newSparseHeader, "Downloaded file should have the same size as the original")
	require.NoError(t, newSparseData.Close())

	// Check that the reader is functional by performing a label names query
	labelNames, err := newReader.LabelNames(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b"}, labelNames)
}

// trackedBucket wraps a BucketReader and tracks details about downloaded files
type trackedBucket struct {
	objstore.InstrumentedBucketReader
	getWasCalled   bool
	downloadedPath string
}

func (b *trackedBucket) ReaderWithExpectedErrs(objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return b
}

func (b *trackedBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.getWasCalled = true
	b.downloadedPath = name
	return b.InstrumentedBucketReader.Get(ctx, name)
}
