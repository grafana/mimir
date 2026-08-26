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
	"github.com/oklog/ulid/v2"
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
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
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
	_, err = NewStreamBinaryReader(ctx, blockID, bkt, tmpDir, Config{}, 3, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
	require.NoError(t, err)

	// Confirm sparse index headers can be read from disk on subsequent builds.
	_, _, _, err = DownloadAndLoadSparseHeader(ctx, blockID, bkt, tmpDir, 3, log.NewNopLogger())
	require.NoError(t, err)

	// Confirm end-to-end success of second build.
	_, err = NewStreamBinaryReader(ctx, blockID, bkt, tmpDir, Config{}, 3, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
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
				r1, err := NewStreamBinaryReader(ctx, blockID, bkt, tmpDir, Config{}, 3, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
				require.NoError(t, err)
				requireCleanup(t, r1.Close)
				// Read sparse index headers to disk on second build.
				r2, err := NewStreamBinaryReader(ctx, blockID, bkt, tmpDir, Config{}, 3, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
				require.NoError(t, err)
				requireCleanup(t, r2.Close)

				// Check correctness of sparse index headers.
				compareIndexToHeader(t, b, r2)
				compareIndexToHeaderPostings(t, b, r2)
				require.False(t, r2.postingsOffsetTable.IsRemote(),
					"postings offsets should be read from local disk when the bucket reader is disabled")

				// Build the sparse index-header by reading the postings offset table from object storage.
				bucketDir := filepath.Join(tmpDir, "bucket-reader")
				bucketCfg := Config{BucketReader: BucketReaderConfig{
					Enabled:             true,
					BucketIndexSections: SectionPostingsOffsetsTable,
				}}
				r3, err := NewStreamBinaryReader(ctx, blockID, bkt, bucketDir, bucketCfg, 3, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
				require.NoError(t, err)
				requireCleanup(t, r3.Close)
				require.True(t, r3.postingsOffsetTable.IsRemote(),
					"postings offsets should be read from object storage when the bucket reader is enabled")

				// Check correctness of sparse index headers built from the bucket.
				compareIndexToHeader(t, b, r3)
				compareIndexToHeaderPostings(t, b, r3)

				// TODO: the full index-header, including the postings offset table, is still written to local
				//  disk even when th2e bucket reader is enabled. Once WriteBinary can emit a symbols-only
				//  index-header, assert that the one written to bucketDir has no postings offset table.
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
	r, err := NewStreamBinaryReader(ctx, blockID, bkt, tmpDir, Config{}, 3, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
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
	newReader, err := NewStreamBinaryReader(ctx, blockID, bkt, tmpDir, Config{}, 32, logger, NewStreamBinaryReaderMetrics(nil))
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
	origReader, err := NewStreamBinaryReader(ctx, blockID, bkt, tmpDir, Config{}, samplingRate, logger, NewStreamBinaryReaderMetrics(nil))
	require.NoError(t, err)
	require.NoError(t, origReader.Close())

	// Get the generated sparse header file path
	sparseHeadersPath := filepath.Join(tmpDir, blockID.String(), block.SparseIndexHeaderFilename)

	// Read the sparse header file content and save its size
	originalSparseData, err := os.ReadFile(sparseHeadersPath)
	require.NoError(t, err)
	originalSparseHeader, err := unzipSparseHeader(originalSparseData, logger)
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
	newReader, err := NewStreamBinaryReader(ctx, blockID, trackedBkt, tmpDir, Config{}, samplingRate, logger, NewStreamBinaryReaderMetrics(nil))
	require.NoError(t, err)
	defer newReader.Close()

	// The sparse header file should have been downloaded from object store
	require.True(t, trackedBkt.getWasCalled, "The sparse header file should have been requested from the bucket")
	require.Equal(t, sparseHeaderObjPath, trackedBkt.downloadedPath, "The correct path should have been downloaded")

	// Verify that the sparse header file exists locally
	newSparseData, err := os.ReadFile(sparseHeadersPath)
	require.NoError(t, err)
	newSparseHeader, err := unzipSparseHeader(newSparseData, logger)
	require.NoError(t, err)
	require.Equal(t, originalSparseHeader, newSparseHeader, "Downloaded file should have the same size as the original")

	// Check that the reader is functional by performing a label names query
	labelNames, err := newReader.LabelNames(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b"}, labelNames)
}

// TestStreamBinaryReader_IndexHeaderVersionOnDisk tests which index-header format StreamBinaryReader
// ends up with on local disk, for each combination of V2 writing being enabled and of which format
// (if any) an earlier configuration already left on disk.
func TestStreamBinaryReader_IndexHeaderVersionOnDisk(t *testing.T) {
	const samplingRate = 3

	ctx := context.Background()
	logger := log.NewNopLogger()

	tmpDir := t.TempDir()

	ubkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	bkt := objstore.WithNoopInstr(ubkt)

	t.Cleanup(func() {
		require.NoError(t, bkt.Close())
		require.NoError(t, ubkt.Close())
	})

	blockID, err := block.CreateBlock(
		ctx, tmpDir,
		generateLabels(generateSymbols("name", 5), generateSymbols("value", 50)),
		100, 0, 1000, labels.FromStrings("ext1", "1"),
	)
	require.NoError(t, err)
	_, err = block.Upload(ctx, logger, bkt, filepath.Join(tmpDir, blockID.String()), nil)
	require.NoError(t, err)

	indexFile, err := fileutil.OpenMmapFile(filepath.Join(tmpDir, blockID.String(), block.IndexFilename))
	require.NoError(t, err)
	requireCleanup(t, indexFile.Close)
	indexBytes := realByteSlice(indexFile.Bytes())

	// Writing V2 is only coherent alongside the bucket reader, since the postings offsets it omits
	// have to come from somewhere; config validation rejects the other combination.
	writeV2Cfg := Config{BucketReader: BucketReaderConfig{
		Enabled:             true,
		BucketIndexSections: SectionPostingsOffsetsTable,
		WriteV2IndexHeader:  true,
	}}
	require.NoError(t, writeV2Cfg.Validate())

	for _, tc := range []struct {
		name                     string
		extantIndexHeaderVersion string
		cfg                      Config
		expectVersion            int
		expectRemote             bool
	}{
		{
			name: "write-v2 disabled, nothing on disk", extantIndexHeaderVersion: "", cfg: Config{},
			expectVersion: BinaryFormatV1, expectRemote: false,
		},
		{
			name: "write-v2 enabled, nothing on disk", extantIndexHeaderVersion: "", cfg: writeV2Cfg,
			expectVersion: BinaryFormatV2, expectRemote: true,
		},
		{
			name: "write-v2 enabled, v1 on disk", extantIndexHeaderVersion: "v1", cfg: writeV2Cfg,
			expectVersion: BinaryFormatV1, expectRemote: true,
		},
		{
			name: "vwrite-v2 enabled, v2 on disk", extantIndexHeaderVersion: "v2", cfg: writeV2Cfg,
			expectVersion: BinaryFormatV2, expectRemote: true,
		},

		{
			name: "write-v2 disabled, v1 on disk", extantIndexHeaderVersion: "v1", cfg: Config{},
			expectVersion: BinaryFormatV1, expectRemote: false,
		},
		{
			name: "write-v2 disabled, v2 on disk", extantIndexHeaderVersion: "v2", cfg: Config{},
			expectVersion: BinaryFormatV1, expectRemote: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readerDir := filepath.Join(tmpDir, tc.name)

			if tc.extantIndexHeaderVersion != "" {
				seedIndexHeaderOnDisk(t, ctx, bkt, blockID, readerDir, tc.extantIndexHeaderVersion == "v2")
			}

			reader, err := NewStreamBinaryReader(ctx, blockID, bkt, readerDir, tc.cfg, samplingRate, logger, NewStreamBinaryReaderMetrics(nil))
			require.NoError(t, err)
			requireCleanup(t, reader.Close)

			require.Equal(t, tc.expectVersion, reader.IndexHeaderVersion())
			// Assert the format on disk too, not just what the reader reports,
			// so this still fails if the reader and the file ever disagree.
			require.Equal(t, byte(tc.expectVersion), readIndexHeaderFromDisk(t, readerDir, blockID)[4])

			require.Equal(t, tc.expectRemote, reader.postingsOffsetTable.IsRemote())

			// The reader must resolve symbols, label values, and postings correctly against the block index.
			compareIndexToHeader(t, indexBytes, reader)
			compareIndexToHeaderPostings(t, indexBytes, reader)
		})
	}
}

// seedIndexHeaderOnDisk writes an index-header of the given format under dir, standing in for one
// left behind by an earlier configuration.
func seedIndexHeaderOnDisk(t *testing.T, ctx context.Context, bkt objstore.InstrumentedBucketReader, blockID ulid.ULID, dir string, writeV2 bool) {
	t.Helper()

	blockDir := filepath.Join(dir, blockID.String())
	require.NoError(t, os.MkdirAll(blockDir, os.ModePerm))
	require.NoError(t, WriteBinary(ctx, bkt, blockID, filepath.Join(blockDir, block.IndexHeaderFilename), writeV2))
}

// readIndexHeaderFromDisk reads the raw index-header bytes a StreamBinaryReader wrote under dir.
func readIndexHeaderFromDisk(t *testing.T, dir string, blockID ulid.ULID) []byte {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(dir, blockID.String(), block.IndexHeaderFilename))
	require.NoError(t, err)
	require.Greater(t, len(raw), HeaderLen, "index-header on disk is too short to contain its header")

	return raw
}

// readSparseHeaderFromDisk reads back the sparse index-header a StreamBinaryReader wrote under dir.
func readSparseHeaderFromDisk(t *testing.T, dir string, blockID ulid.ULID, logger log.Logger) *indexheaderpb.Sparse {
	t.Helper()

	gzipped, err := os.ReadFile(filepath.Join(dir, blockID.String(), block.SparseIndexHeaderFilename))
	require.NoError(t, err)

	raw, err := unzipSparseHeader(gzipped, logger)
	require.NoError(t, err)

	sparse := &indexheaderpb.Sparse{}
	require.NoError(t, sparse.Unmarshal(raw))

	return sparse
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
