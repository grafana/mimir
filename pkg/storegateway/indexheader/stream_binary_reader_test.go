// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	promtestutil "github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	streamindex "github.com/grafana/mimir/pkg/storegateway/indexheader/index"
	"github.com/grafana/mimir/pkg/storegateway/indexheader/indexheaderpb"
)

// TestStreamBinaryReader_ShouldBuildSparseHeadersFromFileSimple tests if StreamBinaryReader constructs
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
	header, err := bucketAndDiskSparseHeaderLoader{localPath: sparseHeadersPath, logger: log.NewNopLogger()}.SparseHeader()
	require.NoError(t, err)

	err = r.loadFromSparseIndexHeader(header, 3)
	require.NoError(t, err)
}

func TestStreamBinaryReader_SparseHeaderWithPersistanceFailure(t *testing.T) {
	level.Error(level.Info(log.NewLogfmtLogger(os.Stdout))).Log("msg", "test")
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

	// Write sparse index header and index header to disk on first build.
	_, err = NewStreamBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)

	// Delete sparse index header file and object.
	sparseHeadersPath := filepath.Join(blockID.String(), block.SparseIndexHeaderFilename)
	require.NoError(t, os.Remove(path.Join(tmpDir, sparseHeadersPath)))
	require.NoError(t, bkt.Delete(ctx, block.SparseIndexHeaderFilename))

	// Change bucket to one which will fail to persist the sparse index header.
	errBkt := &bucket.ErrorInjectedBucketClient{Bucket: bkt, Injector: func(bucket.Operation, string) error { return fmt.Errorf("test error") }}
	_, err = NewStreamBinaryReader(ctx, log.NewNopLogger(), errBkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err, "error while persisting sparse index header should not affect the creation of StreamBinaryReader")
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

func TestBucketAndDiskSparseHeaderLoader_SparseHeader(t *testing.T) {
	tmpDir := t.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	loader := bucketAndDiskSparseHeaderLoader{
		ctx:       context.Background(),
		bkt:       bkt,
		localPath: path.Join(tmpDir, block.SparseIndexHeaderFilename),
		logger:    log.NewNopLogger(),
	}

	// SparseHeader not found on disk or in object store
	_, err = loader.SparseHeader()
	require.Error(t, err)

	// SparseHeader found on disk
	sparseHeader := &indexheaderpb.Sparse{
		Symbols: &indexheaderpb.Symbols{
			Offsets:      []int64{0, 1, 2, 3, 4},
			SymbolsCount: 6,
		},
		PostingsOffsetTable: &indexheaderpb.PostingOffsetTable{
			Postings: map[string]*indexheaderpb.PostingValueOffsets{
				"a": {
					Offsets:       []*indexheaderpb.PostingOffset{{Value: "1", TableOff: 1}},
					LastValOffset: 5,
				},
			},
		},
	}

	err = loader.PersistSparseHeader(sparseHeader)
	require.NoError(t, err)

	loaded, err := loader.SparseHeader()
	require.NoError(t, err)

	require.Equal(t, sparseHeader, loaded)
}

func TestBucketAndDiskSparseHeaderLoader_AvailableOnlyOnDisk(t *testing.T) {
	tmpDir := t.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	loader := bucketAndDiskSparseHeaderLoader{
		ctx:       context.Background(),
		bkt:       bkt,
		localPath: path.Join(tmpDir, block.SparseIndexHeaderFilename),
		logger:    log.NewNopLogger(),
	}

	// Persist a non-zero-valued sparse header
	sparseHeader := &indexheaderpb.Sparse{
		Symbols: &indexheaderpb.Symbols{
			SymbolsCount: 7,
		},
	}

	err = loader.PersistSparseHeader(sparseHeader)
	assert.NoError(t, err)

	// Sparse header is deleted in the bucket, but invoking SparseHeader() returns the header
	require.NoError(t, bkt.Delete(context.Background(), block.SparseIndexHeaderFilename))

	loaded, err := loader.SparseHeader()
	assert.NoError(t, err)
	assert.Equal(t, sparseHeader, loaded)
}

func TestBucketAndDiskSparseHeaderLoader_AvailableOnlyInBucket(t *testing.T) {
	tmpDir := t.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	loader := bucketAndDiskSparseHeaderLoader{
		ctx:       context.Background(),
		bkt:       bkt,
		localPath: path.Join(tmpDir, block.SparseIndexHeaderFilename),
		logger:    log.NewNopLogger(),
	}

	// Persist a non-zero-valued sparse header
	sparseHeader := &indexheaderpb.Sparse{
		Symbols: &indexheaderpb.Symbols{
			SymbolsCount: 7,
		},
	}

	err = loader.PersistSparseHeader(sparseHeader)
	assert.NoError(t, err)

	// Sparse header is deleted in the bucket, but invoking SparseHeader() returns the header
	require.NoError(t, os.Remove(loader.localPath))

	loaded, err := loader.SparseHeader()
	assert.NoError(t, err)
	assert.Equal(t, sparseHeader, loaded)

	// The header should now be on disk
	require.NoError(t, bkt.Delete(context.Background(), block.SparseIndexHeaderFilename))
	loaded, err = loader.SparseHeader()
	assert.NoError(t, err)
	assert.Equal(t, sparseHeader, loaded)
}

func TestBucketAndDiskSparseHeaderLoader_PersistingMultipleTimes(t *testing.T) {
	tmpDir := t.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	loader := bucketAndDiskSparseHeaderLoader{
		ctx:       context.Background(),
		bkt:       bkt,
		localPath: path.Join(tmpDir, block.SparseIndexHeaderFilename),
		logger:    log.NewNopLogger(),
	}

	// SparseHeader not found on disk or in object store
	_, err = loader.SparseHeader()
	require.Error(t, err)

	// SparseHeader found on disk
	sparseHeader := &indexheaderpb.Sparse{
		Symbols: &indexheaderpb.Symbols{
			Offsets:      []int64{0, 1, 2, 3, 4},
			SymbolsCount: 6,
		},
		PostingsOffsetTable: &indexheaderpb.PostingOffsetTable{
			Postings: map[string]*indexheaderpb.PostingValueOffsets{
				"a": {
					Offsets:       []*indexheaderpb.PostingOffset{{Value: "1", TableOff: 1}},
					LastValOffset: 5,
				},
			},
		},
	}

	err = loader.PersistSparseHeader(sparseHeader)
	require.NoError(t, err)

	loaded, err := loader.SparseHeader()
	require.NoError(t, err)
	require.Equal(t, sparseHeader, loaded)

	sparseHeader.Symbols.SymbolsCount--
	err = loader.PersistSparseHeader(sparseHeader)
	require.NoError(t, err, "Persisting the a different sparse header multiple times should not return an error and only replace the existing sparse header")

	loaded, err = loader.SparseHeader()
	require.NoError(t, err)
	require.Equal(t, sparseHeader, loaded)
}
