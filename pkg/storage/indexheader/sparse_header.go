// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/binary_reader.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/atomicfs"
	"github.com/grafana/mimir/pkg/util/filepool"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// downloadSparseHeaderToDisk writes the sparse index-header to disk from the bucket if not already on disk.
// It does not load the sparse header values into memory - to do so, use DownloadAndLoadSparseHeader.
//
// It intended for only a best-effort attempt to get the file downloaded during initial lazy reader creation.
// The lazy reader can ignore the failure to find the sparse header on disk or in the bucket
// and later trigger a full rebuild of the sparse header with buildInMemorySparseHeaderFromIndexHeader.
func downloadSparseHeaderToDisk(
	ctx context.Context,
	blockID ulid.ULID,
	bkt objstore.InstrumentedBucketReader,
	dir string,
	l log.Logger,
) error {
	spanLog, ctx := spanlogger.New(ctx, l, tracer, "indexheader.DownloadSparseHeaderToDisk")
	defer spanLog.Finish()

	localBlockDir := filepath.Join(dir, blockID.String())
	localSparseHeaderPath := filepath.Join(localBlockDir, block.SparseIndexHeaderFilename)

	_, err := os.Stat(localSparseHeaderPath)
	if err == nil {
		// The header is already on disk
		return nil
	}

	level.Debug(l).Log("msg", "sparse index-header does not exist on disk; will try bucket")
	bucketSparseHeaderBytes, err := getBucketSparseHeaderBytes(ctx, blockID, bkt, l)
	if err != nil {
		return err
	}

	return atomicfs.CreateFile(localSparseHeaderPath, bytes.NewReader(bucketSparseHeaderBytes))
}

// LoadSparseIndexHeaderFromDisk unmarshalls gzipped proto sparse index-header to the in-memory representation,
// only if the sparse header already exists on disk; otherwise it returns error.
func LoadSparseIndexHeaderFromDisk(
	ctx context.Context,
	blockID ulid.ULID,
	dir string,
	sparseSampleFactor int,
	l log.Logger,
) (
	allSymbolsCount int,
	sparseSymbolsOffsets []int,
	sparsePostingsOffsets map[string]*streamindex.SparseTableOffsetsForLabel,
	err error,
) {
	spanLog, _ := spanlogger.New(ctx, l, tracer, "indexheader.DownloadAndLoadSparseHeader")
	defer spanLog.Finish()

	localBlockDir := filepath.Join(dir, blockID.String())
	localSparseHeaderPath := filepath.Join(localBlockDir, block.SparseIndexHeaderFilename)

	gzipSparseHeaderBytes, err := os.ReadFile(localSparseHeaderPath)
	if err != nil {
		level.Error(spanLog).Log(
			"msg", "sparse index-header does not exist on disk",
			"err", err,
		)
		return 0, nil, nil, err
	}

	sparseHeaderProto := &indexheaderpb.Sparse{}
	if sparseHeaderBytes, err := unzipSparseHeader(gzipSparseHeaderBytes, l); err != nil {
		level.Error(spanLog).Log(
			"msg", "failed to unzip sparse index-header file",
			"err", err,
		)
		return 0, nil, nil, err
	} else if err := sparseHeaderProto.Unmarshal(sparseHeaderBytes); err != nil {
		level.Error(spanLog).Log(
			"msg", "failed to unmarshall zipped sparse index-header to proto",
			"err", err,
		)
		return 0, nil, nil, err
	}

	allSymbolsCount, sparseSymbolsOffsets = streamindex.SparseSymbolsFromProto(sparseHeaderProto.Symbols)
	sparsePostingsOffsets, err = streamindex.SparsePostingsOffsetsTableFromProto(
		sparseHeaderProto.PostingsOffsetTable, sparseSampleFactor,
	)
	if err != nil {
		level.Error(spanLog).Log(
			"msg", "failed to initialize in-memory sparse index-header from proto",
			"err", err,
		)
		return 0, nil, nil, err
	}
	return allSymbolsCount, sparseSymbolsOffsets, sparsePostingsOffsets, nil
}

// DownloadAndLoadSparseHeader unmarshalls gzipped proto sparse index-header to the in-memory representation,
// after loading from the existing file on disk or downloading the file from the bucket.
//
// The in-memory representation is required to initialize a StreamBinaryReader for querying.
// If this fails, the caller must trigger a full rebuild of the sparse header with buildInMemorySparseHeaderFromIndexHeader.
//
// This method is more efficient than calling downloadSparseHeaderToDisk followed by LoadSparseIndexHeaderFromDisk,
// as it returns the in-memory representation from the bucket directly, rather than writing to disk then reading from disk.
func DownloadAndLoadSparseHeader(
	ctx context.Context,
	blockID ulid.ULID,
	bkt objstore.InstrumentedBucketReader,
	dir string,
	sparseSampleFactor int,
	l log.Logger,
) (
	allSymbolsCount int,
	sparseSymbolsOffsets []int,
	sparsePostingsOffsets map[string]*streamindex.SparseTableOffsetsForLabel,
	err error,
) {
	spanLog, ctx := spanlogger.New(ctx, l, tracer, "indexheader.DownloadAndLoadSparseHeader")
	defer spanLog.Finish()

	localBlockDir := filepath.Join(dir, blockID.String())
	localSparseHeaderPath := filepath.Join(localBlockDir, block.SparseIndexHeaderFilename)

	// First, try to read sparse index-header gzipped proto from previous write to local disk
	gzipSparseHeaderBytes, err := os.ReadFile(localSparseHeaderPath)
	if err != nil {
		// Not on disk, next try to read from bucket
		level.Info(spanLog).Log(
			"msg", "sparse index-header not found on local disk; will try bucket",
			"path", localSparseHeaderPath, "err", err,
		)
		gzipSparseHeaderBytes, err = getBucketSparseHeaderBytes(ctx, blockID, bkt, l)
		if err != nil {
			// Not present or not readable from bucket either
			if bkt.IsObjNotFoundErr(err) {
				level.Info(spanLog).Log("msg", "sparse index-header not found in bucket")
			} else {
				// Any other error besides object does not exist should be logged at error level
				level.Error(spanLog).Log(
					"msg", "failed to read existing sparse index-header from bucket",
					"err", err,
				)
			}
			return 0, nil, nil, err
		}

		// Successfully read gzipped proto bytes from bucket; attempt to write to disk.
		if err := atomicfs.CreateFile(localSparseHeaderPath, bytes.NewReader(gzipSparseHeaderBytes)); err != nil {
			// Log an error in case there are disk issues, but we can still continue.
			level.Error(spanLog).Log(
				"msg", "failed to write bucket sparse index-header to disk",
				"err", err,
			)
		}
	}

	// If we reach this point, we got the zipped sparse header from disk or bucket. Unmarshall the proto.
	sparseHeaderProto := &indexheaderpb.Sparse{}
	if sparseHeaderBytes, err := unzipSparseHeader(gzipSparseHeaderBytes, l); err != nil {
		level.Error(spanLog).Log(
			"msg", "failed to unzip sparse index-header file",
			"err", err,
		)
		return 0, nil, nil, err
	} else if err := sparseHeaderProto.Unmarshal(sparseHeaderBytes); err != nil {
		level.Error(spanLog).Log(
			"msg", "failed to unmarshall zipped sparse index-header to proto",
			"err", err,
		)
		return 0, nil, nil, err
	}

	// Finally, convert from proto to the in-memory representation used by index-header readers.
	allSymbolsCount, sparseSymbolsOffsets = streamindex.SparseSymbolsFromProto(sparseHeaderProto.Symbols)
	sparsePostingsOffsets, err = streamindex.SparsePostingsOffsetsTableFromProto(
		sparseHeaderProto.PostingsOffsetTable, sparseSampleFactor,
	)
	if err != nil {
		level.Error(spanLog).Log(
			"msg", "failed to initialize in-memory sparse index-header from proto",
			"err", err,
		)
		return 0, nil, nil, err
	}
	return allSymbolsCount, sparseSymbolsOffsets, sparsePostingsOffsets, nil
}

// getBucketSparseHeaderBytes reads the raw sparse header bytes from object storage.
// The bucket reader passed in must be prefixed with the tenant ID TSDB path in the object storage.
// This does not write the header bytes to local disk.
func getBucketSparseHeaderBytes(
	ctx context.Context,
	blockID ulid.ULID,
	bkt objstore.InstrumentedBucketReader,
	l log.Logger,
) ([]byte, error) {
	if bkt == nil {
		return nil, fmt.Errorf("bucket is nil")
	}
	sparseHeaderObjPath := filepath.Join(blockID.String(), block.SparseIndexHeaderFilename)

	reader, err := bkt.ReaderWithExpectedErrs(bkt.IsObjNotFoundErr).Get(ctx, sparseHeaderObjPath)
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithLogOnErr(l, reader, "failed to close sparse index-header bucket read")

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func unzipSparseHeader(gZippedSparseData []byte, l log.Logger) ([]byte, error) {
	gzipReader, err := gzip.NewReader(bytes.NewReader(gZippedSparseData))
	if err != nil {
		return nil, fmt.Errorf("failed to create sparse index-header gzip reader: %w", err)
	}
	defer runutil.CloseWithLogOnErr(l, gzipReader, "failed to close sparse index-header gzip reader")

	sparseData, err := io.ReadAll(gzipReader)
	if err != nil {
		return nil, fmt.Errorf("failed to unzip sparse index-header: %w", err)
	}

	return sparseData, err
}

// BuildAndWriteSparseHeaderFromTSDBIndex builds the sparse index-header from the full Prometheus block index on disk
// and writes it to disk in the same directory.
// This can be used to generate sparse headers in components with a full block index on disk:
// classic ingesters, block-builders, and compactors.
func BuildAndWriteSparseHeaderFromTSDBIndex(
	blockID ulid.ULID,
	dir string,
	sparseSampleFactor int,
	l log.Logger,
) (err error) {
	metrics := filepool.NewFilePoolMetrics(nil)

	blockDir := filepath.Join(dir, blockID.String())
	indexPath := filepath.Join(blockDir, block.IndexFilename)
	sparseHeaderPath := filepath.Join(blockDir, block.SparseIndexHeaderFilename)

	filePoolDecbufFactory := streamencoding.NewFilePoolDecbufFactory(indexPath, 0, metrics)
	defer runutil.CloseWithErrCapture(&err, filePoolDecbufFactory, "build sparse index header")

	indexTOC, err := tocFromDiskTSDBIndex(blockID, dir)
	if err != nil {
		return err
	}

	allSymbolsCount, sparseSymbolsOffsets, sparsePostingsOffsets, err := buildInMemorySparseHeaderFromIndexHeader(
		indexTOC,
		filePoolDecbufFactory,
		sparseSampleFactor,
		false, l,
	)
	if err != nil {
		return fmt.Errorf("cannot build sparse index-header values from full index: %w", err)
	}

	sparseHeaderProto := &indexheaderpb.Sparse{
		Symbols:             streamindex.SparseSymbolsToProto(allSymbolsCount, sparseSymbolsOffsets),
		PostingsOffsetTable: streamindex.SparsePostingsOffsetsTableToProto(sparsePostingsOffsets, sparseSampleFactor),
	}

	if err := writeSparseHeaderProtoToDisk(sparseHeaderPath, sparseHeaderProto, l); err != nil {
		return fmt.Errorf("failed to write sparse index-header to disk in protobuf format: %w", err)
	}

	return nil
}

func buildInMemorySparseHeaderFromIndexHeader(
	toc *TOCCompat,
	decbufFactory streamencoding.DecbufFactory,
	sparseSampleFactor int,
	doChecksum bool,
	l log.Logger,
) (
	allSymbolsCount int,
	sparseSymbolsOffsets []int,
	sparsePostingsOffsets map[string]*streamindex.SparseTableOffsetsForLabel,
	err error,
) {
	start := time.Now()
	defer func() {
		if err != nil {
			level.Info(l).Log("msg", "created sparse index-header from full index-header", "elapsed", time.Since(start))
		}
	}()
	level.Info(l).Log("msg", "creating sparse index-header from full index-header")

	allSymbolsCount, sparseSymbolsOffsets, err = streamindex.SparseValuesFromSymbolsTable(
		decbufFactory, int(toc.Symbols), doChecksum,
	)
	if err != nil {
		return -1, nil, nil, err
	}

	sparsePostingsOffsets, err = streamindex.SparseValuesFromPostingsOffsetsTable(decbufFactory, int(toc.PostingsOffsetTable), toc.PostingsListEnd, sparseSampleFactor, doChecksum)
	if err != nil {
		return -1, nil, nil, err
	}

	return allSymbolsCount, sparseSymbolsOffsets, sparsePostingsOffsets, nil
}

func writeSparseHeaderProtoToDisk(path string, sparseHeaders *indexheaderpb.Sparse, l log.Logger) (err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			level.Info(l).Log("msg", "wrote sparse index-header to disk in protobuf format", "elapsed", time.Since(start))
		}
	}()
	level.Info(l).Log("msg", "writing sparse index-header to disk in protobuf format")

	out, err := sparseHeaders.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshall sparse index-header: %w", err)
	}

	gzipped := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(gzipped)

	if _, err := gzipWriter.Write(out); err != nil {
		return fmt.Errorf("failed to gzip sparse index-header: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close sparse index-header gzip writer: %w", err)
	}

	return atomicfs.CreateFile(path, gzipped)
}
