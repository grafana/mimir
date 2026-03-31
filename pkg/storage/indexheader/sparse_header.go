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

	"github.com/grafana/mimir/pkg/util/spanlogger"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/atomicfs"
)

// DownloadSparseHeaderToDisk writes the sparse index-header to disk from the bucket if not already on disk.
// It does not load the sparse header values into memory - to do so, use LoadExistingSparseHeader.
//
// It intended for only a best-effort attempt to get the file downloaded during initial lazy reader creation.
// The lazy reader can ignore the failure to find the sparse header on disk or in the bucket
// and later trigger a full rebuild of the sparse header with BuildSparseHeaderFromIndexHeader.
func DownloadSparseHeaderToDisk(
	ctx context.Context,
	blockID ulid.ULID,
	tenantBkt objstore.InstrumentedBucketReader,
	localTenantDir string,
	ll log.Logger,
) error {
	localBlockDir := filepath.Join(localTenantDir, blockID.String())
	localSparseHeaderPath := filepath.Join(localBlockDir, block.SparseIndexHeaderFilename)

	_, err := os.Stat(localSparseHeaderPath)
	if err == nil {
		// The header is already on disk
		return nil
	}

	level.Debug(ll).Log("msg", "sparse index-header does not exist on disk; will try bucket")
	bucketSparseHeaderBytes, err := GetBucketSparseHeaderBytes(ctx, blockID, tenantBkt, ll)
	if err != nil {
		return err
	}

	return atomicfs.CreateFile(localSparseHeaderPath, bytes.NewReader(bucketSparseHeaderBytes))
}

// LoadExistingSparseHeader unmarshalls the gzipped proto sparse index-header to the in-memory representation
// after writing the sparse index-header to disk from the bucket if not already on disk.
//
// The in-memory representation is required to initialize a StreamBinaryReader for querying.
// If this fails, the caller must trigger a full rebuild of the sparse header with BuildSparseHeaderFromIndexHeader.
func LoadExistingSparseHeader(
	ctx context.Context,
	blockID ulid.ULID,
	tenantBkt objstore.InstrumentedBucketReader,
	localTenantDir string,
	sparseSampleFactor int,
	ll log.Logger,
) (
	allSymbolsCount int,
	sparseSymbolsOffsets []int,
	sparsePostingsOffsets map[string]*streamindex.SparseTableOffsetsForLabel,
	err error,
) {
	spanLog, ctx := spanlogger.New(ctx, ll, tracer, "indexheader.LoadExistingSparseHeader")
	defer spanLog.Finish()

	localBlockDir := filepath.Join(localTenantDir, blockID.String())
	localSparseHeaderPath := filepath.Join(localBlockDir, block.SparseIndexHeaderFilename)

	// First, try to read sparse index-header gzipped proto from previous write to local disk
	gzipSparseHeaderBytes, err := os.ReadFile(localSparseHeaderPath)
	if err != nil {
		// Not on disk, next try to read from bucket
		level.Info(spanLog).Log(
			"msg", "sparse index-header not found on local disk; will try bucket",
			"path", localSparseHeaderPath, "err", err,
		)
		gzipSparseHeaderBytes, err = GetBucketSparseHeaderBytes(ctx, blockID, tenantBkt, ll)
		if err != nil {
			// Not present or not readable from bucket either
			if tenantBkt.IsObjNotFoundErr(err) {
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
	if sparseHeaderBytes, err := UnzipSparseHeader(gzipSparseHeaderBytes, ll); err != nil {
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

// GetBucketSparseHeaderBytes reads the raw sparse header bytes from object storage.
// The bucket reader passed in must be prefixed with the tenant ID TSDB path in the object storage.
// This does not write the header bytes to local disk.
func GetBucketSparseHeaderBytes(
	ctx context.Context,
	id ulid.ULID,
	tenantBkt objstore.InstrumentedBucketReader,
	ll log.Logger,
) ([]byte, error) {
	if tenantBkt == nil {
		return nil, fmt.Errorf("bucket is nil")
	}
	sparseHeaderObjPath := filepath.Join(id.String(), block.SparseIndexHeaderFilename)

	reader, err := tenantBkt.ReaderWithExpectedErrs(tenantBkt.IsObjNotFoundErr).Get(ctx, sparseHeaderObjPath)
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithLogOnErr(ll, reader, "failed to close sparse index-header bucket read")

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func UnzipSparseHeader(gZippedSparseData []byte, ll log.Logger) ([]byte, error) {
	gzipped := bytes.NewReader(gZippedSparseData)
	gzipReader, err := gzip.NewReader(gzipped)
	if err != nil {
		return nil, fmt.Errorf("failed to create sparse index-header gzip reader: %w", err)
	}
	defer runutil.CloseWithLogOnErr(ll, gzipReader, "failed to close sparse index-header gzip reader")

	sparseData, err := io.ReadAll(gzipReader)
	if err != nil {
		return nil, fmt.Errorf("failed to unzip sparse index-header: %w", err)
	}

	return sparseData, err
}

func BuildSparseHeaderFromIndexHeader(
	toc *TOCCompat,
	decbufFactory streamencoding.DecbufFactory,
	sparseSampleFactor int,
	doChecksum bool,
	ll log.Logger,
) (
	allSymbolsCount int,
	sparseSymbolsOffsets []int,
	sparsePostingsOffsets map[string]*streamindex.SparseTableOffsetsForLabel,
	err error,
) {
	start := time.Now()
	defer func() {
		level.Info(ll).Log("msg", "created sparse index-header from full index-header", "elapsed", time.Since(start))
	}()

	level.Info(ll).Log("msg", "creating sparse index-header from full index-header")

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

func WriteSparseHeaderProtoToDisk(path string, sparseHeaders *indexheaderpb.Sparse) error {
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
