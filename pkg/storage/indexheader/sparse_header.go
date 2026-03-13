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
)

// ToSparseSymbols loads all symbols data into a sparse protobuf format to be persisted to disk
func SparseSymbolsFromOffsets(allSymbolsCount int, sparseOffsets []int) (sparse *indexheaderpb.Symbols) {
	sparseSymbols := &indexheaderpb.Symbols{}

	offsets := make([]int64, len(sparseOffsets))
	for i, offset := range sparseOffsets {
		offsets[i] = int64(offset)
	}

	sparseSymbols.Offsets = offsets
	sparseSymbols.SymbolsCount = int64(allSymbolsCount)

	return sparseSymbols
}

// DownloadOrGenerateSparseHeader loads the sparse header from disk, object store, or constructs it from the index-header.
// It prioritizes: 1) Local file 2) Object store 3) Generating from the index-header
// It returns an error if the sparse header cannot be loaded from any of the sources.
// If the sparse header was not found on disk, it will try to write it after generating or downloading it. If writing fails, DownloadOrGenerateSparseHeader does not return an error.
func DownloadOrGenerateSparseHeader(
	ctx context.Context,
	blockID ulid.ULID,
	tenantBkt objstore.InstrumentedBucketReader,
	localTenantDir string,
	postingOffsetsInMemSampling int,
	cfg Config, ll log.Logger,
) error {
	localBlockDir := filepath.Join(localTenantDir, blockID.String())
	localSparseHeaderPath := filepath.Join(localBlockDir, block.SparseIndexHeaderFilename)

	// Check if sparse header is already on disk or download if it exists in the bucket block
	err := downloadBucketSparseHeader(ctx, blockID, tenantBkt, localTenantDir, ll)
	if err != nil && !tenantBkt.IsObjNotFoundErr(err) {
		return err
	}

	r := &StreamBinaryReader{
		factory: streamencoding.NewFilePoolDecbufFactory(binPath, cfg.MaxIdleFileHandles, metrics.filePool),
	}

	if r.toc, r.indexHeaderVersion, err = TOCFromIndexHeader(castagnoliTable, r.factory, logger); err != nil {
		return nil, fmt.Errorf("cannot read table-of-contents: %w", err)
	}

	// Load symbols and postings offset table
	if err = r.loadSparseHeader(ctx, logger, cfg, postingOffsetsInMemSampling, sparseHeadersPath, bkt, id); err != nil {
		return nil, fmt.Errorf("cannot load sparse index-header: %w", err)
	}

	// Sparse header is not already on disk and does not exist in the bucket block
	buildSparseHeaderFromIndexHeader(
		r.toc, r.factory, postingOffsetsInMemSampling, cfg.VerifyOnLoad, ll,
	)

	if err = loadFromIndexHeader(ll, cfg, postingOffsetsInMemSampling); err != nil {
		return fmt.Errorf("cannot load sparse index-header from full index-header: %w", err)
	}
	level.Info(ll).Log("msg", "generated sparse index-header from full index-header")

	sparseHeaders := &indexheaderpb.Sparse{}
	sparseHeaders.Symbols = r.symbols.ToSparseSymbols()
	sparseHeaders.PostingsOffsetTable = r.postingsOffsetTable.ToSparsePostingOffsetTable()
	tryWriteSparseHeadersToFile(ll, sparseHeadersPath, sparseHeaders)

	// 1. Try to load from local file first
	localSparseHeaderBytes, err := os.ReadFile(sparseHeadersPath)
	if err == nil {
		level.Debug(ll).Log("msg", "loading sparse index-header from local disk")
		err = loadFromSparseIndexHeader(ll, localSparseHeaderBytes, postingOffsetsInMemSampling)
		if err == nil {
			return nil
		}
		level.Warn(ll).Log("msg", "failed to load sparse index-header from disk; will try bucket", "err", err)
	} else if os.IsNotExist(err) {
		level.Debug(ll).Log("msg", "sparse index-header does not exist on disk; will try bucket")
	} else {
		level.Warn(ll).Log("msg", "failed to read sparse index-header from disk; will try bucket", "err", err)
	}

	// 2. Fall back to the bucket
	bucketSparseHeaderBytes, err := getSparseHeaderBytes(ctx, blockID, tenantBkt, ll)
	if err == nil {
		// Try to load the downloaded sparse header

		if err == nil {
			sparseHeaders := &indexheaderpb.Sparse{}
			sparseHeaders.Symbols = r.symbols.ToSparseSymbols()
			sparseHeaders.PostingsOffsetTable = r.postingsOffsetTable.ToSparsePostingOffsetTable()
			tryWriteSparseHeadersToFile(ll, sparseHeadersPath, sparseHeaders)
			return nil
		}
		level.Warn(ll).Log("msg", "failed to load sparse index-header from bucket; reconstructing", "err", err)
	} else {
		level.Info(ll).Log("msg", "could not download sparse index-header from bucket; reconstructing from index-header", "err", err)
	}

	// 3. Generate from index-header as a last resort
	if err = loadFromIndexHeader(ll, cfg, postingOffsetsInMemSampling); err != nil {
		return fmt.Errorf("cannot load sparse index-header from full index-header: %w", err)
	}
	level.Info(ll).Log("msg", "generated sparse index-header from full index-header")

	sparseHeaders := &indexheaderpb.Sparse{}
	sparseHeaders.Symbols = r.symbols.ToSparseSymbols()
	sparseHeaders.PostingsOffsetTable = r.postingsOffsetTable.ToSparsePostingOffsetTable()
	tryWriteSparseHeadersToFile(ll, sparseHeadersPath, sparseHeaders)

	return nil
}

// loadFromSparseIndexHeader load from sparse index-header on disk.
func loadFromSparseIndexHeader(logger log.Logger, sparseData []byte, postingOffsetsInMemSampling int) (err error) {
	start := time.Now()
	defer func() {
		level.Info(logger).Log("msg", "loaded sparse index-header from disk", "elapsed", time.Since(start))
	}()

	level.Debug(logger).Log("msg", "loading sparse index-header from disk")
	sparseHeaders, err := decodeGZipSparseHeader(sparseData, logger)
	if err != nil {
		return err
	}

	r.symbols, err = streamindex.NewSymbolsTableReaderFromSparseHeader(r.factory, sparseHeaders.Symbols, int(r.toc.Symbols))
	if err != nil {
		return fmt.Errorf("cannot load symbols from sparse index-header: %w", err)
	}

	r.postingsOffsetTable, err = streamindex.NewPostingOffsetTableFromSparseHeader(r.factory, sparseHeaders.PostingsOffsetTable, int(r.toc.PostingsOffsetTable), postingOffsetsInMemSampling)
	if err != nil {
		return fmt.Errorf("cannot load postings offset table from sparse index-header: %w", err)
	}

	return nil
}

func getSparseValuesFromIndexHeader(
	toc *TOCCompat,
	decbufFactory streamencoding.DecbufFactory,
	postingOffsetsInMemSampling int,
	doChecksum bool,
	ll log.Logger,
) (
	allSymbolsCount int,
	sparseSymbolsOffsets []int,
	err error,
) {
	start := time.Now()
	defer func() {
		level.Info(ll).Log("msg", "loaded sparse index-header from full index-header", "elapsed", time.Since(start))
	}()

	level.Info(ll).Log("msg", "loading sparse index-header from full index-header")

	allSymbolsCount, sparseSymbolsOffsets, err = streamindex.SparseValuesFromSymbolsTable(
		decbufFactory, int(toc.Symbols), doChecksum,
	)
	if err != nil {
		return -1, nil, err
	}

	postingsOffsetTable, err := streamindex.NewPostingOffsetTableReaderFromIndexHeader(
		decbufFactory,
		int(toc.PostingsOffsetTable),
		toc.IndexVersion,
		toc.PostingsListEnd,
		postingOffsetsInMemSampling,
		doChecksum,
	)
	if err != nil {
		return -1, nil, fmt.Errorf("cannot load postings offset table from full index-header: %w", err)
	}

	return allSymbolsCount, sparseSymbolsOffsets, nil
}

func decodeGZipSparseHeader(sparseData []byte, ll log.Logger) (*indexheaderpb.Sparse, error) {
	sparseHeaders := &indexheaderpb.Sparse{}

	gzipped := bytes.NewReader(sparseData)
	gzipReader, err := gzip.NewReader(gzipped)
	if err != nil {
		return nil, fmt.Errorf("failed to create sparse index-header gzip reader: %w", err)
	}
	defer runutil.CloseWithLogOnErr(ll, gzipReader, "close sparse index-header gzip reader")

	sparseData, err = io.ReadAll(gzipReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read sparse index-header: %w", err)
	}

	if err := sparseHeaders.Unmarshal(sparseData); err != nil {
		return nil, fmt.Errorf("failed to decode sparse index-header file: %w", err)
	}
	return sparseHeaders, err
}

func downloadBucketSparseHeader(
	ctx context.Context,
	id ulid.ULID,
	tenantBkt objstore.InstrumentedBucketReader,
	localTenantDir string,
	ll log.Logger,
) error {
	localBlockDir := filepath.Join(localTenantDir, id.String())
	localSparseHeaderPath := filepath.Join(localBlockDir, block.SparseIndexHeaderFilename)

	_, err := os.Stat(localSparseHeaderPath)
	if err == nil {
		// The header is already on disk
		return nil
	}

	level.Debug(ll).Log("msg", "sparse index-header does not exist on disk; will try bucket")

	bucketSparseHeaderBytes, err := getSparseHeaderBytes(ctx, id, tenantBkt, ll)
	if err != nil {
		return err
	}

	return atomicfs.CreateFile(localSparseHeaderPath, bytes.NewReader(bucketSparseHeaderBytes))
}

// getSparseHeaderBytes reads the raw sparse header bytes from object storage with bucket.Get.
// The bucket reader passed in must be prefixed with the tenant ID TSDB path in the object storage.
// This does not write the header bytes to local disk.
func getSparseHeaderBytes(
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
		return nil, fmt.Errorf("getting sparse index-header from bucket: %w", err)
	}
	defer runutil.CloseWithLogOnErr(ll, reader, "close sparse index-header reader")

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading sparse index-header from bucket: %w", err)
	}

	// Check if we've been canceled after downloading
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	level.Info(ll).Log("msg", "downloaded sparse index-header from bucket")

	return data, nil
}
