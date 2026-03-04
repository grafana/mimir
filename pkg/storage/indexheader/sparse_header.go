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

func buildSparseHeaderFromIndexHeader(
	indexVersion int,
	toc *TOCCompat,
	decbufFactory streamencoding.DecbufFactory,
	postingOffsetsInMemSampling int,
	doChecksum bool,
	ll log.Logger,
) (*streamindex.Symbols, streamindex.PostingOffsetTable, error) {
	start := time.Now()
	defer func() {
		level.Info(ll).Log("msg", "loaded sparse index-header from full index-header", "elapsed", time.Since(start))
	}()

	level.Info(ll).Log("msg", "loading sparse index-header from full index-header")

	symbols, err := streamindex.NewSymbols(decbufFactory, indexVersion, int(toc.Symbols), doChecksum)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot load symbols from full index-header: %w", err)
	}

	postingsOffsetTable, err := streamindex.NewPostingOffsetTable(
		decbufFactory,
		int(toc.PostingsOffsetTable),
		indexVersion,
		toc.PostingsListEnd,
		postingOffsetsInMemSampling,
		doChecksum,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot load postings offset table from full index-header: %w", err)
	}

	return symbols, postingsOffsetTable, nil
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

func downloadSparseHeaderBytes(
	ctx context.Context,
	id ulid.ULID,
	bkt objstore.InstrumentedBucketReader,
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
	bucketSparseHeaderBytes, err := getSparseHeaderBytes(ctx, id, bkt, ll)
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
