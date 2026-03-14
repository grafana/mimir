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

// DownloadOrBuildSparseHeader loads the sparse header from disk, object store, or constructs it from the index-header.
// It prioritizes:
// 1) Confirming existing of sparse header on local disk from previous calls
// 2) Downloading the sparse header from object store
// 3) Generating the sparse header from the full index-header on local disk
// It returns an error if the sparse header values cannot be loaded into memory from any of the sources.
// It does not return an error if it fails to write the sparse in-memory values to local disk.
func DownloadOrBuildSparseHeader(
	ctx context.Context,
	blockID ulid.ULID,
	tenantBkt objstore.InstrumentedBucketReader,
	localTenantDir string,
	cfg Config,
	sparseSampleFactor int,
	doChecksum bool,
	ll log.Logger,
	metrics *StreamBinaryReaderMetrics,
) (
	allSymbolsCount int,
	sparseSymbolsOffsets []int,
	sparsePostingsOffsets map[string]*streamindex.LabelSparsePostingsOffsets,
	err error,
) {

	localBlockDir := filepath.Join(localTenantDir, blockID.String())
	localSparseHeaderPath := filepath.Join(localBlockDir, block.SparseIndexHeaderFilename)
	localIndexHeaderPath := filepath.Join(localBlockDir, block.IndexHeaderFilename)

	// Check if sparse header is already on disk or download if it exists in the bucket block
	err = downloadBucketSparseHeader(ctx, blockID, tenantBkt, localTenantDir, ll)
	if err == nil {
		// Sparse header was already on disk or downloaded successfully from bucket;
		// TODO Load sparse header values from on-disk proto representation.
	} else {
		// Sparse header does not exist on disk or in bucket, or failed to download;
		// build sparse header values from full index-header
		filePoolDecbufFactory := streamencoding.NewFilePoolDecbufFactory(
			localIndexHeaderPath, cfg.MaxIdleFileHandles, metrics.filePool,
		)
		defer filePoolDecbufFactory.Close()

		indexHeaderTOC, _, err := TOCFromIndexHeader(castagnoliTable, filePoolDecbufFactory, ll)
		if err != nil {
			return -1, nil, nil, fmt.Errorf("cannot read table-of-contents: %w", err)
		}
		allSymbolsCount, sparseSymbolsOffsets, sparsePostingsOffsets, err = buildSparseValuesFromIndexHeader(
			indexHeaderTOC, filePoolDecbufFactory, sparseSampleFactor, doChecksum, ll,
		)
		if err != nil {
			return -1, nil, nil, fmt.Errorf("cannot build sparse index-header values from full index-header: %w", err)
		}
		level.Info(ll).Log("msg", "built sparse index-header values from full index-header")
	}

	sparseHeaders := &indexheaderpb.Sparse{}
	sparseHeaders.Symbols = sparseSymbolsValuesToProto(allSymbolsCount, sparseSymbolsOffsets)
	sparseHeaders.PostingsOffsetTable = sparsePostingsOffsetsTableValuesToProto(sparsePostingsOffsets, sparseSampleFactor)

	tryWriteSparseHeadersToFile(ll, localSparseHeaderPath, sparseHeaders)

	return allSymbolsCount, sparseSymbolsOffsets, sparsePostingsOffsets, nil
}

// LoadSparseHeaderFromDisk load from sparse index-header on disk.
func LoadSparseHeaderFromDisk(localSparseHeaderPath string, sparseSampleFactor int, ll log.Logger) (
	allSymbolsCount int,
	sparseSymbolsOffsets []int,
	sparsePostingsOffsets map[string]*streamindex.LabelSparsePostingsOffsets,
	err error,
) {
	sparseData, err := os.ReadFile(localSparseHeaderPath)
	if err != nil {
		return 0, nil, nil, err
	}
	sparseHeaderProto, err := decodeGZipSparseHeader(sparseData, ll)
	if err != nil {
		return 0, nil, nil, err
	}
	allSymbolsCount, sparseSymbolsOffsets = sparseSymbolsValuesFromProto(sparseHeaderProto.Symbols)
	sparsePostingsOffsets, err = sparsePostingsOffsetsTableValuesFromProto(
		sparseHeaderProto.PostingsOffsetTable, sparseSampleFactor,
	)
	if err != nil {
		return 0, nil, nil, err
	}

	return allSymbolsCount, sparseSymbolsOffsets, sparsePostingsOffsets, nil
}

func buildSparseValuesFromIndexHeader(
	toc *TOCCompat,
	decbufFactory streamencoding.DecbufFactory,
	sparseSampleFactor int,
	doChecksum bool,
	ll log.Logger,
) (
	allSymbolsCount int,
	sparseSymbolsOffsets []int,
	sparsePostingsOffsets map[string]*streamindex.LabelSparsePostingsOffsets,
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
		return -1, nil, nil, err
	}

	sparsePostingsOffsets, err = streamindex.SparseValuesFromPostingsOffsetsTable(
		decbufFactory, int(toc.PostingsOffsetTable), doChecksum, sparseSampleFactor, toc.PostingsOffsetTable,
	)
	if err != nil {
		return -1, nil, nil, err
	}

	return allSymbolsCount, sparseSymbolsOffsets, sparsePostingsOffsets, nil
}

// sparseSymbolsValuesFromProto loads the protobuf format to in-memory sparse symbols data
func sparseSymbolsValuesFromProto(proto *indexheaderpb.Symbols) (allSymbolsCount int, sparseOffsets []int) {
	allSymbolsCount = int(proto.SymbolsCount)
	sparseOffsets = make([]int, len(proto.Offsets))

	for i, offset := range proto.Offsets {
		sparseOffsets[i] = int(offset)
	}

	return allSymbolsCount, sparseOffsets
}

// sparseSymbolsValuesToProto loads the in-memory sparse symbols data into the protobuf format
func sparseSymbolsValuesToProto(allSymbolsCount int, sparseOffsets []int) *indexheaderpb.Symbols {
	proto := &indexheaderpb.Symbols{}

	offsets := make([]int64, len(sparseOffsets))
	for i, offset := range sparseOffsets {
		offsets[i] = int64(offset)
	}

	proto.Offsets = offsets
	proto.SymbolsCount = int64(allSymbolsCount)

	return proto
}

// sparsePostingsOffsetsTableValuesToProto loads the protobuf format to in-memory sparse postings offsets data
func sparsePostingsOffsetsTableValuesFromProto(proto *indexheaderpb.PostingOffsetTable, sparseSampleFactor int) (
	sparsePostingsOffsets map[string]*streamindex.LabelSparsePostingsOffsets,
	err error,
) {
	protoSampleFactor := int(proto.GetPostingOffsetInMemorySampling())
	if protoSampleFactor == 0 {
		return nil, fmt.Errorf("sparse index-header sampling rate not set")
	}

	if protoSampleFactor > sparseSampleFactor {
		return nil, fmt.Errorf("sparse index-header sampling rate exceeds in-mem-sampling rate")
	}

	// if the sampling rate in the sparse index-header is set lower (more frequent) than
	// the configured sparseSampleFactor we downsample to the configured rate
	step, ok := stepSize(protoSampleFactor, sparseSampleFactor)
	if !ok {
		return nil, fmt.Errorf("sparse index-header sampling rate not compatible with in-mem-sampling rate")
	}

	sparsePostingsOffsets = make(map[string]*streamindex.LabelSparsePostingsOffsets, len(proto.Postings))
	for sName, sOffsets := range proto.Postings {

		olen := len(sOffsets.Offsets)
		downsampledLen := (olen + step - 1) / step
		if (olen > 1) && (downsampledLen == 1) {
			downsampledLen++
		}

		sparsePostingsOffsets[sName] = &streamindex.LabelSparsePostingsOffsets{
			SparseTableOffsets: make([]streamindex.LabelValuePostingsOffset, downsampledLen),
		}
		for i, sPostingOff := range sOffsets.Offsets {
			if i%step == 0 {
				sparsePostingsOffsets[sName].SparseTableOffsets[i/step] = streamindex.LabelValuePostingsOffset{
					Value: sPostingOff.Value, Offset: int(sPostingOff.TableOff),
				}
			}

			if i == olen-1 {
				sparsePostingsOffsets[sName].SparseTableOffsets[downsampledLen-1] = streamindex.LabelValuePostingsOffset{
					Value: sPostingOff.Value, Offset: int(sPostingOff.TableOff),
				}
			}
		}
		sparsePostingsOffsets[sName].LastValOffset = sOffsets.LastValOffset
	}
	return sparsePostingsOffsets, err
}

func stepSize(cur, tgt int) (int, bool) {
	if cur > tgt || cur <= 0 || tgt <= 0 || tgt%cur != 0 {
		return 0, false
	}
	return tgt / cur, true
}

// sparsePostingsOffsetsTableValuesToProto loads sparse postings offset table data into the protobuf format
func sparsePostingsOffsetsTableValuesToProto(
	sparsePostingsOffsets map[string]*streamindex.LabelSparsePostingsOffsets,
	sparseSampleFactor int,
) *indexheaderpb.PostingOffsetTable {
	proto := &indexheaderpb.PostingOffsetTable{
		Postings:                      make(map[string]*indexheaderpb.PostingValueOffsets, len(sparsePostingsOffsets)),
		PostingOffsetInMemorySampling: int64(sparseSampleFactor),
	}

	for labelName, offsets := range sparsePostingsOffsets {
		proto.Postings[labelName] = &indexheaderpb.PostingValueOffsets{}
		postingOffsets := make([]*indexheaderpb.PostingOffset, len(offsets.SparseTableOffsets))

		for i, tableOffset := range offsets.SparseTableOffsets {
			postingOffsets[i] = &indexheaderpb.PostingOffset{Value: tableOffset.Value, TableOff: int64(tableOffset.Offset)}
		}
		proto.Postings[labelName].Offsets = postingOffsets
		proto.Postings[labelName].LastValOffset = offsets.LastValOffset
	}

	return proto
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
	bucketSparseHeaderBytes, err := getSparseHeaderBytesFromBucket(ctx, id, tenantBkt, ll)
	if err != nil {
		return err
	}

	return atomicfs.CreateFile(localSparseHeaderPath, bytes.NewReader(bucketSparseHeaderBytes))
}

// getSparseHeaderBytesFromBucket reads the raw sparse header bytes from object storage with bucket.Get.
// The bucket reader passed in must be prefixed with the tenant ID TSDB path in the object storage.
// This does not write the header bytes to local disk.
func getSparseHeaderBytesFromBucket(
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

// writeSparseHeadersToFile uses protocol buffer to write sparseHeaders to disk at sparseHeadersPath.
func writeSparseHeadersToFile(sparseHeadersPath string, sparseHeaders *indexheaderpb.Sparse) (retErr error) {
	out, err := sparseHeaders.Marshal()
	if err != nil {
		return fmt.Errorf("failed to encode sparse index-header: %w", err)
	}

	gzipped := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(gzipped)

	if _, err := gzipWriter.Write(out); err != nil {
		return fmt.Errorf("failed to gzip sparse index-header: %w", err)
	}

	if err := gzipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close gzip sparse index-header: %w", err)
	}

	if err := atomicfs.CreateFile(sparseHeadersPath, gzipped); err != nil {
		return fmt.Errorf("failed to write sparse index-header file: %w", err)
	}

	return nil
}

// tryWriteSparseHeadersToFile attempts to write the sparse header to disk.
// If it fails, it will log a warning.
func tryWriteSparseHeadersToFile(logger log.Logger, sparseHeadersPath string, sparseHeaders *indexheaderpb.Sparse) {
	start := time.Now()
	level.Debug(logger).Log("msg", "writing sparse index-header to disk")

	err := writeSparseHeadersToFile(sparseHeadersPath, sparseHeaders)

	if err != nil {
		logger = log.With(level.Warn(logger), "msg", "error writing sparse header to disk; will continue loading block", "err", err)
	} else {
		logger = log.With(level.Info(logger), "msg", "wrote sparse header to disk")
	}
	logger.Log("elapsed", time.Since(start))
}
