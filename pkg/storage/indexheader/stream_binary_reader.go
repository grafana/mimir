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
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/atomicfs"
	"github.com/grafana/mimir/pkg/util/filepool"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type StreamBinaryReaderMetrics struct {
	filePool *filepool.FilePoolMetrics
}

func NewStreamBinaryReaderMetrics(reg prometheus.Registerer) *StreamBinaryReaderMetrics {
	return &StreamBinaryReaderMetrics{
		filePool: filepool.NewFilePoolMetrics(reg),
	}
}

// SplitStreamBinaryReader reads each index-header section from the configured source.
// The index-header sections are the Symbols table and Postings Offsets table.
type SplitStreamBinaryReader struct {
	symbolsTOC         *TOCCompat
	postingsOffsetsTOC *TOCCompat

	symbolsDecbufFactory         streamencoding.DecbufFactory
	postingsOffsetsDecbufFactory streamencoding.DecbufFactory

	symbolsTable        *streamindex.SymbolsTableReader
	postingsOffsetTable streamindex.PostingOffsetsTableReader

	// In-memory table of symbol lookups for all label names present in block;
	// Populated in the constructor by pulling all label names from the Postings Offsets table
	// and performing reverse symbol lookups with the Symbols Table.
	//
	// The reverse lookups require significant I/O on the Symbols Table,
	// which is the reason why reading the Symbols Table from the bucket is not yet supported.
	//
	// Label names account for ~half of all symbol lookups and total size of this table is minimal.
	nameSymbols map[uint32]string

	// Direct cache of resolved symbol lookups values;
	// Much faster than an LRU cache and still provides a reasonable cache hit ratio.
	valueSymbolsMu sync.Mutex
	valueSymbols   [valueSymbolsCacheSize]struct {
		index  uint32
		symbol string
	}
}

func NewSplitStreamBinaryReader(
	ctx context.Context,
	blockID ulid.ULID,
	tenantBkt objstore.InstrumentedBucketReader,
	localTenantDir string,
	cfg Config,
	postingOffsetsInMemSampling int,
	ll log.Logger,
	metrics *StreamBinaryReaderMetrics,
) (*SplitStreamBinaryReader, error) {
	var err error
	spanLog, ctx := spanlogger.New(ctx, ll, tracer, "indexheader.NewStreamBinaryReader")
	defer spanLog.Finish()

	// Ensure local block directory exists on disk to hold the sparse index-header
	// and any sections of the full index-header which are not read from the bucket.
	localBlockDir := filepath.Join(localTenantDir, blockID.String())
	if df, err := os.Open(localBlockDir); err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(localBlockDir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("cannot create local block dir: %w", err)
		}
	} else {
		_ = df.Close()
	}

	localIndexHeaderPath := filepath.Join(localBlockDir, block.IndexHeaderFilename)

	// Ensure index-header is downloaded to local block directory
	if _, err := os.Stat(localIndexHeaderPath); err != nil {
		level.Debug(spanLog).Log(
			"msg", "failed to stat index-header on local disk; will create from bucket block index",
			"path", localIndexHeaderPath, "err", err,
		)
		start := time.Now()
		if err := WriteBinary(ctx, tenantBkt, blockID, localIndexHeaderPath); err != nil {
			return nil, fmt.Errorf("cannot write index header: %w", err)
		}
		level.Debug(spanLog).Log(
			"msg", "created index-header on local disk from bucket block index",
			"path", localIndexHeaderPath, "elapsed", time.Since(start),
		)
	}

	streamBinaryReader := &SplitStreamBinaryReader{}

	// Set up each of the Symbols table and Postings Offsets table readers
	// and their respective table of contents and DecbufFactory implementation.
	// Currently, the only supported index-header section to read from the bucket is SectionPostingsOffsetTable.
	// Symbols are always read from disk.
	filePoolDecbufFactory := streamencoding.NewFilePoolDecbufFactory(
		localIndexHeaderPath, cfg.MaxIdleFileHandles, metrics.filePool,
	)

	indexHeaderTOC, _, err := TOCFromIndexHeader(castagnoliTable, filePoolDecbufFactory, ll)
	if err != nil {
		return nil, fmt.Errorf("cannot read table-of-contents from index-header on disk: %w", err)
	}

	streamBinaryReader.symbolsDecbufFactory = filePoolDecbufFactory
	streamBinaryReader.symbolsTOC = indexHeaderTOC

	if cfg.BucketReader.Enabled {
		bucketBlockIndexPath := filepath.Join(blockID.String(), block.IndexFilename)
		bucketBlockIndexDecbufFactory := streamencoding.NewBucketDecbufFactory(ctx, tenantBkt, bucketBlockIndexPath)
		indexAttrs, err := tenantBkt.Attributes(ctx, bucketBlockIndexPath)
		if err != nil {
			return nil, fmt.Errorf("get index file attributes: %w", err)
		}
		bucketBlockTOC, err := TOCFromBucketTSDBIndex(ctx, tenantBkt, bucketBlockIndexPath, indexAttrs)
		if err != nil {
			return nil, err
		}

		switch cfg.BucketReader.BucketIndexSections {
		case SectionPostingsOffsetsTable:
			streamBinaryReader.postingsOffsetsDecbufFactory = bucketBlockIndexDecbufFactory
			streamBinaryReader.postingsOffsetsTOC = bucketBlockTOC
		default:
			// Invalid BucketIndexSections are already rejected by config validation
		}
	} else {

	}

	//if cfg.BucketReader.BucketIndexSections == SectionAll {
	//	// Both Symbols and Postings Offsets are read from bucket
	//	streamBinaryReader.symbolsDecbufFactory = bucketBlockIndexDecbufFactory
	//	streamBinaryReader.postingsOffsetsDecbufFactory = bucketBlockIndexDecbufFactory
	//
	//	streamBinaryReader.symbolsTOC = bucketBlockTOC
	//	streamBinaryReader.postingsOffsetsTOC = bucketBlockTOC
	//} else {
	//
	//}

	//sparseHeadersPath := filepath.Join(localBlockDir, block.SparseIndexHeaderFilename)

	// Load symbols and postings offset table
	if err = streamBinaryReader.loadSparseHeader(ctx, ll, cfg, postingOffsetsInMemSampling, sparseHeadersPath, tenantBkt, blockID); err != nil {
		return nil, fmt.Errorf("cannot load sparse index-header: %w", err)
	}

	labelNames, err := streamBinaryReader.postingsOffsetTable.LabelNames()
	if err != nil {
		return nil, fmt.Errorf("load label names: %w", err)
	}

	streamBinaryReader.nameSymbols = make(map[uint32]string, len(labelNames))
	if err = streamBinaryReader.symbolsTable.ForEachSymbol(labelNames, func(sym string, offset uint32) error {
		streamBinaryReader.nameSymbols[offset] = sym
		return nil
	}); err != nil {
		return nil, fmt.Errorf("cache label names: %w", err)
	}

	return streamBinaryReader, nil
}

type StreamBinaryReader struct {
	factory streamencoding.DecbufFactory
	toc     *TOCCompat

	// Symbols struct that keeps only 1/postingOffsetsInMemSampling in the memory, then looks up the
	// rest via seeking to offsets in the index-header.
	symbols *streamindex.SymbolsTableReader

	// In-memory table label name symbol lookups;
	// total size is minimal and label names account for ~half of all symbol lookups.
	nameSymbols map[uint32]string
	// Direct cache of values. This is much faster than an LRU cache and still provides
	// a reasonable cache hit ratio.
	valueSymbolsMx sync.Mutex
	valueSymbols   [valueSymbolsCacheSize]struct {
		// index in TSDB v1 is the offset of the symbol in the index-header file.
		// In TSDB v2 it is the sequence number of the symbol in the TSDB index (starting at 0).
		index  uint32
		symbol string
	}

	postingsOffsetTable streamindex.PostingOffsetsTableReader

	indexHeaderVersion int
}

// NewStreamBinaryReader loads or builds new index-header if not present on disk.
func NewStreamBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.InstrumentedBucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int, metrics *StreamBinaryReaderMetrics, cfg Config) (*StreamBinaryReader, error) {
	spanLog, ctx := spanlogger.New(ctx, logger, tracer, "indexheader.NewStreamBinaryReader")
	defer spanLog.Finish()

	dir = filepath.Join(dir, id.String())
	if df, err := os.Open(dir); err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("cannot create index-header dir: %w", err)
		}
	} else {
		_ = df.Close()
	}

	binPath := filepath.Join(dir, block.IndexHeaderFilename)
	sparseHeadersPath := filepath.Join(dir, block.SparseIndexHeaderFilename)

	// First, try to initialize from the binary header file
	br, err := NewFileStreamBinaryReader(ctx, binPath, id, sparseHeadersPath, postingOffsetsInMemSampling, spanLog, bkt, metrics, cfg)
	if err == nil {
		return br, nil
	}

	level.Debug(spanLog).Log("msg", "failed to read index-header from disk; recreating", "path", binPath, "err", err)

	start := time.Now()
	if err := WriteBinary(ctx, bkt, id, binPath); err != nil {
		return nil, fmt.Errorf("cannot write index header: %w", err)
	}

	level.Debug(spanLog).Log("msg", "built index-header file", "path", binPath, "elapsed", time.Since(start))
	return NewFileStreamBinaryReader(ctx, binPath, id, sparseHeadersPath, postingOffsetsInMemSampling, spanLog, bkt, metrics, cfg)
}

// loadFromIndexHeader loads in symbols and postings offset table from the index-header.
func (r *StreamBinaryReader) loadFromIndexHeader(logger log.Logger, cfg Config, postingOffsetsInMemSampling int) error {
	var err error
	r.symbols, r.postingsOffsetTable, err = buildSparseHeaderFromIndexHeader(
		r.toc, r.factory, postingOffsetsInMemSampling, cfg.VerifyOnLoad, logger,
	)
	return err
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

func (r *StreamBinaryReader) IndexVersion(context.Context) (int, error) {
	return r.toc.IndexVersion, nil
}

func (r *StreamBinaryReader) PostingsOffset(_ context.Context, name string, value string) (index.Range, error) {
	rng, found, err := r.postingsOffsetTable.PostingsOffset(name, value)
	if err != nil {
		return index.Range{}, err
	}
	if !found {
		return index.Range{}, NotFoundRangeErr
	}
	return rng, nil
}

func (r *StreamBinaryReader) LookupSymbol(_ context.Context, o uint32) (string, error) {
	if s, ok := r.nameSymbols[o]; ok {
		return s, nil
	}

	cacheIndex := o % valueSymbolsCacheSize
	r.valueSymbolsMx.Lock()
	if cached := r.valueSymbols[cacheIndex]; cached.index == o && cached.symbol != "" {
		v := cached.symbol
		r.valueSymbolsMx.Unlock()
		return v, nil
	}
	r.valueSymbolsMx.Unlock()

	s, err := r.symbols.Lookup(o)
	if err != nil {
		return s, err
	}

	r.valueSymbolsMx.Lock()
	r.valueSymbols[cacheIndex].index = o
	r.valueSymbols[cacheIndex].symbol = s
	r.valueSymbolsMx.Unlock()

	return s, nil
}

type cachedLabelNamesSymbolsReader struct {
	labelNames map[uint32]string
	r          streamindex.SymbolsReader
}

func (c cachedLabelNamesSymbolsReader) Close() error {
	return c.r.Close()
}

func (c cachedLabelNamesSymbolsReader) Read(u uint32) (string, error) {
	if s, ok := c.labelNames[u]; ok {
		return s, nil
	}
	return c.r.Read(u)
}

func (r *StreamBinaryReader) SymbolsReader(context.Context) (streamindex.SymbolsReader, error) {
	return cachedLabelNamesSymbolsReader{
		labelNames: r.nameSymbols,
		r:          r.symbols.Reader(),
	}, nil
}

func (r *StreamBinaryReader) LabelValuesOffsets(ctx context.Context, name string, prefix string, filter func(string) bool) ([]streamindex.PostingListOffset, error) {
	return r.postingsOffsetTable.LabelValuesOffsets(ctx, name, prefix, filter)
}

func (r *StreamBinaryReader) LabelNames(context.Context) ([]string, error) {
	return r.postingsOffsetTable.LabelNames()
}

func (r *StreamBinaryReader) Close() error {
	return r.factory.Close()
}

// TOC returns the table of contents for the index-header.
func (r *StreamBinaryReader) TOC() *TOCCompat {
	return r.toc
}

// IndexHeaderVersion returns the version of the index-header file format.
func (r *StreamBinaryReader) IndexHeaderVersion() int {
	return r.indexHeaderVersion
}
