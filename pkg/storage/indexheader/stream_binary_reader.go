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
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type StreamBinaryReaderMetrics struct {
	decbufFactory *streamencoding.DecbufFactoryMetrics
}

func NewStreamBinaryReaderMetrics(reg prometheus.Registerer) *StreamBinaryReaderMetrics {
	return &StreamBinaryReaderMetrics{
		decbufFactory: streamencoding.NewDecbufFactoryMetrics(reg),
	}
}

type StreamBinaryReader struct {
	factory streamencoding.DecbufFactory
	toc     *TOCCompat

	// Symbols struct that keeps only 1/postingOffsetsInMemSampling in the memory, then looks up the
	// rest via seeking to offsets in the index-header.
	symbols *streamindex.Symbols

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

	postingsOffsetTable streamindex.PostingOffsetTable

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

// NewFileStreamBinaryReader loads sparse index-headers from disk, then from the bucket, or constructs it from the index-header if neither of the two available.
func NewFileStreamBinaryReader(ctx context.Context, binPath string, id ulid.ULID, sparseHeadersPath string, postingOffsetsInMemSampling int, logger log.Logger, bkt objstore.InstrumentedBucketReader, metrics *StreamBinaryReaderMetrics, cfg Config) (bw *StreamBinaryReader, err error) {
	logger = log.With(logger, "path", sparseHeadersPath, "inmem_sampling_rate", postingOffsetsInMemSampling)

	r := &StreamBinaryReader{
		factory: streamencoding.NewFilePoolDecbufFactory(binPath, cfg.MaxIdleFileHandles, metrics.decbufFactory),
	}

	if r.toc, r.indexHeaderVersion, err = TOCFromIndexHeader(castagnoliTable, r.factory, logger); err != nil {
		return nil, fmt.Errorf("cannot read table-of-contents: %w", err)
	}

	// Load symbols and postings offset table
	if err = r.loadSparseHeader(ctx, logger, cfg, postingOffsetsInMemSampling, sparseHeadersPath, bkt, id); err != nil {
		return nil, fmt.Errorf("cannot load sparse index-header: %w", err)
	}

	labelNames, err := r.postingsOffsetTable.LabelNames()
	if err != nil {
		return nil, fmt.Errorf("cannot load label names from postings offset table: %w", err)
	}

	r.nameSymbols = make(map[uint32]string, len(labelNames))
	if err = r.symbols.ForEachSymbol(labelNames, func(sym string, offset uint32) error {
		r.nameSymbols[offset] = sym
		return nil
	}); err != nil {
		return nil, err
	}

	return r, err
}

// loadSparseHeader loads the sparse header from disk, object store, or constructs it from the index-header.
// It prioritizes: 1) Local file 2) Object store 3) Generating from the index-header
// It returns an error if the sparse header cannot be loaded from any of the sources.
// If the sparse header was not found on disk, it will try to write it after generating or downloading it. If writing fails, loadSparseHeader does not return an error.
func (r *StreamBinaryReader) loadSparseHeader(ctx context.Context, logger log.Logger, cfg Config, postingOffsetsInMemSampling int, sparseHeadersPath string, bkt objstore.InstrumentedBucketReader, id ulid.ULID) error {
	// 1. Try to load from local file first
	localSparseHeaderBytes, err := os.ReadFile(sparseHeadersPath)
	if err == nil {
		level.Debug(logger).Log("msg", "loading sparse index-header from local disk")
		err = r.loadFromSparseIndexHeader(logger, localSparseHeaderBytes, postingOffsetsInMemSampling)
		if err == nil {
			return nil
		}
		level.Warn(logger).Log("msg", "failed to load sparse index-header from disk; will try bucket", "err", err)
	} else if os.IsNotExist(err) {
		level.Debug(logger).Log("msg", "sparse index-header does not exist on disk; will try bucket")
	} else {
		level.Warn(logger).Log("msg", "failed to read sparse index-header from disk; will try bucket", "err", err)
	}

	// 2. Fall back to the bucket
	bucketSparseHeaderBytes, err := getSparseHeaderBytes(ctx, id, bkt, logger)
	if err == nil {
		// Try to load the downloaded sparse header
		err = r.loadFromSparseIndexHeader(logger, bucketSparseHeaderBytes, postingOffsetsInMemSampling)
		if err == nil {
			sparseHeaders := &indexheaderpb.Sparse{}
			sparseHeaders.Symbols = r.symbols.ToSparseSymbols()
			sparseHeaders.PostingsOffsetTable = r.postingsOffsetTable.ToSparsePostingOffsetTable()
			tryWriteSparseHeadersToFile(logger, sparseHeadersPath, sparseHeaders)
			return nil
		}
		level.Warn(logger).Log("msg", "failed to load sparse index-header from bucket; reconstructing", "err", err)
	} else {
		level.Info(logger).Log("msg", "could not download sparse index-header from bucket; reconstructing from index-header", "err", err)
	}

	// 3. Generate from index-header as a last resort
	if err = r.loadFromIndexHeader(logger, cfg, postingOffsetsInMemSampling); err != nil {
		return fmt.Errorf("cannot load sparse index-header from full index-header: %w", err)
	}
	level.Info(logger).Log("msg", "generated sparse index-header from full index-header")

	sparseHeaders := &indexheaderpb.Sparse{}
	sparseHeaders.Symbols = r.symbols.ToSparseSymbols()
	sparseHeaders.PostingsOffsetTable = r.postingsOffsetTable.ToSparsePostingOffsetTable()
	tryWriteSparseHeadersToFile(logger, sparseHeadersPath, sparseHeaders)

	return nil
}

// loadFromSparseIndexHeader load from sparse index-header on disk.
func (r *StreamBinaryReader) loadFromSparseIndexHeader(logger log.Logger, sparseData []byte, postingOffsetsInMemSampling int) (err error) {
	start := time.Now()
	defer func() {
		level.Info(logger).Log("msg", "loaded sparse index-header from disk", "elapsed", time.Since(start))
	}()

	level.Debug(logger).Log("msg", "loading sparse index-header from disk")
	sparseHeaders, err := decodeGZipSparseHeader(sparseData, logger)
	if err != nil {
		return err
	}

	r.symbols, err = streamindex.NewSymbolsFromSparseHeader(r.factory, sparseHeaders.Symbols, r.toc.IndexVersion, int(r.toc.Symbols))
	if err != nil {
		return fmt.Errorf("cannot load symbols from sparse index-header: %w", err)
	}

	r.postingsOffsetTable, err = streamindex.NewPostingOffsetTableFromSparseHeader(r.factory, sparseHeaders.PostingsOffsetTable, int(r.toc.PostingsOffsetTable), postingOffsetsInMemSampling)
	if err != nil {
		return fmt.Errorf("cannot load postings offset table from sparse index-header: %w", err)
	}

	return nil
}

// loadFromIndexHeader loads in symbols and postings offset table from the index-header.
func (r *StreamBinaryReader) loadFromIndexHeader(logger log.Logger, cfg Config, postingOffsetsInMemSampling int) error {
	var err error
	r.symbols, r.postingsOffsetTable, err = buildSparseHeaderFromIndexHeader(
		r.toc.IndexVersion, r.toc, r.factory, postingOffsetsInMemSampling, cfg.VerifyOnLoad, logger,
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
