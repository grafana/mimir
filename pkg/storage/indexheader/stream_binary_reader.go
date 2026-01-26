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
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/atomicfs"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type StreamBinaryReaderMetrics struct {
	decbufFactory      *streamencoding.DecbufFactoryMetrics
	indexVersionLoaded *prometheus.CounterVec
}

func NewStreamBinaryReaderMetrics(reg prometheus.Registerer) *StreamBinaryReaderMetrics {
	return &StreamBinaryReaderMetrics{
		decbufFactory: streamencoding.NewDecbufFactoryMetrics(reg),
		indexVersionLoaded: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "indexheader_tsdb_index_loaded_total",
			Help: "Total number of index headers loaded by TSDB index file format version.",
		}, []string{"version"}),
	}
}

type StreamBinaryReader struct {
	factory streamencoding.DecbufFactory
	toc     *BinaryTOC

	// Symbols struct that keeps only 1/postingOffsetsInMemSampling in the memory, then looks up the
	// rest via seeking to offsets in the index-header.
	symbols *streamindex.Symbols
	// Cache of the label name symbol lookups,
	// as there are not many and they are half of all lookups.
	// For index v1 the symbol reference is the index header symbol reference, not the prometheus TSDB index symbol reference.
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

	version      int
	indexVersion int
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
	logger = log.With(logger, "id", id, "path", sparseHeadersPath, "inmem_sampling_rate", postingOffsetsInMemSampling)

	r := &StreamBinaryReader{
		factory: streamencoding.NewFilePoolDecbufFactory(binPath, cfg.MaxIdleFileHandles, metrics.decbufFactory),
	}

	// Create a new raw decoding buffer with access to the entire index-header file to
	// read initial version information and the table of contents.
	d := r.factory.NewRawDecbuf()
	defer runutil.CloseWithErrCapture(&err, &d, "new file stream binary reader")
	if err = d.Err(); err != nil {
		return nil, fmt.Errorf("cannot create decoding buffer: %w", err)
	}

	// Grab the full length of the index header before we read any of it. This is needed
	// so that we can skip directly to the table of contents at the end of file.
	indexHeaderSize := d.Len()
	if magic := d.Be32(); magic != MagicIndex {
		return nil, fmt.Errorf("invalid magic number %x", magic)
	}

	level.Debug(logger).Log("msg", "index header file size", "bytes", indexHeaderSize)

	r.version = int(d.Byte())
	r.indexVersion = int(d.Byte())

	// As of now this value is also the actual end of the last posting list. In the future
	// it may be some bytes after the actual end (e.g. in case Prometheus starts adding padding
	// after the last posting list).
	// This value used to be the offset of the postings offset table up to and including Mimir 2.7.
	// After that this is the offset of the label indices table.
	// So what we read here will depend on what version of Mimir created the index header file.
	indexLastPostingListEndBound := d.Be64()

	if err = d.Err(); err != nil {
		return nil, fmt.Errorf("cannot read version and index version: %w", err)
	}

	if r.version != BinaryFormatV1 {
		return nil, fmt.Errorf("unknown index-header file version %d", r.version)
	}

	// Keep track of the TSDB index file format version. We want to remove support for
	// the v1 format but need to confirm that we don't have any uses of it. Mimir has
	// never written the v1 format but it's possible that instances of it were uploaded
	// via the compactor. See https://github.com/grafana/mimir/issues/13808
	metrics.indexVersionLoaded.WithLabelValues(strconv.Itoa(r.indexVersion)).Inc()
	if r.indexVersion != index.FormatV2 {
		level.Warn(logger).Log("msg", "deprecated TSDB index version loaded for index-header", "version", r.indexVersion)
	}

	r.toc, err = newBinaryTOCFromFile(d, indexHeaderSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read table-of-contents: %w", err)
	}

	// Load symbols and postings offset table
	if err = r.loadSparseHeader(ctx, logger, cfg, indexLastPostingListEndBound, postingOffsetsInMemSampling, sparseHeadersPath, bkt, id); err != nil {
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
func (r *StreamBinaryReader) loadSparseHeader(ctx context.Context, logger log.Logger, cfg Config, indexLastPostingListEndBound uint64, postingOffsetsInMemSampling int, sparseHeadersPath string, bkt objstore.InstrumentedBucketReader, id ulid.ULID) error {
	// Only v2 indexes use sparse headers
	if r.indexVersion != index.FormatV2 {
		return r.loadFromIndexHeader(logger, cfg, indexLastPostingListEndBound, postingOffsetsInMemSampling)
	}

	// 1. Try to load from local file first
	localSparseHeader, err := os.Open(sparseHeadersPath)
	if err == nil {
		defer localSparseHeader.Close()
		level.Debug(logger).Log("msg", "loading sparse index-header from local disk")
		err = r.loadFromSparseIndexHeader(ctx, logger, localSparseHeader, postingOffsetsInMemSampling)
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
	bucketSparseHeaderReader, err := tryOpenBucketSparseHeader(ctx, logger, bkt, id)
	if err == nil {
		defer runutil.CloseWithLogOnErr(logger, bucketSparseHeaderReader, "close sparse index-header reader")

		// Try to load from the sparse header
		err = r.loadFromSparseIndexHeader(ctx, logger, bucketSparseHeaderReader, postingOffsetsInMemSampling)
		if err == nil {
			// Check if we've been canceled after downloading
			if err := ctx.Err(); err != nil {
				return err
			}
			tryWriteSparseHeadersToFile(logger, sparseHeadersPath, r)
			return nil
		}
		level.Warn(logger).Log("msg", "failed to load sparse index-header from bucket; reconstructing", "err", err)
	} else {
		level.Info(logger).Log("msg", "could not download sparse index-header from bucket; reconstructing from index-header", "err", err)
	}

	// 3. Generate from index-header as a last resort
	if err = r.loadFromIndexHeader(logger, cfg, indexLastPostingListEndBound, postingOffsetsInMemSampling); err != nil {
		return fmt.Errorf("cannot load sparse index-header from full index-header: %w", err)
	}
	level.Info(logger).Log("msg", "generated sparse index-header from full index-header")
	tryWriteSparseHeadersToFile(logger, sparseHeadersPath, r)

	return nil
}

// tryOpenBucketSparseHeader attempts to open a reader for the sparse header from the object store.
// It returns the reader if successful, nil + error otherwise.
func tryOpenBucketSparseHeader(ctx context.Context, logger log.Logger, bkt objstore.InstrumentedBucketReader, id ulid.ULID) (io.ReadCloser, error) {
	if bkt == nil {
		return nil, fmt.Errorf("bucket is nil")
	}
	sparseHeaderObjPath := filepath.Join(id.String(), block.SparseIndexHeaderFilename)

	return bkt.ReaderWithExpectedErrs(bkt.IsObjNotFoundErr).Get(ctx, sparseHeaderObjPath)
}

// loadFromSparseIndexHeader load from sparse index-header on disk.
func (r *StreamBinaryReader) loadFromSparseIndexHeader(ctx context.Context, logger log.Logger, sparseData io.Reader, postingOffsetsInMemSampling int) (err error) {
	start := time.Now()
	defer func() {
		level.Info(logger).Log("msg", "loaded sparse index-header from disk", "elapsed", time.Since(start))
	}()

	level.Debug(logger).Log("msg", "loading sparse index-header from disk")
	sparseHeaders, err := decodeSparseData(ctx, sparseData)
	if err != nil {
		return err
	}

	r.symbols, err = streamindex.NewSymbolsFromSparseHeader(r.factory, sparseHeaders.Symbols, r.indexVersion, int(r.toc.Symbols))
	if err != nil {
		return fmt.Errorf("cannot load symbols from sparse index-header: %w", err)
	}

	r.postingsOffsetTable, err = streamindex.NewPostingOffsetTableFromSparseHeader(r.factory, sparseHeaders.PostingsOffsetTable, int(r.toc.PostingsOffsetTable), postingOffsetsInMemSampling)
	if err != nil {
		return fmt.Errorf("cannot load postings offset table from sparse index-header: %w", err)
	}

	return nil
}

func decodeSparseData(ctx context.Context, sparseData io.Reader) (*indexheaderpb.Sparse, error) {
	sparseHeaders := indexheaderpb.Sparse{}
	// ParseProtoReader requires a max size; we supply a very broad estimate.
	// We could read the size from last 4 bytes of the file (per gzip spec), but it's probably not worth it.
	const maxSize = 1 << 31 // 2GiB
	_, err := util.ParseProtoReader(ctx, sparseData, 0, maxSize, nil, &sparseHeaders, util.Gzip)
	if err != nil {
		return nil, fmt.Errorf("failed to read sparse index-header: %w", err)
	}
	return &sparseHeaders, nil
}

// loadFromIndexHeader loads in symbols and postings offset table from the index-header.
func (r *StreamBinaryReader) loadFromIndexHeader(logger log.Logger, cfg Config, indexLastPostingListEndBound uint64, postingOffsetsInMemSampling int) (err error) {
	start := time.Now()
	defer func() {
		level.Info(logger).Log("msg", "loaded sparse index-header from full index-header", "elapsed", time.Since(start))
	}()

	level.Info(logger).Log("msg", "loading sparse index-header from full index-header")

	r.symbols, err = streamindex.NewSymbols(r.factory, r.indexVersion, int(r.toc.Symbols), cfg.VerifyOnLoad)
	if err != nil {
		return fmt.Errorf("cannot load symbols from full index-header: %w", err)
	}

	r.postingsOffsetTable, err = streamindex.NewPostingOffsetTable(r.factory, int(r.toc.PostingsOffsetTable), r.indexVersion, indexLastPostingListEndBound, postingOffsetsInMemSampling, cfg.VerifyOnLoad)
	if err != nil {
		return fmt.Errorf("cannot load postings offset table from full index-header: %w", err)
	}

	return nil
}

// writeSparseHeadersToFile uses protocol buffer to write StreamBinaryReader to disk at sparseHeadersPath.
func writeSparseHeadersToFile(sparseHeadersPath string, reader *StreamBinaryReader) (retErr error) {
	sparseHeaders := &indexheaderpb.Sparse{}
	sparseHeaders.Symbols = reader.symbols.ToSparseSymbols()
	sparseHeaders.PostingsOffsetTable = reader.postingsOffsetTable.ToSparsePostingOffsetTable()

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
func tryWriteSparseHeadersToFile(logger log.Logger, sparseHeadersPath string, reader *StreamBinaryReader) {
	start := time.Now()
	level.Debug(logger).Log("msg", "writing sparse index-header to disk")

	err := writeSparseHeadersToFile(sparseHeadersPath, reader)

	if err != nil {
		logger = log.With(level.Warn(logger), "msg", "error writing sparse header to disk; will continue loading block", "err", err)
	} else {
		logger = log.With(level.Info(logger), "msg", "wrote sparse header to disk")
	}
	logger.Log("elapsed", time.Since(start))
}

// newBinaryTOCFromFile return parsed TOC from given Decbuf. The Decbuf is expected to be
// configured to access the entirety of the index-header file.
func newBinaryTOCFromFile(d streamencoding.Decbuf, indexHeaderSize int) (*BinaryTOC, error) {
	tocOffset := indexHeaderSize - BinaryTOCLen
	if d.ResetAt(tocOffset); d.Err() != nil {
		return nil, d.Err()
	}

	if d.CheckCrc32(castagnoliTable); d.Err() != nil {
		return nil, d.Err()
	}

	d.ResetAt(tocOffset)
	symbols := d.Be64()
	postingsOffsetTable := d.Be64()

	if err := d.Err(); err != nil {
		return nil, err
	}

	return &BinaryTOC{
		Symbols:             symbols,
		PostingsOffsetTable: postingsOffsetTable,
	}, nil
}

func (r *StreamBinaryReader) IndexVersion(context.Context) (int, error) {
	return r.indexVersion, nil
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
	if r.indexVersion == index.FormatV1 {
		// For v1 little trick is needed. Refs are actual offset inside index, not index-header. This is different
		// of the header length difference between two files.
		o += HeaderLen - index.HeaderLen
	}

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
func (r *StreamBinaryReader) TOC() *BinaryTOC {
	return r.toc
}

// IndexHeaderVersion returns the version of the index-header file format.
func (r *StreamBinaryReader) IndexHeaderVersion() int {
	return r.version
}
