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
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	streamencoding "github.com/grafana/mimir/pkg/storegateway/indexheader/encoding"
	streamindex "github.com/grafana/mimir/pkg/storegateway/indexheader/index"
	"github.com/grafana/mimir/pkg/storegateway/indexheader/indexheaderpb"
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
	factory *streamencoding.DecbufFactory
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
func NewStreamBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.Bucket, dir string, id ulid.ULID, postingOffsetsInMemSampling int, metrics *StreamBinaryReaderMetrics, cfg Config) (*StreamBinaryReader, error) {
	spanLog, ctx := spanlogger.NewWithLogger(ctx, logger, "indexheader.NewStreamBinaryReader")
	defer spanLog.Finish()

	binPath := filepath.Join(dir, id.String(), block.IndexHeaderFilename)
	sparseLoader := bucketAndDiskSparseHeaderLoader{
		ctx:       ctx,
		bkt:       objstore.NewPrefixedBucket(bkt, id.String()),
		localPath: filepath.Join(dir, id.String(), block.SparseIndexHeaderFilename),
		logger:    log.With(logger, "id", id.String()),
	}
	br, err := newFileStreamBinaryReader(sparseLoader, binPath, id, postingOffsetsInMemSampling, spanLog, metrics, cfg)
	if err == nil {
		return br, nil
	}

	level.Debug(spanLog).Log("msg", "failed to read index-header from disk; recreating", "path", binPath, "err", err)

	start := time.Now()
	if err := WriteBinary(ctx, bkt, id, binPath); err != nil {
		return nil, fmt.Errorf("cannot write index header: %w", err)
	}

	level.Debug(spanLog).Log("msg", "built index-header file", "path", binPath, "elapsed", time.Since(start))
	return newFileStreamBinaryReader(sparseLoader, binPath, id, postingOffsetsInMemSampling, spanLog, metrics, cfg)
}

// newFileStreamBinaryReader loads sparse index-headers from disk or constructs it from the index-header if not available.
func newFileStreamBinaryReader(loader bucketAndDiskSparseHeaderLoader, binPath string, id ulid.ULID, postingOffsetsInMemSampling int, logger log.Logger, metrics *StreamBinaryReaderMetrics, cfg Config) (bw *StreamBinaryReader, err error) {
	r := &StreamBinaryReader{
		factory: streamencoding.NewDecbufFactory(binPath, cfg.MaxIdleFileHandles, metrics.decbufFactory),
	}
	logger = log.With(logger, "id", id)
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

	r.toc, err = newBinaryTOCFromFile(d, indexHeaderSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read table-of-contents: %w", err)
	}

	// Load in sparse symbols and postings offset table; from disk if this is a v2 index.
	if r.indexVersion == index.FormatV2 {
		if err = r.loadIndexHeaderV2(logger, cfg, indexLastPostingListEndBound, postingOffsetsInMemSampling, loader); err != nil {
			return nil, fmt.Errorf("cannot load sparse index-header: %w", err)
		}
	} else {
		if err = r.loadFromIndexHeader(logger, cfg, indexLastPostingListEndBound, postingOffsetsInMemSampling); err != nil {
			return nil, fmt.Errorf("cannot load sparse index-header: %w", err)
		}
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

func (r *StreamBinaryReader) loadIndexHeaderV2(logger log.Logger, cfg Config, indexLastPostingListEndBound uint64, postingOffsetsInMemSampling int, loader bucketAndDiskSparseHeaderLoader) error {
	start := time.Now()
	defer func() {
		level.Info(logger).Log("msg", "loaded index-header", "elapsed", time.Since(start))
	}()

	level.Debug(logger).Log("msg", "loading index-header")

	sparseHeader, err := loader.SparseHeader()
	if err == nil {
		// Read persisted sparseHeader from disk to memory.
		if err := r.loadFromSparseIndexHeader(sparseHeader, postingOffsetsInMemSampling); err != nil {
			return err
		}
		return nil
	}

	// If a ready sparseHeader couldn't be found, construct sparseHeader and write it.
	if err := r.loadFromIndexHeader(logger, cfg, indexLastPostingListEndBound, postingOffsetsInMemSampling); err != nil {
		return err
	}
	if err := loader.PersistSparseHeader(r.sparseHeader()); err != nil {
		level.Error(logger).Log("msg", "couldn't persist sparse index header; next time this block is loaded will be as slow as this one", "err", err)
		// Don't fail the whole operation if we can't persist the sparse index-header.
		// The block is loaded so we can serve queries.
		return nil
	}

	return nil
}

// loadFromSparseIndexHeader load from sparse index-header on disk.
func (r *StreamBinaryReader) loadFromSparseIndexHeader(sparseHeaders *indexheaderpb.Sparse, postingOffsetsInMemSampling int) (err error) {
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

// newBinaryTOCFromFile return parsed TOC from given Decbuf. The Decbuf is expected to be
// configured to access the entirety of the index-header file.
func newBinaryTOCFromFile(d streamencoding.Decbuf, indexHeaderSize int) (*BinaryTOC, error) {
	tocOffset := indexHeaderSize - binaryTOCLen
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

func (r *StreamBinaryReader) IndexVersion() (int, error) {
	return r.indexVersion, nil
}

func (r *StreamBinaryReader) PostingsOffset(name, value string) (index.Range, error) {
	rng, found, err := r.postingsOffsetTable.PostingsOffset(name, value)
	if err != nil {
		return index.Range{}, err
	}
	if !found {
		return index.Range{}, NotFoundRangeErr
	}
	return rng, nil
}

func (r *StreamBinaryReader) LookupSymbol(o uint32) (string, error) {
	if r.indexVersion == index.FormatV1 {
		// For v1 little trick is needed. Refs are actual offset inside index, not index-header. This is different
		// of the header length difference between two files.
		o += headerLen - index.HeaderLen
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

func (r *StreamBinaryReader) SymbolsReader() (streamindex.SymbolsReader, error) {
	return cachedLabelNamesSymbolsReader{
		labelNames: r.nameSymbols,
		r:          r.symbols.Reader(),
	}, nil
}

func (r *StreamBinaryReader) LabelValuesOffsets(ctx context.Context, name string, prefix string, filter func(string) bool) ([]streamindex.PostingListOffset, error) {
	return r.postingsOffsetTable.LabelValuesOffsets(ctx, name, prefix, filter)
}

func (r *StreamBinaryReader) LabelNames() ([]string, error) {
	return r.postingsOffsetTable.LabelNames()
}

func (r *StreamBinaryReader) Close() error {
	r.factory.Stop()
	return nil
}

func (r *StreamBinaryReader) sparseHeader() *indexheaderpb.Sparse {
	sparseHeader := &indexheaderpb.Sparse{}
	sparseHeader.Symbols = r.symbols.NewSparseSymbol()
	sparseHeader.PostingsOffsetTable = r.postingsOffsetTable.NewSparsePostingOffsetTable()
	return sparseHeader
}

type bucketAndDiskSparseHeaderLoader struct {
	ctx       context.Context
	bkt       objstore.Bucket
	localPath string
	logger    log.Logger
}

func (l bucketAndDiskSparseHeaderLoader) SparseHeader() (*indexheaderpb.Sparse, error) {
	defer func(start time.Time) {
		level.Debug(l.logger).Log("msg", "read and parsed sparse index header", "path", l.localPath, "elapsed", time.Since(start))
	}(time.Now())

	sparseData, existsLocally, err := l.sparseHeaderBytes()
	if err != nil {
		return nil, err
	}
	if len(sparseData) > 0 && !existsLocally {
		err = l.persistHeaderToDisk(sparseData)
		if err != nil {
			level.Error(l.logger).Log("msg", "couldn't persist sparse index to disk; will download from object store next time too", "path", l.localPath, "err", err)
			// otherwise ignore the error because loading the header is the priority; not persisting it to disk
		}
	}

	sparseHeaders, err := l.parseSparseHeader(sparseData)
	if err != nil {
		return nil, fmt.Errorf("cannot parse sparse index-header: %w", err)
	}
	return sparseHeaders, nil
}

func (l bucketAndDiskSparseHeaderLoader) sparseHeaderBytes() (b []byte, existsLocally bool, _ error) {
	b, err := os.ReadFile(l.localPath)
	if err == nil {
		return b, true, nil
	}
	if !os.IsNotExist(err) {
		level.Warn(l.logger).Log("msg", "failed to read sparse index-header from disk; trying to find it in object store", "err", err)
	}

	reader, err := l.bkt.Get(l.ctx, block.SparseIndexHeaderFilename)
	if err != nil {
		level.Warn(l.logger).Log("msg", "failed to find sparse index-header in object store; recreating", "err", err)
		return nil, false, err
	}
	b, err = io.ReadAll(reader)
	if err != nil {
		level.Warn(l.logger).Log("msg", "failed to read sparse index-header from object store; recreating", "err", err)
		return nil, false, err
	}
	return b, false, nil
}

func (l bucketAndDiskSparseHeaderLoader) parseSparseHeader(sparseData []byte) (*indexheaderpb.Sparse, error) {
	gzipped := bytes.NewReader(sparseData)
	gzipReader, err := gzip.NewReader(gzipped)
	if err != nil {
		return nil, fmt.Errorf("failed to create sparse index-header reader: %w", err)
	}

	sparseData, err = io.ReadAll(gzipReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read sparse index-header: %w", err)
	}

	sparseHeaders := &indexheaderpb.Sparse{}
	if err := sparseHeaders.Unmarshal(sparseData); err != nil {
		return nil, fmt.Errorf("failed to decode sparse index-header file: %w", err)
	}
	return sparseHeaders, nil
}

// PersistSparseHeader uses protocol buffer to write StreamBinaryReader to disk at path and to object storage at id.String/block.SparseIndexHeaderFilename
func (l bucketAndDiskSparseHeaderLoader) PersistSparseHeader(sparseHeader *indexheaderpb.Sparse) (err error) {
	defer func(start time.Time) {
		logger := level.Info(l.logger)
		if err != nil {
			logger = log.With(level.Error(l.logger), "err", err)
		}
		logger.Log("msg", "persisted index-header", "path", l.localPath, "elapsed", time.Since(start))
	}(time.Now())
	level.Debug(l.logger).Log("msg", "persisting sparse index-header", "path", l.localPath)

	sparseData, err := l.serializeSparseHeader(sparseHeader)
	if err != nil {
		return err
	}
	err = l.persistHeaderToDisk(sparseData)
	if err != nil {
		return err
	}
	return l.persistHeaderToBucket(sparseData)
}

func (l bucketAndDiskSparseHeaderLoader) serializeSparseHeader(sparseHeader *indexheaderpb.Sparse) ([]byte, error) {
	out, err := sparseHeader.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to encode sparse index-header: %w", err)
	}

	gzipped := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(gzipped)

	if _, err := gzipWriter.Write(out); err != nil {
		return nil, fmt.Errorf("failed to gzip sparse index-header: %w", err)
	}

	if err := gzipWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close gzip sparse index-header: %w", err)
	}
	return gzipped.Bytes(), nil
}

func (l bucketAndDiskSparseHeaderLoader) persistHeaderToDisk(gzipped []byte) error {
	if err := os.WriteFile(l.localPath, gzipped, 0600); err != nil {
		return fmt.Errorf("failed to write sparse index-header to disk: %w", err)
	}

	return nil
}

func (l bucketAndDiskSparseHeaderLoader) persistHeaderToBucket(gzipped []byte) error {
	if err := l.bkt.Upload(l.ctx, block.SparseIndexHeaderFilename, bytes.NewReader(gzipped)); err != nil {
		return fmt.Errorf("failed to upload sparse index-header to object store: %w", err)
	}
	return nil
}
