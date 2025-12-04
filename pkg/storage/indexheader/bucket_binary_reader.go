// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// BucketBinaryReader reads index header data directly from object storage.
type BucketBinaryReader struct {
	bkt     objstore.BucketReader
	factory *streamencoding.BucketDecbufFactory

	toc *index.TOC

	// Symbols struct that keeps only 1/postingOffsetsInMemSampling in the memory, then looks up the
	// rest via seeking to offsets in the index-header.
	symbols             *streamindex.Symbols
	postingsOffsetTable streamindex.PostingOffsetTable

	// Cache of the label name symbol lookups,
	// as there are not many and they are half of all lookups.
	// For index v1 the symbol reference is the index header symbol reference, not the prometheus TSDB index symbol reference.
	nameSymbols map[uint32]string
	// Direct cache of values. This is much faster than an LRU cache and still provides
	// a reasonable cache hit ratio.
	valueSymbolsMu sync.Mutex
	valueSymbols   [valueSymbolsCacheSize]struct {
		index  uint32
		symbol string
	}

	indexVersion int
}

// NewBucketBinaryReader creates a new BucketBinaryReader that reads index header data
// directly from the TSDB index file in object storage.
func NewBucketBinaryReader(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.InstrumentedBucketReader,
	dir string,
	blockID ulid.ULID,
	postingOffsetsInMemSampling int,
	cfg Config,
) (*BucketBinaryReader, error) {
	spanLog, ctx := spanlogger.New(ctx, logger, tracer, "indexheader.NewBucketBinaryReader")
	defer spanLog.Finish()

	dir = filepath.Join(dir, blockID.String())
	if df, err := os.Open(dir); err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("cannot create index-header dir: %w", err)
		}
	} else {
		_ = df.Close()
	}

	indexPath := filepath.Join(blockID.String(), block.IndexFilename)

	r := &BucketBinaryReader{
		bkt: bkt,

		factory: streamencoding.NewBucketDecbufFactory(ctx, bkt, indexPath),
	}

	indexAttrs, err := bkt.Attributes(ctx, indexPath)
	if err != nil {
		return nil, fmt.Errorf("get index file attributes: %w", err)
	}

	// Read the index file header (magic + versions)
	headerBytes, err := r.fetchRange(ctx, indexPath, 0, index.HeaderLen)
	if err != nil {
		return nil, fmt.Errorf("read index file header: %w", err)
	}

	if len(headerBytes) != index.HeaderLen {
		return nil, fmt.Errorf("unexpected index file header length: %d", len(headerBytes))
	}

	if magic := binary.BigEndian.Uint32(headerBytes[0:4]); magic != index.MagicIndex {
		return nil, fmt.Errorf("invalid magic number %x", magic)
	}

	r.indexVersion = int(headerBytes[4])

	if r.indexVersion != index.FormatV1 && r.indexVersion != index.FormatV2 {
		return nil, fmt.Errorf("unknown index format version %d", r.indexVersion)
	}

	tocBytes, err := r.fetchRange(ctx, indexPath, indexAttrs.Size-indexTOCLen-crc32.Size, indexTOCLen+crc32.Size)
	if err != nil {
		return nil, fmt.Errorf("read index TOC: %w", err)
	}

	r.toc, err = index.NewTOCFromByteSlice(realByteSlice(tocBytes))
	if err != nil {
		return nil, fmt.Errorf("parse index TOC: %w", err)
	}

	// Determine the end bound for posting lists
	// For index v2, this is the offset of label indices table
	// For index v1, this is the offset of postings table
	indexLastPostingListEndBound := r.toc.PostingsTable
	if r.indexVersion == index.FormatV2 {
		indexLastPostingListEndBound = r.toc.LabelIndicesTable
	}

	sparseHeadersPath := filepath.Join(dir, block.SparseIndexHeaderFilename)

	// Load symbols and postings offset table
	if err = r.loadSparseHeader(ctx, logger, cfg, indexLastPostingListEndBound, postingOffsetsInMemSampling, sparseHeadersPath, bkt, blockID); err != nil {
		return nil, fmt.Errorf("cannot load sparse index-header: %w", err)
	}

	labelNames, err := r.postingsOffsetTable.LabelNames()
	if err != nil {
		return nil, fmt.Errorf("load label names: %w", err)
	}

	r.nameSymbols = make(map[uint32]string, len(labelNames))
	if err = r.symbols.ForEachSymbol(labelNames, func(sym string, offset uint32) error {
		r.nameSymbols[offset] = sym
		return nil
	}); err != nil {
		return nil, fmt.Errorf("cache label names: %w", err)
	}

	return r, nil
}

// loadSparseHeader loads the sparse header from disk, object store, or constructs it from the index-header.
// It prioritizes: 1) Local file 2) Object store 3) Generating from the index-header
// It returns an error if the sparse header cannot be loaded from any of the sources.
// If the sparse header was not found on disk, it will try to write it after generating or downloading it. If writing fails, loadSparseHeader does not return an error.
func (r *BucketBinaryReader) loadSparseHeader(
	ctx context.Context,
	logger log.Logger,
	cfg Config,
	indexLastPostingListEndBound uint64,
	postingOffsetsInMemSampling int,
	sparseHeadersPath string,
	bkt objstore.InstrumentedBucketReader,
	id ulid.ULID,
) (err error) {
	logger = log.With(logger, "id", id, "path", sparseHeadersPath, "inmem_sampling_rate", postingOffsetsInMemSampling)

	// Only v2 indexes use sparse headers
	if r.indexVersion != index.FormatV2 {
		return r.loadFromIndexHeader(logger, cfg, indexLastPostingListEndBound, postingOffsetsInMemSampling)
	}

	// 1. Try to load from local file first
	sparseData, err := os.ReadFile(sparseHeadersPath)
	if err == nil {
		level.Debug(logger).Log("msg", "loading sparse index-header from local disk")
		err = r.loadFromSparseIndexHeader(logger, sparseData, postingOffsetsInMemSampling)
		if err == nil {
			return nil
		}
		level.Warn(logger).Log("msg", "failed to load sparse index-header from disk; will try bucket", "err", err)
	} else if os.IsNotExist(err) {
		level.Debug(logger).Log("msg", "sparse index-header does not exist on disk; will try bucket")
	} else {
		level.Warn(logger).Log("msg", "failed to read sparse index-header from disk; will try bucket", "err", err)
	}

	defer func() {
		if err != nil {
			return
		}

		start := time.Now()
		level.Debug(logger).Log("msg", "writing sparse index-header to disk")

		sparseHeaders := &indexheaderpb.Sparse{
			Symbols:             r.symbols.NewSparseSymbol(),
			PostingsOffsetTable: r.postingsOffsetTable.NewSparsePostingOffsetTable(),
		}
		err := writeSparseHeadersToFile(sparseHeadersPath, sparseHeaders)
		if err != nil {
			logger = log.With(level.Warn(logger), "msg", "error writing sparse header to disk; will continue loading block", "err", err)
		} else {
			logger = log.With(level.Info(logger), "msg", "wrote sparse header to disk")
		}
		logger.Log("elapsed", time.Since(start))
	}()

	// 2. Fall back to the bucket
	sparseData, err = tryReadBucketSparseHeader(ctx, logger, bkt, id)
	if err == nil {
		// Try to load the downloaded sparse header
		err = r.loadFromSparseIndexHeader(logger, sparseData, postingOffsetsInMemSampling)
		if err == nil {
			return nil
		}
		level.Warn(logger).Log("msg", "failed to load sparse index-header from bucket; reconstructing", "err", err)
	} else {
		level.Info(logger).Log("msg", "could not download sparse index-header from bucket; reconstructing from index-header", "err", err)
	}

	if err = r.loadFromIndexHeader(logger, cfg, indexLastPostingListEndBound, postingOffsetsInMemSampling); err != nil {
		return fmt.Errorf("cannot load sparse index-header from full index-header: %w", err)
	}
	level.Info(logger).Log("msg", "generated sparse index-header from full index-header")

	return nil
}

// loadFromSparseIndexHeader load from sparse index-header on disk.
func (r *BucketBinaryReader) loadFromSparseIndexHeader(logger log.Logger, sparseData []byte, postingOffsetsInMemSampling int) (err error) {
	start := time.Now()
	defer func() {
		level.Info(logger).Log("msg", "loaded sparse index-header from disk", "elapsed", time.Since(start))
	}()

	level.Debug(logger).Log("msg", "loading sparse index-header from disk")
	sparseHeaders, err := decodeSparseData(logger, sparseData)
	if err != nil {
		return err
	}

	r.symbols, err = streamindex.NewSymbolsFromSparseHeader(r.factory, sparseHeaders.Symbols, r.indexVersion, int(r.toc.Symbols))
	if err != nil {
		return fmt.Errorf("cannot load symbols from sparse index-header: %w", err)
	}

	r.postingsOffsetTable, err = streamindex.NewPostingOffsetTableFromSparseHeader(r.factory, sparseHeaders.PostingsOffsetTable, int(r.toc.PostingsTable), postingOffsetsInMemSampling)
	if err != nil {
		return fmt.Errorf("cannot load postings offset table from sparse index-header: %w", err)
	}

	return nil
}

// loadFromIndexHeader loads in symbols and postings offset table from the index-header.
func (r *BucketBinaryReader) loadFromIndexHeader(logger log.Logger, cfg Config, indexLastPostingListEndBound uint64, postingOffsetsInMemSampling int) (err error) {
	start := time.Now()
	defer func() {
		level.Info(logger).Log("msg", "loaded sparse index-header from full index-header", "elapsed", time.Since(start))
	}()

	level.Info(logger).Log("msg", "loading sparse index-header from full index-header")

	r.symbols, err = streamindex.NewSymbols(r.factory, r.indexVersion, int(r.toc.Symbols), cfg.VerifyOnLoad)
	if err != nil {
		return fmt.Errorf("cannot load symbols from full index-header: %w", err)
	}

	r.postingsOffsetTable, err = streamindex.NewPostingOffsetTable(r.factory, int(r.toc.PostingsTable), r.indexVersion, indexLastPostingListEndBound, postingOffsetsInMemSampling, cfg.VerifyOnLoad)
	if err != nil {
		return fmt.Errorf("cannot load postings offset table from full index-header: %w", err)
	}

	return nil
}

// fetchRange fetches a range of bytes from the object storage.
func (r *BucketBinaryReader) fetchRange(ctx context.Context, objectPath string, offset, length int64) ([]byte, error) {
	rc, err := r.bkt.GetRange(ctx, objectPath, offset, length)
	if err != nil {
		return nil, fmt.Errorf("get range [%d, %d): %w", offset, offset+length, err)
	}
	defer runutil.CloseWithErrCapture(&err, rc, "close range reader %s", objectPath)

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read range data: %w", err)
	}

	if int64(len(data)) != length {
		return nil, fmt.Errorf("expected %d bytes, got %d", length, len(data))
	}

	return data, nil
}

// Close implements Reader.
func (r *BucketBinaryReader) Close() error {
	r.factory.Stop()
	return nil
}

// IndexVersion implements Reader.
func (r *BucketBinaryReader) IndexVersion(context.Context) (int, error) {
	return r.indexVersion, nil
}

// PostingsOffset implements Reader.
func (r *BucketBinaryReader) PostingsOffset(_ context.Context, name string, value string) (index.Range, error) {
	rng, found, err := r.postingsOffsetTable.PostingsOffset(name, value)
	if err != nil {
		return index.Range{}, err
	}
	if !found {
		return index.Range{}, NotFoundRangeErr
	}
	return rng, nil
}

// LookupSymbol implements Reader.
func (r *BucketBinaryReader) LookupSymbol(_ context.Context, o uint32) (string, error) {
	if r.indexVersion == index.FormatV1 {
		// For v1, refs are actual offset inside index, not index-header.
		// Adjust for the header length difference.
		o += HeaderLen - index.HeaderLen
	}

	if s, ok := r.nameSymbols[o]; ok {
		return s, nil
	}

	cacheIndex := o % valueSymbolsCacheSize
	r.valueSymbolsMu.Lock()
	if cached := r.valueSymbols[cacheIndex]; cached.index == o && cached.symbol != "" {
		v := cached.symbol
		r.valueSymbolsMu.Unlock()
		return v, nil
	}
	r.valueSymbolsMu.Unlock()

	s, err := r.symbols.Lookup(o)
	if err != nil {
		return s, err
	}

	r.valueSymbolsMu.Lock()
	r.valueSymbols[cacheIndex].index = o
	r.valueSymbols[cacheIndex].symbol = s
	r.valueSymbolsMu.Unlock()

	return s, nil
}

// SymbolsReader implements Reader.
func (r *BucketBinaryReader) SymbolsReader(context.Context) (streamindex.SymbolsReader, error) {
	return cachedLabelNamesSymbolsReader{
		labelNames: r.nameSymbols,
		r:          r.symbols.Reader(),
	}, nil
}

// LabelValuesOffsets implements Reader.
func (r *BucketBinaryReader) LabelValuesOffsets(ctx context.Context, name string, prefix string, filter func(string) bool) ([]streamindex.PostingListOffset, error) {
	return r.postingsOffsetTable.LabelValuesOffsets(ctx, name, prefix, filter)
}

// LabelNames implements Reader.
func (r *BucketBinaryReader) LabelNames(context.Context) ([]string, error) {
	return r.postingsOffsetTable.LabelNames()
}
