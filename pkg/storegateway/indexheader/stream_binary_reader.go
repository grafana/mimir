// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	stream_encoding "github.com/grafana/mimir/pkg/storegateway/indexheader/encoding"
	stream_index "github.com/grafana/mimir/pkg/storegateway/indexheader/index"
)

type StreamBinaryReader struct {
	factory *stream_encoding.DecbufFactory
	toc     *BinaryTOC

	// Symbols struct that keeps only 1/postingOffsetsInMemSampling in the memory, then looks up the rest via mmap.
	symbols *stream_index.Symbols
	// Cache of the label name symbol lookups,
	// as there are not many and they are half of all lookups.
	nameSymbols map[uint32]string
	// Direct cache of values. This is much faster than an LRU cache and still provides
	// a reasonable cache hit ratio.
	valueSymbolsMx sync.Mutex
	valueSymbols   [valueSymbolsCacheSize]struct {
		index  uint32
		symbol string
	}

	postingsOffsetTable stream_index.PostingOffsetTable

	version      int
	indexVersion int
}

// NewStreamBinaryReader loads or builds new index-header if not present on disk.
func NewStreamBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int) (*StreamBinaryReader, error) {
	binfn := filepath.Join(dir, id.String(), block.IndexHeaderFilename)
	br, err := newFileStreamBinaryReader(binfn, postingOffsetsInMemSampling)
	if err == nil {
		return br, nil
	}

	level.Debug(logger).Log("msg", "failed to read index-header from disk; recreating", "path", binfn, "err", err)

	start := time.Now()
	if err := WriteBinary(ctx, bkt, id, binfn); err != nil {
		return nil, fmt.Errorf("cannot write index header: %w", err)
	}

	level.Debug(logger).Log("msg", "built index-header file", "path", binfn, "elapsed", time.Since(start))
	return newFileStreamBinaryReader(binfn, postingOffsetsInMemSampling)
}

func newFileStreamBinaryReader(path string, postingOffsetsInMemSampling int) (bw *StreamBinaryReader, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer func() {
		runutil.CloseWithErrCapture(&err, f, "index header close")
	}()

	r := &StreamBinaryReader{
		factory: stream_encoding.NewDecbufFactory(path),
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	indexHeaderSize := stat.Size()
	fileReader, err := stream_encoding.NewFileReader(f, 0, int(indexHeaderSize))
	if err != nil {
		return nil, fmt.Errorf("cannot create file reader: %w", err)
	}

	d := stream_encoding.NewRawDecbuf(fileReader)
	if err = d.Err(); err != nil {
		return nil, fmt.Errorf("cannot create decoding buffer: %w", err)
	}

	if magic := d.Be32(); magic != MagicIndex {
		return nil, fmt.Errorf("invalid magic number %x", magic)
	}

	r.version = d.ByteInt()
	r.indexVersion = d.ByteInt()
	indexLastPostingEnd := d.Be64()

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

	r.symbols, err = stream_index.NewSymbols(r.factory, r.indexVersion, int(r.toc.Symbols))
	if err != nil {
		return nil, fmt.Errorf("cannot load symbols: %w", err)
	}

	r.postingsOffsetTable, err = stream_index.NewPostingOffsetTable(r.factory, int(r.toc.PostingsOffsetTable), r.indexVersion, indexLastPostingEnd, postingOffsetsInMemSampling)
	if err != nil {
		return nil, err
	}

	labelNames, err := r.postingsOffsetTable.LabelNames()
	if err != nil {
		return nil, err
	}

	r.nameSymbols = make(map[uint32]string, len(labelNames))
	if err := r.symbols.ForEachSymbol(labelNames, func(sym string, offset uint32) error {
		r.nameSymbols[offset] = sym
		return nil
	}); err != nil {
		return nil, err
	}

	return r, nil
}

// newBinaryTOCFromByteSlice return parsed TOC from given Decbuf. The Decbuf is expected to be
// configured to access the entirety of the index-header file.
func newBinaryTOCFromFile(d stream_encoding.Decbuf, indexHeaderSize int64) (*BinaryTOC, error) {
	tocOffset := int(indexHeaderSize - binaryTOCLen)
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

// TODO(bwplotka): Get advantage of multi value offset fetch.
func (r *StreamBinaryReader) PostingsOffset(name, value string) (index.Range, error) {
	rngs, err := r.postingsOffsetTable.PostingsOffset(name, value)
	if err != nil {
		return index.Range{}, err
	}
	if len(rngs) != 1 {
		return index.Range{}, NotFoundRangeErr
	}
	return rngs[0], nil
}

func (r *StreamBinaryReader) LookupSymbol(o uint32) (string, error) {
	cacheIndex := o % valueSymbolsCacheSize
	r.valueSymbolsMx.Lock()
	if cached := r.valueSymbols[cacheIndex]; cached.index == o && cached.symbol != "" {
		v := cached.symbol
		r.valueSymbolsMx.Unlock()
		return v, nil
	}
	r.valueSymbolsMx.Unlock()

	if s, ok := r.nameSymbols[o]; ok {
		return s, nil
	}

	if r.indexVersion == index.FormatV1 {
		// For v1 little trick is needed. Refs are actual offset inside index, not index-header. This is different
		// of the header length difference between two files.
		o += headerLen - index.HeaderLen
	}

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

func (r *StreamBinaryReader) LabelValues(name string, filter func(string) bool) ([]string, error) {
	return r.postingsOffsetTable.LabelValues(name, filter)
}

// TODO: could this use nameSymbols somehow? Or be constructed up-front like nameSymbols is?
func (r *StreamBinaryReader) LabelNames() ([]string, error) {
	return r.postingsOffsetTable.LabelNames()
}

func (r *StreamBinaryReader) Close() error { return nil }
