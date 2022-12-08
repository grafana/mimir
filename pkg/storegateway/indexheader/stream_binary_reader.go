// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/binary_reader.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	streamencoding "github.com/grafana/mimir/pkg/storegateway/indexheader/encoding"
	streamindex "github.com/grafana/mimir/pkg/storegateway/indexheader/index"
)

type StreamBinaryReader struct {
	factory *streamencoding.DecbufFactory
	toc     *BinaryTOC

	// Symbols struct that keeps only 1/postingOffsetsInMemSampling in the memory, then looks up the
	// rest via seeking to offsets in the index-header.
	symbols *streamindex.Symbols
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

	postingsOffsetTable streamindex.PostingOffsetTable

	version      int
	indexVersion int
}

// NewStreamBinaryReader loads or builds new index-header if not present on disk.
func NewStreamBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int, cfg Config) (*StreamBinaryReader, error) {
	binfn := filepath.Join(dir, id.String(), block.IndexHeaderFilename)
	br, err := newFileStreamBinaryReader(binfn, postingOffsetsInMemSampling, cfg)
	if err == nil {
		return br, nil
	}

	level.Debug(logger).Log("msg", "failed to read index-header from disk; recreating", "path", binfn, "err", err)

	start := time.Now()
	if err := WriteBinary(ctx, bkt, id, binfn); err != nil {
		return nil, fmt.Errorf("cannot write index header: %w", err)
	}

	level.Debug(logger).Log("msg", "built index-header file", "path", binfn, "elapsed", time.Since(start))
	return newFileStreamBinaryReader(binfn, postingOffsetsInMemSampling, cfg)
}

func newFileStreamBinaryReader(path string, postingOffsetsInMemSampling int, cfg Config) (bw *StreamBinaryReader, err error) {
	r := &StreamBinaryReader{factory: streamencoding.NewDecbufFactory(path, cfg.FileHandlePoolSize)}

	// Create a new raw decoding buffer with access to the entire index-header file to
	// read initial version information and the table of contents.
	d := r.factory.NewRawDecbuf()
	defer r.factory.CloseWithErrCapture(&err, d, "new file stream binary reader")
	if err = d.Err(); err != nil {
		return nil, fmt.Errorf("cannot create decoding buffer: %w", err)
	}

	// Grab the full length of the index header before we read any of it. This is needed
	// so that we can skip directly to the table of contents at the end of file.
	indexHeaderSize := d.Len()
	if magic := d.Be32(); magic != MagicIndex {
		return nil, fmt.Errorf("invalid magic number %x", magic)
	}

	r.version = int(d.Byte())
	r.indexVersion = int(d.Byte())
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

	r.symbols, err = streamindex.NewSymbols(r.factory, r.indexVersion, int(r.toc.Symbols))
	if err != nil {
		return nil, fmt.Errorf("cannot load symbols: %w", err)
	}

	r.postingsOffsetTable, err = streamindex.NewPostingOffsetTable(r.factory, int(r.toc.PostingsOffsetTable), r.indexVersion, indexLastPostingEnd, postingOffsetsInMemSampling)
	if err != nil {
		return nil, err
	}

	labelNames, err := r.postingsOffsetTable.LabelNames()
	if err != nil {
		return nil, err
	}

	r.nameSymbols = make(map[uint32]string, len(labelNames))
	if err = r.symbols.ForEachSymbol(labelNames, func(sym string, offset uint32) error {
		r.nameSymbols[offset] = sym
		return nil
	}); err != nil {
		return nil, err
	}

	return r, nil
}

// newBinaryTOCFromByteSlice return parsed TOC from given Decbuf. The Decbuf is expected to be
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

func (r *StreamBinaryReader) LabelNames() ([]string, error) {
	return r.postingsOffsetTable.LabelNames()
}

func (r *StreamBinaryReader) Close() error {
	r.factory.Stop()
	return nil
}
