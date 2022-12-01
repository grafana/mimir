// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	stream_encoding "github.com/grafana/mimir/pkg/storegateway/indexheader/encoding"
	stream_index "github.com/grafana/mimir/pkg/storegateway/indexheader/index"
)

type StreamBinaryReader struct {
	f *os.File

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

	postingsOffsetTable *stream_index.PostingOffsetTable

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
		return nil, errors.Wrap(err, "write index header")
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
		if err != nil {
			runutil.CloseWithErrCapture(&err, f, "index header close")
		}
	}()

	r := &StreamBinaryReader{
		f:       f,
		factory: stream_encoding.NewDecbufFactory(path),
	}

	headerBytes := make([]byte, headerLen)
	n, err := f.ReadAt(headerBytes, 0)
	if err != nil {
		return nil, err
	}
	if n != headerLen {
		return nil, errors.Wrapf(stream_encoding.ErrInvalidSize, "insufficient bytes read for header (got %d, wanted %d)", n, headerLen)
	}
	headerByteSlice := realByteSlice(headerBytes)

	if m := binary.BigEndian.Uint32(headerByteSlice.Range(0, 4)); m != MagicIndex {
		return nil, errors.Errorf("invalid magic number %x", m)
	}
	r.version = int(headerByteSlice.Range(4, 5)[0])
	r.indexVersion = int(headerByteSlice.Range(5, 6)[0])

	indexLastPostingEnd := int64(binary.BigEndian.Uint64(headerByteSlice.Range(6, headerLen)))

	if r.version != BinaryFormatV1 {
		return nil, errors.Errorf("unknown index header file version %d", r.version)
	}

	r.toc, err = newBinaryTOCFromFile(f)
	if err != nil {
		return nil, errors.Wrap(err, "read index header TOC")
	}

	r.symbols, err = stream_index.NewSymbols(r.factory, r.indexVersion, int(r.toc.Symbols))
	if err != nil {
		return nil, errors.Wrap(err, "load symbols")
	}

	r.postingsOffsetTable, err = stream_index.NewPostingsOffsetTable(r.factory, int(r.toc.PostingsOffsetTable), r.indexVersion, indexLastPostingEnd, postingOffsetsInMemSampling)
	if err != nil {
		return nil, err
	}

	r.nameSymbols = make(map[uint32]string, r.postingsOffsetTable.LabelNameCount())
	if err := r.postingsOffsetTable.ForEachLabelName(func(k string) error {
		// TODO: ReverseLookup() opens the index-header for every look up. Add bulk method?
		off, err := r.symbols.ReverseLookup(k)
		if err != nil {
			return errors.Wrap(err, "reverse symbol lookup")
		}
		r.nameSymbols[off] = k
		return nil
	}); err != nil {
		return nil, err
	}

	return r, nil
}

// newBinaryTOCFromByteSlice return parsed TOC from given index header byte slice.
func newBinaryTOCFromFile(f *os.File) (*BinaryTOC, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat")
	}
	fSize := info.Size()

	if fSize < binaryTOCLen {
		return nil, stream_encoding.ErrInvalidSize
	}

	r, err := stream_encoding.NewFileReader(f, int(fSize-binaryTOCLen), binaryTOCLen)
	if err != nil {
		return nil, errors.Wrap(err, "create reader for binary TOC")
	}

	d := stream_encoding.NewRawDecbuf(r)
	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "decode index header TOC")
	}

	if d.CheckCrc32(castagnoliTable); d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "read index header TOC")
	}

	d.ResetAt(0)

	if err := d.Err(); err != nil {
		return nil, err
	}

	toc := BinaryTOC{
		Symbols:             d.Be64(),
		PostingsOffsetTable: d.Be64(),
	}

	if err := d.Err(); err != nil {
		return nil, err
	}

	return &toc, nil
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

func (r *StreamBinaryReader) Close() error { return r.f.Close() }
