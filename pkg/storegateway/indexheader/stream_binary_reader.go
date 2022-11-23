// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	//stream_encoding "github.com/grafana/mimir/pkg/storegateway/indexheader/encoding"
	stream_index "github.com/grafana/mimir/pkg/storegateway/indexheader/index"
)

type StreamBinaryReader struct {
	f *os.File

	toc *BinaryTOC

	// Map of LabelName to a list of some LabelValues's position in the offset table.
	// The first and last values for each name are always present, we keep only 1/postingOffsetsInMemSampling of the rest.
	postings map[string]*postingValueOffsets
	// For the v1 format, labelname -> labelvalue -> offset.
	postingsV1 map[string]map[string]index.Range

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

	version             int
	indexVersion        int
	indexLastPostingEnd int64

	postingOffsetsInMemSampling int
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
		f:                           f,
		postings:                    map[string]*postingValueOffsets{},
		postingOffsetsInMemSampling: postingOffsetsInMemSampling,
	}

	headerBytes := make([]byte, headerLen)
	n, err := f.ReadAt(headerBytes, 0)
	if err != nil {
		return nil, err
	}
	if n != headerLen {
		return nil, errors.Wrapf(encoding.ErrInvalidSize, "insufficient bytes read for header (got %d, wanted %d)", n, headerLen)
	}
	headerByteSlice := realByteSlice(headerBytes)

	if m := binary.BigEndian.Uint32(headerByteSlice.Range(0, 4)); m != MagicIndex {
		return nil, errors.Errorf("invalid magic number %x", m)
	}
	r.version = int(headerByteSlice.Range(4, 5)[0])
	r.indexVersion = int(headerByteSlice.Range(5, 6)[0])

	r.indexLastPostingEnd = int64(binary.BigEndian.Uint64(headerByteSlice.Range(6, headerLen)))

	if r.version != BinaryFormatV1 {
		return nil, errors.Errorf("unknown index header file version %d", r.version)
	}

	r.toc, err = newBinaryTOCFromFile(f)
	if err != nil {
		return nil, errors.Wrap(err, "read index header TOC")
	}

	// TODO: Symbols byteslice is offset by 14 bytes _BUT_ symbol lookups for v1 are always off by 14 bytes
	symbolsByteSlice, err := readDecbufBytes(f, int64(r.toc.Symbols))
	if err != nil {
		return nil, errors.Wrap(err, "read symbols")
	}

	lengthBytes := make([]byte, 4)
	n, err = f.ReadAt(lengthBytes, int64(r.toc.Symbols))
	if err != nil {
		return nil, err
	}
	//length := len(lengthBytes) + int(binary.BigEndian.Uint32(lengthBytes)) + 4

	//fr := stream_encoding.NewFileReader(f, int(r.toc.Symbols), length)
	r.symbols, err = stream_index.NewSymbols(symbolsByteSlice, r.indexVersion, 0)
	if err != nil {
		return nil, errors.Wrap(err, "read symbols")
	}

	// TODO(bwplotka): Consider contributing to Prometheus to allow specifying custom number for symbolsFactor.
	//	r.symbols, err = stream_index.NewSymbols(symbolsByteSlice, r.indexVersion, 0)
	//	if err != nil {
	//		return nil, errors.Wrap(err, "read symbols")
	//	}

	var lastKey []string
	if r.indexVersion == index.FormatV1 {
		// Earlier V1 formats don't have a sorted postings offset table, so
		// load the whole offset table into memory.
		r.postingsV1 = map[string]map[string]index.Range{}

		offTableByteSlice, err := readDecbufBytes(f, int64(r.toc.PostingsOffsetTable))
		if err != nil {
			return nil, errors.Wrap(err, "read symbols")
		}

		//fmt.Println("ReadOffsetTablev1")
		var prevRng index.Range
		if err := index.ReadOffsetTable(offTableByteSlice, 0, func(key []string, off uint64, _ int) error {
			if len(key) != 2 {
				return errors.Errorf("unexpected key length for posting table %d", len(key))
			}

			if lastKey != nil {
				prevRng.End = int64(off - crc32.Size)
				r.postingsV1[lastKey[0]][lastKey[1]] = prevRng
			}

			if _, ok := r.postingsV1[key[0]]; !ok {
				r.postingsV1[key[0]] = map[string]index.Range{}
				r.postings[key[0]] = nil // Used to get a list of labelnames in places.
			}

			lastKey = key
			prevRng = index.Range{Start: int64(off + postingLengthFieldSize)}
			return nil
		}); err != nil {
			return nil, errors.Wrap(err, "read postings table")
		}
		if lastKey != nil {
			prevRng.End = r.indexLastPostingEnd - crc32.Size
			r.postingsV1[lastKey[0]][lastKey[1]] = prevRng
		}
	} else {
		lastTableOff := 0
		valueCount := 0

		//fmt.Println("ReadOffsetTablenotv1")
		offTableByteSlice, err := readDecbufBytes(f, int64(r.toc.PostingsOffsetTable))
		if err != nil {
			return nil, errors.Wrap(err, "read symbols")
		}

		// For the postings offset table we keep every label name but only every nth
		// label value (plus the first and last one), to save memory.
		if err := stream_index.ReadOffsetTable(offTableByteSlice, 0, func(key []string, off uint64, tableOff int) error {
			if len(key) != 2 {
				return errors.Errorf("unexpected key length for posting table %d", len(key))
			}

			//fmt.Printf("ReadOffsetTable call off=%v tableOff=%v\n", off, tableOff)

			if _, ok := r.postings[key[0]]; !ok {
				// Not seen before label name.
				r.postings[key[0]] = &postingValueOffsets{}
				if lastKey != nil {
					// Always include last value for each label name, unless it was just added in previous iteration based
					// on valueCount.
					if (valueCount-1)%postingOffsetsInMemSampling != 0 {
						r.postings[lastKey[0]].offsets = append(r.postings[lastKey[0]].offsets, postingOffset{value: lastKey[1], tableOff: lastTableOff})
					}
					r.postings[lastKey[0]].lastValOffset = int64(off - crc32.Size)
					lastKey = nil
				}
				valueCount = 0
			}

			lastKey = key
			lastTableOff = tableOff
			valueCount++

			if (valueCount-1)%postingOffsetsInMemSampling == 0 {
				r.postings[key[0]].offsets = append(r.postings[key[0]].offsets, postingOffset{value: key[1], tableOff: tableOff})
			}

			return nil
		}); err != nil {
			return nil, errors.Wrap(err, "read postings table")
		}
		if lastKey != nil {
			if (valueCount-1)%postingOffsetsInMemSampling != 0 {
				// Always include last value for each label name if not included already based on valueCount.
				r.postings[lastKey[0]].offsets = append(r.postings[lastKey[0]].offsets, postingOffset{value: lastKey[1], tableOff: lastTableOff})
			}
			// In any case lastValOffset is unknown as don't have next posting anymore. Guess from TOC table.
			// In worst case we will overfetch a few bytes.
			r.postings[lastKey[0]].lastValOffset = r.indexLastPostingEnd - crc32.Size
		}
		// Trim any extra space in the slices.
		for k, v := range r.postings {
			l := make([]postingOffset, len(v.offsets))
			copy(l, v.offsets)
			r.postings[k].offsets = l
		}
	}

	r.nameSymbols = make(map[uint32]string, len(r.postings))
	for k := range r.postings {
		if k == "" {
			continue
		}
		off, err := r.symbols.ReverseLookup(k)
		if err != nil {
			return nil, errors.Wrap(err, "reverse symbol lookup")
		}
		r.nameSymbols[off] = k
	}

	//fmt.Println("End")
	return r, nil
}

// readDecbufBytes reads Decbuf encoded bytes from a file which can then be
// passed to NewDecbuf for parsing.
func readDecbufBytes(f *os.File, off int64) (index.ByteSlice, error) {
	// Pull the length field out first, so we know how much data to read.
	// Decbuf begins with a big endian uint32 length field.
	lengthBytes := make([]byte, 4)
	n, err := f.ReadAt(lengthBytes, off)
	if err != nil {
		return nil, err
	}
	if n != len(lengthBytes) {
		return nil, errors.Errorf("insufficient bytes read (got %d, wanted %d)", n, len(lengthBytes))
	}
	length := binary.BigEndian.Uint32(lengthBytes)

	// We now read: [4 byte length] + [<size> bytes data] + [4 byte CRC]
	// Note we re-read the size field for simplicity; it needs to be passed to NewDecbuf.
	allBytes := make([]byte, 4+int(length)+4)
	n, err = f.ReadAt(allBytes, off)
	if err != nil {
		return nil, err
	}
	if n != len(allBytes) {
		return nil, errors.Errorf("insufficient bytes read (got %d, wanted %d)", n, len(allBytes))
	}
	return realByteSlice(allBytes), nil
}

// newBinaryTOCFromByteSlice return parsed TOC from given index header byte slice.
func newBinaryTOCFromFile(f *os.File) (*BinaryTOC, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat")
	}
	fSize := info.Size()

	if fSize < binaryTOCLen {
		return nil, encoding.ErrInvalidSize
	}

	tocBytes := make([]byte, binaryTOCLen)
	n, err := f.ReadAt(tocBytes, fSize-binaryTOCLen)
	if err != nil {
		return nil, err
	}
	if n != len(tocBytes) {
		return nil, errors.Wrapf(encoding.ErrInvalidSize, "insufficient bytes read for binary toc (read %db, needed %db)", n, headerLen)
	}
	b := realByteSlice(tocBytes)

	expCRC := binary.BigEndian.Uint32(b[len(b)-4:])
	d := encoding.Decbuf{B: b[:len(b)-4]}

	if d.Crc32(castagnoliTable) != expCRC {
		return nil, errors.Wrap(encoding.ErrInvalidChecksum, "read index header TOC")
	}

	if err := d.Err(); err != nil {
		return nil, err
	}

	return &BinaryTOC{
		Symbols:             d.Be64(),
		PostingsOffsetTable: d.Be64(),
	}, nil
}

func (r *StreamBinaryReader) IndexVersion() (int, error) {
	return r.indexVersion, nil
}

// TODO(bwplotka): Get advantage of multi value offset fetch.
func (r *StreamBinaryReader) PostingsOffset(name, value string) (index.Range, error) {
	rngs, err := r.postingsOffset(name, value)
	if err != nil {
		return index.Range{}, err
	}
	if len(rngs) != 1 {
		return index.Range{}, NotFoundRangeErr
	}
	return rngs[0], nil
}

func skipNAndName2(d *encoding.Decbuf, buf *int) {
	if *buf == 0 {
		// Keycount+LabelName are always the same number of bytes,
		// and it's faster to skip than parse.
		*buf = d.Len()
		d.Uvarint()      // Keycount.
		d.UvarintBytes() // Label name.
		*buf -= d.Len()
		return
	}
	d.Skip(*buf)
}

func (r *StreamBinaryReader) postingsOffset(name string, values ...string) ([]index.Range, error) {
	rngs := make([]index.Range, 0, len(values))
	if r.indexVersion == index.FormatV1 {
		e, ok := r.postingsV1[name]
		if !ok {
			return nil, nil
		}
		for _, v := range values {
			rng, ok := e[v]
			if !ok {
				continue
			}
			rngs = append(rngs, rng)
		}
		return rngs, nil
	}

	e, ok := r.postings[name]
	if !ok {
		return nil, nil
	}

	if len(values) == 0 {
		return nil, nil
	}

	buf := 0
	valueIndex := 0
	for valueIndex < len(values) && values[valueIndex] < e.offsets[0].value {
		// Discard values before the start.
		valueIndex++
	}

	var newSameRngs []index.Range // The start, end offsets in the postings table in the original index file.
	for valueIndex < len(values) {
		wantedValue := values[valueIndex]

		i := sort.Search(len(e.offsets), func(i int) bool { return e.offsets[i].value >= wantedValue })
		if i == len(e.offsets) {
			// We're past the end.
			break
		}
		if i > 0 && e.offsets[i].value != wantedValue {
			// Need to look from previous entry.
			i--
		}

		//fmt.Println("NewDecBufAt")
		bs, err := readDecbufBytes(r.f, int64(r.toc.PostingsOffsetTable))
		if err != nil {
			return nil, errors.Wrap(err, "read postings")
		}

		// Don't Crc32 the entire postings offset table, this is very slow
		// so hope any issues were caught at startup.
		d := encoding.NewDecbufAt(bs, 0, nil)
		d.Skip(e.offsets[i].tableOff)

		// Iterate on the offset table.
		newSameRngs = newSameRngs[:0]
		for d.Err() == nil {
			// Posting format entry is as follows:
			// │ ┌────────────────────────────────────────┐ │
			// │ │  n = 2 <1b>                            │ │
			// │ ├──────────────────────┬─────────────────┤ │
			// │ │ len(name) <uvarint>  │ name <bytes>    │ │
			// │ ├──────────────────────┼─────────────────┤ │
			// │ │ len(value) <uvarint> │ value <bytes>   │ │
			// │ ├──────────────────────┴─────────────────┤ │
			// │ │  offset <uvarint64>                    │ │
			// │ └────────────────────────────────────────┘ │
			// First, let's skip n and name.
			skipNAndName2(&d, &buf)
			value := d.UvarintBytes() // Label value.
			postingOffset := int64(d.Uvarint64())

			if len(newSameRngs) > 0 {
				// We added some ranges in previous iteration. Use next posting offset as end of all our new ranges.
				for j := range newSameRngs {
					newSameRngs[j].End = postingOffset - crc32.Size
				}
				rngs = append(rngs, newSameRngs...)
				newSameRngs = newSameRngs[:0]
			}

			for string(value) >= wantedValue {
				// If wantedValue is equals of greater than current value, loop over all given wanted values in the values until
				// this is no longer true or there are no more values wanted.
				// This ensures we cover case when someone asks for postingsOffset(name, value1, value1, value1).

				// Record on the way if wanted value is equal to the current value.
				if string(value) == wantedValue {
					newSameRngs = append(newSameRngs, index.Range{Start: postingOffset + postingLengthFieldSize})
				}
				valueIndex++
				if valueIndex == len(values) {
					break
				}
				wantedValue = values[valueIndex]
			}

			if i+1 == len(e.offsets) {
				// No more offsets for this name.
				// Break this loop and record lastOffset on the way for ranges we just added if any.
				for j := range newSameRngs {
					newSameRngs[j].End = e.lastValOffset
				}
				rngs = append(rngs, newSameRngs...)
				break
			}

			if valueIndex != len(values) && wantedValue <= e.offsets[i+1].value {
				// wantedValue is smaller or same as the next offset we know about, let's iterate further to add those.
				continue
			}

			// Nothing wanted or wantedValue is larger than next offset we know about.
			// Let's exit and do binary search again / exit if nothing wanted.

			if len(newSameRngs) > 0 {
				// We added some ranges in this iteration. Use next posting offset as the end of our ranges.
				// We know it exists as we never go further in this loop than e.offsets[i, i+1].

				skipNAndName2(&d, &buf)
				d.UvarintBytes() // Label value.
				postingOffset := int64(d.Uvarint64())

				for j := range newSameRngs {
					newSameRngs[j].End = postingOffset - crc32.Size
				}
				rngs = append(rngs, newSameRngs...)
			}
			break
		}
		if d.Err() != nil {
			return nil, errors.Wrap(d.Err(), "get postings offset entry")
		}
	}

	return rngs, nil
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
	if r.indexVersion == index.FormatV1 {
		e, ok := r.postingsV1[name]
		if !ok {
			return nil, nil
		}
		values := make([]string, 0, len(e))
		for k := range e {
			if filter == nil || filter(k) {
				values = append(values, k)
			}
		}
		sort.Strings(values)
		return values, nil

	}
	e, ok := r.postings[name]
	if !ok {
		return nil, nil
	}
	if len(e.offsets) == 0 {
		return nil, nil
	}
	values := make([]string, 0, len(e.offsets)*r.postingOffsetsInMemSampling)

	bs, err := readDecbufBytes(r.f, int64(r.toc.PostingsOffsetTable))
	if err != nil {
		return nil, errors.Wrap(err, "read postings")
	}

	d := encoding.NewDecbufAt(bs, 0, nil)
	d.Skip(e.offsets[0].tableOff)
	lastVal := e.offsets[len(e.offsets)-1].value

	skip := 0
	for d.Err() == nil {
		if skip == 0 {
			// These are always the same number of bytes,
			// and it's faster to skip than parse.
			skip = d.Len()
			d.Uvarint()      // Keycount.
			d.UvarintBytes() // Label name.
			skip -= d.Len()
		} else {
			d.Skip(skip)
		}
		s := yoloString(d.UvarintBytes()) // Label value.
		if filter == nil || filter(s) {
			values = append(values, s)
		}
		if s == lastVal {
			break
		}
		d.Uvarint64() // Offset.
	}
	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "get postings offset entry")
	}
	return values, nil
}

func (r *StreamBinaryReader) LabelNames() ([]string, error) {
	allPostingsKeyName, _ := index.AllPostingsKey()
	labelNames := make([]string, 0, len(r.postings))
	for name := range r.postings {
		if name == allPostingsKeyName {
			// This is not from any metric.
			continue
		}
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)
	return labelNames, nil
}

func (r *StreamBinaryReader) Close() error { return r.f.Close() }
