// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/index.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"unsafe"

	"github.com/grafana/dskit/runutil"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
)

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

type SymbolsTableReader struct {
	indexVersion  int
	tableOffset   int
	decbufFactory streamencoding.DecbufFactory

	allSymbolsCount int
	sparseOffsets   []int
}

const symbolFactor = 32

//// NewSymbolsTableReaderFromSparseHeader reads from sparse index header and returns a Symbols object for symbol lookups.
//func NewSymbolsTableReaderFromSparseHeader(factory streamencoding.DecbufFactory, sparseSymbols *indexheaderpb.Symbols, offset int) (s *SymbolsTableReader, err error) {
//	s = &SymbolsTableReader{
//		decbufFactory: factory,
//		tableOffset:   offset,
//	}
//
//	s.sparseOffsets = make([]int, len(sparseSymbols.Offsets))
//
//	for i, offset := range sparseSymbols.Offsets {
//		s.sparseOffsets[i] = int(offset)
//	}
//
//	s.allSymbolsCount = int(sparseSymbols.SymbolsCount)
//
//	return s, nil
//}
//
//// NewSymbolsTableReaderFromIndexHeader returns a SymbolsTableReader object for symbol lookups.
//func NewSymbolsTableReaderFromIndexHeader(
//	decbufFactory streamencoding.DecbufFactory, indexVersion, tableOffset int, doChecksum bool,
//) (s *SymbolsTableReader, err error) {
//	allSymbolsCount, sparseOffsets, err := SparseValuesFromSymbolsTable(decbufFactory, tableOffset, doChecksum)
//	if err != nil {
//		return nil, err
//	}
//
//	return &SymbolsTableReader{
//		indexVersion:    indexVersion,
//		tableOffset:     tableOffset,
//		decbufFactory:   decbufFactory,
//		allSymbolsCount: allSymbolsCount,
//		sparseOffsets:   sparseOffsets,
//	}, nil
//}

func SparseValuesFromSymbolsTable(
	decbufFactory streamencoding.DecbufFactory,
	tableOffset int,
	doChecksum bool,
) (allSymbolsCount int, sparseSymbolsOffsets []int, err error) {
	var decbuf streamencoding.Decbuf
	if doChecksum {
		decbuf = decbufFactory.NewDecbufAtChecked(tableOffset, castagnoliTable)
	} else {
		decbuf = decbufFactory.NewDecbufAtUnchecked(tableOffset)
	}

	defer runutil.CloseWithErrCapture(&err, &decbuf, "decode symbols table")
	if err := decbuf.Err(); err != nil {
		return -1, nil, fmt.Errorf("init symbol table decoding buffer: %w", decbuf.Err())
	}

	// Symbols table format:
	// ┌────────────────────┬─────────────────────┐
	// │ len <4b>           │ #symbols <4b>       │
	// ├────────────────────┴─────────────────────┤
	// │ ┌──────────────────────┬───────────────┐ │
	// │ │ len(str_1) <uvarint> │ str_1 <bytes> │ │
	// │ ├──────────────────────┴───────────────┤ │
	// │ │                . . .                 │ │
	// │ ├──────────────────────┬───────────────┤ │
	// │ │ len(str_n) <uvarint> │ str_n <bytes> │ │
	// │ └──────────────────────┴───────────────┘ │
	// ├──────────────────────────────────────────┤
	// │ CRC32 <4b>                               │
	// └──────────────────────────────────────────┘

	// Get symbols count; decbuf has already consumed the len field.
	allSymbolsCount = decbuf.Be32int()

	seen := 0
	sparseSymbolsOffsets = make([]int, 0, 1+allSymbolsCount/symbolFactor)
	for decbuf.Err() == nil && seen < allSymbolsCount {
		if seen%symbolFactor == 0 {
			sparseSymbolsOffsets = append(sparseSymbolsOffsets, decbuf.Offset())
		}
		decbuf.SkipUvarintBytes() // The symbol.
		seen++
	}

	if decbuf.Err() != nil {
		return -1, nil, decbuf.Err()
	}

	return allSymbolsCount, sparseSymbolsOffsets, nil
}

var ErrSymbolNotFound = errors.New("symbol not found")

// Lookup takes a symbol reference and returns the symbol string.
// For TSDB index v2, the reference is expected to be the sequence number of the symbol (starting at 0).
// If the symbol reference is beyond the last symbol in the symbols table, the return error's cause will be ErrSymbolNotFound.
func (s *SymbolsTableReader) Lookup(o uint32) (sym string, err error) {
	d := s.decbufFactory.NewDecbufAtUnchecked(s.tableOffset)
	defer runutil.CloseWithErrCapture(&err, &d, "lookup symbol")
	if err := d.Err(); err != nil {
		return "", err
	}

	if int(o) >= s.allSymbolsCount {
		return "", fmt.Errorf("%w: symbol offset %d", ErrSymbolNotFound, o)
	}
	d.ResetAt(s.sparseOffsets[int(o/symbolFactor)])
	// Walk until we find the one we want.
	for i := o - (o / symbolFactor * symbolFactor); i > 0; i-- {
		d.SkipUvarintBytes()
	}

	sym = d.UvarintStr()
	if d.Err() != nil {
		return "", d.Err()
	}
	return sym, nil
}

// ReverseLookup returns an error with cause ErrSymbolNotFound if the symbol cannot be found.
func (s *SymbolsTableReader) ReverseLookup(sym string) (o uint32, err error) {
	if len(s.sparseOffsets) == 0 {
		return 0, fmt.Errorf("unknown symbol %q - no symbols", sym)
	}

	d := s.decbufFactory.NewDecbufAtUnchecked(s.tableOffset)
	defer runutil.CloseWithErrCapture(&err, &d, "reverse lookup symbol")
	if err := d.Err(); err != nil {
		return 0, err
	}

	return s.reverseLookup(sym, d)
}

// ForEachSymbol performs a reverse lookup on each syms and passes the symbol and offset to f.
// For TSDB index v1, the symbol reference is the offset of the symbol in the index header file (not the TSDB index file).
// For TSDB index v2, the symbol reference is the sequence number of the symbol (starting at 0).
//
// If the reference of a symbol cannot be looked up, iteration stops immediately and the error is
// returned. If f returns an error, iteration stops immediately and the error is returned.
// ForEachSymbol returns an error with cause ErrSymbolNotFound if any symbol cannot be found.
func (s *SymbolsTableReader) ForEachSymbol(syms []string, f func(sym string, offset uint32) error) (err error) {
	if len(s.sparseOffsets) == 0 {
		return errors.New("no symbols")
	}

	d := s.decbufFactory.NewDecbufAtUnchecked(s.tableOffset)
	defer runutil.CloseWithErrCapture(&err, &d, "iterate over symbols")
	if err := d.Err(); err != nil {
		return err
	}

	for _, sym := range syms {
		offset, err := s.reverseLookup(sym, d)
		if err != nil {
			return fmt.Errorf("cannot lookup %q: %w", sym, err)
		}

		err = f(sym, offset)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *SymbolsTableReader) reverseLookup(sym string, d streamencoding.Decbuf) (uint32, error) {
	i := sort.Search(len(s.sparseOffsets), func(i int) bool {
		d.ResetAt(s.sparseOffsets[i])
		return string(d.UnsafeUvarintBytes()) > sym
	})

	if i > 0 {
		i--
	}

	d.ResetAt(s.sparseOffsets[i])
	res := i * symbolFactor
	var lastSymbol string
	for d.Err() == nil && res <= s.allSymbolsCount {
		lastSymbol = yoloString(d.UnsafeUvarintBytes())
		if lastSymbol >= sym {
			break
		}
		res++
	}
	if err := d.Err(); err != nil {
		if errors.Is(err, streamencoding.ErrInvalidSize) {
			return 0, fmt.Errorf("%w: %q", ErrSymbolNotFound, sym)
		}
		return 0, d.Err()
	}
	if lastSymbol != sym {
		return 0, fmt.Errorf("%w: %q", ErrSymbolNotFound, sym)
	}

	return uint32(res), nil
}

// SymbolsReader sequentially reads symbols from a TSDB block index.
type SymbolsReader interface {
	io.Closer

	// Read should return the string for the requested symbol.
	// Read should be called only with increasing symbols IDs;
	// this also means that it is not valid to call Read with the same symbol ID multiple times.
	Read(uint32) (string, error)
}

func (s *SymbolsTableReader) Reader() SymbolsReader {
	d := s.decbufFactory.NewDecbufAtUnchecked(s.tableOffset)
	d.ResetAt(s.sparseOffsets[0])

	return &SymbolsTableReaderV2{
		d:             &d,
		offsets:       s.sparseOffsets,
		lastSymbolRef: uint32(s.allSymbolsCount - 1),
	}
}

var errReverseSymbolsReader = errors.New("trying to read symbol at earlier position")

type SymbolsTableReaderV2 struct {
	d *streamencoding.Decbuf
	// atSymbol is the index the symbol currently pointed by the Decbuf head
	atSymbol      uint32
	lastSymbolRef uint32
	offsets       []int
}

func (r *SymbolsTableReaderV2) Close() error {
	return r.d.Close()
}

func (r *SymbolsTableReaderV2) Read(o uint32) (string, error) {
	d := r.d
	if err := d.Err(); err != nil {
		return "", err
	}

	if o < r.atSymbol {
		return "", fmt.Errorf("%w: at %d requesting %d", errReverseSymbolsReader, r.atSymbol, o)
	}
	if o > r.lastSymbolRef {
		return "", fmt.Errorf("%w: %d", ErrSymbolNotFound, o)
	}

	if targetOffsetIdx, currentOffsetIdx := o/symbolFactor, r.atSymbol/symbolFactor; targetOffsetIdx > currentOffsetIdx {
		// Only ResetAt a bigger offset than the current one.
		// We don't want to ResetAt an offset we've gone past: that will reverse the file reader, and we will do unnecessary reads.
		d.ResetAt(r.offsets[int(targetOffsetIdx)])
		r.atSymbol = targetOffsetIdx * symbolFactor
	}
	// We've offset to the right group of symbolFactor symbols. Now skip until the requested symbol within that group.
	for i := o - r.atSymbol; i > 0; i-- {
		d.SkipUvarintBytes()
		r.atSymbol++
	}
	sym := d.UvarintStr()
	r.atSymbol++

	if err := d.Err(); err != nil {
		return "", err
	}

	return sym, nil
}

func yoloString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b)) // nolint:gosec
}
