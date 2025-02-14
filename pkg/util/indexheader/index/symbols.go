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
	"github.com/prometheus/prometheus/tsdb/index"

	streamencoding "github.com/grafana/mimir/pkg/util/indexheader/encoding"
	"github.com/grafana/mimir/pkg/util/indexheader/indexheaderpb"
)

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

type Symbols struct {
	factory *streamencoding.DecbufFactory

	version     int
	tableOffset int

	offsets []int
	seen    int
}

const symbolFactor = 32

// NewSymbolsFromSparseHeader reads from sparse index header and returns a Symbols object for symbol lookups.
func NewSymbolsFromSparseHeader(factory *streamencoding.DecbufFactory, symbols *indexheaderpb.Symbols, version int, offset int) (s *Symbols, err error) {
	s = &Symbols{
		factory:     factory,
		version:     version,
		tableOffset: offset,
	}

	s.offsets = make([]int, len(symbols.Offsets))

	for i, offset := range symbols.Offsets {
		s.offsets[i] = int(offset)
	}

	s.seen = int(symbols.SymbolsCount)

	return s, nil
}

// NewSparseSymbol loads all symbols data into an index-header sparse to be persisted to disk
func (s *Symbols) NewSparseSymbol() (sparse *indexheaderpb.Symbols) {
	sparseSymbols := &indexheaderpb.Symbols{}

	offsets := make([]int64, len(s.offsets))

	for i, offset := range s.offsets {
		offsets[i] = int64(offset)
	}

	sparseSymbols.Offsets = offsets
	sparseSymbols.SymbolsCount = int64(s.seen)

	return sparseSymbols
}

// NewSymbols returns a Symbols object for symbol lookups.
func NewSymbols(factory *streamencoding.DecbufFactory, version, offset int, doChecksum bool) (s *Symbols, err error) {
	var d streamencoding.Decbuf
	if doChecksum {
		d = factory.NewDecbufAtChecked(offset, castagnoliTable)
	} else {
		d = factory.NewDecbufAtUnchecked(offset)
	}

	defer runutil.CloseWithErrCapture(&err, &d, "read symbols")
	if err := d.Err(); err != nil {
		return nil, fmt.Errorf("decode symbol table: %w", d.Err())
	}

	s = &Symbols{
		factory:     factory,
		version:     version,
		tableOffset: offset,
	}

	cnt := d.Be32int()
	s.offsets = make([]int, 0, 1+cnt/symbolFactor)
	for d.Err() == nil && s.seen < cnt {
		if s.seen%symbolFactor == 0 {
			s.offsets = append(s.offsets, d.Position())
		}
		d.SkipUvarintBytes() // The symbol.
		s.seen++
	}

	if d.Err() != nil {
		return nil, d.Err()
	}

	return s, nil
}

var ErrSymbolNotFound = errors.New("symbol not found")

// Lookup takes a symbol reference and returns the symbol string.
// For TSDB index v1, the reference is expected to be the offset of the symbol in the index header file (not the TSDB index file).
// For TSDB index v2, the reference is expected to be the sequence number of the symbol (starting at 0).
// If the symbol reference is beyond the last symbol in the symbols table, the return error's cause will be ErrSymbolNotFound.
func (s *Symbols) Lookup(o uint32) (sym string, err error) {
	d := s.factory.NewDecbufAtUnchecked(s.tableOffset)
	defer runutil.CloseWithErrCapture(&err, &d, "lookup symbol")
	if err := d.Err(); err != nil {
		return "", err
	}

	if s.version == index.FormatV2 {
		if int(o) >= s.seen {
			return "", fmt.Errorf("%w: symbol offset %d", ErrSymbolNotFound, o)
		}
		d.ResetAt(s.offsets[int(o/symbolFactor)])
		// Walk until we find the one we want.
		for i := o - (o / symbolFactor * symbolFactor); i > 0; i-- {
			d.SkipUvarintBytes()
		}
	} else {
		// In v1, o is relative to the beginning of the whole index header file, so we
		// need to adjust for the fact our view into the file starts at the beginning
		// of the symbol table.
		offsetInTable := int(o) - s.tableOffset
		if offsetInTable >= d.Len() {
			return "", fmt.Errorf("%w: symbol offset %d", ErrSymbolNotFound, o)
		}
		d.ResetAt(offsetInTable)
	}
	sym = d.UvarintStr()
	if d.Err() != nil {
		return "", d.Err()
	}
	return sym, nil
}

// ReverseLookup returns an error with cause ErrSymbolNotFound if the symbol cannot be found.
func (s *Symbols) ReverseLookup(sym string) (o uint32, err error) {
	if len(s.offsets) == 0 {
		return 0, fmt.Errorf("unknown symbol %q - no symbols", sym)
	}

	d := s.factory.NewDecbufAtUnchecked(s.tableOffset)
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
func (s *Symbols) ForEachSymbol(syms []string, f func(sym string, offset uint32) error) (err error) {
	if len(s.offsets) == 0 {
		return errors.New("no symbols")
	}

	d := s.factory.NewDecbufAtUnchecked(s.tableOffset)
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

func (s *Symbols) reverseLookup(sym string, d streamencoding.Decbuf) (uint32, error) {
	i := sort.Search(len(s.offsets), func(i int) bool {
		d.ResetAt(s.offsets[i])
		return string(d.UnsafeUvarintBytes()) > sym
	})

	if i > 0 {
		i--
	}

	d.ResetAt(s.offsets[i])
	res := i * symbolFactor
	var lastPosition int
	var lastSymbol string
	for d.Err() == nil && res <= s.seen {
		lastPosition = d.Position()
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
	if s.version == index.FormatV2 {
		return uint32(res), nil
	}
	// Since the symbols in v1 are relative to the start of the index
	// and the Decbuf position is relative to the start of the Decbuf,
	// we need to offset by the relative position of the symbols table in the index header.
	return uint32(lastPosition + s.tableOffset), nil
}

// SymbolsReader sequentially reads symbols from a TSDB block index.
type SymbolsReader interface {
	io.Closer

	// Read should return the string for the requested symbol.
	// Read should be called only with increasing symbols IDs;
	// this also means that it is not valid to call Read with the same symbol ID multiple times.
	Read(uint32) (string, error)
}

func (s *Symbols) Reader() SymbolsReader {
	d := s.factory.NewDecbufAtUnchecked(s.tableOffset)
	d.ResetAt(s.offsets[0])

	if s.version == index.FormatV2 {
		return &SymbolsTableReaderV2{
			d:             &d,
			offsets:       s.offsets,
			lastSymbolRef: uint32(s.seen - 1),
		}
	}
	return &SymbolsTableReaderV1{
		d:           &d,
		tableOffset: s.tableOffset,
	}
}

type SymbolsTableReaderV1 struct {
	// atSymbol is the offset of the symbol in the symbols table
	atSymbol    uint32
	d           *streamencoding.Decbuf
	tableOffset int
}

func (r *SymbolsTableReaderV1) Close() error {
	return r.d.Close()
}

var errReverseSymbolsReader = errors.New("trying to read symbol at earlier position")

func (r *SymbolsTableReaderV1) Read(o uint32) (string, error) {
	d := r.d
	if err := d.Err(); err != nil {
		return "", err
	}

	if o < r.atSymbol {
		return "", fmt.Errorf("%w: at %d requesting %d", errReverseSymbolsReader, r.atSymbol, o)
	}

	// In v1, o is relative to the beginning of the whole index header file, so we
	// need to adjust for the fact our view into the file starts at the beginning
	// of the symbol table.
	d.ResetAt(int(o) - r.tableOffset)
	sym := d.UvarintStr()
	r.atSymbol = o + 1

	if err := d.Err(); err != nil {
		return "", err
	}

	return sym, nil
}

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
		return "", fmt.Errorf("unknown symbol offset %d", o)
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
