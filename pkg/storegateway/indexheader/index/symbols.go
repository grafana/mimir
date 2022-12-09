// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/index.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"unsafe"

	"github.com/prometheus/prometheus/tsdb/index"

	streamencoding "github.com/grafana/mimir/pkg/storegateway/indexheader/encoding"
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
	tableLength int
	tableOffset int

	offsets []int
	seen    int
}

const symbolFactor = 32

// NewSymbols returns a Symbols object for symbol lookups.
func NewSymbols(factory *streamencoding.DecbufFactory, version, offset int) (s *Symbols, err error) {
	d := factory.NewDecbufAtChecked(offset, castagnoliTable)
	defer factory.CloseWithErrCapture(&err, d, "read symbols")
	if err := d.Err(); err != nil {
		return nil, fmt.Errorf("decode symbol table: %w", d.Err())
	}

	s = &Symbols{
		factory:     factory,
		version:     version,
		tableLength: d.Len() + 4, // NewDecbufAtChecked has already read the size of the table (4 bytes) by the time we get here.
		tableOffset: offset,
	}

	origLen := d.Len()
	cnt := d.Be32int()
	basePos := 4
	s.offsets = make([]int, 0, 1+cnt/symbolFactor)
	for d.Err() == nil && s.seen < cnt {
		if s.seen%symbolFactor == 0 {
			s.offsets = append(s.offsets, basePos+origLen-d.Len())
		}
		d.UvarintBytes() // The symbol.
		s.seen++
	}

	if d.Err() != nil {
		return nil, d.Err()
	}

	return s, nil
}

func (s *Symbols) Lookup(o uint32) (sym string, err error) {
	d := s.factory.NewDecbufAtUnchecked(s.tableOffset)
	defer s.factory.CloseWithErrCapture(&err, d, "lookup symbol")
	if err := d.Err(); err != nil {
		return "", err
	}

	if s.version == index.FormatV2 {
		if int(o) >= s.seen {
			return "", fmt.Errorf("unknown symbol offset %d", o)
		}
		d.ResetAt(s.offsets[int(o/symbolFactor)])
		// Walk until we find the one we want.
		for i := o - (o / symbolFactor * symbolFactor); i > 0; i-- {
			d.UvarintBytes()
		}
	} else {
		// In v1, o is relative to the beginning of the whole index header file, so we
		// need to adjust for the fact our view into the file starts at the beginning
		// of the symbol table.
		offsetInTable := int(o) - s.tableOffset
		d.ResetAt(offsetInTable)
	}
	sym = d.UvarintStr()
	if d.Err() != nil {
		return "", d.Err()
	}
	return sym, nil
}

func (s *Symbols) ReverseLookup(sym string) (o uint32, err error) {
	if len(s.offsets) == 0 {
		return 0, fmt.Errorf("unknown symbol %q - no symbols", sym)
	}

	d := s.factory.NewDecbufAtUnchecked(s.tableOffset)
	defer s.factory.CloseWithErrCapture(&err, d, "reverse lookup symbol")
	if err := d.Err(); err != nil {
		return 0, err
	}

	return s.reverseLookup(sym, d)
}

// ForEachSymbol performs a reverse lookup on each syms and passes the symbol and offset to f.
// If the offset of a symbol cannot be looked up, iteration stops immediately and the error is
// returned. If f returns an error, iteration stops immediately and the error is returned.
func (s *Symbols) ForEachSymbol(syms []string, f func(sym string, offset uint32) error) (err error) {
	if len(s.offsets) == 0 {
		return errors.New("no symbols")
	}

	d := s.factory.NewDecbufAtUnchecked(s.tableOffset)
	defer s.factory.CloseWithErrCapture(&err, d, "iterate over symbols")
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
		return string(d.UvarintBytes()) > sym
	})

	if i > 0 {
		i--
	}

	d.ResetAt(s.offsets[i])
	res := i * symbolFactor
	var lastLen int
	var lastSymbol string
	for d.Err() == nil && res <= s.seen {
		lastLen = d.Len()
		lastSymbol = yoloString(d.UvarintBytes())
		if lastSymbol >= sym {
			break
		}
		res++
	}
	if d.Err() != nil {
		return 0, d.Err()
	}
	if lastSymbol != sym {
		return 0, fmt.Errorf("unknown symbol %q", sym)
	}
	if s.version == index.FormatV2 {
		return uint32(res), nil
	}
	return uint32(s.tableLength - lastLen), nil
}

func (s *Symbols) Size() int {
	return len(s.offsets) * 8
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}
