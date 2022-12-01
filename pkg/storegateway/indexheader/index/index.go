// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/index.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"hash/crc32"
	"sort"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/index"

	stream_encoding "github.com/grafana/mimir/pkg/storegateway/indexheader/encoding"
)

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

// ByteSlice abstracts a byte slice.
type ByteSlice interface {
	Len() int
	Range(start, end int) []byte
}

type Symbols struct {
	factory *stream_encoding.DecbufFactory

	version     int
	tableLength int
	tableOffset int

	offsets []int
	seen    int
}

const symbolFactor = 32

// NewSymbols returns a Symbols object for symbol lookups.
func NewSymbols(factory *stream_encoding.DecbufFactory, version, offset int) (*Symbols, error) {
	d := factory.NewDecbufAtChecked(offset, castagnoliTable)
	defer d.Close()
	if err := d.Err(); err != nil {
		return nil, errors.Wrap(d.Err(), "decode symbol table")
	}

	s := &Symbols{
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

func (s *Symbols) Lookup(o uint32) (string, error) {
	d := s.factory.NewDecbufAtUnchecked(s.tableOffset)
	defer d.Close()
	if err := d.Err(); err != nil {
		return "", err
	}

	if s.version == index.FormatV2 {
		if int(o) >= s.seen {
			return "", errors.Errorf("unknown symbol offset %d", o)
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
	sym := d.UvarintStr()
	if d.Err() != nil {
		return "", d.Err()
	}
	return sym, nil
}

func (s *Symbols) ReverseLookup(sym string) (uint32, error) {
	if len(s.offsets) == 0 {
		return 0, errors.Errorf("unknown symbol %q - no symbols", sym)
	}

	d := s.factory.NewDecbufAtUnchecked(s.tableOffset)
	defer d.Close()
	if err := d.Err(); err != nil {
		return 0, err
	}

	i := sort.Search(len(s.offsets), func(i int) bool {
		d.ResetAt(s.offsets[i])
		return yoloString(d.UvarintBytes()) > sym
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
		return 0, errors.Errorf("unknown symbol %q", sym)
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
