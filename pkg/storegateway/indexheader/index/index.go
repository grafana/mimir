// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"hash/crc32"
	"os"
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
	// TODO: we shouldn't be sharing a single file descriptor here, as we will use it from multiple goroutines simultaneously -
	//  pass in file path and create readers when required? Or pass in a Reader factory: `func() Reader` that we call to create
	//  a new reader whenever needed. This could open the file fresh for the FileReader case. This also opens up the possibility
	//  of pooling the `bufio.Reader`s to avoid allocating when they're created.
	f           *os.File
	version     int
	tableLength int
	tableOffset int

	offsets []int
	seen    int
}

const symbolFactor = 32

// NewSymbols returns a Symbols object for symbol lookups.
// f should contain a Decbuf-encoded symbol table at offset.
func NewSymbols(f *os.File, version, offset int) (*Symbols, error) {
	d := stream_encoding.NewDecbufFromFile(f, offset, castagnoliTable)
	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "decode symbol table")
	}

	s := &Symbols{
		f:           f,
		version:     version,
		tableLength: d.Len() + 4, // NewDecbufFromFile has already read the size of the table (4 bytes) by the time we get here.
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

// newDecbufWithoutIntegrityCheck returns a Decbuf for reading the contents of this symbol table.
// It does not check the integrity of the symbol table, as it is assumed that this is
// done in NewSymbols.
func (s Symbols) newDecbufWithoutIntegrityCheck() stream_encoding.Decbuf {
	return stream_encoding.NewDecbufFromFile(s.f, s.tableOffset, nil)
}

func (s Symbols) Lookup(o uint32) (string, error) {
	d := s.newDecbufWithoutIntegrityCheck()
	if d.Err() != nil {
		return "", d.Err()
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

func (s Symbols) ReverseLookup(sym string) (uint32, error) {
	if len(s.offsets) == 0 {
		return 0, errors.Errorf("unknown symbol %q - no symbols", sym)
	}

	d := s.newDecbufWithoutIntegrityCheck()
	if d.Err() != nil {
		return 0, d.Err()
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

func (s Symbols) Size() int {
	return len(s.offsets) * 8
}

// ReadOffsetTable reads an offset table and at the given position calls f for each
// found entry. If f returns an error it stops decoding and returns the received error.
func ReadOffsetTable(src *os.File, off uint64, f func([]string, uint64, int) error) error {
	d := stream_encoding.NewDecbufFromFile(src, int(off), castagnoliTable)
	startLen := d.Len()
	cnt := d.Be32()

	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		offsetPos := startLen - d.Len()
		keyCount := d.Uvarint()
		// The Postings offset table takes only 2 keys per entry (name and value of label),
		// and the LabelIndices offset table takes only 1 key per entry (a label name).
		// Hence setting the size to max of both, i.e. 2.
		keys := make([]string, 0, 2)

		for i := 0; i < keyCount; i++ {
			keys = append(keys, d.UvarintStr())
		}
		o := d.Uvarint64()
		if d.Err() != nil {
			break
		}
		if err := f(keys, o, offsetPos); err != nil {
			return err
		}
		cnt--
	}
	return d.Err()
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}
