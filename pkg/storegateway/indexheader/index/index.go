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
	"encoding/binary"
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
	// pass in file path and create readers when required?
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
	lengthBytes := make([]byte, 4)
	n, err := f.ReadAt(lengthBytes, int64(offset))
	if err != nil {
		return nil, err
	}
	if n != 4 {
		return nil, errors.Wrapf(stream_encoding.ErrInvalidSize, "insufficient bytes read for symbol table size (got %d, wanted %d)", n, 4)
	}

	s := &Symbols{
		f:           f,
		version:     version,
		tableLength: len(lengthBytes) + int(binary.BigEndian.Uint32(lengthBytes)) + 4,
		tableOffset: offset,
	}

	r, err := stream_encoding.NewFileReader(f, offset, s.tableLength)
	if err != nil {
		return nil, errors.Wrap(err, "create symbol table file reader")
	}

	d := stream_encoding.NewDecbuf(r, 0, castagnoliTable)
	if d.Err() != nil {
		return nil, errors.Wrap(err, "decode symbol table")
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

// newRawDecbuf returns a Decbuf for reading the contents of this symbol table.
// It does not check the integrity of the symbol table, as it is assumed that this is
// done in NewSymbols.
func (s Symbols) newRawDecbuf() (stream_encoding.Decbuf, error) {
	r, err := stream_encoding.NewFileReader(s.f, s.tableOffset, s.tableLength)
	if err != nil {
		return stream_encoding.Decbuf{}, errors.Wrap(err, "create symbol table file reader")
	}

	return stream_encoding.NewDecbufRawReader(r), nil
}

func (s Symbols) Lookup(o uint32) (string, error) {
	d, err := s.newRawDecbuf()
	if err != nil {
		return "", err
	}

	if s.version == index.FormatV2 {
		if int(o) >= s.seen {
			return "", errors.Errorf("unknown symbol offset %d", o)
		}
		d.Skip(s.offsets[int(o/symbolFactor)])
		// Walk until we find the one we want.
		for i := o - (o / symbolFactor * symbolFactor); i > 0; i-- {
			d.UvarintBytes()
		}
	} else {
		// In v1, o is relative to the beginning of the whole index header file, so we
		// need to adjust for the fact our view into the file starts at the beginning
		// of the symbol table.
		offsetInTable := int(o) - s.tableOffset
		d.Skip(offsetInTable)
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

	i := sort.Search(len(s.offsets), func(i int) bool {
		// TODO: don't create a new Decbuf instance for every call of this function -
		// instead, add a Seek() method to Decbuf and use the one instance for the entirety of ReverseLookup()
		d, err := s.newRawDecbuf()
		if err != nil {
			panic(err)
		}

		d.Skip(s.offsets[i])
		return yoloString(d.UvarintBytes()) > sym
	})

	d, err := s.newRawDecbuf()
	if err != nil {
		return 0, err
	}

	if i > 0 {
		i--
	}
	d.Skip(s.offsets[i])
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
func ReadOffsetTable(bs ByteSlice, off uint64, f func([]string, uint64, int) error) error {
	d := stream_encoding.NewDecbufAt(bs, int(off), castagnoliTable)
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
