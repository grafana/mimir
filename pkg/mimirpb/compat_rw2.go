// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"errors"
	"fmt"
	"sync"
)

// Remote Write 2.0 related variables and functions.
var (
	errorUnexpectedRW1Timeseries      = errors.New("proto: Remote Write 1.0 field Timeseries in non-Remote Write 1.0 message")
	errorUnexpectedRW1Metadata        = errors.New("proto: Remote Write 1.0 field Metadata in non-Remote Write 1.0 message")
	errorUnexpectedRW2Timeseries      = errors.New("proto: Remote Write 2.0 field Timeseries in non-Remote Write 2.0 message")
	errorUnexpectedRW2Symbols         = errors.New("proto: Remote Write 2.0 field Symbols in non-Remote Write 2.0 message")
	errorOddNumberOfLabelRefs         = errors.New("proto: Remote Write 2.0 odd number of label references")
	errorOddNumberOfExemplarLabelRefs = errors.New("proto: Remote Write 2.0 odd number of exemplar label references")
	errorInvalidLabelRef              = errors.New("proto: Remote Write 2.0 invalid label reference")
	errorInvalidExemplarLabelRef      = errors.New("proto: Remote Write 2.0 invalid exemplar label reference")
	errorInternalRW2                  = errors.New("proto: Remote Write 2.0 internal error")
	errorInvalidHelpRef               = errors.New("proto: Remote Write 2.0 invalid help reference")
	errorInvalidUnitRef               = errors.New("proto: Remote Write 2.0 invalid unit reference")
	errorInvalidFirstSymbol           = errors.New("proto: Remote Write 2.0 symbols must start with empty string")
)

// rw2SymbolPageSize is the size of each page in bits.
const rw2SymbolPageSize = 16

// rw2PagedSymbols is a structure that holds symbols in pages.
// The problem this solves is that protobuf doesn't tell us
// how many symbols there are in advance. Without this paging
// mechanism, we would have to allocate a large amount of memory
// or do reallocation. This is a compromise between the two.
type rw2PagedSymbols struct {
	count         uint32
	pages         []*[]string
	offset        uint32
	commonSymbols []string
}

func (ps *rw2PagedSymbols) append(symbol string) {
	symbolPage := ps.count >> rw2SymbolPageSize
	if int(symbolPage) >= len(ps.pages) {
		ps.pages = append(ps.pages, rw2PagedSymbolsPool.Get().(*[]string))
	}
	*ps.pages[symbolPage] = append(*ps.pages[symbolPage], symbol)
	ps.count++
}

func (ps *rw2PagedSymbols) releasePages() {
	for _, page := range ps.pages {
		*page = (*page)[:0]
		rw2PagedSymbolsPool.Put(page)
	}
	ps.pages = nil
	ps.count = 0
}

func (ps *rw2PagedSymbols) get(ref uint32) (string, error) {
	// RW2.0 Spec: The first element of the symbols table MUST be an empty string.
	if ref == 0 {
		return "", nil
	}

	if ref < ps.offset {
		if len(ps.commonSymbols) == 0 {
			return "", fmt.Errorf("symbol %d is under the offset %d, but no common symbols table was registered", ref, ps.offset)
		}
		if ref >= uint32(len(ps.commonSymbols)) {
			return "", fmt.Errorf("common symbol reference %d is out of bounds", ref)
		}
		return ps.commonSymbols[ref], nil
	}
	ref = ref - ps.offset
	if ref < ps.count {
		page := ps.pages[ref>>rw2SymbolPageSize]
		return (*page)[ref&((1<<rw2SymbolPageSize)-1)], nil
	}
	return "", fmt.Errorf("symbol reference %d (offset %d) is out of bounds", ref, ps.offset)
}

var (
	rw2PagedSymbolsPool = sync.Pool{
		New: func() interface{} {
			page := make([]string, 0, 1<<rw2SymbolPageSize)
			return &page
		},
	}
)

const (
	// Confusingly the Remote Write 1.0 protocol is version 0.1.0.
	// However we just want to track adoption and not confuse the
	// user, so we'll stick to 1.0 and 2.0.
	// https://prometheus.io/docs/specs/prw/remote_write_spec/#protocol
	RemoteWriteVersion1 = "1.0"
	// https://prometheus.io/docs/specs/prw/remote_write_spec_2_0/#x-prometheus-remote-write-version
	RemoteWriteVersion2 = "2.0"
)

func (m *WriteRequest) ProtocolVersion() string {
	// Always default to 1.0.
	if m == nil || !m.unmarshalFromRW2 {
		return RemoteWriteVersion1
	}
	return RemoteWriteVersion2
}
