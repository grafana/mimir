// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
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

	zeroSymbol labels.Symbol
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
	stringPages   []*[]string
	symbolPages   []*[]labels.Symbol
	offset        uint32
	commonStrings []string
	commonSymbols []labels.Symbol
}

func (ps *rw2PagedSymbols) append(symbol string) {
	symbolPage := ps.count >> rw2SymbolPageSize
	if int(symbolPage) >= len(ps.stringPages) {
		ps.stringPages = append(ps.stringPages, rw2PagedStringsPool.Get().(*[]string))
	}
	*ps.stringPages[symbolPage] = append(*ps.stringPages[symbolPage], symbol)
	ps.count++
}

func (ps *rw2PagedSymbols) releasePages() {
	for _, page := range ps.stringPages {
		*page = (*page)[:0]
		rw2PagedStringsPool.Put(page)
	}
	ps.stringPages = nil

	for _, page := range ps.symbolPages {
		clear(*page)
		*page = (*page)[:0]

		if cap(*page) > 0 {
			rw2PagedSymbolsPool.Put(page)
		}
	}
	ps.symbolPages = nil

	ps.count = 0
}

func (ps *rw2PagedSymbols) get(ref uint32) (string, error) {
	// RW2.0 Spec: The first element of the symbols table MUST be an empty string.
	if ref == 0 {
		return "", nil
	}

	if ref < ps.offset {
		if len(ps.commonStrings) == 0 {
			return "", fmt.Errorf("get: symbol %d is under the offset %d, but no common symbols table was registered", ref, ps.offset)
		}
		if ref >= uint32(len(ps.commonStrings)) {
			return "", fmt.Errorf("get: common symbol reference %d is out of bounds", ref)
		}
		return ps.commonStrings[ref], nil
	}

	ref = ref - ps.offset
	if ref >= ps.count {
		return "", fmt.Errorf("get: symbol reference %d (offset %d) is out of bounds", ref, ps.offset)
	}

	return ps.lookupString(ref), nil
}

func (ps *rw2PagedSymbols) lookupString(ref uint32) string {
	page := ps.stringPages[ref>>rw2SymbolPageSize]
	return (*page)[ref&((1<<rw2SymbolPageSize)-1)]
}

func (ps *rw2PagedSymbols) getSymbol(ref uint32) (labels.Symbol, error) {
	// RW2.0 Spec: The first element of the symbols table MUST be an empty string.
	if ref == 0 {
		return labels.EmptySymbol, nil
	}

	if ref < ps.offset {
		if len(ps.commonStrings) == 0 {
			return labels.EmptySymbol, fmt.Errorf("getSymbol: symbol %d is under the offset %d, but no common symbols table was registered", ref, ps.offset)
		}
		if ref >= uint32(len(ps.commonStrings)) {
			return labels.EmptySymbol, fmt.Errorf("getSymbol: common symbol reference %d is out of bounds", ref)
		}
		return ps.commonSymbols[ref], nil
	}

	ref = ref - ps.offset
	if ref >= ps.count {
		return labels.EmptySymbol, fmt.Errorf("getSymbol: symbol reference %d (offset %d) is out of bounds", ref, ps.offset)
	}

	pageIdx := ref >> rw2SymbolPageSize

	if len(ps.symbolPages) <= int(pageIdx) {
		ps.symbolPages = slices.Grow(ps.symbolPages, int(pageIdx+1))
		ps.symbolPages = ps.symbolPages[:pageIdx+1]
	}

	if ps.symbolPages[pageIdx] == nil {
		page := rw2PagedSymbolsPool.Get().(*[]labels.Symbol)
		*page = (*page)[:1<<rw2SymbolPageSize]
		ps.symbolPages[pageIdx] = page
	}

	page := *ps.symbolPages[pageIdx]
	symbolIdx := ref & ((1 << rw2SymbolPageSize) - 1)
	if existing := page[symbolIdx]; existing != zeroSymbol {
		return existing, nil
	}

	symbol := labels.NewSymbol(ps.lookupString(ref))
	page[symbolIdx] = symbol

	return symbol, nil
}

var (
	rw2PagedStringsPool = sync.Pool{
		New: func() interface{} {
			page := make([]string, 0, 1<<rw2SymbolPageSize)
			return &page
		},
	}

	rw2PagedSymbolsPool = sync.Pool{
		New: func() interface{} {
			page := make([]labels.Symbol, 0, 1<<rw2SymbolPageSize)
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

func (m *WriteRequest) GetLabels(ts PreallocTimeseries) (labels.Labels, error) {
	if len(ts.LabelSymbols) == 0 {
		return FromLabelAdaptersToLabels(ts.Labels), nil
	}

	if len(ts.LabelSymbols)%2 != 0 {
		return labels.EmptyLabels(), errorOddNumberOfLabelRefs
	}

	labelCount := len(ts.LabelSymbols) / 2
	lbls := make(labels.Labels, 0, labelCount)

	for i := range labelCount {
		name, err := m.rw2symbols.getSymbol(ts.LabelSymbols[2*i])
		if err != nil {
			return labels.EmptyLabels(), err
		}

		value, err := m.rw2symbols.getSymbol(ts.LabelSymbols[2*i+1])
		if err != nil {
			return labels.EmptyLabels(), err
		}

		lbls = append(lbls, labels.SymbolisedLabel{
			Name:  name,
			Value: value,
		})
	}

	return lbls, nil
}

func (m *WriteRequest) FreeSymbols() {
	m.rw2symbols.releasePages()
}
