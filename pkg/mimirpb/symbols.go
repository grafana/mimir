// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"sync"

	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
)

type StringSymbolizer interface {
	Symbolize(str string) uint32
	Symbols() []string
	Reset()
}

var _ StringSymbolizer = &writev2.SymbolsTable{}

var (
	baseSymbolsMapCapacity   = 0
	baseSymbolsSliceCapacity = 40

	symbolsTablePool = sync.Pool{
		New: func() interface{} {
			return NewFastSymbolsTable(baseSymbolsMapCapacity)
		},
	}
	symbolsSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]string, 0, baseSymbolsSliceCapacity)
		},
	}
)

func symbolsTableFromPool() *FastSymbolsTable {
	return symbolsTablePool.Get().(*FastSymbolsTable)
}

func reuseSymbolsTable(t *FastSymbolsTable) {
	t.Reset()
	symbolsTablePool.Put(t)
}

func symbolsSliceFromPool() []string {
	return symbolsSlicePool.Get().([]string)
}

func reuseSymbolsSlice(s []string) {
	if cap(s) == 0 {
		return
	}

	for i := range s {
		s[i] = ""
	}
	symbolsSlicePool.Put(s[:0])
}

// FastSymbolsTable is an optimized, alternate implementation of writev2.SymbolsTable.
type FastSymbolsTable struct {
	symbolsMap     map[string]uint32
	usedSymbolRefs int
	commonSymbols  []string
	offset         uint32
}

func NewFastSymbolsTable(capacityHint int) *FastSymbolsTable {
	return &FastSymbolsTable{
		symbolsMap:     make(map[string]uint32, capacityHint),
		usedSymbolRefs: 0,
	}
}

func (t *FastSymbolsTable) ConfigureCommonSymbols(offset uint32, commonSymbols []string) {
	t.offset = offset
	t.commonSymbols = commonSymbols
}

func (t *FastSymbolsTable) Symbolize(str string) uint32 {
	if str == "" {
		// 0 means empty string, even if an offset is provided.
		return 0
	}
	if t.commonSymbols != nil {
		// TODO: CommonSymbols is bounded size, it small enough to where linear search is faster?
		for i := range t.commonSymbols {
			if str == t.commonSymbols[i] {
				return uint32(i)
			}
		}
	}

	if ref, ok := t.symbolsMap[str]; ok {
		return ref
	}
	// Symbol indexes in the map start at 1 because 0 is always reserved, and we don't need to use space to store it.
	ref := uint32(len(t.symbolsMap)) + t.offset + 1
	t.symbolsMap[str] = ref
	return ref
}

func (t *FastSymbolsTable) CountSymbols() int {
	return len(t.symbolsMap) + 1
}

func (t *FastSymbolsTable) Symbols() []string {
	syms := make([]string, 0, t.CountSymbols())
	return t.SymbolsPrealloc(syms)
}

func (t *FastSymbolsTable) SymbolsPrealloc(prealloc []string) []string {
	if cap(prealloc) < t.CountSymbols() {
		prealloc = make([]string, 0, t.CountSymbols())
	}
	for range len(t.symbolsMap) + 1 {
		prealloc = append(prealloc, "")
	}

	for k, v := range t.symbolsMap {
		prealloc[v-t.offset] = k
	}
	return prealloc
}

func (t *FastSymbolsTable) Reset() {
	clear(t.symbolsMap)
	t.usedSymbolRefs = 0
	t.offset = 0
	t.commonSymbols = nil
}
