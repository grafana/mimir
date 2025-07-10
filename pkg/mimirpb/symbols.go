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
			val := make([]string, 0, baseSymbolsSliceCapacity)
			return &val
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

func symbolsSliceFromPool() *[]string {
	return symbolsSlicePool.Get().(*[]string)
}

func reuseSymbolsSlice(s *[]string) {
	*s = (*s)[:0]
	symbolsSlicePool.Put(s)
}

// FastSymbolsTable is an optimized, alternate implementation of writev2.SymbolsTable.
type FastSymbolsTable struct {
	symbolsMap     map[string]uint32
	cachedSymbols  *[]string
	usedSymbolRefs int
}

func NewFastSymbolsTable(capacityHint int) *FastSymbolsTable {
	return &FastSymbolsTable{
		symbolsMap:     make(map[string]uint32, capacityHint),
		usedSymbolRefs: 0,
	}
}

func (t *FastSymbolsTable) Symbolize(str string) uint32 {
	if str == "" {
		return 0
	}
	if ref, ok := t.symbolsMap[str]; ok {
		return ref
	}
	ref := uint32(len(t.symbolsMap)) + 1
	t.symbolsMap[str] = ref
	if t.cachedSymbols != nil {
		reuseSymbolsSlice(t.cachedSymbols)
		t.cachedSymbols = nil
	}
	return ref
}

func (t *FastSymbolsTable) Symbols() []string {
	if t.cachedSymbols != nil {
		return *t.cachedSymbols
	}
	syms := symbolsSliceFromPool()
	if cap(*syms) < len(t.symbolsMap)+1 {
		*syms = make([]string, 0, len(t.symbolsMap)+1)
	}
	for range len(t.symbolsMap) + 1 {
		*syms = append(*syms, "")
	}

	for k, v := range t.symbolsMap {
		(*syms)[v] = k
	}
	t.cachedSymbols = syms
	return *t.cachedSymbols
}

func (t *FastSymbolsTable) Reset() {
	clear(t.symbolsMap)
	if t.cachedSymbols != nil {
		reuseSymbolsSlice(t.cachedSymbols)
		t.cachedSymbols = nil
	}
	t.usedSymbolRefs = 0
}
