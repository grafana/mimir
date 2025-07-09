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
	baseSymbolsMapCapacity = 0

	symbolsTablePool = sync.Pool{
		New: func() interface{} {
			return NewFastSymbolsTable(baseSymbolsMapCapacity)
		},
	}
)

func symbolsTableFromPool() *FastSymbolsTable {
	return symbolsTablePool.Get().(*FastSymbolsTable)
}

func reuseSymbolsMap(t *FastSymbolsTable) {
	t.Reset()
	symbolsTablePool.Put(t)
}

// FastSymbolsTable is an optimized, alternate implementation of writev2.SymbolsTable.
type FastSymbolsTable struct {
	symbolsMap     map[string]uint32
	cachedSymbols  []string
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
	t.cachedSymbols = nil
	return ref
}

func (t *FastSymbolsTable) Symbols() []string {
	if t.cachedSymbols != nil {
		return t.cachedSymbols
	}
	t.cachedSymbols = make([]string, len(t.symbolsMap)+1)
	t.cachedSymbols[0] = ""
	for k, v := range t.symbolsMap {
		t.cachedSymbols[v] = k
	}
	return t.cachedSymbols
}

func (t *FastSymbolsTable) Reset() {
	clear(t.symbolsMap)
	t.cachedSymbols = nil
	t.usedSymbolRefs = 0
}
