package mimirpb

import (
	"sync"

	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
)

// StringSymbolizer handles deduplication of string into Remote-Write 2.0-style references.

type StringSymbolizer interface {
	Symbolize(str string) uint32
	Symbols() []string
	Reset()
}

var _ StringSymbolizer = &writev2.SymbolsTable{}

var _ StringSymbolizer = &FastSymbolsTable{}

var (
	symbolizerPool = sync.Pool{
		New: func() interface{} {
			return NewFastSymbolsTable()
		},
	}
)

func symbolizerFromPool() *FastSymbolsTable {
	return symbolizerPool.Get().(*FastSymbolsTable)
}

func reuseSymbolizer(s *FastSymbolsTable) {
	s.Reset()
	symbolizerPool.Put(s)
}

type FastSymbolsTable struct {
	symbolsMap map[string]uint32
	// symbolsMap     sync.Map
	cachedSymbols  []string
	usedSymbolRefs int
}

func NewFastSymbolsTable() *FastSymbolsTable {
	return &FastSymbolsTable{
		symbolsMap:     map[string]uint32{},
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
