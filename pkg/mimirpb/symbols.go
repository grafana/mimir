package mimirpb

import writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"

// StringSymbolizer handles deduplication of string into Remote-Write 2.0-style references.
type StringSymbolizer interface {
	Symbolize(str string) uint32
	Symbols() []string
}

var _ StringSymbolizer = &writev2.SymbolsTable{}

var _ StringSymbolizer = &FastSymbolsTable{}

type FastSymbolsTable struct {
	symbolsMap    map[string]uint32
	cachedSymbols []string
}

func NewFastSymbolsTable() *FastSymbolsTable {
	return &FastSymbolsTable{
		symbolsMap: map[string]uint32{"": 0},
	}
}

func (t *FastSymbolsTable) Symbolize(str string) uint32 {
	if ref, ok := t.symbolsMap[str]; ok {
		return ref
	}
	ref := uint32(len(t.symbolsMap))
	t.symbolsMap[str] = ref
	t.cachedSymbols = nil
	return ref
}

func (t *FastSymbolsTable) Symbols() []string {
	if t.cachedSymbols != nil {
		return t.cachedSymbols
	}
	t.cachedSymbols = make([]string, 0, len(t.symbolsMap)+1)
	for k, _ := range t.symbolsMap {
		t.cachedSymbols = append(t.cachedSymbols, k)
	}
	return t.cachedSymbols
}
