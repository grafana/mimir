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

const (
	minPreallocatedSymbolsPerRequest = 100
	maxPreallocatedSymbolsPerRequest = 100000
)

var (
	symbolsTablePool = sync.Pool{
		New: func() interface{} {
			return NewFastSymbolsTable(minPreallocatedSymbolsPerRequest)
		},
	}
	symbolsSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]string, 0, minPreallocatedSymbolsPerRequest)
		},
	}
)

func symbolsTableFromPool() *FastSymbolsTable {
	return symbolsTablePool.Get().(*FastSymbolsTable)
}

func reuseSymbolsTable(t *FastSymbolsTable) {
	// Prevent very large capacity maps from being put back in the pool.
	// Otherwise, a few requests with a huge number of symbols might poison the pool by growing all the pooled maps,
	// which never shrink (in go 1.24.x) causing in-use heap memory to significantly increase.
	if t.CapLowerBound() > maxPreallocatedSymbolsPerRequest {
		return
	}

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
	// Prevent very large slices from being put back in the pool.
	// Otherwise, a few requests with a huge number of symbols might poison the pool by growing all the pooled slices,
	// causing in-use heap memory to significantly increase and never shrink.
	if cap(s) > maxPreallocatedSymbolsPerRequest {
		return
	}

	for i := range s {
		s[i] = ""
	}
	//nolint:staticcheck // SA6002: We can't put a pointer back in the pool, since it holds slices instead.
	symbolsSlicePool.Put(s[:0])
}

// FastSymbolsTable is an optimized, alternate implementation of writev2.SymbolsTable.
type FastSymbolsTable struct {
	symbolsMap                   map[string]uint32
	symbolsMapCapacityLowerBound int

	usedSymbolRefs int
	commonSymbols  *CommonSymbols
	offset         uint32
}

func NewFastSymbolsTable(capacityHint int) *FastSymbolsTable {
	return &FastSymbolsTable{
		symbolsMap:                   make(map[string]uint32, capacityHint),
		symbolsMapCapacityLowerBound: capacityHint,
		usedSymbolRefs:               0,
	}
}

func (t *FastSymbolsTable) ConfigureCommonSymbols(offset uint32, commonSymbols *CommonSymbols) {
	t.offset = offset
	t.commonSymbols = commonSymbols
}

func (t *FastSymbolsTable) Symbolize(str string) uint32 {
	if str == "" {
		// 0 means empty string, even if an offset is provided.
		return 0
	}
	if t.commonSymbols != nil {
		commonSymbols := t.commonSymbols.GetMap()
		if ref, ok := commonSymbols[str]; ok {
			return ref
		}
	}

	if ref, ok := t.symbolsMap[str]; ok {
		return ref
	}
	// Symbol indexes in the map start at 1 because 0 is always reserved, and we don't need to use space to store it.
	symMapLen := len(t.symbolsMap)
	ref := uint32(symMapLen) + t.offset + 1
	t.symbolsMap[str] = ref
	if symMapLen+1 > t.symbolsMapCapacityLowerBound {
		t.symbolsMapCapacityLowerBound = symMapLen + 1
	}
	return ref
}

func (t *FastSymbolsTable) CountSymbols() int {
	return len(t.symbolsMap) + 1
}

func (t *FastSymbolsTable) CapLowerBound() int {
	return t.symbolsMapCapacityLowerBound
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

func (t *FastSymbolsTable) SymbolsSizeProto() int {
	var l, n int
	for k := range t.symbolsMap {
		l = len(k)
		n += 1 + l + sovMimir(uint64(l))
	}
	return n
}

func (t *FastSymbolsTable) Reset() {
	clear(t.symbolsMap)
	// We intentionally don't reset symbolsMapCapacityLowerBound.
	// At the time of writing (go 1.24.x) the map will never reduce its capacity
	// when its contents are cleared, but also the capacity cannot be queried.
	t.usedSymbolRefs = 0
	t.offset = 0
	t.commonSymbols = nil
}

// CommonSymbols is used to store a shared, pre-loaded, immutable symbols table that's re-used multiple times.
// It can look like either a slice, where the position in the slice is the symbol, or a map from string to symbol.
// Thread safe.
type CommonSymbols struct {
	symbols []string
	mapFunc func() map[string]uint32
}

func NewCommonSymbols(symbols []string) *CommonSymbols {
	mapFunc := sync.OnceValue(func() map[string]uint32 {
		res := make(map[string]uint32, len(symbols))
		for ref, str := range symbols {
			res[str] = uint32(ref)
		}
		return res
	})
	return &CommonSymbols{
		symbols: symbols,
		mapFunc: mapFunc,
	}
}

func (cs *CommonSymbols) GetSlice() []string {
	if cs == nil {
		return nil
	}
	return cs.symbols
}

func (cs *CommonSymbols) GetMap() map[string]uint32 {
	if cs == nil {
		return nil
	}
	return cs.mapFunc()
}
