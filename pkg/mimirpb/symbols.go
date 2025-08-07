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
	//nolint:staticcheck
	symbolsSlicePool.Put(s[:0])
}

// FastSymbolsTable is an optimized, alternate implementation of writev2.SymbolsTable.
type FastSymbolsTable struct {
	symbolsMap                   map[string]uint32
	symbolsMapCapacityLowerBound int

	usedSymbolRefs int
	commonSymbols  []string
	offset         uint32
}

func NewFastSymbolsTable(capacityHint int) *FastSymbolsTable {
	return &FastSymbolsTable{
		symbolsMap:                   make(map[string]uint32, capacityHint),
		symbolsMapCapacityLowerBound: capacityHint,
		usedSymbolRefs:               0,
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
