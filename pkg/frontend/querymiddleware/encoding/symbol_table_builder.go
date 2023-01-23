// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bytes"
	"sync"
)

// TODO: ditto for symbolLengths: pooling might help here as well
type symbolTableBuilder struct {
	buf             *bytes.Buffer
	invertedSymbols map[string]uint64 // TODO: might be able to save resizing this by scanning through response once and allocating a map big enough to hold all symbols (ie. not just unique symbols)
	symbolLengths   []uint64
}

var bufferPool = sync.Pool{New: func() any { return &bytes.Buffer{} }}
var symbolLengthPool = sync.Pool{New: func() any { return []uint64{} }}

func newSymbolTableBuilder() symbolTableBuilder {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()

	symbolLengths := symbolLengthPool.Get().([]uint64)
	symbolLengths = symbolLengths[:0]

	return symbolTableBuilder{
		buf:             buf,
		invertedSymbols: map[string]uint64{},
		symbolLengths:   symbolLengths,
	}
}

func (b *symbolTableBuilder) GetOrPutSymbol(symbol string) uint64 {
	if i, ok := b.invertedSymbols[symbol]; ok {
		return i
	}

	i := uint64(len(b.invertedSymbols))
	b.invertedSymbols[symbol] = i
	b.buf.WriteString(symbol)
	b.symbolLengths = append(b.symbolLengths, uint64(len(symbol)))

	return i
}

// Build returns the final representation of this symbol table, ready for transmission.
// It is not valid to call GetOrPutSymbol after calling Build.
func (b *symbolTableBuilder) Build() (string, []uint64) {
	s := b.buf.String()

	bufferPool.Put(b.buf)
	symbolLengthPool.Put(b.symbolLengths)

	return s, b.symbolLengths
}
