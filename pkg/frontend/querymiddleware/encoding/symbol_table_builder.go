// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import "strings"

// TODO: might be able to make this better if we can use a pool of buffers rather than creating a new one each time
// (strings.Builder.Reset() discards the underlying buffer)
type symbolTableBuilder struct {
	b               *strings.Builder
	invertedSymbols map[string]uint64 // TODO: might be able to save resizing this by scanning through response once and allocating a map big enough to hold all symbols (ie. not just unique symbols)
}

func newSymbolTableBuilder() symbolTableBuilder {
	return symbolTableBuilder{
		b:               &strings.Builder{},
		invertedSymbols: map[string]uint64{},
	}
}

func (b *symbolTableBuilder) GetOrPutSymbol(symbol string) uint64 {
	if i, ok := b.invertedSymbols[symbol]; ok {
		return i
	}

	i := uint64(len(b.invertedSymbols))
	b.invertedSymbols[symbol] = i

	if i > 0 {
		b.b.Grow(len(symbol) + 1)
		b.b.WriteByte(0)
		b.b.WriteString(symbol)
	} else {
		b.b.WriteString(symbol)
	}

	return i
}

func (b *symbolTableBuilder) Build() (string, uint64) {
	return b.b.String(), uint64(len(b.invertedSymbols))
}
