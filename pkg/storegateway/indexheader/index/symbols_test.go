// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/index_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"hash/crc32"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	streamencoding "github.com/grafana/mimir/pkg/storegateway/indexheader/encoding"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestMain(m *testing.M) {
	test.VerifyNoLeakTestMain(m)
}

func TestSymbolsV1(t *testing.T) {
	const (
		numSymbols       = 100
		symbolsTableSize = 294         // excludes leading length field and the trailing crc32
		tableLengthSize  = 4           // number of bytes which store the length of the symbols table
		filePrefix       = "something" // simulate the file containing something other than the symbols table
	)
	var (
		sortedSymbols    [numSymbols]string
		sortedSymbolRefs [numSymbols]uint32
		refToSymbol      = make(map[uint32]string, numSymbols)
	)

	for i := 0; i < numSymbols; i++ {
		sortedSymbols[i] = strconv.Itoa(i)
	}
	// We must write the symbols sorted in the table, the TSDB index does the same.
	slices.Sort(sortedSymbols[:])

	buf := encoding.Encbuf{}
	buf.PutUvarintStr(filePrefix)
	symbolsStart := buf.Len()
	buf.PutBE32int(symbolsTableSize)
	buf.PutBE32int(numSymbols)

	for i, symbol := range sortedSymbols {
		// The symbol reference in v1 is the offset of the string (and its varint length) in the index header file,
		// so we use the length of the buffer as the reference (i.e. length of the file).
		symbolRef := uint32(buf.Len())
		refToSymbol[symbolRef] = symbol
		sortedSymbolRefs[i] = symbolRef
		buf.PutUvarintStr(symbol)
	}
	checksum := crc32.Checksum(buf.Get()[symbolsStart+tableLengthSize:], castagnoliTable)
	buf.PutBE32(checksum)

	dir := t.TempDir()
	filePath := path.Join(dir, "index")
	require.NoError(t, os.WriteFile(filePath, buf.Get(), 0700))

	reg := prometheus.NewPedanticRegistry()
	df := streamencoding.NewDecbufFactory(filePath, 0, log.NewNopLogger(), streamencoding.NewDecbufFactoryMetrics(reg))
	s, err := NewSymbols(df, index.FormatV1, symbolsStart)
	require.NoError(t, err)

	// We store only 4 offsets to symbols.
	require.Len(t, s.offsets, 4)

	t.Run("Lookup", func(t *testing.T) {
		for ref, symbol := range refToSymbol {
			s, err := s.Lookup(ref)
			require.NoError(t, err)
			require.Equal(t, symbol, s)
		}

		// Look beyond the last symbol
		_, err = s.Lookup(uint32(buf.Len() + 1))
		require.ErrorIs(t, err, ErrSymbolNotFound)
	})

	t.Run("ReverseLookup", func(t *testing.T) {
		for ref, symbol := range refToSymbol {
			r, err := s.ReverseLookup(symbol)
			require.NoError(t, err)
			require.Equal(t, ref, r)
		}
		_, err = s.ReverseLookup("100")
		require.ErrorIs(t, err, ErrSymbolNotFound)
	})

	t.Run("ForEachSymbol", func(t *testing.T) {
		// Use ForEachSymbol to build an offset -> symbol mapping and ensure
		// that it matches the expected offsets and symbols.
		actual := make(map[uint32]string)
		err = s.ForEachSymbol(sortedSymbols[:], func(sym string, offset uint32) error {
			actual[offset] = sym
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, refToSymbol, actual)
	})
}

func TestSymbolsV2(t *testing.T) {
	buf := encoding.Encbuf{}

	// Add prefix to the buffer to simulate symbols as part of larger buffer.
	buf.PutUvarintStr("something")

	symbolsStart := buf.Len()
	buf.PutBE32int(204) // Length of symbols table.
	buf.PutBE32int(100) // Number of symbols.
	for i := 0; i < 100; i++ {
		// i represents index in unicode characters table.
		buf.PutUvarintStr(string(rune(i))) // Symbol.
	}
	checksum := crc32.Checksum(buf.Get()[symbolsStart+4:], castagnoliTable)
	buf.PutBE32(checksum) // Check sum at the end.

	dir := t.TempDir()
	filePath := path.Join(dir, "index")
	require.NoError(t, os.WriteFile(filePath, buf.Get(), 0700))

	reg := prometheus.NewPedanticRegistry()
	df := streamencoding.NewDecbufFactory(filePath, 0, log.NewNopLogger(), streamencoding.NewDecbufFactoryMetrics(reg))
	s, err := NewSymbols(df, index.FormatV2, symbolsStart)
	require.NoError(t, err)

	// We store only 4 offsets to symbols.
	require.Len(t, s.offsets, 4)

	for i := 99; i >= 0; i-- {
		s, err := s.Lookup(uint32(i))
		require.NoError(t, err)
		require.Equal(t, string(rune(i)), s)
	}
	_, err = s.Lookup(100)
	require.ErrorIs(t, err, ErrSymbolNotFound)

	for i := 99; i >= 0; i-- {
		r, err := s.ReverseLookup(string(rune(i)))
		require.NoError(t, err)
		require.Equal(t, uint32(i), r)
	}
	_, err = s.ReverseLookup(string(rune(100)))
	require.ErrorIs(t, err, ErrSymbolNotFound)

	// Use ForEachSymbol to build an offset -> symbol mapping and ensure
	// that it matches the expected offsets and symbols.
	var symbols []string
	expected := make(map[uint32]string)
	for i := 99; i >= 0; i-- {
		symbols = append(symbols, string(rune(i)))
		expected[uint32(i)] = string(rune(i))
	}

	actual := make(map[uint32]string)
	err = s.ForEachSymbol(symbols, func(sym string, offset uint32) error {
		actual[offset] = sym
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
