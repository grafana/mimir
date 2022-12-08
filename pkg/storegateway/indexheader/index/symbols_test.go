// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/index_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"hash/crc32"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"

	streamencoding "github.com/grafana/mimir/pkg/storegateway/indexheader/encoding"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestSymbols(t *testing.T) {
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

	df := streamencoding.NewDecbufFactory(filePath, 0)
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
	require.Error(t, err)

	for i := 99; i >= 0; i-- {
		r, err := s.ReverseLookup(string(rune(i)))
		require.NoError(t, err)
		require.Equal(t, uint32(i), r)
	}
	_, err = s.ReverseLookup(string(rune(100)))
	require.Error(t, err)

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
