// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/index_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestMain(m *testing.M) {
	test.VerifyNoLeakTestMain(m)
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

	bkt, err := filesystem.NewBucket(dir)
	require.NoError(t, err)
	instBkt := objstore.WithNoopInstr(bkt)
	t.Cleanup(func() {
		require.NoError(t, bkt.Close())
	})

	reg := prometheus.NewPedanticRegistry()
	diskDecbufFactory := streamencoding.NewFilePoolDecbufFactory(filePath, 0, streamencoding.NewDecbufFactoryMetrics(reg))
	bucketDecbufFactory := streamencoding.NewBucketDecbufFactory(context.Background(), instBkt, "index")

	factories := map[string]streamencoding.DecbufFactory{
		"disk":   diskDecbufFactory,
		"bucket": bucketDecbufFactory,
	}

	for factoryName, decbufFactory := range factories {
		s, err := NewSymbols(decbufFactory, index.FormatV2, symbolsStart, true)
		require.NoError(t, err)

		// We store only 4 offsets to symbols.
		require.Len(t, s.offsets, 4)

		t.Run(fmt.Sprintf("Lookup/DecbufFactory=%s", factoryName), func(t *testing.T) {
			for i := 99; i >= 0; i-- {
				s, err := s.Lookup(uint32(i))
				require.NoError(t, err)
				require.Equal(t, string(rune(i)), s)
			}
			_, err = s.Lookup(100)
			require.ErrorIs(t, err, ErrSymbolNotFound)
		})

		t.Run(fmt.Sprintf("ReverseLookup/DecbufFactory=%s", factoryName), func(t *testing.T) {
			for i := 99; i >= 0; i-- {
				r, err := s.ReverseLookup(string(rune(i)))
				require.NoError(t, err)
				require.Equal(t, uint32(i), r)
			}
			_, err = s.ReverseLookup(string(rune(100)))
			require.ErrorIs(t, err, ErrSymbolNotFound)
		})

		t.Run(fmt.Sprintf("ForEachSymbol/DecbufFactory=%s", factoryName), func(t *testing.T) {
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
		})

		t.Run(fmt.Sprintf("ReadIterateAllSymbols/DecbufFactory=%s", factoryName), func(t *testing.T) {
			r := s.Reader()

			for i := uint32(0); i <= 99; i++ {
				sym, err := r.Read(i)
				require.NoError(t, err)
				require.Equal(t, string(rune(i)), sym)
			}

			require.NoError(t, r.Close())
		})

		t.Run(fmt.Sprintf("ReadTwoDistantSymbols/DecbufFactory=%s", factoryName), func(t *testing.T) {
			r := s.Reader()

			s7, err := r.Read(7)
			require.NoError(t, err)
			require.Equal(t, string(rune(7)), s7)

			s97, err := r.Read(97)
			require.NoError(t, err)
			require.Equal(t, string(rune(97)), s97)

			require.NoError(t, r.Close())
		})

		t.Run(fmt.Sprintf("ReadTwoSymbolsReverseOrder/DecbufFactory=%s", factoryName), func(t *testing.T) {
			r := s.Reader()

			_, err = r.Read(20)
			require.NoError(t, err)

			_, err = r.Read(10)
			require.ErrorIs(t, err, errReverseSymbolsReader)

			require.NoError(t, r.Close())
		})
	}

}
