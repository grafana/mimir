// SPDX-License-Identifier: AGPL-3.0-only

package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	"github.com/grafana/mimir/pkg/util/filepool"
)

func makeV4SymbolSection(symbols []string) (data []byte, offsets []int) {
	var b encoding.Encbuf
	offsets = []int{}
	b.PutBE64(0)
	b.PutBE32int(len(symbols))
	for i, symbol := range symbols {
		if i%symbolFactor == 0 {
			offsets = append(offsets, b.Len())
		}
		b.PutUvarintStr(symbol)
	}
	binary.BigEndian.PutUint64(b.B, uint64(b.Len()-8))
	b.PutBE32(crc32.Checksum(b.B[8:], castagnoliTable))
	var directory encoding.Encbuf
	directory.PutBE32(symbolFactor)
	previous := 0
	for _, offset := range offsets {
		directory.PutUvarint64(uint64(offset - previous))
		previous = offset
	}
	b.PutBE32int(directory.Len())
	b.PutBytes(directory.B)
	b.PutBE32(crc32.Checksum(directory.B, castagnoliTable))
	return b.B, offsets
}

func v4SymbolFactory(t *testing.T, data []byte) *streamencoding.FilePoolDecbufFactory {
	t.Helper()
	filename := filepath.Join(t.TempDir(), "symbols")
	require.NoError(t, os.WriteFile(filename, data, 0600))
	factory := streamencoding.NewFilePoolDecbufFactory(filename, 0, filepool.NewFilePoolMetrics(nil))
	t.Cleanup(func() { require.NoError(t, factory.Close()) })
	return factory
}

func TestSparseValuesFromV4SymbolsTable(t *testing.T) {
	for _, count := range []int{0, 1, 31, 32, 33, 64, 65} {
		for _, long := range []bool{false, true} {
			t.Run(fmt.Sprintf("count=%d/long=%v", count, long), func(t *testing.T) {
				symbols := make([]string, count)
				for i := range symbols {
					symbols[i] = fmt.Sprintf("%04d", i)
					if long {
						symbols[i] += strings.Repeat("x", 8192)
					}
				}
				data, expected := makeV4SymbolSection(symbols)
				const offset = 17
				end := offset + len(data)
				data = append(append(make([]byte, offset), data...), make([]byte, 7)...)
				factory := v4SymbolFactory(t, data)
				for _, checksum := range []bool{false, true} {
					gotCount, gotOffsets, err := SparseValuesFromV4SymbolsTable(t.Context(), factory, offset, end, checksum)
					require.NoError(t, err)
					require.Equal(t, count, gotCount)
					require.Equal(t, len(expected), len(gotOffsets))
					require.Equal(t, expected, gotOffsets)
					reader, err := NewSymbolsTableReader(index.FormatV4, factory, offset, gotCount, gotOffsets)
					require.NoError(t, err)
					for i, expected := range symbols {
						actual, err := reader.Lookup(uint32(i))
						require.NoError(t, err)
						require.Equal(t, expected, actual)
						id, err := reader.ReverseLookup(expected)
						require.NoError(t, err)
						require.Equal(t, uint32(i), id)
					}
					_, err = reader.Lookup(uint32(count))
					require.ErrorIs(t, err, ErrSymbolNotFound)
				}
			})
		}
	}
}

func TestSparseValuesFromV4SymbolsTableCorruption(t *testing.T) {
	symbols := make([]string, 33)
	for i := range symbols {
		symbols[i] = fmt.Sprintf("%04d", i)
	}
	original, _ := makeV4SymbolSection(symbols)
	payloadEnd := 8 + int(binary.BigEndian.Uint64(original))
	directory := payloadEnd + 4
	recalculate := func(b []byte) {
		binary.BigEndian.PutUint32(b[len(b)-4:], crc32.Checksum(b[directory+4:len(b)-4], castagnoliTable))
	}
	for _, tc := range []struct {
		name     string
		mutate   func([]byte) []byte
		endDelta int
		checksum bool
		want     error
		message  string
	}{
		{name: "symbol-checksum", mutate: func(b []byte) []byte { b[payloadEnd] ^= 1; return b }, checksum: true, want: streamencoding.ErrInvalidChecksum},
		{name: "unchecked-symbol-checksum", mutate: func(b []byte) []byte { b[payloadEnd] ^= 1; return b }},
		{name: "directory-checksum", mutate: func(b []byte) []byte { b[len(b)-1] ^= 1; return b }, want: streamencoding.ErrInvalidChecksum},
		{name: "interval", mutate: func(b []byte) []byte { binary.BigEndian.PutUint32(b[directory+4:], 64); recalculate(b); return b }, message: "invalid symbol sampling interval"},
		{name: "first-offset", mutate: func(b []byte) []byte { b[directory+8] = 13; recalculate(b); return b }, message: "invalid first symbol offset"},
		{name: "zero-delta", mutate: func(b []byte) []byte { b[directory+8] = 0; recalculate(b); return b }, want: streamencoding.ErrInvalidSize},
		{name: "missing-directory", mutate: func(b []byte) []byte { return b[:directory] }, want: streamencoding.ErrInvalidSize},
		{name: "payload-overflow", mutate: func(b []byte) []byte { binary.BigEndian.PutUint64(b, math.MaxUint64); return b }, want: streamencoding.ErrInvalidSize},
		{name: "directory-overflow", mutate: func(b []byte) []byte { binary.BigEndian.PutUint32(b[directory:], math.MaxUint32); return b }, want: streamencoding.ErrInvalidSize},
		{name: "delta-overflow", mutate: func(b []byte) []byte {
			b = binary.AppendUvarint(b[:directory+9], math.MaxUint64)
			b = append(b, make([]byte, 4)...)
			binary.BigEndian.PutUint32(b[directory:], uint32(len(b)-directory-8))
			recalculate(b)
			return b
		}, want: streamencoding.ErrInvalidSize},
		{name: "count-too-small", mutate: func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:], 32); return b }, message: "invalid symbol directory length"},
		{name: "count-too-large", mutate: func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:], 34); return b }, want: streamencoding.ErrInvalidSize},
		{name: "symbol-tail", mutate: func(b []byte) []byte { b[payloadEnd-5] = 3; return b }, message: "invalid symbol count"},
		{name: "count-overflow", mutate: func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:], math.MaxUint32); return b }, want: streamencoding.ErrInvalidSize},
		{name: "extra-directory-bytes", endDelta: -1, want: streamencoding.ErrInvalidSize},
		{name: "truncated-file", endDelta: 1, want: streamencoding.ErrInvalidSize},
		{name: "symbol-crosses-boundary", mutate: func(b []byte) []byte { b[payloadEnd-5] = 127; return b }, want: streamencoding.ErrInvalidSize},
		{name: "unsampled-duplicate", mutate: func(b []byte) []byte {
			// Duplicate the first symbol at ordinal 1. Both sampled values remain ordered.
			copy(b[18:22], b[13:17])
			binary.BigEndian.PutUint32(b[payloadEnd:], crc32.Checksum(b[8:payloadEnd], castagnoliTable))
			return b
		}, checksum: true, message: "unsorted symbols"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := slices.Clone(original)
			if tc.mutate != nil {
				data = tc.mutate(data)
			}
			_, _, err := SparseValuesFromV4SymbolsTable(t.Context(), v4SymbolFactory(t, data), 0, len(data)+tc.endDelta, tc.checksum)
			if tc.message != "" {
				require.EqualError(t, err, tc.message)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
		})
	}
	for _, offset := range []int{-1, len(original) + 1, math.MaxInt} {
		_, _, err := SparseValuesFromV4SymbolsTable(t.Context(), nil, offset, len(original), false)
		require.ErrorIs(t, err, streamencoding.ErrInvalidSize)
	}
	t.Run("last-group-order", func(t *testing.T) {
		data, _ := makeV4SymbolSection([]string{"a", "c", "b"})
		_, _, err := SparseValuesFromV4SymbolsTable(t.Context(), v4SymbolFactory(t, data), 0, len(data), true)
		require.EqualError(t, err, "unsorted symbols")
	})
}

func TestSparseValuesFromV4SymbolsTableWideOffsets(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("wide symbol offsets require 64-bit integers")
	}
	for _, tc := range []struct {
		name          string
		start, second int64
		count         int
	}{
		{name: "wide-table-offset", start: 1<<32 + 17, second: 14, count: 1},
		{name: "wide-symbol-offset", start: 17, second: 1<<32 + 12, count: 33},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "symbols")
			f, err := os.Create(filename)
			require.NoError(t, err)
			payloadEnd := tc.second + 2
			if tc.count == 1 {
				payloadEnd = 14
			}
			var header encoding.Encbuf
			header.PutBE64(uint64(payloadEnd - 8))
			header.PutBE32int(tc.count)
			header.PutUvarintStr("a")
			_, err = f.WriteAt(header.B, tc.start)
			require.NoError(t, err)
			if tc.count > 1 {
				_, err = f.WriteAt([]byte{1, 'z'}, tc.start+tc.second)
				require.NoError(t, err)
			}
			var directory encoding.Encbuf
			directory.PutBE32(symbolFactor)
			directory.PutUvarint64(12)
			if tc.count > 1 {
				directory.PutUvarint64(uint64(tc.second - 12))
			}
			var framed encoding.Encbuf
			framed.PutBE32int(directory.Len())
			framed.PutBytes(directory.B)
			framed.PutBE32(crc32.Checksum(directory.B, castagnoliTable))
			_, err = f.WriteAt(framed.B, tc.start+payloadEnd+4)
			require.NoError(t, err)
			require.NoError(t, f.Close())
			factory := streamencoding.NewFilePoolDecbufFactory(filename, 0, filepool.NewFilePoolMetrics(nil))
			t.Cleanup(func() { require.NoError(t, factory.Close()) })
			count, offsets, err := SparseValuesFromV4SymbolsTable(t.Context(), factory, int(tc.start), int(tc.start+payloadEnd+4+int64(framed.Len())), false)
			require.NoError(t, err)
			require.Equal(t, tc.count, count)
			expected := []int{12}
			if tc.count > 1 {
				expected = append(expected, int(tc.second))
			}
			require.Equal(t, expected, offsets)
		})
	}
}

// Record reads crossing the interior of the payload, excluding framing, read-ahead
// and the final group. Running directory loading alone excludes label-name lookups.
type symbolPayloadReadCounter struct {
	objstore.BucketReader
	start, end, bytesRead int64
}

func (b *symbolPayloadReadCounter) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.bytesRead += max(0, min(off+length, b.end)-max(off, b.start))
	return b.BucketReader.GetRange(ctx, name, off, length)
}

func TestSparseValuesFromV4SymbolsTable_PayloadReads(t *testing.T) {
	symbols := make([]string, 1024)
	for i := range symbols {
		symbols[i] = fmt.Sprintf("%04096d", i)
	}
	section, offsets := makeV4SymbolSection(symbols)
	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(t.Context(), "symbols", bytes.NewReader(section)))
	ranges := &symbolPayloadReadCounter{
		BucketReader: bkt,
		start:        streamencoding.ReadBufferSize,
		end:          int64(offsets[len(offsets)-1]),
	}
	require.Greater(t, ranges.end-ranges.start, int64(streamencoding.ReadBufferSize))
	factory := streamencoding.NewBucketDecbufFactory(ranges, "symbols")
	_, _, err := SparseValuesFromV4SymbolsTable(t.Context(), factory, 0, len(section), true)
	require.NoError(t, err)
	require.GreaterOrEqual(t, ranges.bytesRead, ranges.end-ranges.start)

	ranges.bytesRead = 0
	count, loaded, err := SparseValuesFromV4SymbolsTable(t.Context(), factory, 0, len(section), false)
	require.NoError(t, err)
	require.Equal(t, len(symbols), count)
	require.Equal(t, offsets, loaded)
	require.Zero(t, ranges.bytesRead)
}
