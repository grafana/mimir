// SPDX-License-Identifier: AGPL-3.0-only
package zstd

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
	"io"
	"strings"
	"testing"
)

func TestCompressor(t *testing.T) {
	c := newCompressor()
	require.Equal(t, "zstd", c.Name())

	testCases := []struct {
		name  string
		input string
	}{
		{
			name:  "empty",
			input: "",
		},
		{
			name:  "short",
			input: "hello world",
		},
		{
			name:  "long",
			input: strings.Repeat("123456789", 1024),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Compress
			buf := bytes.NewBuffer(make([]byte, 0, 1024))
			wc, err := c.Compress(buf)
			require.NoError(t, err)

			_, err = wc.Write([]byte(tc.input))
			require.NoError(t, err)
			assert.NoError(t, wc.Close())

			// Decompress
			r, err := c.Decompress(buf)
			require.NoError(t, err)
			expected, err := io.ReadAll(r)
			require.NoError(t, err)

			assert.Equal(t, tc.input, string(expected))
		})
	}
}

func BenchmarkZSTDCompress(b *testing.B) {
	data := []byte(strings.Repeat("123456789", 1024))
	c := newCompressor()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w, err := c.Compress(io.Discard)
		require.NoError(b, err)
		_, err = w.Write(data)
		require.NoError(b, err)
		require.NoError(b, w.Close())
	}
}

func BenchmarkZSTDDecompress(b *testing.B) {
	data := []byte(strings.Repeat("123456789", 1024))
	c := newCompressor()
	var buf bytes.Buffer
	w, err := c.Compress(&buf)
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, w.Close())
	})
	_, err = w.Write(data)
	require.NoError(b, err)
	reader := bytes.NewReader(buf.Bytes())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := c.Decompress(reader)
		require.NoError(b, err)
		_, err = io.ReadAll(r)
		require.NoError(b, err)
		_, err = reader.Seek(0, io.SeekStart)
		require.NoError(b, err)
	}
}

func BenchmarkZSTDGRPCCompressionPerf(b *testing.B) {
	data := []byte(strings.Repeat("123456789", 1024))
	grpcc := encoding.GetCompressor(Name)

	// Reset the timer to exclude setup time from the measurements
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			var buf bytes.Buffer
			writer, err := grpcc.Compress(&buf)
			require.NoError(b, err)
			_, err = writer.Write(data)
			require.NoError(b, err)
			err = writer.Close()
			require.NoError(b, err)

			compressedData := buf.Bytes()
			reader, err := grpcc.Decompress(bytes.NewReader(compressedData))
			require.NoError(b, err)
			var result bytes.Buffer
			_, err = result.ReadFrom(reader)
			require.NoError(b, err)
		}
	}
}
