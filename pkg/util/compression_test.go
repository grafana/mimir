// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func gzipCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write(data)
	require.NoError(t, err)
	require.NoError(t, gw.Close())
	return buf.Bytes()
}

func zstdCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	_, err = zw.Write(data)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func TestPooledGzipReader_Decompress(t *testing.T) {
	testData := []byte("Hello, World! This is test data for gzip compression.")
	compressed := gzipCompress(t, testData)

	reader, err := NewPooledGzipReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, testData, decompressed)
}

func TestPooledGzipReader_CloseReturnsToPool(t *testing.T) {
	testData := []byte("Test data for pool return verification.")
	compressed := gzipCompress(t, testData)

	// First read and close - should return to pool.
	reader1, err := NewPooledGzipReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	_, err = io.ReadAll(reader1)
	require.NoError(t, err)
	require.NoError(t, reader1.Close())

	// Second read - should reuse from pool.
	reader2, err := NewPooledGzipReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	defer reader2.Close()

	decompressed, err := io.ReadAll(reader2)
	require.NoError(t, err)
	require.Equal(t, testData, decompressed)
}

func TestPooledGzipReader_MultipleCloseSafe(t *testing.T) {
	testData := []byte("Test data for multiple close.")
	compressed := gzipCompress(t, testData)

	reader, err := NewPooledGzipReader(bytes.NewReader(compressed))
	require.NoError(t, err)

	_, err = io.ReadAll(reader)
	require.NoError(t, err)

	// Multiple close calls should be safe.
	require.NoError(t, reader.Close())
	require.NoError(t, reader.Close())
	require.NoError(t, reader.Close())
}

func TestPooledGzipReader_InvalidData(t *testing.T) {
	invalidData := []byte("not gzipped data")

	_, err := NewPooledGzipReader(bytes.NewReader(invalidData))
	require.Error(t, err)
}

func TestPooledZstdReader_Decompress(t *testing.T) {
	testData := []byte("Hello, World! This is test data for zstd compression.")
	compressed := zstdCompress(t, testData)

	reader, err := NewPooledZstdReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, testData, decompressed)
}

func TestPooledZstdReader_CloseReturnsToPool(t *testing.T) {
	testData := []byte("Test data for pool return verification.")
	compressed := zstdCompress(t, testData)

	// First read and close - should return to pool.
	reader1, err := NewPooledZstdReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	_, err = io.ReadAll(reader1)
	require.NoError(t, err)
	require.NoError(t, reader1.Close())

	// Second read - should reuse from pool.
	reader2, err := NewPooledZstdReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	defer reader2.Close()

	decompressed, err := io.ReadAll(reader2)
	require.NoError(t, err)
	require.Equal(t, testData, decompressed)
}

func TestPooledZstdReader_MultipleCloseSafe(t *testing.T) {
	testData := []byte("Test data for multiple close.")
	compressed := zstdCompress(t, testData)

	reader, err := NewPooledZstdReader(bytes.NewReader(compressed))
	require.NoError(t, err)

	_, err = io.ReadAll(reader)
	require.NoError(t, err)

	// Multiple close calls should be safe.
	require.NoError(t, reader.Close())
	require.NoError(t, reader.Close())
	require.NoError(t, reader.Close())
}

func TestPooledZstdReader_InvalidData(t *testing.T) {
	invalidData := []byte("not zstd data")

	// zstd.NewReader doesn't fail on invalid data - error occurs on read.
	reader, err := NewPooledZstdReader(bytes.NewReader(invalidData))
	require.NoError(t, err)
	defer reader.Close()

	_, err = io.ReadAll(reader)
	require.Error(t, err)
}

func TestPooledGzipReader_Concurrent(t *testing.T) {
	testData := []byte("Concurrent access test data for gzip compression.")
	compressed := gzipCompress(t, testData)

	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			reader, err := NewPooledGzipReader(bytes.NewReader(compressed))
			require.NoError(t, err)
			defer reader.Close()

			decompressed, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, testData, decompressed)
		}()
	}

	wg.Wait()
}

func TestPooledZstdReader_Concurrent(t *testing.T) {
	testData := []byte("Concurrent access test data for zstd compression.")
	compressed := zstdCompress(t, testData)

	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			reader, err := NewPooledZstdReader(bytes.NewReader(compressed))
			require.NoError(t, err)
			defer reader.Close()

			decompressed, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, testData, decompressed)
		}()
	}

	wg.Wait()
}

func BenchmarkGzipReader_Pooled(b *testing.B) {
	testData := make([]byte, 10000)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, _ = gw.Write(testData)
	_ = gw.Close()
	compressed := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewPooledGzipReader(bytes.NewReader(compressed))
		_, _ = io.ReadAll(reader)
		_ = reader.Close()
	}
}

func BenchmarkGzipReader_Unpooled(b *testing.B) {
	testData := make([]byte, 10000)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, _ = gw.Write(testData)
	_ = gw.Close()
	compressed := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := gzip.NewReader(bytes.NewReader(compressed))
		_, _ = io.ReadAll(reader)
		_ = reader.Close()
	}
}

func BenchmarkZstdReader_Pooled(b *testing.B) {
	testData := make([]byte, 10000)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	var buf bytes.Buffer
	zw, _ := zstd.NewWriter(&buf)
	_, _ = zw.Write(testData)
	_ = zw.Close()
	compressed := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewPooledZstdReader(bytes.NewReader(compressed))
		_, _ = io.ReadAll(reader)
		_ = reader.Close()
	}
}

func BenchmarkZstdReader_Unpooled(b *testing.B) {
	testData := make([]byte, 10000)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	var buf bytes.Buffer
	zw, _ := zstd.NewWriter(&buf)
	_, _ = zw.Write(testData)
	_ = zw.Close()
	compressed := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := zstd.NewReader(bytes.NewReader(compressed))
		_, _ = io.ReadAll(reader)
	}
}
