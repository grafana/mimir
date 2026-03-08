// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"compress/gzip"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/atomic"
)

// Global pools for decompressor reuse. These reduce allocations by reusing
// gzip.Reader and zstd.Decoder objects across requests.
var (
	gzipReaderPool = &sync.Pool{}
	zstdReaderPool = &sync.Pool{}
)

// pooledGzipReader wraps gzip.Reader with pool return on Close.
type pooledGzipReader struct {
	*gzip.Reader
	pool     *sync.Pool
	returned atomic.Bool
}

// Read implements io.Reader.
func (r *pooledGzipReader) Read(p []byte) (n int, err error) {
	return r.Reader.Read(p)
}

// Close implements io.Closer and returns reader to pool.
// Safe to call multiple times - only the first call returns to pool.
func (r *pooledGzipReader) Close() error {
	// Use atomic swap to ensure we only return to pool once.
	if r.returned.Swap(true) {
		// Already returned to pool.
		return nil
	}

	// Close the underlying reader to release resources.
	err := r.Reader.Close()
	r.pool.Put(r)
	return err
}

// NewPooledGzipReader creates or retrieves a pooled gzip reader.
// The returned reader MUST be closed via Close() to return it to the pool.
// It is safe to call Close() multiple times.
func NewPooledGzipReader(r io.Reader) (io.ReadCloser, error) {
	pr, inPool := gzipReaderPool.Get().(*pooledGzipReader)
	if !inPool {
		// First time or pool empty - create new reader.
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		return &pooledGzipReader{
			Reader: gr,
			pool:   gzipReaderPool,
		}, nil
	}

	// Reset the atomic flag for reuse.
	pr.returned.Store(false)

	// Reuse pooled reader.
	if err := pr.Reset(r); err != nil {
		// Don't return broken reader to pool.
		return nil, err
	}
	return pr, nil
}

// pooledZstdReader wraps zstd.Decoder with pool return on Close.
type pooledZstdReader struct {
	*zstd.Decoder
	pool     *sync.Pool
	returned atomic.Bool
}

// Read implements io.Reader.
func (r *pooledZstdReader) Read(p []byte) (n int, err error) {
	return r.Decoder.Read(p)
}

// Close implements io.Closer and returns reader to pool.
// Note: We don't call Decoder.Close() because that prevents reuse.
// Instead, we reset with nil to release references.
// Safe to call multiple times - only the first call returns to pool.
func (r *pooledZstdReader) Close() error {
	// Use atomic swap to ensure we only return to pool once.
	if r.returned.Swap(true) {
		// Already returned to pool.
		return nil
	}

	// Reset with nil to release references to the underlying reader.
	err := r.Reset(nil)
	r.pool.Put(r)
	return err
}

// NewPooledZstdReader creates or retrieves a pooled zstd decoder.
// The returned reader MUST be closed via Close() to return it to the pool.
// It is safe to call Close() multiple times.
func NewPooledZstdReader(r io.Reader) (io.ReadCloser, error) {
	pr, inPool := zstdReaderPool.Get().(*pooledZstdReader)
	if !inPool {
		// First time or pool empty - create new decoder.
		zr, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}
		return &pooledZstdReader{
			Decoder: zr,
			pool:    zstdReaderPool,
		}, nil
	}

	// Reset the atomic flag for reuse.
	pr.returned.Store(false)

	// Reuse pooled decoder.
	if err := pr.Reset(r); err != nil {
		// Don't return broken reader to pool.
		return nil, err
	}
	return pr, nil
}
