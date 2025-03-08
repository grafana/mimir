// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-license: Apache-2.0

// Package zstd is a wrapper for using github.com/klauspost/compress/zstd
// with gRPC.
package zstd

import (
	"errors"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/encoding"
)

const (
	// Name is the name of the S2 compressor.
	Name = "zstd"
)

var encoderOptions = []zstd.EOption{
	// The default zstd window size is 8MB, which is much larger than the
	// typical RPC message and wastes a bunch of memory.
	zstd.WithWindowSize(512 * 1024),
	// The default zstd compression level is 2
	zstd.WithEncoderLevel(zstd.SpeedDefault),
}
var decoderOptions = []zstd.DOption{
	// If the decoder concurrency level is not 1, we would need to call
	// Close() to avoid leaking resources when the object is released
	// from compressor.decoderPool.
	zstd.WithDecoderConcurrency(1),
}

type compressor struct {
	name             string
	poolCompressor   sync.Pool
	poolDecompressor sync.Pool
}

type writer struct {
	*zstd.Encoder
	pool *sync.Pool
}

type reader struct {
	*zstd.Decoder
	pool *sync.Pool
}

func init() {
	encoding.RegisterCompressor(newCompressor())
}

func newCompressor() *compressor {
	c := &compressor{
		name: Name,
	}
	c.poolCompressor.New = func() interface{} {
		w, err := zstd.NewWriter(io.Discard, encoderOptions...)
		if err != nil {
			return nil
		}
		return &writer{Encoder: w, pool: &c.poolCompressor}
	}
	return c
}

func (c *compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	z := c.poolCompressor.Get().(*writer)
	z.Encoder.Reset(w)
	return z, nil
}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	z, inPool := c.poolDecompressor.Get().(*reader)
	if !inPool {
		newR, err := zstd.NewReader(r, decoderOptions...)
		if err != nil {
			return nil, err
		}
		return &reader{Decoder: newR, pool: &c.poolDecompressor}, nil
	}
	err := z.Reset(r)
	return z, err
}

func (c *compressor) Name() string {
	return c.name
}

func (zw *writer) Close() error {
	err := zw.Encoder.Close()
	zw.pool.Put(zw)
	return err
}

func (zr *reader) Read(p []byte) (n int, err error) {
	n, err = zr.Decoder.Read(p)
	if errors.Is(err, io.EOF) {
		zr.pool.Put(zr)
	}
	return n, err
}
