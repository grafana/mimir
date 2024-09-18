// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/mostynb/go-grpc-compression/blob/f7e92b39057ca421a6485f650243a3e804036498/internal/s2/s2.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright 2022 Mostyn Bramley-Moore.

// Package s2 is an experimental wrapper for using
// github.com/klauspost/compress/s2 stream compression with gRPC.
package s2

import (
	"errors"
	"io"
	"sync"

	"github.com/klauspost/compress/s2"
	"google.golang.org/grpc/encoding"
)

const (
	// Name is the name of the S2 compressor.
	Name = "s2"
	// SnappyCompatName is the name of the Snappy compatible S2 compressor.
	SnappyCompatName = "s2-snappy"
)

type compressor struct {
	name             string
	poolCompressor   sync.Pool
	poolDecompressor sync.Pool
}

type writer struct {
	*s2.Writer
	pool *sync.Pool
}

type reader struct {
	*s2.Reader
	pool *sync.Pool
}

func init() {
	encoding.RegisterCompressor(newCompressor(false))
	encoding.RegisterCompressor(newCompressor(true))
}

func newCompressor(snappyCompat bool) *compressor {
	opts := []s2.WriterOption{s2.WriterConcurrency(1)}
	var name string
	if snappyCompat {
		opts = append(opts, s2.WriterSnappyCompat())
		name = SnappyCompatName
	} else {
		name = Name
	}
	c := &compressor{
		name: name,
	}
	c.poolCompressor.New = func() interface{} {
		w := s2.NewWriter(io.Discard, opts...)
		return &writer{Writer: w, pool: &c.poolCompressor}
	}
	return c
}

func (c *compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	s := c.poolCompressor.Get().(*writer)
	s.Writer.Reset(w)
	return s, nil
}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	s, inPool := c.poolDecompressor.Get().(*reader)
	if !inPool {
		newR := s2.NewReader(r)
		return &reader{Reader: newR, pool: &c.poolDecompressor}, nil
	}
	s.Reset(r)
	return s, nil
}

func (c *compressor) Name() string {
	return c.name
}

func (s *writer) Close() error {
	err := s.Writer.Close()
	s.pool.Put(s)
	return err
}

func (s *reader) Read(p []byte) (n int, err error) {
	n, err = s.Reader.Read(p)
	if errors.Is(err, io.EOF) {
		s.pool.Put(s)
	}
	return n, err
}
