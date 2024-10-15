// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"bytes"
	"sync"

	"github.com/grafana/mimir/pkg/util/pool"
)

// Pool is an abstraction for a pool of byte slices.
type Pool interface {
	// Get returns a new byte slices that fits the given size.
	Get(sz int) []byte

	// Put puts a slice back into the pool.
	Put(s []byte)
}

type bufferPool struct {
	p sync.Pool
}

func (p *bufferPool) Get(_ int) []byte { return p.p.Get().([]byte) }
func (p *bufferPool) Put(s []byte)     { p.p.Put(s) } //nolint:staticcheck

// NewBufferPool returns a new Pool for byte slices.
func NewBufferPool() Pool {
	return &bufferPool{
		p: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 256*1024)
			},
		},
	}
}

// NewBucketedBufferPool returns a new Pool for byte slices with bucketing.
// The pool will have buckets for sizes from minSize to maxSize increasing by the given factor.
func NewBucketedBufferPool(minSize, maxSize int, factor float64) Pool {
	return pool.NewBucketedPool(minSize, maxSize, factor, func(sz int) []byte {
		return make([]byte, 0, sz)
	})
}

// RequestBuffers provides pooled request buffers.
type RequestBuffers struct {
	p       Pool
	buffers []*bytes.Buffer
	// Allows avoiding heap allocation
	buffersBacking [10]*bytes.Buffer
}

// NewRequestBuffers returns a new RequestBuffers given a Pool.
func NewRequestBuffers(p Pool) *RequestBuffers {
	rb := &RequestBuffers{
		p: p,
	}
	rb.buffers = rb.buffersBacking[:0]
	return rb
}

// Get obtains a buffer from the pool. It will be returned back to the pool when CleanUp is called.
func (rb *RequestBuffers) Get(size int) *bytes.Buffer {
	if rb == nil || rb.p == nil {
		if size < 0 {
			size = 0
		}
		return bytes.NewBuffer(make([]byte, 0, size))
	}

	b := rb.p.Get(size)
	buf := bytes.NewBuffer(b)
	buf.Reset()
	if size > 0 {
		buf.Grow(size)
	}

	rb.buffers = append(rb.buffers, buf)
	return buf
}

// CleanUp releases buffers back to the pool.
func (rb *RequestBuffers) CleanUp() {
	for i, b := range rb.buffers {
		// Make sure the backing array doesn't retain a reference
		rb.buffers[i] = nil
		rb.p.Put(b.Bytes())
	}
	rb.buffers = rb.buffers[:0]
}
