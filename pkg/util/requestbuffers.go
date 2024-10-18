// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"bytes"
	"sync"
)

const defaultPoolBufferCap = 256 * 1024

// Pool is an abstraction for a pool of byte slices.
type Pool interface {
	// Get returns a new byte slices.
	Get() []byte

	// Put puts a slice back into the pool.
	Put(s []byte)
}

type bufferPool struct {
	maxBufferCap int
	p            sync.Pool
}

func (p *bufferPool) Get() []byte { return p.p.Get().([]byte) }
func (p *bufferPool) Put(s []byte) {
	if p.maxBufferCap > 0 && cap(s) > p.maxBufferCap {
		return // Discard large buffers
	}
	p.p.Put(s) //nolint:staticcheck
}

// NewBufferPool returns a new Pool for byte slices.
// If maxBufferCapacity is 0, the pool will not have a maximum capacity.
func NewBufferPool(maxBufferCapacity int) Pool {
	return &bufferPool{
		maxBufferCap: maxBufferCapacity,
		p: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, defaultPoolBufferCap)
			},
		},
	}
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

	b := rb.p.Get()
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
