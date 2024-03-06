// SPDX-License-Identifier: AGPL-3.0-only
package util

import (
	"bytes"
)

// Pool is an abstraction of sync.Pool, for testability.
type Pool interface {
	// Get a pooled object.
	Get() any
	// Put back an object into the pool.
	Put(any)
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
	if rb == nil {
		if size < 0 {
			size = 0
		}
		return bytes.NewBuffer(make([]byte, 0, size))
	}

	b := rb.p.Get().(*bytes.Buffer)
	b.Reset()
	if size > 0 {
		b.Grow(size)
	}
	rb.buffers = append(rb.buffers, b)
	return b
}

// CleanUp releases buffers back to the pool.
func (rb *RequestBuffers) CleanUp() {
	for i, b := range rb.buffers {
		// Make sure the backing array doesn't retain a reference
		rb.buffers[i] = nil
		rb.p.Put(b)
	}
	rb.buffers = rb.buffers[:0]
}
