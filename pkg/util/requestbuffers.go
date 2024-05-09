// SPDX-License-Identifier: AGPL-3.0-only
package util

import (
	"bytes"
)

// The maximum buffer size allowed in the pool.
// A sane default value of 1MB is chosen, which should be sufficient for most use cases, though it could be
// revisited if necessary.
const maxInPoolRequestBufferSize = 1024 * 1024

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
// If size exceeds the maximum buffer size allowed in the pool, a new buffer from the heap is returned.
func (rb *RequestBuffers) Get(size int) *bytes.Buffer {
	if rb == nil || size > maxInPoolRequestBufferSize {
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
		if b.Cap() > maxInPoolRequestBufferSize {
			continue // Avoid pooling large buffers
		}
		rb.p.Put(b)
	}
	rb.buffers = rb.buffers[:0]
}
