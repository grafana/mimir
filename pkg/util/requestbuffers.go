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
	taintOnCleanUp []byte
}

// TaintBuffersOnCleanUp configures a RequestBuffers to write some repeated
// "taint" word into buffers before they are released back into the pool.
//
// Useful for making use-after-release bugs noticeable during testing.
func TaintBuffersOnCleanUp(taintWord []byte) func(r *RequestBuffers) {
	return func(r *RequestBuffers) {
		r.taintOnCleanUp = taintWord
	}
}

// NewRequestBuffers returns a new RequestBuffers given a Pool.
func NewRequestBuffers(p Pool, cfg ...func(*RequestBuffers)) *RequestBuffers {
	rb := &RequestBuffers{
		p: p,
	}
	for _, cfg := range cfg {
		cfg(rb)
	}
	rb.buffers = rb.buffersBacking[:0]
	return rb
}
