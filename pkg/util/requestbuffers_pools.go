// SPDX-License-Identifier: AGPL-3.0-only

//go:build !goexperiment.arenas

package util

import (
	"bytes"

	"github.com/grafana/mimir/pkg/util/arena"
)

// Get obtains a buffer from the pool. It will be returned back to the pool when CleanUp is called.
// The arena parameter is ignored in non-arena builds.
func (rb *RequestBuffers) Get(a *arena.Arena, size int) *bytes.Buffer {
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
	if rb == nil {
		return
	}
	for i, b := range rb.buffers {
		// Make sure the backing array doesn't retain a reference
		rb.buffers[i] = nil
		buf := b.Bytes()
		if len(rb.taintOnCleanUp) > 0 {
			for i := range buf {
				buf[i] = rb.taintOnCleanUp[i%len(rb.taintOnCleanUp)]
			}
		}
		if rb.p != nil {
			rb.p.Put(buf)
		}
	}
	rb.buffers = rb.buffers[:0]
}
