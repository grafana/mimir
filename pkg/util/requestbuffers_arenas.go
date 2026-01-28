// SPDX-License-Identifier: AGPL-3.0-only

//go:build goexperiment.arenas

package util

import (
	"bytes"

	"github.com/grafana/mimir/pkg/util/arena"
)

// Get obtains a buffer using the provided arena. The pool is ignored in arena builds.
func (rb *RequestBuffers) Get(a *arena.Arena, size int) *bytes.Buffer {
	if size < 0 {
		size = 0
	}
	buf := bytes.NewBuffer(arena.MakeSlice[byte](a, size, size))
	buf.Reset()
	return buf
}

// CleanUp is mostly a no-op in arena builds, except for tainting buffers if configured.
// The buffers list is cleared, but buffers are not returned to any pool.
func (rb *RequestBuffers) CleanUp() {
	if rb == nil {
		return
	}
	for i, b := range rb.buffers {
		// Make sure the backing array doesn't retain a reference
		rb.buffers[i] = nil
		if len(rb.taintOnCleanUp) > 0 {
			buf := b.Bytes()
			for i := range buf {
				buf[i] = rb.taintOnCleanUp[i%len(rb.taintOnCleanUp)]
			}
		}
		// No rb.p.Put(buf) in arena builds - memory is managed by arena
	}
	rb.buffers = rb.buffers[:0]
}
