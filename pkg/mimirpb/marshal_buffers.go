// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"math/bits"
	"sync"
)

// marshalBuffers are the buffers the codec marshals messages into, which gRPC gives back once it has written them.
// gRPC's default pool zeroes a buffer's whole capacity on each Get, which marshaling then writes all of again: for a
// query response of a few kilobytes in a buffer of the next tier up, zeroing was a third of what marshaling cost.
var marshalBuffers dirtyBufferPool

const (
	// Messages below gRPC's pooling threshold aren't marshaled into pooled buffers.
	minDirtyBufferShift = 10
	// Larger buffers aren't kept: a few such messages would hold that much memory for good.
	maxDirtyBufferShift = 26
)

// dirtyBufferPool keeps buffers by power-of-two capacity and gives them back as they are: whoever gets one writes all of
// the length it asked for before reading any of it.
type dirtyBufferPool struct {
	pools [maxDirtyBufferShift - minDirtyBufferShift + 1]sync.Pool
}

func (p *dirtyBufferPool) Get(size int) *[]byte {
	shift := max(bits.Len(uint(max(size, 1)-1)), minDirtyBufferShift)
	if shift > maxDirtyBufferShift {
		buf := make([]byte, size)
		return &buf
	}
	if buf, ok := p.pools[shift-minDirtyBufferShift].Get().(*[]byte); ok {
		*buf = (*buf)[:size]
		return buf
	}
	buf := make([]byte, size, 1<<shift)
	return &buf
}

func (p *dirtyBufferPool) Put(buf *[]byte) {
	capacity := cap(*buf)
	if capacity == 0 || capacity&(capacity-1) != 0 {
		return
	}
	shift := bits.Len(uint(capacity)) - 1
	if shift < minDirtyBufferShift || shift > maxDirtyBufferShift {
		return
	}
	p.pools[shift-minDirtyBufferShift].Put(buf)
}
