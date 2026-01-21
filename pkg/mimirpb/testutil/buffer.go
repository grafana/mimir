// SPDX-License-Identifier: AGPL-3.0-only

package testutil

import (
	"fmt"

	"go.uber.org/atomic"
	"google.golang.org/grpc/mem"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// TrackBufferRefCount instruments the WriteRequest's underlying buffer's
// reference count. It can be inspected with [BufferRefCount].
func TrackBufferRefCount(wr *mimirpb.WriteRequest) {
	buf := wr.Buffer()
	if buf == nil {
		// Just set some fake buffer. We care about the reference count, not the
		// data itself.
		buf = mem.SliceBuffer([]byte("fake data"))
	}
	ibuf := &memBufferWithInstrumentedRefCount{Buffer: buf}
	ibuf.refCount.Add(1) // Match the refCount of buf.Buffer
	wr.SetBuffer(ibuf)
}

// BufferRefCount returns the current reference of a WriteRequest's buffer.
// [TrackBufferRefCount] must have been called on the WriteRequest beforehand.
// Deprecated: Use [TrackedBuffer] and [TrackedBuffer.RefCount] instead, as this
// function will panic if the buffer has been freed (which sets it to nil).
func BufferRefCount(wr *mimirpb.WriteRequest) int {
	switch b := wr.Buffer().(type) {
	case *memBufferWithInstrumentedRefCount:
		return int(b.refCount.Load())
	default:
		panic(fmt.Errorf("expected WriteRequest.Buffer to be a *memBufferWithInstrumentedRefCount from asDeserializedWriteRequest, got %T", wr.Buffer()))
	}
}

// TrackedBuffer represents an instrumented buffer whose reference count can be
// inspected even after the buffer has been freed from its WriteRequest.
type TrackedBuffer struct {
	buf *memBufferWithInstrumentedRefCount
}

// TrackBuffer instruments the WriteRequest's underlying buffer's reference
// count and returns a TrackedBuffer that can be used to inspect the reference
// count even after the buffer has been freed. Use [TrackedBuffer.RefCount] to
// get the current reference count.
func TrackBuffer(wr *mimirpb.WriteRequest) TrackedBuffer {
	buf := wr.Buffer()
	if buf == nil {
		// Just set some fake buffer. We care about the reference count, not the
		// data itself.
		buf = mem.SliceBuffer([]byte("fake data"))
	}
	ibuf := &memBufferWithInstrumentedRefCount{Buffer: buf}
	ibuf.refCount.Add(1) // Match the refCount of buf.Buffer
	wr.SetBuffer(ibuf)
	return TrackedBuffer{buf: ibuf}
}

// RefCount returns the current reference count of the tracked buffer.
func (tb TrackedBuffer) RefCount() int {
	return int(tb.buf.refCount.Load())
}

type memBufferWithInstrumentedRefCount struct {
	mem.Buffer
	refCount atomic.Int64
}

func (b *memBufferWithInstrumentedRefCount) Ref() {
	b.Buffer.Ref()

	b.refCount.Add(1)
}

func (b *memBufferWithInstrumentedRefCount) Free() {
	b.Buffer.Free()

	refCount := b.refCount.Sub(1)
	if refCount < 0 {
		panic("memBufferWithInstrumentedRefCount reference count below zero")
	}
}
