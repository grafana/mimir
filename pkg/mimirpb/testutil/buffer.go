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
func BufferRefCount(wr *mimirpb.WriteRequest) int {
	switch b := wr.Buffer().(type) {
	case *memBufferWithInstrumentedRefCount:
		return int(b.refCount.Load())
	default:
		panic(fmt.Errorf("expected WriteRequest.Buffer to be a *memBufferWithInstrumentedRefCount from asDeserializedWriteRequest, got %T", wr.Buffer()))
	}
}

type memBufferWithInstrumentedRefCount struct {
	mem.Buffer
	refCount atomic.Int64
}

func (b *memBufferWithInstrumentedRefCount) Ref() {
	b.refCount.Add(1)
	b.Buffer.Ref()
}

func (b *memBufferWithInstrumentedRefCount) Free() {
	b.refCount.Sub(1)
	b.Buffer.Free()
}
