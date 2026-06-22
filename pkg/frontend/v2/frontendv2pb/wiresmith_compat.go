// SPDX-License-Identifier: AGPL-3.0-only

package frontendv2pb

import (
	"sync"

	"google.golang.org/grpc/mem"
)

// grpcBuffers is a side-channel map from *QueryResultStreamRequest to the gRPC
// receive-frame buffer that backs yoloString fields inside LabelAdapter values.
// LabelAdapter.UnmarshalWiresmith aliases Name/Value directly into the gRPC frame
// buffer (see pkg/mimirpb/timeseries.go); keeping the buffer alive here prevents
// the frame from being returned to the pool while those strings are live.
// The gRPC codec calls SetBuffer on messages that implement MessageWithBufferRef.
//
// Known limitation — bounded permanent leak: if a consumer calls
// ProtobufResponseStream.Close() while a QueryResultStreamRequest is already
// buffered in the 1-element messages channel, Go's select is free to take the
// notifyClosed branch rather than the messages branch, leaving that message
// permanently in the channel. Nobody reads it, FreeBuffer is never called, and
// the sync.Map entry (the *QueryResultStreamRequest key plus its mem.Buffer) is
// never removed and cannot be GC'd (the map holds a strong reference to both).
// The leak is bounded: at most one entry per early-closed stream, so the total
// retained memory is proportional to concurrent abandoned streams at any moment.
// The proper fix — wiresmith `unique`-interned buffer-independent strings that
// eliminate the yoloString frame-aliasing entirely — is tracked as wiresmith bead
// wiresmith-egvq (P1, prerequisite for the mimir migration upstream merge).
var grpcBuffers sync.Map // map[*QueryResultStreamRequest]mem.Buffer

// SetBuffer satisfies mimirpb.MessageWithBufferRef; called by the gRPC codec after
// unmarshalling so the receive-frame buffer reference count is incremented.
func (m *QueryResultStreamRequest) SetBuffer(buf mem.Buffer) {
	if buf != nil {
		grpcBuffers.Store(m, buf)
	}
}

// FreeBuffer satisfies mimirpb.MessageWithBufferRef; releases the receive-frame buffer
// once the caller is done with any unsafe string references inside nested gogo messages.
func (m *QueryResultStreamRequest) FreeBuffer() {
	if v, ok := grpcBuffers.LoadAndDelete(m); ok {
		v.(mem.Buffer).Free()
	}
}

// noopBufferHolder is returned by Buffer() for test/compat code that calls buf.Free().
// The actual buffer is managed via grpcBuffers; this is a separate read-only accessor.
type noopBufferHolder struct{}

func (noopBufferHolder) Free() {}

// Buffer returns the stored gRPC receive-frame buffer, or a no-op holder if none.
// Callers that use buf.Free() to release the frame should call FreeBuffer() directly;
// this method exists for compatibility with code that stores the result for deferred free.
func (m *QueryResultStreamRequest) Buffer() mem.Buffer {
	if v, ok := grpcBuffers.Load(m); ok {
		return v.(mem.Buffer)
	}
	return nil
}
