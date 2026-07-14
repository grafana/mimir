// SPDX-License-Identifier: AGPL-3.0-only

package frontendv2pb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc/mem"
)

// grpcBuffersLen reports the number of live entries in the grpcBuffers side-table.
// Test-only helper used to assert the buffer registry drains back to zero.
func grpcBuffersLen() int {
	n := 0
	grpcBuffers.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

type countingBufferPool struct {
	frees atomic.Int64
}

func (p *countingBufferPool) Get(length int) *[]byte {
	b := make([]byte, length)
	return &b
}

func (p *countingBufferPool) Put(*[]byte) {
	p.frees.Inc()
}

// TestQueryResultStreamRequest_BufferReleaseSemantics locks in the sync.Map delete
// semantics the leak fix relies on: SetBuffer registers exactly one entry, FreeBuffer
// removes it and releases the buffer once, and a redundant FreeBuffer is a safe no-op.
// That idempotency is what keeps the stream's Close drain and the runtime.AddCleanup
// backstop from double-freeing a buffer.
func TestQueryResultStreamRequest_BufferReleaseSemantics(t *testing.T) {
	require.Equal(t, 0, grpcBuffersLen(), "registry must start empty")

	pool := &countingBufferPool{}
	data := make([]byte, 64*1024)
	require.False(t, mem.IsBelowBufferPoolingThreshold(cap(data)), "test buffer must be poolable so its release is observable")
	buf := mem.NewBuffer(&data, pool)

	msg := &QueryResultStreamRequest{}

	msg.SetBuffer(nil)
	require.Equal(t, 0, grpcBuffersLen(), "SetBuffer(nil) must not register an entry")

	msg.SetBuffer(buf)
	require.Equal(t, 1, grpcBuffersLen())
	require.Equal(t, buf, msg.Buffer())

	msg.FreeBuffer()
	require.Equal(t, 0, grpcBuffersLen(), "FreeBuffer must remove the entry")
	require.Nil(t, msg.Buffer())
	require.Equal(t, int64(1), pool.frees.Load(), "buffer released exactly once")

	require.NotPanics(t, msg.FreeBuffer)
	require.Equal(t, 0, grpcBuffersLen())
	require.Equal(t, int64(1), pool.frees.Load(), "a redundant FreeBuffer must not release the buffer again")
}
