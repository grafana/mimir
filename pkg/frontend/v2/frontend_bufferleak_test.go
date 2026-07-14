// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc/mem"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// countingBufferPool is a mem.BufferPool that counts how many buffers are returned to it,
// i.e. freed. mem.Buffer has unexported methods so tests cannot supply a fake; a pooled
// buffer is the smallest real mem.Buffer whose release we can observe.
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

// newPooledBuffer returns a real, poolable mem.Buffer whose Free is observable via pool.
func newPooledBuffer(t *testing.T, pool mem.BufferPool) mem.Buffer {
	t.Helper()
	data := make([]byte, 64*1024)
	require.False(t, mem.IsBelowBufferPoolingThreshold(cap(data)), "test buffer must be poolable so its release is observable")
	return mem.NewBuffer(&data, pool)
}

func newTestResponseStream(t *testing.T) *ProtobufResponseStream {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	sl, _ := spanlogger.New(ctx, log.NewNopLogger(), tracer, "test")
	s := newProtobufResponseStream(ctx, ctx, cancel, sl)
	s.closeStream = func() {}
	return s
}

// TestProtobufResponseStream_CloseReleasesBufferedMessage is a regression test for the
// bounded buffer leak (wiresmith-egvq): a message left buffered in the 1-element channel
// when Close is called must have its retained gRPC frame buffer released, not leaked.
func TestProtobufResponseStream_CloseReleasesBufferedMessage(t *testing.T) {
	stream := newTestResponseStream(t)

	pool := &countingBufferPool{}
	msg := newStringMessage("buffered but never consumed")
	msg.SetBuffer(newPooledBuffer(t, pool))
	require.NoError(t, stream.write(msg, nil))

	// Precondition: the buffer is registered while the message sits unread in the channel.
	require.NotNil(t, msg.Buffer())
	require.Zero(t, pool.frees.Load())

	stream.Close() // No consumer ever calls Next().

	require.Nil(t, msg.Buffer(), "Close must drain the buffered message and remove its grpcBuffers entry")
	require.Equal(t, int64(1), pool.frees.Load(), "the retained gRPC frame buffer must be released exactly once")
}

// TestProtobufResponseStream_ConsumeThenCloseNoDoubleFree covers the normal path: once a
// message has been consumed and freed by the caller, Close (and a second Close) must not
// free its buffer again or panic.
func TestProtobufResponseStream_ConsumeThenCloseNoDoubleFree(t *testing.T) {
	stream := newTestResponseStream(t)

	pool := &countingBufferPool{}
	msg := newStringMessage("consumed normally")
	msg.SetBuffer(newPooledBuffer(t, pool))
	require.NoError(t, stream.write(msg, nil))

	got, err := stream.Next(t.Context())
	require.NoError(t, err)
	require.Same(t, msg, got)

	got.FreeBuffer() // Caller releases the buffer, per the Next contract.
	require.Nil(t, msg.Buffer())
	require.Equal(t, int64(1), pool.frees.Load())

	require.NotPanics(t, stream.Close)
	require.NotPanics(t, stream.Close)
	require.Equal(t, int64(1), pool.frees.Load(), "Close must not free an already-released buffer again")
	require.Nil(t, msg.Buffer())
}

// TestProtobufResponseStream_AbandonedStreamReleasesBufferOnGC exercises the
// runtime.AddCleanup backstop: if a stream is dropped with a message still buffered and
// Close is never called, the retained buffer is released once the stream is collected.
func TestProtobufResponseStream_AbandonedStreamReleasesBufferOnGC(t *testing.T) {
	pool := &countingBufferPool{}
	msg := newStringMessage("abandoned without Close")
	msg.SetBuffer(newPooledBuffer(t, pool))

	// Buffer the message on a stream, then drop every reference to the stream without
	// draining or closing it. Done inside a closure so the stream is unreachable after it returns.
	func() {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		sl, _ := spanlogger.New(ctx, log.NewNopLogger(), tracer, "test")
		s := newProtobufResponseStream(ctx, ctx, cancel, sl)
		require.NoError(t, s.write(msg, nil))
	}()

	require.Eventually(t, func() bool {
		runtime.GC()
		return pool.frees.Load() == 1
	}, 10*time.Second, 5*time.Millisecond, "AddCleanup backstop must release the abandoned buffer once the stream is collected")
	require.Nil(t, msg.Buffer())
}
