// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const testMethod = "/test.Service/Method"

const testTimeout = 10 * time.Second

type testBackend struct {
	conn *grpc.ClientConn
}

func (b testBackend) Name() string           { return "test" }
func (b testBackend) Conn() *grpc.ClientConn { return b.conn }

type backendHandler func(stream grpc.ServerStream) error

// startBackend starts a gRPC server that sends every call to the handler.
func startBackend(t *testing.T, handler backendHandler) Backend {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer(
		grpc.ForceServerCodec(FrameCodec{}),
		grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error { return handler(stream) }),
	)
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)

	return dialBackend(t, listener.Addr().String())
}

func dialBackend(t *testing.T, addr string) Backend {
	t.Helper()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return testBackend{conn: conn}
}

// unreachableBackend returns a backend with an address that refuses connections.
func unreachableBackend(t *testing.T) Backend {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return dialBackend(t, addr)
}

// recvAll reads request frames until the client closes its side.
func recvAll(t *testing.T, stream grpc.ServerStream) []Frame {
	t.Helper()

	var frames []Frame
	for {
		var f Frame
		if err := stream.RecvMsg(&f); err != nil {
			// A clean end is io.EOF. Any other error means the call ended early.
			// The tests assert the outcome, so both cases return the frames so far.
			return frames
		}
		frames = append(frames, f)
	}
}

func sendAll(stream grpc.ServerStream, frames ...Frame) error {
	for _, f := range frames {
		if err := stream.SendMsg(&f); err != nil {
			return err
		}
	}
	return nil
}

type pairRecorder struct {
	mu    sync.Mutex
	pairs []ResponseFramePair
}

func (r *pairRecorder) record(_ log.Logger, _ string, pair ResponseFramePair) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pairs = append(r.pairs, pair)
}

func (r *pairRecorder) get() []ResponseFramePair {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ResponseFramePair(nil), r.pairs...)
}

type teeHarness struct {
	tee      *Tee
	client   *grpc.ClientConn
	recorder *pairRecorder
}

func newTeeHarness(t *testing.T, cfg TeeConfig, primary, secondary Backend) *teeHarness {
	t.Helper()

	tee := NewTee(cfg, NewBackendType("test", NewMethodCodecTable()), log.NewNopLogger())
	tee.SetBackends(primary, secondary)
	recorder := &pairRecorder{}
	tee.compare = recorder.record

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(
		grpc.ForceServerCodec(FrameCodec{}),
		grpc.UnknownServiceHandler(tee.Handle),
	)
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)
	t.Cleanup(tee.Stop)

	client, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &teeHarness{tee: tee, client: client, recorder: recorder}
}

func defaultTestConfig() TeeConfig {
	return TeeConfig{SecondaryDrainTimeout: time.Minute, SecondaryRequestBufferSize: 16}
}

// call sends the request frames through the tee and reads the response until the call ends.
// It returns the received frames and the final error. The error is nil for a clean end.
func (h *teeHarness) call(ctx context.Context, requests ...Frame) ([]Frame, error) {
	stream, err := h.client.NewStream(ctx, backendClientStreamDesc, testMethod, grpc.ForceCodec(FrameCodec{}))
	if err != nil {
		return nil, err
	}
	for _, f := range requests {
		if err := stream.SendMsg(&f); err != nil {
			return nil, err
		}
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}

	var frames []Frame
	for {
		var f Frame
		if err := stream.RecvMsg(&f); err != nil {
			if errors.Is(err, io.EOF) {
				return frames, nil
			}
			return frames, err
		}
		frames = append(frames, f)
	}
}

// waitDone waits until every goroutine of finished calls has returned.
func (h *teeHarness) waitDone(t *testing.T) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		h.tee.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("the tee goroutines did not finish")
	}
}

func f(s string) Frame { return Frame(s) }

func fp(s string) *Frame { fr := Frame(s); return &fr }

func TestTee_ForwardsPrimaryAndCollectsBoth(t *testing.T) {
	primaryReqs := make(chan []Frame, 1)
	secondaryReqs := make(chan []Frame, 1)
	primary := startBackend(t, func(stream grpc.ServerStream) error {
		primaryReqs <- recvAll(t, stream)
		return sendAll(stream, f("p1"), f("p2"))
	})
	secondary := startBackend(t, func(stream grpc.ServerStream) error {
		secondaryReqs <- recvAll(t, stream)
		return sendAll(stream, f("s1"), f("s2"))
	})

	h := newTeeHarness(t, defaultTestConfig(), primary, secondary)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	frames, err := h.call(ctx, f("req1"), f("req2"))
	require.NoError(t, err)
	require.Equal(t, []Frame{f("p1"), f("p2")}, frames)
	h.waitDone(t)

	require.Equal(t, []ResponseFramePair{
		{primary: fp("p1"), secondary: fp("s1")},
		{primary: fp("p2"), secondary: fp("s2")},
	}, h.recorder.get())
	require.Equal(t, []Frame{f("req1"), f("req2")}, <-primaryReqs)
	require.Equal(t, []Frame{f("req1"), f("req2")}, <-secondaryReqs)
}

func TestTee_SlowSecondaryDoesNotDelayClient(t *testing.T) {
	release := make(chan struct{})
	primary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		return sendAll(stream, f("p1"), f("p2"))
	})
	secondary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		select {
		case <-release:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
		return sendAll(stream, f("s1"))
	})

	h := newTeeHarness(t, defaultTestConfig(), primary, secondary)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	// The call returns while the secondary backend still waits.
	frames, err := h.call(ctx, f("req"))
	require.NoError(t, err)
	require.Equal(t, []Frame{f("p1"), f("p2")}, frames)

	close(release)
	h.waitDone(t)

	require.Equal(t, []ResponseFramePair{
		{primary: fp("p1"), secondary: fp("s1")},
		{primary: fp("p2")},
	}, h.recorder.get())
}

func TestTee_SecondaryCanceledAfterDrainTimeout(t *testing.T) {
	secondaryCanceled := make(chan struct{})
	primary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		return sendAll(stream, f("p1"))
	})
	secondary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		<-stream.Context().Done()
		close(secondaryCanceled)
		return stream.Context().Err()
	})

	cfg := defaultTestConfig()
	cfg.SecondaryDrainTimeout = 100 * time.Millisecond
	h := newTeeHarness(t, cfg, primary, secondary)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	frames, err := h.call(ctx, f("req"))
	require.NoError(t, err)
	require.Equal(t, []Frame{f("p1")}, frames)

	select {
	case <-secondaryCanceled:
	case <-time.After(testTimeout):
		t.Fatal("the tee did not cancel the secondary call")
	}
	h.waitDone(t)
	require.Equal(t, []ResponseFramePair{{primary: fp("p1")}}, h.recorder.get())
}

func TestTee_SecondaryOpenFailureDoesNotFailClient(t *testing.T) {
	primary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		return sendAll(stream, f("p1"), f("p2"))
	})

	h := newTeeHarness(t, defaultTestConfig(), primary, unreachableBackend(t))
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	frames, err := h.call(ctx, f("req"))
	require.NoError(t, err)
	require.Equal(t, []Frame{f("p1"), f("p2")}, frames)
	h.waitDone(t)

	require.Equal(t, []ResponseFramePair{
		{primary: fp("p1")},
		{primary: fp("p2")},
	}, h.recorder.get())
}

func TestTee_SecondaryErrorDoesNotFailClient(t *testing.T) {
	primary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		return sendAll(stream, f("p1"), f("p2"))
	})
	secondary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		if err := sendAll(stream, f("s1")); err != nil {
			return err
		}
		return status.Error(codes.Internal, "secondary failed")
	})

	h := newTeeHarness(t, defaultTestConfig(), primary, secondary)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	frames, err := h.call(ctx, f("req"))
	require.NoError(t, err)
	require.Equal(t, []Frame{f("p1"), f("p2")}, frames)
	h.waitDone(t)

	require.Equal(t, []ResponseFramePair{
		{primary: fp("p1"), secondary: fp("s1")},
		{primary: fp("p2")},
	}, h.recorder.get())
}

func TestTee_PrimaryErrorReachesClientAndSecondaryContinues(t *testing.T) {
	primary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		if err := sendAll(stream, f("p1")); err != nil {
			return err
		}
		return status.Error(codes.NotFound, "primary failed")
	})
	secondary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		return sendAll(stream, f("s1"), f("s2"))
	})

	h := newTeeHarness(t, defaultTestConfig(), primary, secondary)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	frames, err := h.call(ctx, f("req"))
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Equal(t, []Frame{f("p1")}, frames)
	h.waitDone(t)

	require.Equal(t, []ResponseFramePair{
		{primary: fp("p1"), secondary: fp("s1")},
		{secondary: fp("s2")},
	}, h.recorder.get())
}

func TestTee_ClientCancelDoesNotCancelSecondary(t *testing.T) {
	primaryCanceled := make(chan struct{})
	release := make(chan struct{})
	secondaryCtxErr := make(chan error, 1)

	primary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		if err := sendAll(stream, f("p1")); err != nil {
			return err
		}
		<-stream.Context().Done()
		close(primaryCanceled)
		return stream.Context().Err()
	})
	secondary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		<-release
		secondaryCtxErr <- stream.Context().Err()
		return sendAll(stream, f("s1"))
	})

	h := newTeeHarness(t, defaultTestConfig(), primary, secondary)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	callCtx, cancelCall := context.WithCancel(ctx)
	defer cancelCall()
	stream, err := h.client.NewStream(callCtx, backendClientStreamDesc, testMethod, grpc.ForceCodec(FrameCodec{}))
	require.NoError(t, err)
	req := f("req")
	require.NoError(t, stream.SendMsg(&req))
	require.NoError(t, stream.CloseSend())
	var first Frame
	require.NoError(t, stream.RecvMsg(&first))
	require.Equal(t, f("p1"), first)

	// The client cancels. The primary backend sees the cancel, and only after that the secondary backend continues.
	cancelCall()
	select {
	case <-primaryCanceled:
	case <-time.After(testTimeout):
		t.Fatal("the primary call was not canceled")
	}
	close(release)

	select {
	case err := <-secondaryCtxErr:
		require.NoError(t, err)
	case <-time.After(testTimeout):
		t.Fatal("the secondary backend did not continue")
	}
	h.waitDone(t)

	require.Equal(t, []ResponseFramePair{{primary: fp("p1"), secondary: fp("s1")}}, h.recorder.get())
}

func TestTee_EmptyFramesKeepPairing(t *testing.T) {
	primary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		return sendAll(stream, f(""), f("p2"))
	})
	secondary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		return sendAll(stream, f(""), f("s2"))
	})

	h := newTeeHarness(t, defaultTestConfig(), primary, secondary)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	frames, err := h.call(ctx, f("req"))
	require.NoError(t, err)
	require.Len(t, frames, 2)
	h.waitDone(t)

	// An empty message has no bytes, but it is still a frame and must keep its place in the pairing.
	pairs := h.recorder.get()
	require.Len(t, pairs, 2)
	require.NotNil(t, pairs[0].primary)
	require.Empty(t, *pairs[0].primary)
	require.NotNil(t, pairs[0].secondary)
	require.Empty(t, *pairs[0].secondary)
	require.Equal(t, ResponseFramePair{primary: fp("p2"), secondary: fp("s2")}, pairs[1])
}

func TestTee_SecondaryQueueOverflowDoesNotAffectPrimary(t *testing.T) {
	bigFrame := Frame(strings.Repeat("x", 256*1024))
	var requests []Frame
	for range 20 {
		requests = append(requests, bigFrame)
	}

	primary := startBackend(t, func(stream grpc.ServerStream) error {
		recvAll(t, stream)
		return sendAll(stream, f("p1"))
	})
	secondaryCanceled := make(chan struct{})
	secondary := startBackend(t, func(stream grpc.ServerStream) error {
		// This backend never reads the request, so the flow control window fills up.
		<-stream.Context().Done()
		close(secondaryCanceled)
		return stream.Context().Err()
	})

	cfg := defaultTestConfig()
	cfg.SecondaryRequestBufferSize = 1
	h := newTeeHarness(t, cfg, primary, secondary)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	frames, err := h.call(ctx, requests...)
	require.NoError(t, err)
	require.Equal(t, []Frame{f("p1")}, frames)

	// The drain timeout is one minute, so only the queue overflow can cancel the secondary call.
	select {
	case <-secondaryCanceled:
	case <-time.After(testTimeout):
		t.Fatal("the tee did not cancel the secondary call after the queue overflow")
	}
	h.waitDone(t)
	require.Equal(t, []ResponseFramePair{{primary: fp("p1")}}, h.recorder.get())
}
