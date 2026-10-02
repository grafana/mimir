// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/siderolabs/grpc-proxy/blob/v0.5.2/proxy/handler.go
// Provenance-includes-location: https://github.com/siderolabs/grpc-proxy/blob/v0.5.2/proxy/handler_one2one.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Michal Witkowski.
// Provenance-includes-copyright: Andrey Smirnov.

package main

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// secondaryRequestBufferSize is the number of request messages that can wait for the secondary backend.
// If the secondary backend is slower than this, the proxy aborts the secondary call.
const secondaryRequestBufferSize = 16

// clientStreamDescForProxying is the stream description of the backend calls. The proxy does not know the shape
// of a call, so it treats every call as a bidirectional stream.
var clientStreamDescForProxying = &grpc.StreamDesc{
	ServerStreams: true,
	ClientStreams: true,
}

// teeBackend is a backend that the tee handler sends calls to.
type teeBackend interface {
	Name() string
	Conn() *grpc.ClientConn
}

// teeHandler forwards each call to the primary backend, and returns the primary response to the client.
// It also sends each call to the secondary backend, if there is one, and reads the secondary response in the background.
// When both backend calls are complete, it calls onFinish with the collected call.
type teeHandler struct {
	backendType      backendType
	primary          teeBackend
	secondary        teeBackend
	secondaryTimeout time.Duration
	onFinish         func(teeCall)
}

// handle is a grpc.StreamHandler for all methods. Register it with grpc.UnknownServiceHandler.
func (h *teeHandler) handle(_ any, serverStream grpc.ServerStream) error {
	fullMethod, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return status.Errorf(codes.Internal, "lowLevelServerStream doesn't exist in the context")
	}

	// The proxy records, tees, and compares only the calls of methods that the codec knows.
	// It forwards the other calls, for example health checks, to the primary backend only.
	codec := h.backendType.codec
	known := codec == nil || codec.Knows(fullMethod)
	if !known {
		codec = nil
	}

	// Forward the inbound metadata (for example X-Scope-OrgID) to the backends.
	md, _ := metadata.FromIncomingContext(serverStream.Context()) //lint:ignore faillint The proxy forwards all keys.

	var secondary *secondaryCall
	if known && h.secondary != nil {
		secondary = h.startSecondary(serverStream.Context(), fullMethod, md, codec)
	}

	requests := newMessageRecorder(requestDecoder(codec, fullMethod), time.Now())

	// The primary call inherits the cancellation of the client call.
	primaryCtx, primaryCancel := context.WithCancelCause(serverStream.Context())
	defer primaryCancel(errors.New("the proxied call ended"))
	primaryResponses := newMessageRecorder(responseDecoder(codec, fullMethod), time.Now())

	aborted := false
	primaryStream, err := grpc.NewClientStream(metadata.NewOutgoingContext(primaryCtx, md), clientStreamDescForProxying, h.primary.Conn(), fullMethod)
	if err != nil {
		// No request goroutine runs, so the handler stops the secondary call.
		secondary.abort()
	} else {
		aborted, err = h.proxyToPrimary(serverStream, primaryStream, requests, primaryResponses, secondary)
	}
	if serverStream.Context().Err() != nil {
		// The client canceled the call, or its deadline expired. The primary call inherits that, so it is incomplete.
		aborted = true
	}

	if known {
		requestMsgs, requestBytes, _, requestDecodeErr := requests.finish()
		call := teeCall{
			FullMethod:       fullMethod,
			Requests:         requestMsgs,
			RequestBytes:     requestBytes,
			RequestDecodeErr: requestDecodeErr,
			Primary:          primaryResponses.finishBackendCall(h.primary.Name(), err, aborted),
		}
		// The client does not wait for the secondary call.
		go func() {
			if secondary != nil {
				result := secondary.wait()
				call.Secondary = &result
			}
			h.onFinish(call)
		}()
	}

	return err
}

// proxyToPrimary forwards the call between the client and the primary backend.
// It returns true if the client side failed, so the primary call is incomplete. It also returns the final status of the call.
func (h *teeHandler) proxyToPrimary(serverStream grpc.ServerStream, primaryStream grpc.ClientStream, requests, responses *messageRecorder, secondary *secondaryCall) (bool, error) {
	requestErrs := forwardRequests(serverStream, primaryStream, requests, secondary)
	responseErrs := forwardResponses(primaryStream, serverStream, responses)

	// We don't know which side is going to stop sending first, so we need a select between the two.
	for range 2 {
		select {
		case requestErr := <-requestErrs:
			if errors.Is(requestErr, io.EOF) {
				// This is the happy case where the client has encountered io.EOF, and won't be sending anymore.
				// The primary backend may continue sending responses though.
				// CloseSend only half-closes the primary stream, and the proxy keeps forwarding the responses.
				// util.CloseAndExhaust would discard the responses, so it does not fit here.
				_ = primaryStream.CloseSend() //nolint:forbidigo // The proxy reads the responses after the half-close.
				continue
			}
			// The client stream failed (stream disconnected, a read error, and more), so we exit with an error.
			// The deferred cancel of the primary context frees the goroutines of the primary stream.
			return true, status.Errorf(codes.Internal, "failed proxying s2c: %v", requestErr)

		case result := <-responseErrs:
			// This happens when the primary backend has nothing else to offer (io.EOF), or returned a gRPC error.
			// In those two cases we may have received trailers as part of the call.
			// In case of other errors (stream closed) the trailers will be nil.
			serverStream.SetTrailer(primaryStream.Trailer())
			if errors.Is(result.err, io.EOF) {
				return false, nil
			}
			// If the error is not io.EOF, it is the gRPC error of the primary backend, or a failure to send to the client.
			return result.clientSide, result.err
		}
	}

	return false, status.Errorf(codes.Internal, "gRPC proxying should never reach this stage.")
}

// forwardRequests forwards the request messages from the client to the primary backend and the secondary backend.
// It returns a channel that receives the error that stopped the forwarding. The error is io.EOF when the client
// finished sending.
func forwardRequests(src grpc.ServerStream, primary grpc.ClientStream, requests *messageRecorder, secondary *secondaryCall) chan error {
	ret := make(chan error, 1)

	go func() {
		// This goroutine is the only one that sends requests to the secondary call, so it also closes the requests.
		defer secondary.closeSend()

		for {
			f := &frame{}
			if err := src.RecvMsg(f); err != nil {
				ret <- err // this can be io.EOF which is happy case
				return
			}
			requests.record(f.payload)
			secondary.send(f.payload)
			if err := primary.SendMsg(f); err != nil {
				ret <- err
				return
			}
		}
	}()

	return ret
}

// forwardResult is the reason that the forwarding of the responses stopped.
type forwardResult struct {
	err error
	// clientSide is true if sending to the client failed.
	clientSide bool
}

// forwardResponses forwards the headers and the response messages from the primary backend to the client.
func forwardResponses(src grpc.ClientStream, dst grpc.ServerStream, responses *messageRecorder) chan forwardResult {
	ret := make(chan forwardResult, 1)

	go func() {
		// Send the header metadata first.
		md, err := src.Header()
		if err != nil {
			ret <- forwardResult{err: err}
			return
		}
		if md != nil {
			if err := dst.SendHeader(md); err != nil {
				ret <- forwardResult{err: err, clientSide: true}
				return
			}
		}

		for {
			f := &frame{}
			if err := src.RecvMsg(f); err != nil {
				ret <- forwardResult{err: err} // this can be io.EOF which is happy case
				return
			}
			responses.record(f.payload)
			if err := dst.SendMsg(f); err != nil {
				ret <- forwardResult{err: err, clientSide: true}
				return
			}
		}
	}()

	return ret
}

// secondaryCall is a call to the secondary backend. It runs in the background, and it never writes to the client.
type secondaryCall struct {
	requests chan []byte
	// closed is true after the requests channel is closed. Only the goroutine that sends the requests uses it.
	closed  bool
	aborted atomic.Bool
	cancel  context.CancelFunc

	done   chan struct{}
	result backendCall
}

// startSecondary starts the call to the secondary backend.
// The secondary call does not inherit the cancellation of the client call, so it can finish after the client call ended.
func (h *teeHandler) startSecondary(parent context.Context, fullMethod string, md metadata.MD, codec MessageCodec) *secondaryCall {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), h.secondaryTimeout)
	s := &secondaryCall{
		requests: make(chan []byte, secondaryRequestBufferSize),
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	go s.run(metadata.NewOutgoingContext(ctx, md), h.secondary, fullMethod, responseDecoder(codec, fullMethod))
	return s
}

func (s *secondaryCall) run(ctx context.Context, backend teeBackend, fullMethod string, decode func([]byte) (proto.Message, error)) {
	defer close(s.done)
	defer s.cancel()

	responses := newMessageRecorder(decode, time.Now())
	stream, err := grpc.NewClientStream(ctx, clientStreamDescForProxying, backend.Conn(), fullMethod)
	if err != nil {
		s.result = responses.finishBackendCall(backend.Name(), err, s.aborted.Load())
		return
	}

	// Send the requests in a separate goroutine, so the secondary call can also be bidirectional.
	// The goroutine reads the requests until the channel is closed, so the client side never blocks.
	go func() {
		var sendErr error
		for payload := range s.requests {
			if sendErr == nil {
				sendErr = stream.SendMsg(&frame{payload: payload})
			}
		}
		if sendErr == nil && !s.aborted.Load() {
			_ = stream.CloseSend() //nolint:forbidigo // The proxy reads the responses after the half-close.
		}
	}()

	for {
		f := &frame{}
		if err = stream.RecvMsg(f); err != nil {
			break
		}
		responses.record(f.payload)
	}
	if errors.Is(err, io.EOF) {
		err = nil
	}
	s.result = responses.finishBackendCall(backend.Name(), err, s.aborted.Load())
}

// send queues a request message for the secondary backend. It never blocks.
// If the queue is full, it aborts the secondary call, so the secondary backend never slows down the primary call.
func (s *secondaryCall) send(payload []byte) {
	if s == nil || s.closed {
		return
	}
	select {
	case s.requests <- payload:
	default:
		s.abort()
	}
}

// closeSend tells the secondary call that the client sent all the requests.
func (s *secondaryCall) closeSend() {
	if s == nil || s.closed {
		return
	}
	s.closed = true
	close(s.requests)
}

// abort stops the secondary call. The call result is marked as aborted.
// Call abort only from the goroutine that sends the requests.
func (s *secondaryCall) abort() {
	if s == nil {
		return
	}
	s.aborted.Store(true)
	s.cancel()
	s.closeSend()
}

// wait waits until the secondary call is complete, and returns its result.
func (s *secondaryCall) wait() backendCall {
	<-s.done
	return s.result
}
