// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"fmt"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/siderolabs/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

// maxLoggedMessageLength is the maximum length of a decoded message in the logs.
const maxLoggedMessageLength = 1024

// proxiedCall is one proxied gRPC call: all the request messages and all the response messages of the call.
// A unary or server-streaming call has one request message. A client-streaming or bidirectional call can have more.
type proxiedCall struct {
	FullMethod string

	// Requests are the decoded request messages, in the order that the client sent them.
	Requests     []proto.Message
	RequestBytes int

	// Responses are the decoded response messages, in the order that the backend sent them.
	Responses     []proto.Message
	ResponseBytes int

	// Err is the final error of the call, or nil if the call succeeded.
	Err error

	// DecodeErr holds the errors that occurred when the proxy decoded the messages of the call.
	// If DecodeErr is not nil, Requests or Responses can be incomplete.
	DecodeErr error
}

// decodingStreamInterceptor returns a stream interceptor that decodes the requests and responses of each proxied call.
// When a call is complete, the interceptor calls onCall with the collected call. It does not call onCall for methods
// that the codec does not know. The interceptor does not change the forwarded frames.
func decodingStreamInterceptor(codec MessageCodec, onFinish func(proxiedCall)) grpc.StreamServerInterceptor {
	frameCodec := proxy.Codec()

	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		stream := &decodingServerStream{
			ServerStream: ss,
			codec:        codec,
			frameCodec:   frameCodec,
			call:         proxiedCall{FullMethod: info.FullMethod},
		}

		err := handler(srv, stream)

		call, known := stream.finish(err)
		if known {
			onFinish(call)
		}
		return err
	}
}

// decodingServerStream wraps the server stream of a proxied call, and collects the decoded messages of the call.
// RecvMsg receives the request frames from the client, and SendMsg sends the response frames to the client.
// The Sidero gRPC proxy calls RecvMsg and SendMsg from different goroutines.
type decodingServerStream struct {
	grpc.ServerStream

	codec      MessageCodec
	frameCodec encoding.CodecV2

	mtx sync.Mutex
	// done is true when the call is complete.
	// The proxy can call SendMsg after the handler returned an error,
	// so the stream drops the messages that arrive after that.
	done          bool
	unknownMethod bool
	call          proxiedCall
}

func (s *decodingServerStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}

	msg, size, err := s.decode(m, s.codec.DecodeRequest)

	s.mtx.Lock()
	if s.collect(err) {
		s.call.Requests = append(s.call.Requests, msg)
		s.call.RequestBytes += size
	}
	s.mtx.Unlock()

	return nil
}

func (s *decodingServerStream) SendMsg(m any) error {
	msg, size, err := s.decode(m, s.codec.DecodeResponse)

	s.mtx.Lock()
	if s.collect(err) {
		s.call.Responses = append(s.call.Responses, msg)
		s.call.ResponseBytes += size
	}
	s.mtx.Unlock()

	return s.ServerStream.SendMsg(m)
}

// collect records the decode error, and returns true if the stream must keep the decoded message.
// The caller must hold s.mtx.
func (s *decodingServerStream) collect(decodeErr error) bool {
	if s.done {
		return false
	}
	if errors.Is(decodeErr, errUnknownMethod) {
		s.unknownMethod = true
		return false
	}
	if decodeErr != nil {
		s.call.DecodeErr = errors.Join(s.call.DecodeErr, decodeErr)
		return false
	}
	return true
}

// finish marks the call as complete, and returns the collected call.
// It returns false if the codec does not know the method of the call.
func (s *decodingServerStream) finish(callErr error) (proxiedCall, bool) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.done = true
	s.call.Err = callErr
	return s.call, !s.unknownMethod
}

func (s *decodingServerStream) decode(m any, decode func(string, []byte) (proto.Message, error)) (proto.Message, int, error) {
	// The proxy frame type is not exported, but the proxy codec marshals a frame to its payload.
	data, err := s.frameCodec.Marshal(m)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read proxied frame: %w", err)
	}
	// Materialize copies the payload, so the decoded message does not refer to a pooled buffer.
	payload := data.Materialize()
	data.Free()

	msg, err := decode(s.call.FullMethod, payload)
	return msg, len(payload), err
}

// logProxiedCall returns a function that logs each proxied call.
func logProxiedCall(logger log.Logger) func(proxiedCall) {
	return func(call proxiedCall) {
		keyvals := []any{
			"msg", "proxied call",
			"method", call.FullMethod,
			"requests", len(call.Requests),
			"request_bytes", call.RequestBytes,
			"responses", len(call.Responses),
			"response_bytes", call.ResponseBytes,
		}
		if len(call.Requests) > 0 {
			// Most calls have one request message, so the log shows only the first one.
			keyvals = append(keyvals, "first_request", truncate(call.Requests[0].String(), maxLoggedMessageLength))
		}
		if call.Err != nil {
			keyvals = append(keyvals, "err", call.Err)
		}
		if call.DecodeErr != nil {
			level.Warn(logger).Log(append(keyvals, "decode_err", call.DecodeErr)...)
			return
		}
		level.Info(logger).Log(keyvals...)
	}
}

func truncate(s string, maxLength int) string {
	if len(s) <= maxLength {
		return s
	}
	return s[:maxLength] + "..."
}
