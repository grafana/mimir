// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/siderolabs/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

// maxLoggedMessageLength is the maximum length of a decoded message in the logs.
// Series responses can hold many chunks, so the logs show only the start of each message.
const maxLoggedMessageLength = 1024

// decodingStreamInterceptor returns a stream interceptor that decodes the requests and responses
// that the proxy forwards, and logs them. It does not change the forwarded frames.
func decodingStreamInterceptor(codec MessageCodec, logger log.Logger) grpc.StreamServerInterceptor {
	frameCodec := proxy.Codec()

	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &decodingServerStream{
			ServerStream: ss,
			fullMethod:   info.FullMethod,
			codec:        codec,
			frameCodec:   frameCodec,
			logger:       logger,
		})
	}
}

// decodingServerStream wraps the server stream of a proxied call.
// RecvMsg receives the request frames from the client, and SendMsg sends the response frames to the client.
type decodingServerStream struct {
	grpc.ServerStream

	fullMethod string
	codec      MessageCodec
	frameCodec encoding.CodecV2
	logger     log.Logger
}

func (s *decodingServerStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	s.logMessage("request", m, s.codec.DecodeRequest)
	return nil
}

func (s *decodingServerStream) SendMsg(m any) error {
	s.logMessage("response", m, s.codec.DecodeResponse)
	return s.ServerStream.SendMsg(m)
}

func (s *decodingServerStream) logMessage(direction string, m any, decode func(string, []byte) (proto.Message, error)) {
	// The proxy frame type is not exported, but the proxy codec marshals a frame to its payload.
	data, err := s.frameCodec.Marshal(m)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to read proxied frame", "method", s.fullMethod, "direction", direction, "err", err)
		return
	}
	// Materialize copies the payload, so the decoded message does not refer to a pooled buffer.
	payload := data.Materialize()
	data.Free()

	msg, err := decode(s.fullMethod, payload)
	if errors.Is(err, errUnknownMethod) {
		return
	}
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to decode proxied message", "method", s.fullMethod, "direction", direction, "err", err)
		return
	}

	text := msg.String()
	if len(text) > maxLoggedMessageLength {
		text = text[:maxLoggedMessageLength] + "..."
	}
	level.Info(s.logger).Log("msg", "proxied message", "method", s.fullMethod, "direction", direction, "type", proto.MessageName(msg), "size", len(payload), "message", text)
}
