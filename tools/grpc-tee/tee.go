// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Tee struct {
	backendType      *BackendType
	primaryBackend   Backend // nolint: unused
	secondaryBackend Backend // nolint: unused
	logger           log.Logger
}

func NewTee(backendType *BackendType, logger log.Logger) *Tee {
	return &Tee{
		backendType: backendType,
		logger:      logger,
	}
}

func (t *Tee) Handle(_ any, serverStream grpc.ServerStream) error {
	var err error

	fullMethod, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return status.Errorf(codes.Internal, "ServerTransportStream not found in context")
	}

	// STUB IMPL
	var rawInboundReq Frame
	if err := serverStream.RecvMsg(&rawInboundReq); err != nil {
		return status.Errorf(codes.Internal, "receive request: %v", err)
	}

	decodedInboundReq, err := t.backendType.DecodeRequest(fullMethod, rawInboundReq)
	if err != nil {
		level.Error(t.logger).Log("msg", "failed to decode request", "method", fullMethod, "err", err)
		return status.Errorf(codes.InvalidArgument, "decode request: %v", err)
	}
	level.Info(t.logger).Log("msg", "decoded request", "method", fullMethod, "bytes", len(rawInboundReq), "request", decodedInboundReq.String())
	// END STUB IMPL

	// Open both backend streams
	primaryBackendStream, primaryCancel, err := openPrimaryBackendStream(
		serverStream, fullMethod, nil, t.primaryBackend,
	) // TODO handle error

	secondaryBackendStream, secondaryCancel, err := openSecondaryBackendStream(
		serverStream, fullMethod, nil, t.secondaryBackend,
	) // TODO handle error

	// STUB IMPL
	var respBytes []byte
	decodedResp, err := t.backendType.DecodeResponse(fullMethod, respBytes)
	if err != nil {
		return status.Errorf(codes.Internal, "decode response: %v", err)
	}
	level.Info(t.logger).Log("msg", "decoded response", "method", fullMethod, "bytes", len(respBytes), "response", decodedResp.String())

	if err := serverStream.SendMsg(decodedResp); err != nil {
		return status.Errorf(codes.Internal, "send response: %v", err)
	}
	// END STUB IMPL

	// return status.Errorf(codes.Unimplemented, "tee does not forward %s yet", fullMethod
	return nil
}

var backendClientStreamDesc = &grpc.StreamDesc{ // nolint: unused
	// Allow client streaming to be generic across unary vs. stream request types.
	// When we receive an EOF from RecvMsg on our server stream, we have received the last request frame.
	// With client stream enabled, we must manually call CloseSend after forwarding that last request frame.
	ClientStreams: true,
	// Same story as above, but server streaming is more common
	// than client streaming in Mimir so this is a more standard case.
	ServerStreams: true,
}

type metadataTransform func(metadata.MD) metadata.MD

func openPrimaryBackendStream(
	serverStream grpc.ServerStream,
	fullMethod string,
	mdTransform metadataTransform,
	primaryBackend Backend,
) (grpc.ClientStream, context.CancelCauseFunc, error) {
	// FromIncomingContext returns a copy;
	// the transform can update without affecting the original req context.
	md, _ := metadata.FromIncomingContext(serverStream.Context()) //lint:ignore faillint A proxy forwards all keys.
	if mdTransform != nil {
		md = mdTransform(md)
	}

	// The primary call inherits the cancellation of the inbound call.
	primaryCtx, primaryCancel := context.WithCancelCause(serverStream.Context())
	primaryBackendStream, err := openBackendStream(
		primaryCtx, primaryBackend.Conn(), fullMethod,
	)
	return primaryBackendStream, primaryCancel, err
}

func openSecondaryBackendStream(
	serverStream grpc.ServerStream,
	fullMethod string,
	mdTransform metadataTransform,
	secondaryBackend Backend,
) (grpc.ClientStream, context.CancelCauseFunc, error) {
	// FromIncomingContext returns a copy;
	// the transform can update without affecting the original req context.
	md, _ := metadata.FromIncomingContext(serverStream.Context()) //lint:ignore faillint A proxy forwards all keys.
	if mdTransform != nil {
		md = mdTransform(md)
	}

	// The secondary call does not inherit the cancellation of the inbound call
	// so it can finish before or after without affect the primary.
	inboundCtx := serverStream.Context()
	secondaryCtx := context.WithoutCancel(inboundCtx)

	// We expect any inbound call to have a deadline, but create a no-op to use in case.
	cancelDeadline := func() {}
	if deadline, ok := inboundCtx.Deadline(); ok {
		// Take the same deadline as the inbound call if it has one.
		secondaryCtx, cancelDeadline = context.WithDeadline(secondaryCtx, deadline)
	}

	secondaryCtx, cancelCause := context.WithCancelCause(secondaryCtx)
	secondaryCancel := context.CancelCauseFunc(func(cause error) {
		cancelCause(cause)
		cancelDeadline()
	})

	secondaryBackendStream, err := openBackendStream(
		secondaryCtx, secondaryBackend.Conn(), fullMethod,
	)

	return secondaryBackendStream, secondaryCancel, err
}

func openBackendStream( // nolint: unused
	ctx context.Context,
	conn grpc.ClientConnInterface,
	fullMethod string,
) (grpc.ClientStream, error) {
	if s, err := conn.NewStream(
		ctx, backendClientStreamDesc, fullMethod,
	); err != nil {
		return nil, err
	} else {
		return s, nil
	}
}
