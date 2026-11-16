// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"context"
	"errors"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type SecondaryCall struct {
	teeCfg         TeeConfig
	stream         grpc.ClientStream
	streamCancel   context.CancelCauseFunc
	reqFrameBuffer chan *Frame
	resCollector   *ResponseCollector
	l              log.Logger
}

func NewSecondaryCall(
	stream grpc.ClientStream,
	cancel context.CancelCauseFunc,
	resCollector *ResponseCollector,
	teeCfg TeeConfig,
) *SecondaryCall {
	return &SecondaryCall{
		stream:         stream,
		streamCancel:   cancel,
		reqFrameBuffer: make(chan *Frame, teeCfg.SecondaryRequestBufferSize),
		resCollector:   resCollector,
	}
}

// PushReqFrame enqueues a request frame to the secondary backend.
// Caller is responsible for copying frame before push if necessary to prevent concurrent access.
func (sc *SecondaryCall) PushReqFrame(frame *Frame) error {
	if sc.stream.Context().Err() != nil {
		// Stream/Context errors will bubble up from the Run funcs
		return nil
	}
	select {
	case sc.reqFrameBuffer <- frame:
	default:
		level.Warn(sc.l).Log("msg", "the secondary request buffer is full, canceling the secondary call")
		return SecondaryReqBufferFullErr{queueCap: cap(sc.reqFrameBuffer)}
	}
	return nil
}

func (sc *SecondaryCall) Run() error {
	// Not using a WithContext for now;
	// Context propagation and cancellation is limited to this struct's stream context.
	errGrp := errgroup.Group{}
	errGrp.Go(sc.RunSend)
	errGrp.Go(sc.RunRecv)
	return errGrp.Wait()
}

func (sc *SecondaryCall) RunSend() error {
	ctx := sc.stream.Context()

	for {
		select {
		case <-ctx.Done():
			// The secondary gRPC call is done:
			// successful completion, canceled, or deadline exceeded.
			// Only the stream's SendMsg or RecvMsg will have the full error value if one exists;
			// exit cleanly and let any error bubble up from the RecvMsg call in RunRecv.
			level.Debug(sc.l).Log(
				"msg", "the secondary request stream is done",
				"cause", context.Cause(ctx),
			)
			return nil
		case reqFrame, ok := <-sc.reqFrameBuffer:
			if !ok {
				// Close has been called on SecondaryCall and reqFrameBuffer is empty.
				// Attempt to gracefully close the send side of the stream.
				if err := sc.stream.CloseSend(); err != nil {
					level.Debug(sc.l).Log("msg", "failed to close the secondary request stream", "err", err)
				}
				return nil
			}

			sendErr := sc.stream.SendMsg(reqFrame)
			if sendErr == nil {
				continue
			}

			if errors.Is(sendErr, io.EOF) {
				return nil
			}

			// Check if sendErr is part of grpc-tee internal lifecycle management.
			// Do not exit with error in this case.
			cause := context.Cause(ctx)
			if isInternalCancel(cause) {
				level.Debug(sc.l).Log(
					"msg", "the secondary stream was canceled locally",
					"err", sendErr,
					"cause", cause,
				)
				return nil
			}

			// Otherwise sendErr should be caused by the gRPC send stream with the secondary backend.
			// No need to close the send stream; the gRPC stream is already closed on error.
			level.Warn(sc.l).Log(
				"msg", "failed to send a request frame to the secondary backend",
				"err", sendErr,
				"cause", cause,
			)
			return SecondaryBackendSendErr{cause: sendErr}
		}
	}
}

func (sc *SecondaryCall) RunRecv() error {
	ctx := sc.stream.Context()

	for {
		var frame Frame
		recvErr := sc.stream.RecvMsg(frame)
		if recvErr == nil {
			sc.resCollector.PushSecondary(frame)
			continue
		}
		if errors.Is(recvErr, io.EOF) {
			return nil
		}

		// Check if recvErr is part of grpc-tee internal lifecycle management.
		// Do not exit with error in this case.
		cause := context.Cause(ctx)
		if isInternalCancel(cause) {
			level.Debug(sc.l).Log(
				"msg", "the secondary stream was canceled locally",
				"err", recvErr,
				"cause", cause,
			)
			return nil
		}

		// Otherwise recvErr should be caused by the gRPC receive stream with the secondary backend.
		level.Warn(sc.l).Log(
			"msg", "failed to receive a response frame from the secondary backend",
			"err", recvErr,
			"cause", cause,
		)
		return SecondaryBackendRecvErr{cause: recvErr}
	}
}

func isInternalCancel(ctxErr error) bool {
	if ctxErr == nil {
		return false
	}
	return errors.Is(ctxErr, SecondaryReqBufferFullErr{}) ||
		errors.Is(ctxErr, InboundCallRecvErr{})
}

func (sc *SecondaryCall) CancelStream(err error) {
	sc.streamCancel(err)
}

func (sc *SecondaryCall) FinishAndCloseSend() {
	close(sc.reqFrameBuffer)
}
