// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc"
)

// callFinishedError is the cancel cause for a call that finished its work.
type callFinishedError struct{}

func (callFinishedError) Error() string { return "call finished" }

// teeStoppedError is the cancel cause for a secondary call that Tee.Stop canceled.
type teeStoppedError struct{}

func (teeStoppedError) Error() string { return "tee stopped" }

// secondaryOverflowError is the cancel cause for a secondary call that could not keep up with the request frames.
type secondaryOverflowError struct {
	queueSize int
}

func (e secondaryOverflowError) Error() string {
	return fmt.Sprintf("secondary request queue is full, size: %d", e.queueSize)
}

// secondaryDrainTimeoutError is the cancel cause for a secondary call that ran too long after the primary call finished.
type secondaryDrainTimeoutError struct {
	timeout time.Duration
}

func (e secondaryDrainTimeoutError) Error() string {
	return fmt.Sprintf("secondary call did not finish within %s after the primary call", e.timeout)
}

// secondaryIncompleteRequestError is the cancel cause for a secondary call
// when the inbound call failed before the last request frame.
type secondaryIncompleteRequestError struct {
	cause error
}

func (e secondaryIncompleteRequestError) Error() string {
	return fmt.Sprintf("inbound request is incomplete: %v", e.cause)
}

func (e secondaryIncompleteRequestError) Unwrap() error { return e.cause }

// xsecondaryCall holds the secondary stream and the queue of request frames for it.
// The stream and the queue are nil when the tee could not open the secondary stream.
type xsecondaryCall struct {
	stream grpc.ClientStream
	cancel context.CancelCauseFunc
	queue  chan Frame
}

// enqueue copies a request frame into the queue. It never blocks.
// If the queue is full, it cancels the secondary call, so the primary call does not wait.
func (s *xsecondaryCall) enqueue(logger log.Logger, frame Frame) {
	if s.queue == nil || s.stream.Context().Err() != nil {
		return
	}
	select {
	case s.queue <- slices.Clone(frame):
	default:
		level.Warn(logger).Log("msg", "the secondary request queue is full, canceling the secondary call", "queue_size", cap(s.queue))
		s.cancel(secondaryOverflowError{queueSize: cap(s.queue)})
	}
}

// closeQueue tells sendRequests that no more request frames follow.
// Only the request pump calls it.
func (s *xsecondaryCall) closeQueue() {
	if s.queue != nil {
		close(s.queue)
	}
}

// sendRequests sends the frames of the queue to the secondary backend.
// It calls CloseSend after the queue is closed.
// It returns when the queue is closed, or when the secondary call is canceled.
func (s *xsecondaryCall) sendRequests(logger log.Logger) {
	ctx := s.stream.Context()
	sendFailed := false
	for {
		select {
		case <-ctx.Done():
			return
		case frame, ok := <-s.queue:
			if !ok {
				if !sendFailed {
					if err := s.stream.CloseSend(); err != nil {
						level.Debug(logger).Log("msg", "failed to close the secondary request stream", "err", err)
					}
				}
				return
			}
			if sendFailed {
				// Keep the queue empty, so the pump does not see a full queue.
				continue
			}
			if err := s.stream.SendMsg(&frame); err != nil {
				// An io.EOF error means that the call ended. receiveResponses reads the status.
				sendFailed = true
				level.Debug(logger).Log("msg", "failed to send a request frame to the secondary backend", "err", err)
			}
		}
	}
}

// receiveResponses pushes every response frame of the secondary backend into the collector.
// The tee never sends these frames to the client.
// It returns when the call ends, or when the secondary call is canceled.
func (s *xsecondaryCall) receiveResponses(logger log.Logger, collector *ResponseCollector) {
	for {
		// Every message needs a new Frame, because the collector keeps the frame.
		var frame Frame
		if err := s.stream.RecvMsg(&frame); err != nil {
			if !errors.Is(err, io.EOF) {
				level.Warn(logger).Log("msg", "the secondary call failed", "err", err, "cause", context.Cause(s.stream.Context()))
			}
			return
		}
		collector.PushSecondary(&frame)
	}
}

// pumpRequests copies the request frames of the inbound call to the primary backend,
// and a clone of each frame to the secondary call.
// It returns when the inbound call sends EOF, or when the inbound call fails.
// If the primary stream fails, it keeps reading, so the secondary call still gets the full request.
func (t *Tee) pumpRequests(
	logger log.Logger,
	fullMethod string,
	serverStream grpc.ServerStream,
	primaryStream grpc.ClientStream,
	secondary *xsecondaryCall,
) {
	defer secondary.closeQueue()

	primarySendOK := true
	first := true
	for {
		var frame Frame
		if err := serverStream.RecvMsg(&frame); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// The inbound call failed before the last frame, so the secondary request is incomplete.
			secondary.cancel(secondaryIncompleteRequestError{cause: err})
			return
		}

		if first {
			first = false
			if decoded, err := t.backendType.DecodeRequest(fullMethod, frame); err != nil {
				level.Debug(logger).Log("msg", "failed to decode request", "err", err)
			} else {
				level.Debug(logger).Log("msg", "decoded request", "bytes", len(frame), "request", decoded.String())
			}
		}

		if primarySendOK {
			if err := primaryStream.SendMsg(&frame); err != nil {
				// An io.EOF error means that the call ended. forwardPrimaryResponses reads the status.
				primarySendOK = false
				level.Debug(logger).Log("msg", "failed to send a request frame to the primary backend", "err", err)
			}
		}
		secondary.enqueue(logger, frame)
	}

	if primarySendOK {
		if err := primaryStream.CloseSend(); err != nil {
			level.Debug(logger).Log("msg", "failed to close the primary request stream", "err", err)
		}
	}
}

// forwardPrimaryResponses sends every response frame of the primary backend to the client,
// and pushes it into the collector.
// It returns nil after the last frame, or the error of the primary call.
// The error is a gRPC status error, so the client gets the same status.
func forwardPrimaryResponses(
	serverStream grpc.ServerStream,
	primaryStream grpc.ClientStream,
	collector *ResponseCollector,
) error {
	headerSent := false
	forwardHeader := func() error {
		if headerSent {
			return nil
		}
		md, err := primaryStream.Header()
		if err != nil {
			return err
		}
		headerSent = true
		return serverStream.SendHeader(md)
	}

	for {
		// Every message needs a new Frame, because the collector keeps the frame.
		var frame Frame
		if err := primaryStream.RecvMsg(&frame); err != nil {
			// The trailers are valid after RecvMsg returns an error.
			serverStream.SetTrailer(primaryStream.Trailer())
			if !errors.Is(err, io.EOF) {
				return err
			}
			// A response without frames still has headers.
			return forwardHeader()
		}

		// The collector gets the frame first. It sees the frame also if the client is gone.
		// The collector and the client stream share the frame. Nothing may change it.
		collector.PushPrimary(&frame)

		if err := forwardHeader(); err != nil {
			return err
		}
		if err := serverStream.SendMsg(&frame); err != nil {
			return err
		}
	}
}
