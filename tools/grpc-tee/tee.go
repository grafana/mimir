// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Tee struct {
	cfg              TeeConfig
	backendType      *BackendType
	primaryBackend   Backend
	secondaryBackend Backend
	logger           log.Logger
	// compare receives every pair of response frames. Tests replace it.
	compare func(logger log.Logger, fullMethod string, pair ResponseFramePair)

	// stopCtx ends when Stop is called. It cancels the secondary calls that still run.
	stopCtx context.Context
	stop    context.CancelFunc
	// wg tracks the goroutines that finish a call after Handle has returned.
	wg sync.WaitGroup
}

func NewTee(cfg TeeConfig, backendType *BackendType, logger log.Logger) *Tee {
	stopCtx, stop := context.WithCancel(context.Background())
	return &Tee{
		cfg:         cfg,
		backendType: backendType,
		logger:      logger,
		compare:     comparePair,
		stopCtx:     stopCtx,
		stop:        stop,
	}
}

// SetBackends sets the backends. Call it before the gRPC server starts.
func (t *Tee) SetBackends(primary, secondary Backend) {
	t.primaryBackend = primary
	t.secondaryBackend = secondary
}

// Stop cancels the secondary calls that still run, and waits for the goroutines of finished calls.
// Call it after the gRPC server has stopped, so no new call can start.
func (t *Tee) Stop() {
	t.stop()
	t.wg.Wait()
}

// Handle forwards the inbound call to the primary backend and returns the primary response.
// It copies the request frames to the secondary backend.
// It pushes the response frames of both backends into one ResponseCollector.
// The secondary backend can neither delay nor cancel the primary call.
// The primary call does not cancel the secondary call.
// Handle returns when the primary call is finished. The secondary call can finish later.
func (t *Tee) Handle(_ any, serverStream grpc.ServerStream) error {
	fullMethod, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return status.Errorf(codes.Internal, "ServerTransportStream not found in context")
	}
	if t.primaryBackend == nil || t.secondaryBackend == nil {
		return status.Errorf(codes.FailedPrecondition, "tee backends are not configured")
	}
	logger := log.With(t.logger, "method", fullMethod)

	primaryStream, primaryCancel, err := openPrimaryBackendStream(
		serverStream, fullMethod, nil, t.primaryBackend,
	)
	if err != nil {
		// The error of NewStream is a gRPC status error, so the client gets the same code.
		return err
	}
	defer primaryCancel(callFinishedError{})

	secondary := &xsecondaryCall{cancel: func(error) {}}
	secondaryStream, secondaryCancel, err := openSecondaryBackendStream(
		serverStream, fullMethod, nil, t.secondaryBackend,
	)
	if err != nil {
		// The secondary backend never fails the inbound call.
		msg := "failed to open secondary backend stream; continue with primary backend only"
		level.Warn(logger).Log("msg", msg, "err", err)
		//} else {
		//	secondary = &secondaryCall{
		//		stream: secondaryStream,
		//		cancel: secondaryCancel,
		//		queue:  make(chan Frame, max(t.cfg.SecondaryRequestBufferSize, 1)),
		//	}
	}

	err = t.Tee(
		serverStream, primaryStream, primaryCancel, secondaryStream, secondaryCancel,
	)

	// The collector context does not depend on the inbound call.
	// The collector must see every frame, also after the client canceled.
	collector := NewResponseCollector()
	collectCtx, stopCollect := context.WithCancel(context.Background())

	t.wg.Add(2)
	go func() {
		defer t.wg.Done()
		collector.RunCollect(collectCtx)
	}()
	compareDone := make(chan struct{})
	go func() {
		defer t.wg.Done()
		defer close(compareDone)
		for {
			pair, ok := collector.PopSync()
			if !ok {
				return
			}
			t.compare(logger, fullMethod, pair)
		}
	}()

	var secondaryWG sync.WaitGroup
	if secondary.stream != nil {
		secondaryWG.Add(2)
		go func() {
			defer secondaryWG.Done()
			secondary.sendRequests(logger)
		}()
		go func() {
			defer secondaryWG.Done()
			secondary.receiveResponses(logger, collector)
		}()
	}
	secondaryDone := make(chan struct{})
	go func() {
		secondaryWG.Wait()
		close(secondaryDone)
	}()

	// The pump ends when the inbound call ends. Handle does not wait for it.
	go t.pumpRequests(logger, fullMethod, serverStream, primaryStream, secondary)

	err = forwardPrimaryResponses(serverStream, primaryStream, collector)

	// The primary call is finished, so nothing pushes primary frames anymore.
	// Give the secondary call a limited time to finish, then stop the collector.
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

		timer := time.NewTimer(t.cfg.SecondaryDrainTimeout)
		defer timer.Stop()
		select {
		case <-secondaryDone:
		case <-timer.C:
			level.Warn(logger).Log("msg", "the secondary call did not finish in time, canceling it", "timeout", t.cfg.SecondaryDrainTimeout)
			secondary.cancel(secondaryDrainTimeoutError{timeout: t.cfg.SecondaryDrainTimeout})
		case <-t.stopCtx.Done():
			secondary.cancel(teeStoppedError{})
		}
		// Both secondary goroutines return after the cancel.
		<-secondaryDone
		// This releases the context of the secondary call. It has no effect on a canceled call.
		secondary.cancel(callFinishedError{})

		// Both receivers are done, so nothing pushes frames anymore.
		stopCollect()
		<-compareDone
	}()

	return err
}

func (t *Tee) Tee(
	serverStream grpc.ServerStream,
	primaryStream grpc.ClientStream,
	primaryCancel context.CancelCauseFunc,
	secondaryStream grpc.ClientStream,
	secondaryCancel context.CancelCauseFunc,
) error {
	// Start one context to handle the full lifecycle of teeing a call,
	// independent of the client or server gRPC stream contexts.
	// callTeeCancel
	callTeeCtx, _ := context.WithCancelCause(context.Background())

	// The zero-value errgroup does not stop when one of the goroutines errors.
	// The individual goroutine runs are handled with their cancel and close logic.
	callTeeGrp := errgroup.Group{}

	resCollector := NewResponseCollector()
	// Run response collector with the callTeeCtx independent of gRPC stream contexts.
	callTeeGrp.Go(func() error {
		resCollector.RunCollect(callTeeCtx)
		return nil
	})

	// Start secondary call loop to forward pushed request frames to secondary backend
	// and forward response frames from the secondary backend to the collector.
	secondaryCall := NewSecondaryCall(
		secondaryStream, secondaryCancel, resCollector, t.cfg,
	)
	// This run only stops early if secondary context is canceled or times out.
	callTeeGrp.Go(func() error {
		// TODO capture and log error;
		// the errgroup Wait only returns the first error encountered
		// we do not want issues with the secondary to hide errors with primary.
		_ = secondaryCall.Run()
		return nil
	})

	// Read request frames from the inbound call in parallel with the response loops.
	callTeeGrp.Go(func() error {
		return forwardRequests(serverStream, primaryStream, secondaryCall)
	})

	return nil
}

// forwardRequests reads request frames from the inbound call
// and tees them to the primary stream and to the secondary call.
// It returns nil after the last request frame, or the error of the inbound call.
// It is safe to run in its own goroutine.
func forwardRequests(
	serverStream grpc.ServerStream,
	primaryStream grpc.ClientStream,
	secondaryCall *SecondaryCall,
) error {
	// Nothing pushes request frames after this function returns.
	// Signal SecondaryCall to CloseSend on the gRPC stream
	// after forwarding all enqueued request frames.
	defer secondaryCall.FinishAndCloseSend()

	secondaryOK := true
	for {
		var frame Frame // Allocate fresh on each loop for now to minimize memory management.
		if err := serverStream.RecvMsg(&frame); err != nil {
			if errors.Is(err, io.EOF) {
				// The inbound call sent its last request frame.
				// The primary stream is a client stream, so it needs an explicit CloseSend.
				if err := primaryStream.CloseSend(); err != nil {
					return PrimaryBackendSendErr{cause: err}
				}
				return nil // Done
			}
			// Inbound call failed; cancel the secondary call loop.
			recvErr := InboundCallRecvErr{cause: err}
			secondaryCall.CancelStream(recvErr)
			return recvErr
		}

		// Request frame modification is not supported at this point,
		// so the primary and the secondary could share one frame.
		// Copy the frame for the secondary anyway so it never shares memory with the primary stream.
		secondaryFrame := slices.Clone(frame)

		if err := primaryStream.SendMsg(&frame); err != nil {
			// Outbound send to primary failed; cancel the secondary call loop.
			secondaryCall.CancelStream(PrimaryBackendSendErr{cause: err})
		}

		// PushReqFrame does not block - it only enqueues the frame to an internal buffer.
		// It only fails if the secondary backend could not keep with reading request frames
		// and the SecondaryCall's request frame buffer filled up.
		// In that case, cancel the secondary call and continue on only with the primary.
		if secondaryOK {
			if err := secondaryCall.PushReqFrame(&secondaryFrame); err != nil {
				secondaryOK = false
				secondaryCall.CancelStream(err)
			}
		}
	}
}

var backendClientStreamDesc = &grpc.StreamDesc{
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
	primaryCtx = metadata.NewOutgoingContext(primaryCtx, md)
	primaryBackendStream, err := openBackendStream(
		primaryCtx, primaryBackend.Conn(), fullMethod,
	)
	if err != nil {
		primaryCancel(err)
		return nil, nil, err
	}
	return primaryBackendStream, primaryCancel, nil
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
	secondaryCtx = metadata.NewOutgoingContext(secondaryCtx, md)

	secondaryBackendStream, err := openBackendStream(
		secondaryCtx, secondaryBackend.Conn(), fullMethod,
	)
	if err != nil {
		secondaryCancel(err)
		return nil, nil, err
	}
	return secondaryBackendStream, secondaryCancel, nil
}

func openBackendStream(
	ctx context.Context,
	conn grpc.ClientConnInterface,
	fullMethod string,
) (grpc.ClientStream, error) {
	// The backend streams must use the frame codec, so SendMsg and RecvMsg handle *Frame as raw bytes.
	return conn.NewStream(ctx, backendClientStreamDesc, fullMethod, grpc.ForceCodec(FrameCodec{}))
}
