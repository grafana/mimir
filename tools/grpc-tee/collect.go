// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"context"
)

type ResponseFramePair struct {
	primary   Frame
	secondary Frame
}

type ResponseCollector struct {
	// recvPrimary     chan Frame
	// recvSecondary   chan Frame
	// bufferPrimary   list.List[Frame]
	// bufferSecondary list.List[Frame]
	// sendPrimary     chan Frame
	// sendSecondary   chan Frame
	recvPrimary       *stopQueue[Frame]
	recvSecondary     *stopQueue[Frame]
	internalPrimary   chan Frame
	internalSecondary chan Frame
	send              *stopQueue[ResponseFramePair]
}

func NewResponseCollector() *ResponseCollector {
	return &ResponseCollector{
		recvPrimary:       newStopQueue[Frame](),
		recvSecondary:     newStopQueue[Frame](),
		internalPrimary:   make(chan Frame, 1),
		internalSecondary: make(chan Frame, 1),
		// bufferPrimary:   list.List[Frame]{},
		// bufferSecondary: list.List[Frame]{},
		// sendPrimary:     nil,
		// sendSecondary:   nil,
		send: newStopQueue[ResponseFramePair](),
	}
}

// PushPrimary queues a frame from the primary backend.
// A nil frame is ignored because the collector uses nil to mean no frame.
// Callers must stop pushing before they cancel the context passed to RunCollect.
// Frames pushed after cancel are lost.
func (rc *ResponseCollector) PushPrimary(frame Frame) {
	if frame == nil {
		return
	}
	// An error means that RunCollect closed the queue. The frame is lost, as documented.
	_ = rc.recvPrimary.Enqueue(frame)
}

// PushSecondary queues a frame from the secondary backend.
// See PushPrimary for the rules on nil frames and on pushing after cancel.
func (rc *ResponseCollector) PushSecondary(frame Frame) {
	if frame == nil {
		return
	}
	_ = rc.recvSecondary.Enqueue(frame) // See PushPrimary for the error.
}

// PopSync blocks until a pair is available.
// It returns false after RunCollect has sent every pair and finished.
// Only one goroutine can call PopSync.
func (rc *ResponseCollector) PopSync() (ResponseFramePair, bool) {
	return rc.send.Dequeue()
}

func (rc *ResponseCollector) RunCollect(ctx context.Context) {
	// Launch goroutines to read from the recv queues and push to the internal chans.
	// Each goroutine closes its internal chan after its queue is closed and empty.
	go runRcv(rc.recvPrimary, rc.internalPrimary)
	go runRcv(rc.recvSecondary, rc.internalSecondary)

	var primaryFrame, secondaryFrame Frame

	// Loop reading from internal chans in pairs and push the pair to send buffer.
	// Continues until context is done/canceled.
loop:
	for {
		primaryCh, secondaryCh := rc.internalPrimary, rc.internalSecondary
		if primaryFrame != nil {
			// Stop select from reading another frame from internalPrimary
			// until a frame has arrived on internalSecondary.
			primaryCh = nil
		}
		if secondaryFrame != nil {
			secondaryCh = nil // Inverse of above.
		}

		select {
		case <-ctx.Done():
			break loop // Move on to drain stage.
		case primaryFrame = <-primaryCh:
		case secondaryFrame = <-secondaryCh:
		}
		if primaryFrame != nil && secondaryFrame != nil {
			// The send queue stays open until the Close call below, so Enqueue cannot fail.
			_ = rc.send.Enqueue(ResponseFramePair{
				primary:   primaryFrame,
				secondary: secondaryFrame,
			})
			primaryFrame, secondaryFrame = nil, nil
		}
	}

	// Context is done; start drain stage.
	// Collect everything into slices.
	var primaryRest, secondaryRest []Frame

	// Anything already held in primaryRest, secondaryRest is collected first
	if primaryFrame != nil {
		primaryRest = append(primaryRest, primaryFrame)
	}
	if secondaryFrame != nil {
		secondaryRest = append(secondaryRest, secondaryFrame)
	}

	// The runRcv funcs stop after they dequeue every frame that is already in the queues.
	rc.recvPrimary.Close()
	rc.recvSecondary.Close()

	// Collect into slices until both runRcv funcs have drained the recv queues,
	// pushed all items to internal chans, and closed the chans.
	primaryCh, secondaryCh := rc.internalPrimary, rc.internalSecondary
	for primaryCh != nil || secondaryCh != nil {
		select {
		case f, ok := <-primaryCh:
			if !ok {
				primaryCh = nil
				continue
			}
			primaryRest = append(primaryRest, f)
		case f, ok := <-secondaryCh:
			if !ok {
				secondaryCh = nil
				continue
			}
			secondaryRest = append(secondaryRest, f)
		}
	}

	// Send everything left in order. The short side gets nil frames.
	for i := range max(len(primaryRest), len(secondaryRest)) {
		var pair ResponseFramePair
		if i < len(primaryRest) {
			pair.primary = primaryRest[i]
		}
		if i < len(secondaryRest) {
			pair.secondary = secondaryRest[i]
		}
		_ = rc.send.Enqueue(pair)
	}

	rc.send.Close() // PopSync returns false after it reads every pair.
}

// runRcv transfers frames from a recv queue to an internal chan with length 1.
// It returns after the queue is closed and empty, and then closes out.
// Dequeue blocks when the queue is empty, so only one goroutine can read each queue.
func runRcv(in *stopQueue[Frame], out chan<- Frame) {
	defer close(out)
	for {
		frame, ok := in.Dequeue()
		if !ok {
			return
		}
		out <- frame
	}
}
