// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"context"

	"github.com/puzpuzpuz/xsync/v4"
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
	recvPrimary       *xsync.UMPSCQueue[Frame]
	recvSecondary     *xsync.UMPSCQueue[Frame]
	internalPrimary   chan Frame
	internalSecondary chan Frame
	send              *xsync.UMPSCQueue[ResponseFramePair]
}

func NewResponseCollector() *ResponseCollector {
	return &ResponseCollector{
		recvPrimary:       xsync.NewUMPSCQueue[Frame](),
		recvSecondary:     xsync.NewUMPSCQueue[Frame](),
		internalPrimary:   make(chan Frame, 1),
		internalSecondary: make(chan Frame, 1),
		// bufferPrimary:   list.List[Frame]{},
		// bufferSecondary: list.List[Frame]{},
		// sendPrimary:     nil,
		// sendSecondary:   nil,
		send: xsync.NewUMPSCQueue[ResponseFramePair](),
	}
}

// PushPrimary queues a frame from the primary backend.
// A nil frame is ignored because the collector uses nil as its stop signal.
// Callers must stop pushing before they cancel the context passed to RunCollect.
// Frames pushed after cancel are lost.
func (rc *ResponseCollector) PushPrimary(frame Frame) {
	if frame == nil {
		return
	}
	rc.recvPrimary.Enqueue(frame)
}

// PushSecondary queues a frame from the secondary backend.
// See PushPrimary for the rules on nil frames and on pushing after cancel.
func (rc *ResponseCollector) PushSecondary(frame Frame) {
	if frame == nil {
		return
	}
	rc.recvSecondary.Enqueue(frame)
}

// PopSync blocks until a pair is available.
// It returns false after RunCollect has sent every pair and finished.
// Only one goroutine can call PopSync.
func (rc *ResponseCollector) PopSync() (ResponseFramePair, bool) {
	pair := rc.send.Dequeue()
	if pair.primary == nil && pair.secondary == nil {
		// A real pair always holds a frame, so an empty pair is the end marker.
		rc.send.Enqueue(pair) // Keep the marker so later calls also return false.
		return ResponseFramePair{}, false
	}
	return pair, true
}

func (rc *ResponseCollector) RunCollect(ctx context.Context) error {
	// Launch goroutines to read from the recv queues and push to the internal chans.
	// Each goroutine closes its internal chan after it reads the stop signal.
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
		case primaryFrame = <-primaryCh:
		case secondaryFrame = <-secondaryCh:
		case <-ctx.Done():
			break loop
		}
		if primaryFrame != nil && secondaryFrame != nil {
			rc.send.Enqueue(ResponseFramePair{
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

	// The runRcv funcs stop when they dequeue nil values
	rc.recvPrimary.Enqueue(nil)
	rc.recvSecondary.Enqueue(nil)

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
		rc.send.Enqueue(pair)
	}

	rc.send.Enqueue(ResponseFramePair{}) // End marker for PopSync.
	return ctx.Err()
}

// runRcv transfers frames from a recv queue to an internal chan with length 1.
// It returns after it reads a nil frame, which is the stop signal, and closes out.
// Dequeue blocks when the queue is empty, so only one goroutine can read each queue.
func runRcv(in *xsync.UMPSCQueue[Frame], out chan<- Frame) {
	defer close(out)
	for {
		frame := in.Dequeue()
		if frame == nil {
			return
		}
		out <- frame
	}
}
