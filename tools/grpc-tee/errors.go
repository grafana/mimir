// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"errors"
	"fmt"
	"time"
)

// InboundCallRecvErr marks that the server stream failed to receive a request frame from the inbound caller.
type InboundCallRecvErr struct {
	cause error
}

func (e InboundCallRecvErr) Error() string {
	return fmt.Sprintf("failed to receive request frame from inbound caller: %v", e.cause)
}

func (e InboundCallRecvErr) Unwrap() error { return e.cause }

// Is matches any InboundCallRecvErr, regardless of `cause` value.
// Allow callers to use errors.Is on any InboundCallRecvErr value.
func (InboundCallRecvErr) Is(target error) bool {
	var e InboundCallRecvErr
	ok := errors.As(target, &e)
	return ok
}

// PrimaryBackendSendErr marks a failure to send a request frame to the primary backend.
type PrimaryBackendSendErr struct {
	cause error
}

func (e PrimaryBackendSendErr) Error() string {
	return fmt.Sprintf("failed to send request frame to primary backend: %v", e.cause)
}

func (e PrimaryBackendSendErr) Unwrap() error { return e.cause }

// SecondaryBackendSendErr marks a secondary call which failed to send a request frame to the backend.
type SecondaryBackendSendErr struct {
	cause error
}

func (e SecondaryBackendSendErr) Error() string {
	return fmt.Sprintf("failed to send request frame to secondary backend: %v", e.cause)
}

func (e SecondaryBackendSendErr) Unwrap() error { return e.cause }

// SecondaryBackendRecvErr marks a failure to send a request frame to the secondary backend.
type SecondaryBackendRecvErr struct {
	cause error
}

func (e SecondaryBackendRecvErr) Error() string {
	return fmt.Sprintf("failed to receive request frame from secondary backend: %v", e.cause)
}

func (e SecondaryBackendRecvErr) Unwrap() error { return e.cause }

// SecondaryReqBufferFullErr marks a failure by the secondary backend to keep up with request frames.
type SecondaryReqBufferFullErr struct {
	queueCap int
}

func (e SecondaryReqBufferFullErr) Error() string {
	return fmt.Sprintf("secondary request frame buffer is full at capacity: %d", e.queueCap)
}

// Is matches any SecondaryReqBufferFullErr, regardless of `timeout` value.
// Allow callers to use errors.Is on any SecondaryReqBufferFullErr value.
func (SecondaryReqBufferFullErr) Is(target error) bool {
	var e SecondaryReqBufferFullErr
	ok := errors.As(target, &e)
	return ok
}

// SecondaryDrainTimeoutErr marks a secondary call which did not finish
// within the secondary drain timeout after the primary call ended.
type SecondaryDrainTimeoutErr struct {
	timeout time.Duration
}

func (e SecondaryDrainTimeoutErr) Error() string {
	return fmt.Sprintf("secondary call did not finish within %s after the primary call", e.timeout)
}
