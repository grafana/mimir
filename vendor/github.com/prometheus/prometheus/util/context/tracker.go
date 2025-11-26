// Copyright 2025 Grafana Labs
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package context

import (
	"context"
	"sync"
)

// ErrContextsTrackerClosed is the reason to identify ContextsTracker has been explicitly closed by calling Close().
//
// This error is a struct instead of a globally generic error so that computed reference sizes are smaller.
type ErrContextsTrackerClosed struct{}

func (ErrContextsTrackerClosed) Error() string {
	return "contexts tracker is closed"
}

// ErrContextsTrackerCanceled is the reason to identify ContextsTracker has been automatically closed because
// all tracked contexts have been canceled.
//
// This error is a struct instead of a globally generic error so that computed reference sizes are smaller.
type ErrContextsTrackerCanceled struct{}

func (ErrContextsTrackerCanceled) Error() string {
	return "contexts tracker has been canceled"
}

// ContextsTracker is responsible to monitor multiple context.Context and provides an execution
// that gets canceled once all monitored context.Context instances are done.
//
// This type is concurrency safe.
type ContextsTracker struct {
	cancelExecCtx context.CancelFunc
	mx            sync.Mutex

	// Guarded by mx
	closedWithReason error         // Track whether the tracker is closed and why. The tracker is not closed if this is nil.
	trackedCount     int           // Number of tracked contexts.
	trackedStopFuncs []func() bool // The stop watching functions for all tracked contexts.
}

// NewContextsTracker returns a new ContextsTracker along with a context.Context that is done when all the tracked
// context.Context instances are done.
func NewContextsTracker() (*ContextsTracker, context.Context) {
	t := &ContextsTracker{}

	// Create a new execution context that will be canceled only once all tracked contexts have done.
	var execCtx context.Context
	execCtx, t.cancelExecCtx = context.WithCancel(context.Background())

	return t, execCtx
}

// Add the input ctx to the group of monitored context.Context.
// Returns false if the input context couldn't be added to the tracker because the tracker is already closed.
func (t *ContextsTracker) Add(ctx context.Context) error {
	t.mx.Lock()
	defer t.mx.Unlock()

	// Check if we've already done.
	if t.closedWithReason != nil {
		return t.closedWithReason
	}

	// Register a function that will be called once the tracked context has done.
	t.trackedCount++
	t.trackedStopFuncs = append(t.trackedStopFuncs, context.AfterFunc(ctx, t.onTrackedContextDone))

	return nil
}

// Close the tracker. When the tracker is closed, the execution context is canceled
// and resources released.
//
// This function must be called once done to not leak resources.
func (t *ContextsTracker) Close() {
	t.mx.Lock()
	defer t.mx.Unlock()

	t.unsafeClose(ErrContextsTrackerClosed{})
}

// unsafeClose must be called with the t.mx lock hold.
func (t *ContextsTracker) unsafeClose(reason error) {
	if t.closedWithReason != nil {
		return
	}

	t.cancelExecCtx()

	// Stop watching the tracked contexts. It's safe to call the stop function on a context
	// for which was already done.
	for _, fn := range t.trackedStopFuncs {
		fn()
	}

	t.trackedCount = 0
	t.trackedStopFuncs = nil
	t.closedWithReason = reason
}

func (t *ContextsTracker) onTrackedContextDone() {
	t.mx.Lock()
	defer t.mx.Unlock()

	t.trackedCount--

	// If this was the last context to be tracked, we can Close the tracker and cancel the execution context.
	if t.trackedCount == 0 {
		t.unsafeClose(ErrContextsTrackerCanceled{})
	}
}

func (t *ContextsTracker) TrackedContextsCount() int {
	t.mx.Lock()
	defer t.mx.Unlock()

	return t.trackedCount
}
