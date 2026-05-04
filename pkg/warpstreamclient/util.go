// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"sync"
	"sync/atomic"
)

// completion fans-in N independent results into a single done callback that
// fires at most once on whichever happens first: every expected report
// arriving, or ctx being canceled. On report fan-in the first error observed
// (or nil on full success) is delivered; on ctx cancellation ctx.Err() is
// delivered. Late reports after the first firing are no-ops.
//
// If ctx is already canceled when newCompletion is called, done fires
// asynchronously (via context.AfterFunc) shortly after, with ctx.Err() —
// callers must not assume newCompletion returns before done is invoked.
type completion struct {
	pendingResults atomic.Int32
	doneOnce       sync.Once
	done           func(error)

	// stopCtxWatch detaches the AfterFunc registered against ctx.
	stopCtxWatch func() bool

	// firstErr holds the first non-nil error reported, set via CompareAndSwap
	// so the first writer wins.
	firstErr atomic.Pointer[error]
}

func newCompletion(ctx context.Context, pending int32, done func(error)) *completion {
	c := &completion{done: done}
	c.pendingResults.Store(pending)
	c.stopCtxWatch = context.AfterFunc(ctx, func() {
		c.notifyDone(ctx.Err())
	})
	return c
}

// reportResult records the outcome of one result. After the Nth report, fires
// done with the first error observed across reports (or nil on full success).
func (c *completion) reportResult(err error) {
	if err != nil {
		c.firstErr.CompareAndSwap(nil, &err)
	}

	if c.pendingResults.Add(-1) == 0 {
		var firstErr error
		if p := c.firstErr.Load(); p != nil {
			firstErr = *p
		}
		c.notifyDone(firstErr)
	}
}

// notifyDone delivers err to done at most once and detaches the ctx watcher.
func (c *completion) notifyDone(err error) {
	c.stopCtxWatch()
	c.doneOnce.Do(func() { c.done(err) })
}
