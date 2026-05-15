// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"context"
	"strings"
	"sync"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// concurrentSearchResultSet pre-fetches a child SearchResultSet into a
// bounded buffered channel, so a pull-based k-way merger doesn't serialise
// on whichever source it pulls from next.
//
// Value strings are cloned at the channel-send boundary to defuse any
// request-pool buffer aliasing the child might use internally (Mimir's
// unsafeMutableString cross-tenant guard; see CLAUDE.md).
type concurrentSearchResultSet struct {
	child  storage.SearchResultSet
	cancel context.CancelFunc
	ch     <-chan storage.SearchResult
	cur    storage.SearchResult
	done   <-chan struct{}

	// mu guards err and warns. Consumers (notably the merge primitive's
	// per-Next Err()/Warnings() checks) may read at any time during
	// iteration; the producer writes once when it exits. The channel
	// close alone is not a sufficient barrier because readers that
	// haven't observed it (mid-iteration Err()) would race the writer.
	mu    sync.Mutex
	err   error
	warns annotations.Annotations

	closeOnce sync.Once
	closeErr  error
}

// NewConcurrentSearchResultSet starts a producer goroutine. Cancelling ctx
// or calling Close terminates the producer. bufSize clamps to a minimum
// of 1.
func NewConcurrentSearchResultSet(ctx context.Context, child storage.SearchResultSet, bufSize int) storage.SearchResultSet {
	if bufSize <= 0 {
		bufSize = 1
	}
	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan storage.SearchResult, bufSize)
	done := make(chan struct{})
	c := &concurrentSearchResultSet{child: child, cancel: cancel, ch: ch, done: done}

	go func() {
		// Defer order matters. LIFO: capture runs first (writing c.err and
		// c.warns), then close(ch) acts as the memory barrier for the
		// consumer's post-Next reads, then close(done) signals Close.
		// Capturing inside a defer rather than after the loop ensures we
		// also surface the child's terminal state when the producer exits
		// via ctx.Done() rather than a clean child.Next() == false.
		defer close(done)
		defer close(ch)
		defer func() {
			err := child.Err()
			warns := child.Warnings()
			c.mu.Lock()
			c.err = err
			c.warns = warns
			c.mu.Unlock()
		}()
		for child.Next() {
			r := child.At()
			r.Value = strings.Clone(r.Value)
			select {
			case ch <- r:
			case <-ctx.Done():
				return
			}
		}
	}()
	return c
}

// Next blocks until the next result is available, the producer exits, or
// ctx is cancelled. Err and Warnings are safe to call at any time — they
// return the producer's zero values while it is still running and the
// terminal values once it has exited (synchronised by c.mu).
func (c *concurrentSearchResultSet) Next() bool {
	r, ok := <-c.ch
	if !ok {
		return false
	}
	c.cur = r
	return true
}

func (c *concurrentSearchResultSet) At() storage.SearchResult { return c.cur }

func (c *concurrentSearchResultSet) Warnings() annotations.Annotations {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.warns
}

func (c *concurrentSearchResultSet) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Close cancels the producer and closes the child before waiting for the
// producer to exit — required because ctx cancellation alone does not
// unblock a child Recv that doesn't observe our derived context.
// Idempotent.
func (c *concurrentSearchResultSet) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.closeErr = c.child.Close()
		<-c.done
		for range c.ch { //nolint:revive // intentional drain
		}
	})
	return c.closeErr
}
