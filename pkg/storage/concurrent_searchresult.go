// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"context"
	"strings"

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
	err    error
	warns  annotations.Annotations
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
		defer close(done)
		defer close(ch)
		for child.Next() {
			r := child.At()
			r.Value = strings.Clone(r.Value)
			select {
			case ch <- r:
			case <-ctx.Done():
				return
			}
		}
		c.err = child.Err()
		c.warns = child.Warnings()
	}()
	return c
}

// Next blocks until the next result is available, the producer exits, or
// ctx is cancelled. After Next returns false, Err and Warnings are stable
// (the producer's writes are synchronised through the channel close).
func (c *concurrentSearchResultSet) Next() bool {
	r, ok := <-c.ch
	if !ok {
		return false
	}
	c.cur = r
	return true
}

func (c *concurrentSearchResultSet) At() storage.SearchResult         { return c.cur }
func (c *concurrentSearchResultSet) Warnings() annotations.Annotations { return c.warns }
func (c *concurrentSearchResultSet) Err() error                        { return c.err }

// Close cancels the producer, drains the channel, and closes the child.
// Idempotent.
func (c *concurrentSearchResultSet) Close() error {
	c.cancel()
	<-c.done
	for range c.ch { //nolint:revive // intentional drain
	}
	return c.child.Close()
}
