// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"context"
	"strings"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// concurrentSearchResultSet wraps a child storage.SearchResultSet in a
// goroutine that pre-fetches into a bounded buffered channel. Used by the
// store-gateway to fan out per-block searches before merging via Prometheus's
// pairwiseMergeSearchSets, which is otherwise pull-based and serial.
//
// Value strings are cloned at the channel-send boundary so that values
// returned by Next/At do not alias the request's pooled buffer (cross-tenant
// data leakage guard; see CLAUDE.md "Unsafe memory tricks").
type concurrentSearchResultSet struct {
	child  storage.SearchResultSet
	cancel context.CancelFunc
	ch     <-chan storage.SearchResult
	cur    storage.SearchResult
	done   <-chan struct{}
	err    error // populated by the producer; read after producer exits.
	warns  annotations.Annotations
}

// NewConcurrentSearchResultSet starts a producer goroutine that pre-fetches
// results from child into a buffered channel of the given size. Cancelling
// ctx (or calling Close) terminates the producer.
func NewConcurrentSearchResultSet(ctx context.Context, child storage.SearchResultSet, bufSize int) *concurrentSearchResultSet {
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

// Next advances the iterator. It returns false when the producer has emitted
// all results or has been cancelled.
func (c *concurrentSearchResultSet) Next() bool {
	r, ok := <-c.ch
	if !ok {
		return false
	}
	c.cur = r
	return true
}

// At returns the current result. Only valid after Next returned true.
func (c *concurrentSearchResultSet) At() storage.SearchResult { return c.cur }

// Warnings returns warnings accumulated by the child iterator.
func (c *concurrentSearchResultSet) Warnings() annotations.Annotations { return c.warns }

// Err returns the producer's terminal error.
func (c *concurrentSearchResultSet) Err() error { return c.err }

// Close cancels the producer goroutine, drains the channel so the producer
// exits cleanly, and closes the child. Safe to call multiple times.
func (c *concurrentSearchResultSet) Close() error {
	c.cancel()
	<-c.done
	for range c.ch {
	}
	return c.child.Close()
}
