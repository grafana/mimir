// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"errors"
	"runtime"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"
)

var errQueueClosed = errors.New("queue is closed")

type stopItem[T any] struct {
	v    T
	stop bool
}

// stopQueue is an unbounded queue with many producers and one consumer.
// It adds Close to xsync.UMPSCQueue.
type stopQueue[T any] struct {
	q            *xsync.UMPSCQueue[stopItem[T]]
	closeEnqueue atomic.Bool // Set by Close. Enqueue returns errQueueClosed after it.
	closeDequeue atomic.Bool // Set by Dequeue when it reads the stop item.
	inflight     atomic.Int64 // The number of Enqueue calls that are running.
}

func newStopQueue[T any]() *stopQueue[T] {
	return &stopQueue[T]{q: xsync.NewUMPSCQueue[stopItem[T]]()}
}

// Enqueue adds v to the queue. It returns errQueueClosed after Close.
// If Enqueue returns nil, Dequeue returns v before it returns false.
// This holds also when Enqueue runs at the same time as Close.
func (s *stopQueue[T]) Enqueue(v T) error {
	// Raise the counter before the flag read. Close waits for the counter to reach zero,
	// so an Enqueue that sees the flag as false adds v before Close adds the stop item.
	s.inflight.Add(1)
	defer s.inflight.Add(-1)
	if s.closeEnqueue.Load() {
		return errQueueClosed
	}
	s.q.Enqueue(stopItem[T]{v: v})
	return nil
}

// Close ends the queue. Dequeue returns every item added before Close, then returns false.
// A second call to Close does nothing.
// Close waits for the Enqueue calls that are running, so it blocks for a short time.
func (s *stopQueue[T]) Close() {
	if s.closeEnqueue.Swap(true) {
		return
	}
	for s.inflight.Load() != 0 {
		runtime.Gosched()
	}
	s.q.Enqueue(stopItem[T]{stop: true})
}

// Dequeue waits for an item. Only one goroutine can call it.
// It returns the zero value and false after it reads the stop item,
// and on every later call without waiting.
func (s *stopQueue[T]) Dequeue() (T, bool) {
	var zero T
	if s.closeDequeue.Load() {
		return zero, false
	}
	it := s.q.Dequeue()
	if it.stop {
		s.closeDequeue.Store(true)
		return zero, false
	}
	return it.v, true
}
