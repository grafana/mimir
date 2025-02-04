// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package sync

import (
	"container/list"
	"context"
	"sync"
)

// DynamicSemaphore implements a semaphore whose capacity can be changed dynamically at
// run time, and can maintain a set of blocking waiters when the semaphore is full.
type DynamicSemaphore struct {
	mu      sync.Mutex
	size    int64
	cur     int64
	waiters list.List
}

// NewDynamicSemaphore returns a dynamic semaphore with the given initial capacity. Note
// that this is for convenience and to match golang.org/x/sync/semaphore however
// it's possible to use a zero-value semaphore provided SetSize is called before
// use.
func NewDynamicSemaphore(n int64) *DynamicSemaphore {
	return &DynamicSemaphore{size: n}
}

// SetSize dynamically updates the number of available slots. If there are more
// than n slots currently acquired, no further acquires will succeed until
// sufficient have been released to take the total outstanding below n again.
func (s *DynamicSemaphore) SetSize(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.size = n
}

// Acquire attempts to acquire one "slot" in the semaphore, blocking only until
// ctx is Done. On success, returns nil. On failure, returns ctx.Err() and leaves
// the semaphore unchanged.
//
// If ctx is already done, Acquire may still succeed without blocking.
func (s *DynamicSemaphore) Acquire(ctx context.Context) error {
	s.mu.Lock()
	if s.cur < s.size {
		s.cur++
		s.mu.Unlock()
		return nil
	}

	// Need to wait, Add to waiter list
	ready := make(chan struct{})
	elem := s.waiters.PushBack(ready)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		err := ctx.Err()
		s.mu.Lock()
		select {
		case <-ready:
			// Acquired the semaphore after we were canceled.  Rather than trying to
			// fix up the queue, just pretend we didn't notice the cancellation.
			err = nil
		default:
			s.waiters.Remove(elem)
		}
		s.mu.Unlock()
		return err

	case <-ready:
		return nil
	}
}

// TryAcquire attempts to acquire one "slot" in the semaphore without blocking.
// Returns true if successful, else false if no slots are immediately available.
func (s *DynamicSemaphore) TryAcquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cur < s.size {
		s.cur++
		return true
	}
	return false
}

// Release releases the semaphore. It will panic if release is called on an
// empty semphore.
func (s *DynamicSemaphore) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cur < 1 {
		panic("semaphore: bad release")
	}

	next := s.waiters.Front()

	// If there are no waiters, or if we recently resized and cur is too high, just decrement and we're done
	if next == nil || s.cur > s.size {
		s.cur--
		return
	}

	// Need to yield our slot to the next waiter.
	// Remove them from the list
	s.waiters.Remove(next)

	// And trigger it's chan before we release the lock
	close(next.Value.(chan struct{}))
	// Note we _don't_ decrement inflight since the slot was yielded directly.
}

func (s *DynamicSemaphore) IsFull() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur >= s.size
}

// Waiters returns how many callers are blocked waiting for the semaphore.
func (s *DynamicSemaphore) Waiters() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.waiters.Len()
}

func (s *DynamicSemaphore) Inflight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.cur)
}
