// SPDX-License-Identifier: AGPL-3.0-only

package sync

import (
	"container/list"
	"context"
	"sync"
)

// DynamicSemaphore is a semaphore that can be dynamically resized and allows FIFO blocking when full.
type DynamicSemaphore struct {
	mu      sync.Mutex
	size    int       // Guarded by mu
	used    int       // Guarded by mu
	waiters list.List // Guarded by mu
}

func NewDynamicSemaphore(size int) *DynamicSemaphore {
	return &DynamicSemaphore{size: size}
}

// Acquire acquires a permit from the sempahore, blocking until one is made available via Release or the ctx is Done.
// Blocking callers are unblocked in FIFO order as permits are released.
func (s *DynamicSemaphore) Acquire(ctx context.Context) error {
	s.mu.Lock()

	// See if a permit is immediately available
	if s.used < s.size {
		s.used++
		s.mu.Unlock()
		return nil
	}

	// Create a waiter for a permit
	waiter := make(chan struct{})
	waiterElem := s.waiters.PushBack(waiter)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		s.mu.Lock()
		defer s.mu.Unlock()
		// Remove waiter if it wasn't fulfilled
		select {
		case <-waiter:
			return nil
		default:
			s.waiters.Remove(waiterElem)
			return ctx.Err()
		}

	case <-waiter:
		return nil
	}
}

// TryAcquire acquires a permit if one is available, returning true, else returns false if no permits are available.
func (s *DynamicSemaphore) TryAcquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.used < s.size {
		s.used++
		return true
	}
	return false
}

// Release releases a permit which unblocks a waiter if one exists.
// Panics if called without a corresponding call to Acquire or TryAcqyure.
func (s *DynamicSemaphore) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check for invalid state
	if s.used <= 0 {
		panic("DynamicSemaphore: Release called without matching Acquire")
	}

	s.used--

	// If we have waiters and capacity, wake up the next waiter
	if s.used < s.size && s.waiters.Len() > 0 {
		s.wakeAndAcquire(s.waiters.Front())
	}
}

// SetSize resizes the semaphore. If the size is increased, waiters will be unblocked as needed.
func (s *DynamicSemaphore) SetSize(size int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If capacity increased, wake up waiters that can now acquire
	if size > s.size {
		for s.used < size && s.waiters.Len() > 0 {
			s.wakeAndAcquire(s.waiters.Front())
		}
	}

	s.size = size
}

// Wakes a blocked waiter and acquires a permit.
// Requires s.mu to be locked before calling.
func (s *DynamicSemaphore) wakeAndAcquire(waiterElem *list.Element) {
	s.used++
	s.waiters.Remove(waiterElem)
	close(waiterElem.Value.(chan struct{}))
}

func (s *DynamicSemaphore) IsFull() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used >= s.size
}

// Waiters returns how many callers are blocked waiting for permits.
func (s *DynamicSemaphore) Waiters() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.waiters.Len()
}

// Used returns how many permits are currently in use.
func (s *DynamicSemaphore) Used() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}
