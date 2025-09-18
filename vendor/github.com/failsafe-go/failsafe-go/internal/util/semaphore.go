package util

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

// ErrWaitExceeded is returned when maxWaitTime is exceeded while waiting on a permit.
var ErrWaitExceeded = errors.New("wait time exceeded")

// DynamicSemaphore is a semaphore that can be dynamically resized and allows FIFO blocking when full.
//
// This type is concurrency safe.
type DynamicSemaphore struct {
	mu sync.Mutex

	// Guarded by mu
	size    int
	used    int
	waiters list.List
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

	if ctx == nil {
		ctx = context.Background()
	}

	// Create a waiter for a permit
	waiter := make(chan struct{})
	waiterElem := s.waiters.PushBack(waiter)
	s.mu.Unlock()

	select {
	case <-waiter:
		return nil
	case <-ctx.Done():
		return s.drainWaiter(waiter, waiterElem, ctx.Err())
	}
}

// AcquireWithMaxWait acquires a permit from the sempahore, blocking until one is made available via Release, the ctx is Done, or
// the maxWaitTime is hit. Blocking callers are unblocked in FIFO order as permits are released.
func (s *DynamicSemaphore) AcquireWithMaxWait(ctx context.Context, maxWaitTime time.Duration) error {
	s.mu.Lock()

	// See if a permit is immediately available
	if s.used < s.size {
		s.used++
		s.mu.Unlock()
		return nil
	}

	// If semaphore is full and no wait is possible, return immediately
	if maxWaitTime == 0 {
		s.mu.Unlock()
		return ErrWaitExceeded
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// Create a waiter for a permit
	waiter := make(chan struct{})
	waiterElem := s.waiters.PushBack(waiter)
	timer := time.NewTimer(maxWaitTime)
	defer timer.Stop()
	s.mu.Unlock()

	select {
	case <-waiter:
		return nil
	case <-ctx.Done():
		return s.drainWaiter(waiter, waiterElem, ctx.Err())
	case <-timer.C:
		return s.drainWaiter(waiter, waiterElem, ErrWaitExceeded)
	}
}

// drainWaiter drains a waiter when an Acquire attempt times out. Returns nil if the waiter is ready, else the err.
func (s *DynamicSemaphore) drainWaiter(waiter chan struct{}, waiterElem *list.Element, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-waiter:
		// Ignore err if a waiter is ready
		return nil
	default:
		// Remove waiter if done prematurely
		s.waiters.Remove(waiterElem)
		return err
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
