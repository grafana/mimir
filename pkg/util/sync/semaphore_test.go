// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package sync

// Based on https://github.com/golang/sync/blob/master/semaphore/semaphore_test.go

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const maxSleep = 1 * time.Millisecond

func HammerDynamic(sem *DynamicSemaphore, loops int) {
	for i := 0; i < loops; i++ {
		_ = sem.Acquire(context.Background())
		time.Sleep(time.Duration(rand.Int63n(int64(maxSleep/time.Nanosecond))) * time.Nanosecond)
		sem.Release()
	}
}

// TestDynamicSemaphore hammers the semaphore from all available cores to ensure we don't
// hit a panic or race detector notice something wonky.
func TestDynamicSemaphore(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()

	n := runtime.GOMAXPROCS(0)
	loops := 10000 / n
	sem := NewDynamicSemaphore(int64(n))
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			HammerDynamic(sem, loops)
		}()
	}
	wg.Wait()
}

func TestDynamicSemaphorePanic(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("release of an unacquired dynamic semaphore did not panic")
		}
	}()
	w := NewDynamicSemaphore(1)
	w.Release()
}

func checkAcquire(t *testing.T, sem *DynamicSemaphore, wantAcquire bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := sem.Acquire(ctx)
	if wantAcquire {
		require.NoErrorf(t, err, "failed to acquire when we should have")
	} else {
		require.Error(t, err, "failed to block when should be full")
	}
}

func TestDynamicSemaphore_Acquire(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sem := NewDynamicSemaphore(2)

	// Consume one slot [free: 1]
	_ = sem.Acquire(ctx)
	// Should be able to consume another [free: 0]
	checkAcquire(t, sem, true)
	// Should fail to consume another [free: 0]
	checkAcquire(t, sem, false)

	// Drop 2
	sem.Release()
	sem.Release()

	// Should be able to consume another [free: 1]
	checkAcquire(t, sem, true)
	// Should be able to consume another [free: 0]
	checkAcquire(t, sem, true)
	// Should fail to consume another [free: 0]
	checkAcquire(t, sem, false)

	// Now expand the semaphore and we should be able to acquire again [free: 2]
	sem.SetSize(4)

	// Should be able to consume another [free: 1]
	checkAcquire(t, sem, true)
	// Should be able to consume another [free: 0]
	checkAcquire(t, sem, true)
	// Should fail to consume another [free: 0]
	checkAcquire(t, sem, false)

	// Shrinking it should work [free: 0]
	sem.SetSize(3)

	// Should fail to consume another [free: 0]
	checkAcquire(t, sem, false)

	// Drop one [free: 0] (3 slots used are release, Size only 3)
	sem.Release()

	// Should fail to consume another [free: 0]
	checkAcquire(t, sem, false)

	sem.Release()

	// Should be able to consume another [free: 1]
	checkAcquire(t, sem, true)
}

func TestDynamicSemaphore_TryAcquire(t *testing.T) {
	tests := []struct {
		name     string
		size     int64
		acquires int
		expected bool
	}{
		{
			name:     "when empty",
			size:     2,
			acquires: 0,
			expected: true,
		},
		{
			name:     "when partially filled",
			size:     2,
			acquires: 1,
			expected: true,
		},
		{
			name:     "when full",
			size:     2,
			acquires: 2,
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewDynamicSemaphore(tc.size)
			for i := 0; i < tc.acquires; i++ {
				s.TryAcquire()
			}
			assert.Equal(t, tc.expected, s.TryAcquire())
		})
	}
}

func TestDynamicSemaphore_IsFull(t *testing.T) {
	tests := []struct {
		name     string
		size     int64
		acquires int
		expected bool
	}{
		{
			name:     "when empty",
			size:     2,
			acquires: 0,
			expected: false,
		},
		{
			name:     "when partially filled",
			size:     2,
			acquires: 1,
			expected: false,
		},
		{
			name:     "when full",
			size:     2,
			acquires: 2,
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewDynamicSemaphore(tc.size)
			for i := 0; i < tc.acquires; i++ {
				s.TryAcquire()
			}
			assert.Equal(t, tc.expected, s.IsFull())
		})
	}
}

func TestDynamicSemaphore_Waiters(t *testing.T) {
	overloadDuration := 100 * time.Millisecond
	s := NewDynamicSemaphore(1)
	err := s.Acquire(context.Background())
	require.NoError(t, err)

	// When
	go func() {
		_ = s.Acquire(context.Background())
	}()
	go func() {
		_ = s.Acquire(context.Background())
	}()

	time.Sleep(overloadDuration)
	assert.Equal(t, 2, s.Waiters())
	s.Release()
	assert.Equal(t, 1, s.Waiters())
	s.Release()
	assert.Equal(t, 0, s.Waiters())
}
