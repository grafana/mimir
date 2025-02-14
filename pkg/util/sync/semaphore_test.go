// SPDX-License-Identifier: AGPL-3.0-only

package sync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDynamicSemaphore_Acquire(t *testing.T) {
	t.Parallel()

	t.Run("should release permits to waiters", func(t *testing.T) {
		s := NewDynamicSemaphore(1)
		require.NoError(t, s.Acquire(context.Background()))
		require.Equal(t, 1, s.Used())

		go func() {
			_ = s.Acquire(context.Background())
		}()

		waitForWaiters(t, s, 1)
		require.Equal(t, 1, s.Waiters())
		s.Release()
		require.Equal(t, 1, s.Used())
		require.Equal(t, 0, s.Waiters())
	})

	t.Run("should unblock waiters when context completed", func(t *testing.T) {
		s := NewDynamicSemaphore(1)
		require.NoError(t, s.Acquire(context.Background()))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, s.Acquire(ctx), context.Canceled)
		require.Equal(t, 1, s.Used())
		require.Equal(t, 0, s.Waiters())
	})
}

func TestDynamicSemaphore_TryAcquire(t *testing.T) {
	tests := []struct {
		name     string
		size     int
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
			require.Equal(t, tc.expected, s.TryAcquire())
		})
	}
}

func TestDynamicSemaphore_SetSize(t *testing.T) {
	t.Parallel()

	t.Run("should wake waiter when setting larger size", func(t *testing.T) {
		s := NewDynamicSemaphore(1)
		require.NoError(t, s.Acquire(context.Background()))

		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			_ = s.Acquire(context.Background())
			wg.Done()
		}()
		go func() {
			_ = s.Acquire(context.Background())
			wg.Done()
		}()

		waitForWaiters(t, s, 2)

		// Increase size which should release waiters
		s.SetSize(3)
		assert.Equal(t, 0, s.Waiters())
		wg.Wait()
	})

	t.Run("should block acquires when setting smaller size", func(t *testing.T) {
		s := NewDynamicSemaphore(3)
		for i := 0; i < 3; i++ {
			require.NoError(t, s.Acquire(context.Background()))
		}

		s.SetSize(1)
		for i := 0; i < 3; i++ {
			s.Release()
		}

		require.NoError(t, s.Acquire(context.Background()))

		// Should timeout while acquiring permit
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		require.Error(t, s.Acquire(ctx))
		require.Equal(t, 1, s.Used())
		require.Equal(t, 0, s.Waiters())
	})
}

func TestDynamicSemaphore_IsFull(t *testing.T) {
	tests := []struct {
		name     string
		size     int
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
			require.Equal(t, tc.expected, s.IsFull())
		})
	}
}

func TestDynamicSemaphore_Waiters(t *testing.T) {
	s := NewDynamicSemaphore(1)
	require.NoError(t, s.Acquire(context.Background()))

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		_ = s.Acquire(context.Background())
		wg.Done()
	}()
	go func() {
		_ = s.Acquire(context.Background())
		wg.Done()
	}()

	waitForWaiters(t, s, 2)
	s.Release()
	require.Equal(t, 1, s.Waiters())
	s.Release()
	require.Equal(t, 0, s.Waiters())
	wg.Wait()
}

func waitForWaiters(t *testing.T, s *DynamicSemaphore, expected int) {
	require.Eventually(t, func() bool {
		return s.Waiters() == expected
	}, 100*time.Millisecond, 10*time.Millisecond)
}
