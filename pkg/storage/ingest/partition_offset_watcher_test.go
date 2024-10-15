// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestPartitionOffsetWatcher(t *testing.T) {
	const shortSleep = 100 * time.Millisecond

	t.Run("should support a single goroutine waiting for an offset", func(t *testing.T) {
		t.Parallel()

		var (
			ctx       = context.Background()
			w         = newPartitionOffsetWatcher()
			firstDone = atomic.NewBool(false)
		)

		go func() {
			assert.NoError(t, w.Wait(ctx, 2))
			firstDone.Store(true)
		}()

		// Wait a short time and then make sure the wait() hasn't returned yet.
		time.Sleep(shortSleep)
		assert.False(t, firstDone.Load())

		// Notify an offset lower than the expected one. At this point the wait() shouldn't return yet.
		w.Notify(1)

		time.Sleep(shortSleep)
		assert.False(t, firstDone.Load())

		// Notify the expected offset. At this point wait() should return.
		w.Notify(2)
		assert.Eventually(t, firstDone.Load, shortSleep, shortSleep/10)

		assert.Equal(t, 0, w.watchGroupsCount())
	})

	t.Run("should support two goroutines waiting for the same offset", func(t *testing.T) {
		t.Parallel()

		var (
			ctx        = context.Background()
			w          = newPartitionOffsetWatcher()
			firstDone  = atomic.NewBool(false)
			secondDone = atomic.NewBool(false)
		)

		go func() {
			assert.NoError(t, w.Wait(ctx, 2))
			firstDone.Store(true)
		}()

		go func() {
			assert.NoError(t, w.Wait(ctx, 2))
			secondDone.Store(true)
		}()

		// Wait a short time and then make sure the wait() hasn't returned yet.
		time.Sleep(shortSleep)
		assert.False(t, firstDone.Load())
		assert.False(t, secondDone.Load())

		// Notify an offset lower than the expected one. At this point the wait() shouldn't return yet.
		w.Notify(1)

		time.Sleep(shortSleep)
		assert.False(t, firstDone.Load())
		assert.False(t, secondDone.Load())

		// Notify the expected offset. At this point wait() should return.
		w.Notify(2)
		assert.Eventually(t, firstDone.Load, shortSleep, shortSleep/10)
		assert.Eventually(t, secondDone.Load, shortSleep, shortSleep/10)

		assert.Equal(t, 0, w.watchGroupsCount())
	})

	t.Run("should support two goroutines waiting for a different offset", func(t *testing.T) {
		t.Parallel()

		var (
			ctx        = context.Background()
			w          = newPartitionOffsetWatcher()
			firstDone  = atomic.NewBool(false)
			secondDone = atomic.NewBool(false)
		)

		go func() {
			assert.NoError(t, w.Wait(ctx, 1))
			firstDone.Store(true)
		}()

		go func() {
			assert.NoError(t, w.Wait(ctx, 2))
			secondDone.Store(true)
		}()

		// Wait a short time and then make sure the wait() hasn't returned yet.
		time.Sleep(shortSleep)
		assert.False(t, firstDone.Load())
		assert.False(t, secondDone.Load())

		// Notify the offset expected by the 1st goroutine.
		w.Notify(1)
		assert.Eventually(t, firstDone.Load, shortSleep, shortSleep/10)

		time.Sleep(shortSleep)
		assert.False(t, secondDone.Load())

		// Notify the offset expected by the 2nd goroutine.
		w.Notify(2)
		assert.Eventually(t, secondDone.Load, shortSleep, shortSleep/10)

		assert.Equal(t, 0, w.watchGroupsCount())
	})

	t.Run("Wait() should immediately return if the input offset has already been consumed", func(t *testing.T) {
		t.Parallel()

		var (
			ctx = context.Background()
			w   = newPartitionOffsetWatcher()
		)

		w.Notify(10)
		require.NoError(t, w.Wait(ctx, 7))

		assert.Equal(t, 0, w.watchGroupsCount())
	})

	t.Run("Wait() should immediately return if the input offset is -1, even if no consumed offset has been notified yet", func(t *testing.T) {
		t.Parallel()

		var (
			ctx = context.Background()
			w   = newPartitionOffsetWatcher()
		)

		require.NoError(t, w.Wait(ctx, -1))

		assert.Equal(t, 0, w.watchGroupsCount())
	})

	t.Run("Wait() should return as soon as the watcher service is stopped", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		w := newPartitionOffsetWatcher()
		require.NoError(t, services.StartAndAwaitRunning(ctx, w))

		wg := sync.WaitGroup{}

		runAsync(&wg, func() {
			assert.Equal(t, errPartitionOffsetWatcherStopped, w.Wait(ctx, 1))
		})

		runAsync(&wg, func() {
			assert.Equal(t, errPartitionOffsetWatcherStopped, w.Wait(ctx, 2))
		})

		require.NoError(t, services.StopAndAwaitTerminated(ctx, w))

		// Wait until goroutines have done.
		wg.Wait()

		// A call to Wait() on a stopped service should immediately return.
		assert.Equal(t, errPartitionOffsetWatcherStopped, w.Wait(ctx, 3))

		// A call to Notify() on a stopped service should be a no-op.
		w.Notify(1)
	})
}

func TestPartitionOffsetWatcher_Concurrency(t *testing.T) {
	const (
		numTotalNotifications  = 10000
		numNotifyingGoroutines = 10
		numWatchingGoroutines  = 1000
	)

	var (
		ctx, cancelCtx     = context.WithCancel(context.Background())
		w                  = newPartitionOffsetWatcher()
		lastNotifiedOffset = atomic.NewInt64(0)
		lastWatchedOffset  = atomic.NewInt64(0)
		notificationsCount = atomic.NewInt64(0)
	)

	// Ensure the context will be canceled even if the test fail earlier.
	t.Cleanup(cancelCtx)

	wg := sync.WaitGroup{}

	// Start all watching goroutines.
	for i := 0; i < numWatchingGoroutines; i++ {
		runAsync(&wg, func() {
			for ctx.Err() == nil {
				if err := w.Wait(ctx, lastWatchedOffset.Inc()); err != nil && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
		})
	}

	// Start all notifying goroutines.
	for i := 0; i < numNotifyingGoroutines; i++ {
		runAsync(&wg, func() {
			for ctx.Err() == nil {
				// Notify an offset between the last notified offset and the last watched offset.
				// The update to lastNotifiedOffset by multiple goroutines suffer a race condition
				// but it's fine for the purpose of this test.
				maxOffsetRange := lastWatchedOffset.Load() - lastNotifiedOffset.Load()
				if maxOffsetRange <= 0 {
					continue
				}

				offset := lastNotifiedOffset.Load() + rand.Int63n(maxOffsetRange)
				lastNotifiedOffset.Store(offset)

				w.Notify(offset)

				// Cancel the test execution once the required number of notifications have been sent.
				if notificationsCount.Inc() == int64(numTotalNotifications) {
					cancelCtx()
				}
			}
		})
	}

	// Wait until the test is over.
	wg.Wait()

	// We expect a clean state at the end of the test.
	require.Equal(t, 0, w.watchGroupsCount())
}

func BenchmarkPartitionOffsetWatcher(b *testing.B) {
	b.Run("high notify() rate but condition is never met", func(b *testing.B) {
		const numWatchingGoroutines = 1000

		var (
			ctx = context.Background()
			w   = newPartitionOffsetWatcher()
		)

		// Start all watching goroutines.
		wg := sync.WaitGroup{}

		for i := 0; i < numWatchingGoroutines; i++ {
			runAsync(&wg, func() {
				if err := w.Wait(ctx, math.MaxInt64); err != nil {
					b.Fatal(err)
				}
			})
		}

		// Release the watching goroutines when cleaning up.
		b.Cleanup(func() {
			w.Notify(math.MaxInt64)
			wg.Wait()

			// We expect a clean state at the end of the test.
			require.Equal(b, 0, w.watchGroupsCount())
		})

		// Give some time to start all goroutines.
		time.Sleep(time.Second)

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			w.Notify(int64(n))
		}
	})

	b.Run("high concurrency between wait() and notify()", func(b *testing.B) {
		const numWatchingGoroutines = 100

		var (
			ctx, cancelCtx = context.WithCancel(context.Background())
			w              = newPartitionOffsetWatcher()
			nextOffset     = atomic.NewInt64(0)
			done           = atomic.NewBool(false)
		)

		// Start all watching goroutines.
		wg := sync.WaitGroup{}

		for i := 0; i < numWatchingGoroutines; i++ {
			runAsync(&wg, func() {
				for !done.Load() {
					// Continuously wait for an offset soon in the future.
					err := w.Wait(ctx, nextOffset.Inc()+numWatchingGoroutines)

					// When done the context is cancelled, so we don't check for an error in that case.
					if !done.Load() && err != nil {
						b.Fatal(err)
					}
				}
			})
		}

		// Release the watching goroutines when cleaning up.
		b.Cleanup(func() {
			done.Store(true)
			cancelCtx()
			wg.Wait()

			// We expect a clean state at the end of the test.
			require.Equal(b, 0, w.watchGroupsCount())
		})

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			w.Notify(nextOffset.Inc())

			// Give a short time to multiple watching goroutines to call Wait() so that the next Notify()
			// will likely return the result for multiple waiting goroutines at the same time.
			time.Sleep(10 * time.Nanosecond)
		}
	})
}
