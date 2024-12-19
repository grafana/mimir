// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRulerSyncQueue_EnqueueAndPoll(t *testing.T) {
	ctx := context.Background()

	t.Run("poll() should return the enqueued user IDs", func(t *testing.T) {
		q := newRulerSyncQueue(100 * time.Millisecond)

		require.NoError(t, services.StartAndAwaitRunning(ctx, q))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, q))
		})

		q.enqueue("user-1", "user-2", "user-3")

		select {
		case actual := <-q.poll():
			require.ElementsMatch(t, []string{"user-1", "user-2", "user-3"}, actual)
		case <-time.After(time.Second):
			require.Fail(t, "no message received on the channel returned by poll()")
		}
	})

	t.Run("enqueue() should de-duplicate user IDs", func(t *testing.T) {
		q := newRulerSyncQueue(100 * time.Millisecond)

		q.enqueue("user-1", "user-2")
		q.enqueue("user-2", "user-1")

		// Start after the calls to enqueue() to ensure all user IDs are notified in a single message.
		require.NoError(t, services.StartAndAwaitRunning(ctx, q))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, q))
		})

		select {
		case actual := <-q.poll():
			require.ElementsMatch(t, []string{"user-1", "user-2"}, actual)
		case <-time.After(time.Second):
			require.Fail(t, "no message received on the channel returned by poll()")
		}
	})

	t.Run("poll() should block until there's at least 1 user to sync", func(t *testing.T) {
		q := newRulerSyncQueue(100 * time.Millisecond)
		require.NoError(t, services.StartAndAwaitRunning(ctx, q))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, q))
		})

		select {
		case <-q.poll():
			require.Fail(t, "unexpected message has been published on the channel returned by poll()")
		case <-time.After(time.Second):
		}
	})
}

func TestRulerSyncQueue_ShouldNotNotifyChannelMoreFrequentlyThanPollFrequency(t *testing.T) {
	ctx := context.Background()

	const pollFrequency = time.Second
	q := newRulerSyncQueue(pollFrequency)
	require.NoError(t, services.StartAndAwaitRunning(ctx, q))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, q))
	})

	done := make(chan struct{}, 1)
	wg := sync.WaitGroup{}

	// Start a worker which continuously enqueue.
	wg.Add(1)
	go func() {
		defer wg.Done()
		userID := 0

		for {
			select {
			case <-time.After(pollFrequency / 10):
				q.enqueue(fmt.Sprintf("user-%d", userID))
				userID++
			case <-done:
				return
			}
		}
	}()

	// Start a worker which continuously poll from the queue.
	numPolls := 0
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-q.poll():
				numPolls++
			case <-done:
				return
			}
		}
	}()

	// Run for 5x poll frequency and then stop the workers
	time.Sleep(5 * pollFrequency)
	close(done)
	wg.Wait()

	assert.LessOrEqual(t, numPolls, 6)
}

func TestRulerSyncQueue_Concurrency(t *testing.T) {
	const (
		numWriteWorkers = 10
		numReadWorkers  = 10
		pollFrequency   = 10 * time.Millisecond
	)

	ctx := context.Background()

	q := newRulerSyncQueue(pollFrequency)
	require.NoError(t, services.StartAndAwaitRunning(ctx, q))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, q))
	})

	done := make(chan struct{}, 1)
	wg := sync.WaitGroup{}

	// Start write workers.
	wg.Add(numWriteWorkers)

	for i := 0; i < numWriteWorkers; i++ {
		go func() {
			defer wg.Done()

			for {
				select {
				case <-time.After(pollFrequency / 10):
					q.enqueue("input")
				case <-done:
					return
				}
			}
		}()
	}

	// Start read workers.
	wg.Add(numReadWorkers)

	for i := 0; i < numReadWorkers; i++ {
		go func() {
			defer wg.Done()

			for {
				select {
				case values := <-q.poll():
					for v := 0; v < len(values); v++ {
						require.Equal(t, "input", values[v])

						// Manipulate the returned slice so that if there's any race condition
						// then it will be reported when running tests with -race.
						values[v] = "updated"
					}
				case <-done:
					return
				}
			}
		}()
	}

	// Run for some time.
	time.Sleep(2 * time.Second)
	close(done)
	wg.Wait()
}

func TestRulerSyncQueueProcessor(t *testing.T) {
	var (
		callsMx sync.Mutex
		calls   [][]string
	)

	// Create a callback function which keep tracks of all invocations.
	callback := func(_ context.Context, userIDs []string) {
		callsMx.Lock()
		calls = append(calls, userIDs)
		callsMx.Unlock()
	}

	q := newRulerSyncQueue(100 * time.Millisecond)
	p := newRulerSyncQueueProcessor(q, callback)
	ctx := context.Background()

	t.Run("The processor should call the callback function each time some tenants are polled from the queue", func(t *testing.T) {
		require.NoError(t, services.StartAndAwaitRunning(ctx, q))
		require.NoError(t, services.StartAndAwaitRunning(ctx, p))

		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, p))
			require.NoError(t, services.StopAndAwaitTerminated(ctx, q))
		})

		q.enqueue("user-1", "user-2")

		test.Poll(t, time.Second, [][]string{{"user-1", "user-2"}}, func() interface{} {
			callsMx.Lock()
			defer callsMx.Unlock()

			// Make a shallow copy of the calls slice, and sort it to get stable tests.
			callsCopy := append([][]string{}, calls...)
			for i := range callsCopy {
				slices.Sort(callsCopy[i])
			}

			return callsCopy
		})
	})
}
