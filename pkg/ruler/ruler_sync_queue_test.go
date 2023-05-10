// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	util_test "github.com/grafana/mimir/pkg/util/test"
)

func TestRulerSyncQueue_EnqueueAndPoll(t *testing.T) {
	util_test.VerifyNoLeak(t)
	ctx := context.Background()

	t.Run("poll() should return the enqueued user IDs", func(t *testing.T) {
		q := newRulerSyncQueue(100 * time.Millisecond)

		q.enqueue("user-1")
		q.enqueue("user-2")
		q.enqueue("user-3")

		// Start after the calls to enqueue() to ensure all user IDs are notified in a single message.
		require.NoError(t, services.StartAndAwaitRunning(ctx, q))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, q))
		})

		select {
		case actual := <-q.poll():
			require.Equal(t, []string{"user-1", "user-2", "user-3"}, actual)
		case <-time.After(time.Second):
			require.Fail(t, "no message received on the channel returned by poll()")
		}
	})

	t.Run("enqueue() should de-duplicate user IDs", func(t *testing.T) {
		q := newRulerSyncQueue(100 * time.Millisecond)

		q.enqueue("user-1")
		q.enqueue("user-2")
		q.enqueue("user-2")
		q.enqueue("user-1")

		// Start after the calls to enqueue() to ensure all user IDs are notified in a single message.
		require.NoError(t, services.StartAndAwaitRunning(ctx, q))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, q))
		})

		select {
		case actual := <-q.poll():
			require.Equal(t, []string{"user-1", "user-2"}, actual)
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
	util_test.VerifyNoLeak(t)
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
		// Push some users to the queue before starting the processor (this way we get stable tests).
		q.enqueue("user-1")
		q.enqueue("user-2")

		require.NoError(t, services.StartAndAwaitRunning(ctx, q))
		require.NoError(t, services.StartAndAwaitRunning(ctx, p))

		test.Poll(t, time.Second, [][]string{{"user-1", "user-2"}}, func() interface{} {
			callsMx.Lock()
			defer callsMx.Unlock()

			// Make a shallow copy of the calls slice.
			return append([][]string{}, calls...)
		})

		require.NoError(t, services.StopAndAwaitTerminated(ctx, p))
		require.NoError(t, services.StopAndAwaitTerminated(ctx, q))
	})
}
