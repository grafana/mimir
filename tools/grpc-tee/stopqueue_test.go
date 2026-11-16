// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStopQueue_DequeueReturnsItemsThenFalse(t *testing.T) {
	q := newStopQueue[int]()
	require.NoError(t, q.Enqueue(1))
	require.NoError(t, q.Enqueue(2))
	q.Close()

	v, ok := q.Dequeue()
	require.True(t, ok)
	require.Equal(t, 1, v)
	v, ok = q.Dequeue()
	require.True(t, ok)
	require.Equal(t, 2, v)

	// Every later call returns the zero value and false without waiting.
	for range 3 {
		v, ok = q.Dequeue()
		require.False(t, ok)
		require.Zero(t, v)
	}
}

func TestStopQueue_EnqueueAfterCloseFails(t *testing.T) {
	q := newStopQueue[int]()
	q.Close()
	require.ErrorIs(t, q.Enqueue(1), errQueueClosed)

	_, ok := q.Dequeue()
	require.False(t, ok)
}

func TestStopQueue_CloseTwice(t *testing.T) {
	q := newStopQueue[int]()
	require.NoError(t, q.Enqueue(1))
	q.Close()
	q.Close()

	v, ok := q.Dequeue()
	require.True(t, ok)
	require.Equal(t, 1, v)
	_, ok = q.Dequeue()
	require.False(t, ok)
}

// Producers keep calling Enqueue while Close runs.
// Every Enqueue that returns nil must come out of Dequeue before it returns false.
func TestStopQueue_CloseWhileEnqueueing(t *testing.T) {
	const producers = 8

	for range 200 {
		q := newStopQueue[int]()
		var accepted atomic.Int64
		var wg sync.WaitGroup
		for range producers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; ; i++ {
					if err := q.Enqueue(i); err != nil {
						return
					}
					accepted.Add(1)
				}
			}()
		}

		go func() {
			runtime.Gosched()
			q.Close()
		}()

		var got int64
		for {
			if _, ok := q.Dequeue(); !ok {
				break
			}
			got++
		}
		wg.Wait()
		require.Equal(t, accepted.Load(), got)
	}
}

func TestStopQueue_ManyProducers(t *testing.T) {
	const producers, perProducer = 8, 1000

	q := newStopQueue[int]()
	var wg sync.WaitGroup
	for range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perProducer {
				require.NoError(t, q.Enqueue(i))
			}
		}()
	}
	go func() {
		// Close after every producer is done, so the count is exact.
		wg.Wait()
		q.Close()
	}()

	count := 0
	for {
		if _, ok := q.Dequeue(); !ok {
			break
		}
		count++
	}
	require.Equal(t, producers*perProducer, count)
}
