// SPDX-License-Identifier: AGPL-3.0-only

package threadpool

import (
	"context"
	"sync"
	"testing"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/test"
)

func TestThreadpool_Call(t *testing.T) {
	t.Run("pool is stopped", func(t *testing.T) {
		test.VerifyNoLeak(t)

		ctx := context.Background()
		pool := NewThreadpool(1, prometheus.NewPedanticRegistry())
		require.NoError(t, services.StartAndAwaitRunning(ctx, pool))
		require.NoError(t, services.StopAndAwaitTerminated(ctx, pool))

		_, err := pool.Execute(func() (interface{}, error) {
			return 42, nil
		})

		assert.ErrorIs(t, err, ErrPoolStopped)
	})

	t.Run("no threads in pool", func(t *testing.T) {
		test.VerifyNoLeak(t)

		ctx := context.Background()
		pool := NewThreadpool(0, prometheus.NewPedanticRegistry())
		require.NoError(t, services.StartAndAwaitRunning(ctx, pool))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, pool))
		})

		val, err := pool.Execute(func() (interface{}, error) {
			return 27, nil
		})

		assert.Equal(t, 27, val.(int))
		assert.NoError(t, err)
	})

	t.Run("execute a single function", func(t *testing.T) {
		test.VerifyNoLeak(t)

		ctx := context.Background()
		pool := NewThreadpool(1, prometheus.NewPedanticRegistry())
		require.NoError(t, services.StartAndAwaitRunning(ctx, pool))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, pool))
		})

		val, err := pool.Execute(func() (interface{}, error) {
			return 42, nil
		})

		assert.Equal(t, 42, val.(int))
		assert.NoError(t, err)
	})

	t.Run("execute more functions than there are threads", func(t *testing.T) {
		test.VerifyNoLeak(t)

		ctx := context.Background()
		pool := NewThreadpool(1, prometheus.NewPedanticRegistry())
		require.NoError(t, services.StartAndAwaitRunning(ctx, pool))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, pool))
		})

		numJobs := 4
		results := make(chan int, numJobs)

		mtx := sync.Mutex{}
		mtx.Lock()
		cond := sync.NewCond(&mtx)

		// Run more goroutines than there are threads in the thread pool, each
		// trying to submit a job to the pool when awoken and writing the result
		// to a channel.
		for i := 0; i < numJobs; i++ {
			go func() {
				val, _ := pool.Execute(func() (interface{}, error) {
					cond.Wait()
					return 100, nil
				})

				results <- val.(int)
			}()
		}

		// Make sure that all goroutines that were blocked running a job in the
		// thread pool wake up, complete, and write their results to the channel
		for i := 0; i < numJobs; i++ {
			mtx.Lock()
			cond.Broadcast()
			mtx.Unlock()

			val := <-results
			assert.Equal(t, 100, val)
		}
	})
}
