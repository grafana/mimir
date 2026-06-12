// SPDX-License-Identifier: AGPL-3.0-only

package workerpool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func startPool(t *testing.T, cfg Config) *Pool {
	t.Helper()
	p, err := New(cfg, "test", nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), p))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), p))
	})
	return p
}

func TestPool_RunsTasksConcurrently(t *testing.T) {
	const workers = 4
	p := startPool(t, Config{Size: workers})

	var inflight atomic.Int64
	var maxInflight atomic.Int64
	release := make(chan struct{})
	fns := make([]func(), workers)
	for i := range fns {
		fns[i] = func() {
			cur := inflight.Add(1)
			for {
				prev := maxInflight.Load()
				if cur <= prev || maxInflight.CompareAndSwap(prev, cur) {
					break
				}
			}
			<-release
			inflight.Add(-1)
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(fns))
	for _, fn := range fns {
		require.NoError(t, p.Submit("test", "tenant-1", func() {
			defer wg.Done()
			fn()
		}))
	}

	// Wait until all workers have picked up their task.
	require.Eventually(t, func() bool {
		return inflight.Load() == int64(workers)
	}, time.Second, time.Millisecond)
	require.Equal(t, int64(workers), maxInflight.Load())

	close(release)
	wg.Wait()
}

func TestPool_TenantFairness(t *testing.T) {
	// With one worker, a tenant submitting many slow tasks must not prevent
	// another tenant's small task from being picked up promptly.
	p := startPool(t, Config{Size: 1})

	hog := make(chan struct{})
	defer close(hog)

	var hogStarts atomic.Int64
	hogFn := func() {
		hogStarts.Add(1)
		<-hog
	}

	// Submit 8 tasks for the hog tenant - all queued behind the running one.
	for i := 0; i < 8; i++ {
		err := p.Submit("test", "hog", hogFn)
		require.NoError(t, err)
	}

	// Wait for the first hog task to be in flight.
	require.Eventually(t, func() bool { return hogStarts.Load() >= 1 }, time.Second, time.Millisecond)

	// Submit one task for a different tenant. It should run before all
	// remaining hog tasks complete.
	lightStarted := make(chan struct{})
	lightFn := func() { close(lightStarted) }
	err := p.Submit("test", "light", lightFn)
	require.NoError(t, err)

	// Release one hog task so the worker becomes free.
	hog <- struct{}{}

	select {
	case <-lightStarted:
	case <-time.After(time.Second):
		t.Fatalf("light tenant's task was starved (hogStarts=%d)", hogStarts.Load())
	}
}

func TestPool_StopDrainsInflightWork(t *testing.T) {
	p, err := New(Config{Size: 2}, "drain", nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), p))

	var ran atomic.Int64
	var wg sync.WaitGroup
	wg.Add(4)
	for i := 0; i < 4; i++ {
		err := p.Submit("test", "tenant", func() {
			defer wg.Done()
			time.Sleep(50 * time.Millisecond)
			ran.Add(1)
		})
		require.NoError(t, err)
	}

	// All submitted tasks should run to completion before Stop returns.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), p))
	wg.Wait()
	assert.Equal(t, int64(4), ran.Load())
}

func TestPool_StartingBlocksUntilWorkersAreRegistered(t *testing.T) {
	const workers = 4
	p := startPool(t, Config{Size: workers})

	// By the time StartAndAwaitRunning returns (inside startPool), every
	// worker must already be registered with the queue — otherwise a Submit
	// here could land in the queue before any worker is connected.
	require.Equal(t, float64(workers), p.queue.GetConnectedConsumerWorkersMetric())
}

func TestPool_RecordsTaskDuration(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	p, err := New(Config{Size: 2}, "test", reg, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), p))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), p))
	})

	const dimension = "mydim"
	const n = 3
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		require.NoError(t, p.Submit(dimension, "tenant", wg.Done))
	}
	wg.Wait()

	// The worker observes the duration after the task func returns, so the last
	// observation can lag wg.Wait by a moment; poll until all n are recorded.
	require.Eventually(t, func() bool {
		return taskDurationSampleCount(t, reg, dimension) == n
	}, time.Second, time.Millisecond, "expected %d task-duration samples for dimension %q", n, dimension)
}

// taskDurationSampleCount returns the number of samples recorded in
// mimir_workerpool_task_duration_seconds for the given dimension label.
func taskDurationSampleCount(t *testing.T, g prometheus.Gatherer, dimension string) uint64 {
	t.Helper()
	mfs, err := g.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "mimir_workerpool_task_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "dimension" && lp.GetValue() == dimension {
					return m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return 0
}

func TestConfig_Validate(t *testing.T) {
	require.NoError(t, (&Config{Size: 0}).Validate())
	require.NoError(t, (&Config{Size: 4}).Validate())
	require.Error(t, (&Config{Size: -1}).Validate())
}
