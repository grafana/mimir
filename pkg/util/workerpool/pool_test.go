// SPDX-License-Identifier: AGPL-3.0-only

package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		require.NoError(t, p.Submit("tenant-1", func() {
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
		err := p.Submit("hog", hogFn)
		require.NoError(t, err)
	}

	// Wait for the first hog task to be in flight.
	require.Eventually(t, func() bool { return hogStarts.Load() >= 1 }, time.Second, time.Millisecond)

	// Submit one task for a different tenant. It should run before all
	// remaining hog tasks complete.
	lightStarted := make(chan struct{})
	lightFn := func() { close(lightStarted) }
	err := p.Submit("light", lightFn)
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
		err := p.Submit("tenant", func() {
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

func TestConfig_Validate(t *testing.T) {
	require.NoError(t, (&Config{Size: 0}).Validate())
	require.NoError(t, (&Config{Size: 4}).Validate())
	require.Error(t, (&Config{Size: -1}).Validate())
}
