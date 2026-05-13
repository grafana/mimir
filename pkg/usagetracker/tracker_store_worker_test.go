// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

// enableShardWorkers signature: (numWorkers, inboxDepth). Tests below pass
// numWorkers=4 by default to keep the routing path interesting (some workers
// shared across (tenant, shard) pairs) while keeping goroutine counts small.

// TestTrackerStore_ShardWorkers_Equivalence asserts that turning on shard workers
// preserves the user-visible contract of trackerStore.trackSeries when the input
// fits comfortably under the per-tenant limit:
//   - all input series are accepted (no rejections),
//   - the resulting per-tenant series count is identical, and
//   - idle-timeout cleanup evicts both stores identically.
//
// Limit-driven rejections are NOT asserted to be byte-for-byte equivalent.
// The legacy single-goroutine path serialises Put calls within one trackSeries
// invocation, so it produces a deterministic rejected set. The worker path
// runs per-shard Puts concurrently; each Put does an unsynchronised
// load-then-increment on the shared per-tenant series counter, so under
// contention the worker mode may accept up to ~NumShards more series than the
// limit. This is also the existing behaviour across concurrent trackSeries
// calls in legacy mode (see currentLimit's handling of zonesCount); the
// worker mode merely makes it visible within a single call.
func TestTrackerStore_ShardWorkers_Equivalence(t *testing.T) {
	const (
		idleTimeout = 20 * time.Minute
		userID      = "user1"
	)
	limits := limiterMock{userID: 1_000_000}
	now := time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)

	withWorkers := newTrackerStore(idleTimeout, 85, log.NewNopLogger(), limits, noopEvents{}, false)
	withWorkers.enableShardWorkers(4, 16)
	t.Cleanup(withWorkers.stop)

	withoutWorkers := newTrackerStore(idleTimeout, 85, log.NewNopLogger(), limits, noopEvents{}, false)
	t.Cleanup(withoutWorkers.stop)

	series := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

	rA, errA := withWorkers.trackSeries(context.Background(), userID, append([]uint64(nil), series...), now)
	require.NoError(t, errA)
	rB, errB := withoutWorkers.trackSeries(context.Background(), userID, append([]uint64(nil), series...), now)
	require.NoError(t, errB)

	require.Empty(t, rA, "no rejections expected with generous limit (workers)")
	require.Empty(t, rB, "no rejections expected with generous limit (legacy)")
	require.Equal(t, withoutWorkers.seriesCountsForTests(), withWorkers.seriesCountsForTests(), "series counts must match")

	// Cleanup well past the idle timeout: both stores should fully evict.
	cleanupAt := now.Add(2 * idleTimeout)
	withWorkers.cleanup(cleanupAt)
	withoutWorkers.cleanup(cleanupAt)
	require.Equal(t, withoutWorkers.seriesCountsForTests(), withWorkers.seriesCountsForTests())
}

// TestTrackerStore_ShardWorkers_Concurrent fans out many concurrent trackSeries
// calls against a worker-backed store and checks that the final state is
// internally consistent (no double-counts, no lost increments).
func TestTrackerStore_ShardWorkers_Concurrent(t *testing.T) {
	const (
		idleTimeout     = 20 * time.Minute
		userID          = "user1"
		seriesPerWorker = 200
		concurrentCalls = 32
	)
	limits := limiterMock{userID: 1_000_000}
	now := time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)

	store := newTrackerStore(idleTimeout, 85, log.NewNopLogger(), limits, noopEvents{}, false)
	store.enableShardWorkers(4, 8) // small pool + small inbox to exercise blocking
	t.Cleanup(store.stop)

	// Each goroutine writes its own disjoint slice of series so we can verify
	// the total expected count without double-counting.
	var wg sync.WaitGroup
	wg.Add(concurrentCalls)
	for c := 0; c < concurrentCalls; c++ {
		c := c
		go func() {
			defer wg.Done()
			series := make([]uint64, seriesPerWorker)
			for i := range series {
				series[i] = uint64(c)*uint64(seriesPerWorker) + uint64(i) + 1
			}
			rej, err := store.trackSeries(context.Background(), userID, series, now)
			if err != nil {
				t.Errorf("trackSeries: %v", err)
			}
			if len(rej) != 0 {
				t.Errorf("expected no rejections, got %d", len(rej))
			}
		}()
	}
	wg.Wait()

	require.Equal(t, uint64(concurrentCalls*seriesPerWorker), store.seriesCountsForTests()[userID])
}

// TestShardWorkerPool_SubmitRespectsContextCancellation verifies that a submit
// blocked on a full inbox returns ctx.Err() when the context is cancelled,
// and that the submit-blocked counter is incremented.
func TestShardWorkerPool_SubmitRespectsContextCancellation(t *testing.T) {
	store := newTrackerStore(time.Hour, 85, log.NewNopLogger(), limiterMock{"u": 1_000_000}, noopEvents{}, false)
	// Single worker with a 1-deep inbox makes saturation easy to trigger.
	store.enableShardWorkers(1, 1)
	t.Cleanup(store.stop)

	// Bootstrap: triggers tenant creation so we can grab its hash.
	_, err := store.trackSeries(context.Background(), "u", []uint64{1}, time.Now())
	require.NoError(t, err)

	tenant := store.getOrCreateTenant("u")
	defer tenant.RUnlock()

	// Pick a (tenant, shard); we'll wedge the routed worker by holding the
	// underlying map's mutex.
	const shard uint8 = 0
	m := tenant.shards[shard]

	// Wedge the worker by holding the map's mutex. When the worker dequeues an
	// op for this map and tries to Lock(), it'll block.
	released := make(chan struct{})
	wedged := make(chan struct{})
	go func() {
		m.Lock()
		close(wedged)
		<-released
		m.Unlock()
	}()
	<-wedged
	defer close(released)

	// Saturate the inbox. With depth=1 we may need a couple of submits to
	// observe a blocked one (the worker pulls one off immediately and then
	// blocks on the wedged mutex).
	for i := 0; i < 8 && store.shardWorkerSubmitBlocked.Load() == 0; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		op := newShardOp(m, []uint64{uint64(16 * (i + 1))}, 0, true, tenant.series, tenant.currentLimit)
		_ = store.shardWorkers.submit(ctx, tenant.tenantHash, shard, op)
		cancel()
	}
	require.GreaterOrEqual(t, store.shardWorkerSubmitBlocked.Load(), int64(1))

	// Now do the actual cancellation assertion: a submit that blocks should
	// return ctx.Err() once cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	op := newShardOp(m, []uint64{uint64(160)}, 0, true, tenant.series, tenant.currentLimit)
	submitErr := make(chan error, 1)
	go func() { submitErr <- store.shardWorkers.submit(ctx, tenant.tenantHash, shard, op) }()
	time.Sleep(10 * time.Millisecond) // let the goroutine reach the blocking select arm
	cancel()

	select {
	case err := <-submitErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled submit did not return")
	}
}

// TestShardWorkerPool_StopDrainsQueuedOps verifies that ops already in the inbox
// when the pool is shut down still get processed (and their done channels
// closed), so submitters waiting on them aren't deadlocked.
func TestShardWorkerPool_StopDrainsQueuedOps(t *testing.T) {
	store := newTrackerStore(time.Hour, 85, log.NewNopLogger(), limiterMock{"u": 1_000_000}, noopEvents{}, false)
	store.enableShardWorkers(1, 8)
	// Note: we don't t.Cleanup(store.stop) here because the test calls shutdown
	// explicitly. A second call would no-op (stopOnce), but it's clearer this way.

	_, err := store.trackSeries(context.Background(), "u", []uint64{1}, time.Now())
	require.NoError(t, err)

	tenant := store.getOrCreateTenant("u")
	const shard uint8 = 0
	m := tenant.shards[shard]
	tenant.RUnlock()

	// Pause the worker by holding the map's mutex.
	m.Lock()

	// Queue several ops while the worker is wedged. The first one will be
	// pulled out of the inbox into process() and blocked on Lock; the rest sit
	// in the inbox buffer.
	const queued = 4
	ops := make([]*shardOp, queued)
	for i := range ops {
		ops[i] = newShardOp(m, []uint64{16, 32, 48, 64}, 0, true, tenant.series, tenant.currentLimit)
		require.NoError(t, store.shardWorkers.submit(context.Background(), tenant.tenantHash, shard, ops[i]))
	}

	// Initiate shutdown in background, then release the mutex so the drain runs.
	shutdownDone := make(chan struct{})
	go func() {
		store.shardWorkers.shutdown()
		close(shutdownDone)
	}()
	m.Unlock()

	// All queued ops should still complete.
	for i, op := range ops {
		select {
		case <-op.done:
		case <-time.After(2 * time.Second):
			t.Fatalf("queued op %d not drained", i)
		}
	}
	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("worker shutdown did not return")
	}
}

// TestShardWorkerPool_FastPathDoesNotBlock is a smoke test that confirms a
// generously sized pool + inbox does not produce blocked submissions in the
// steady state.
func TestShardWorkerPool_FastPathDoesNotBlock(t *testing.T) {
	store := newTrackerStore(time.Hour, 85, log.NewNopLogger(), limiterMock{"u": 1_000_000}, noopEvents{}, false)
	store.enableShardWorkers(8, 256)
	t.Cleanup(store.stop)

	for i := 0; i < 1000; i++ {
		_, err := store.trackSeries(context.Background(), "u", []uint64{uint64(i + 1)}, time.Now())
		require.NoError(t, err)
	}
	require.Equal(t, int64(0), store.shardWorkerSubmitBlocked.Load())
	// Each call has a single series so it touches exactly one shard => one op.
	require.Equal(t, int64(1000), store.shardWorkerOpsTotal.Load())
}

// TestShardWorkerPool_RoutingIsConsistent verifies that the same (tenantHash,
// shard) always routes to the same worker. Without this property the
// single-writer guarantee for each tenantshard.Map would be lost.
func TestShardWorkerPool_RoutingIsConsistent(t *testing.T) {
	store := newTrackerStore(time.Hour, 85, log.NewNopLogger(), limiterMock{}, noopEvents{}, false)
	store.enableShardWorkers(8, 4)
	t.Cleanup(store.stop)

	const samples = 50
	for tenantID := 0; tenantID < samples; tenantID++ {
		h := hashTenantID(string(rune('a' + tenantID%26)))
		for shard := uint8(0); shard < shards; shard++ {
			w1 := store.shardWorkers.workerFor(h, shard)
			w2 := store.shardWorkers.workerFor(h, shard)
			require.Same(t, w1, w2, "routing must be consistent for the same (tenantHash, shard)")
		}
	}

	// Sanity: routing should also distribute across workers reasonably (at
	// least 2 distinct workers should be used by 8 shards of one tenant when
	// the pool has 8 workers).
	h := hashTenantID("u")
	seen := map[*shardWorker]struct{}{}
	for shard := uint8(0); shard < shards; shard++ {
		seen[store.shardWorkers.workerFor(h, shard)] = struct{}{}
	}
	require.GreaterOrEqual(t, len(seen), 2, "shards of one tenant should fan out across workers")
}
