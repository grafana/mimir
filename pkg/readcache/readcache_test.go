// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

// newTestConfig returns a fully-defaulted Config rooted at t.TempDir.
// Use this in tests where you don't care about the specific values of
// any flag besides DataDir.
//
// If withKafka is true a fake Kafka cluster is created with
// numPartitions partitions and the kafka config is wired to it.
func newTestConfig(t *testing.T, withKafka bool, numPartitions int32) Config {
	t.Helper()

	const topic = "test-topic"

	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError), log.NewNopLogger())
	cfg.DataDir = t.TempDir()
	cfg.InstanceID = "test"
	// Keep the ring's instance ID aligned with the pod's instance ID
	// so Config.Validate() doesn't bounce on the drift check.
	cfg.InstanceRing.InstanceID = cfg.InstanceID
	cfg.KafkaTopic = topic

	var blocksCfg tsdb.BlocksStorageConfig
	blocksCfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
	blocksCfg.TSDB.Dir = t.TempDir()
	cfg.BlocksStorage = blocksCfg

	if withKafka {
		_, addr := testkafka.CreateCluster(t, numPartitions, topic)
		var kafkaCfg ingest.KafkaConfig
		kafkaCfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
		kafkaCfg.Address = flagext.StringSliceCSV{addr}
		kafkaCfg.Topic = topic
		cfg.Kafka = kafkaCfg
	}

	return cfg
}

func TestConfig_Validate(t *testing.T) {
	t.Run("defaults are valid", func(t *testing.T) {
		cfg := newTestConfig(t, false, 0)
		require.NoError(t, cfg.Validate())
	})

	t.Run("missing instance id is rejected", func(t *testing.T) {
		cfg := newTestConfig(t, false, 0)
		cfg.InstanceID = ""
		assert.Error(t, cfg.Validate())
	})

	t.Run("missing data-dir is rejected", func(t *testing.T) {
		cfg := newTestConfig(t, false, 0)
		cfg.DataDir = ""
		assert.Error(t, cfg.Validate())
	})

	t.Run("missing kafka-topic is rejected", func(t *testing.T) {
		cfg := newTestConfig(t, false, 0)
		cfg.KafkaTopic = ""
		assert.Error(t, cfg.Validate())
	})
}

func TestConfig_ParseOwnedPartitions(t *testing.T) {
	tcs := map[string]struct {
		in   string
		want []int32
		err  bool
	}{
		"empty":             {in: "", want: nil},
		"single":            {in: "5", want: []int32{5}},
		"multiple":          {in: "0,1,2", want: []int32{0, 1, 2}},
		"deduplicates":      {in: "1,1,2,1", want: []int32{1, 2}},
		"with whitespace":   {in: " 1 , 2 , 3 ", want: []int32{1, 2, 3}},
		"trailing comma ok": {in: "1,2,3,", want: []int32{1, 2, 3}},
		"negative rejected": {in: "-1", err: true},
		"non-numeric":       {in: "abc", err: true},
	}
	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			cfg := Config{OwnedPartitions: tc.in}
			got, err := cfg.ParseOwnedPartitions()
			if tc.err {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestReadcache_WipeTSDBDirOnStartup(t *testing.T) {
	cfg := newTestConfig(t, false, 0)
	marker := filepath.Join(cfg.DataDir, "stale")
	require.NoError(t, os.WriteFile(marker, []byte("x"), 0o644))

	cfg.WipeTSDBDirOnStartup = true
	limits := validation.NewOverrides(validation.Limits{}, nil)

	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	require.NoError(t, services.StopAndAwaitTerminated(ctx, r))

	_, err = os.Stat(marker)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
}

func TestReadcache_Lifecycle(t *testing.T) {
	cfg := newTestConfig(t, true, 64)
	cfg.OwnedPartitions = "0,7,42"

	limits := validation.NewOverrides(validation.Limits{}, nil)

	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	// Use the same 30s budget as the other Kafka-backed tests in this
	// package. Start/stop of the readcache itself is sub-second; the
	// previous 5s budget was shared with a 64-partition fake-Kafka
	// cluster and flaked under load.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, services.StartAndAwaitRunning(ctx, r))

	owned := r.OwnedPartitions()
	assert.ElementsMatch(t, []int32{0, 7, 42}, owned)

	require.NoError(t, services.StopAndAwaitTerminated(ctx, r))
}

// TestReadcache_ApplyAssignment_AddsAndRemoves exercises the
// reconciliation logic that wires WatchReadcacheAssignments snapshots
// into add/removePartition calls. The Kafka cluster has 4
// partitions; the readcache starts with none and is driven entirely
// by the assignment snapshots.
func TestReadcache_ApplyAssignment_AddsAndRemoves(t *testing.T) {
	cfg := newTestConfig(t, true, 4)
	cfg.InstanceID = "readcache-test"
	cfg.InstanceRing.InstanceID = cfg.InstanceID

	limits := validation.NewOverrides(validation.Limits{}, nil)

	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, r) }()

	require.Empty(t, r.OwnedPartitions(), "cold-start readcache must own nothing until the rebalancer leases partitions")

	now := time.Now()
	leaseEnd := now.Add(time.Hour)

	// First snapshot: lease partitions 0 and 2 to this instance,
	// partition 1 to a different instance (must be ignored), and
	// pre-issue partition 3 in the future (must also be ignored).
	snap1 := []readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "readcache-test", From: now.Add(-time.Minute), To: leaseEnd},
		{PartitionID: 1, InstanceID: "readcache-other", From: now.Add(-time.Minute), To: leaseEnd},
		{PartitionID: 2, InstanceID: "readcache-test", From: now.Add(-time.Minute), To: leaseEnd},
		{PartitionID: 3, InstanceID: "readcache-test", From: now.Add(time.Minute), To: leaseEnd},
	}
	require.NoError(t, r.applyAssignment(ctx, snap1, now))
	assert.Equal(t, []int32{0, 2}, r.OwnedPartitions(),
		"applyAssignment must own only currently-active leases for this instance")

	// Second snapshot: drop partition 2, add partition 3 (which is
	// now active because its From has elapsed in the slightly-later
	// `at` we pass in below).
	snap2 := []readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "readcache-test", From: now.Add(-time.Minute), To: leaseEnd},
		{PartitionID: 3, InstanceID: "readcache-test", From: now.Add(time.Minute), To: leaseEnd},
	}
	require.NoError(t, r.applyAssignment(ctx, snap2, now.Add(2*time.Minute)))
	assert.Equal(t, []int32{0, 3}, r.OwnedPartitions(),
		"applyAssignment must add newly-active partitions and remove dropped ones")

	// Third snapshot: empty (rebalancer says we own nothing).
	require.NoError(t, r.applyAssignment(ctx, nil, now.Add(2*time.Minute)))
	assert.Empty(t, r.OwnedPartitions(),
		"empty snapshot must release all owned partitions")
}

// TestReadcache_ApplyAssignment_IgnoresExpiredLeases checks the
// boundary case where a lease's To is exactly `at`: by
// LogEntry.ActiveAt semantics, To is exclusive, so the partition
// must NOT be owned.
func TestReadcache_ApplyAssignment_IgnoresExpiredLeases(t *testing.T) {
	cfg := newTestConfig(t, true, 2)
	cfg.InstanceID = "readcache-test"
	cfg.InstanceRing.InstanceID = cfg.InstanceID

	limits := validation.NewOverrides(validation.Limits{}, nil)
	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, r) }()

	now := time.Now()
	snap := []readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "readcache-test", From: now.Add(-time.Hour), To: now},
		{PartitionID: 1, InstanceID: "readcache-test", From: now.Add(-time.Hour), To: now.Add(time.Hour)},
	}
	require.NoError(t, r.applyAssignment(ctx, snap, now))
	assert.Equal(t, []int32{1}, r.OwnedPartitions(),
		"a lease whose To == at is expired and must not produce ownership")
}

// TestReadcache_SetAndGetHashRanges_PerPartition verifies that the
// SetHashRanges → GetHashRanges roundtrip preserves the per-partition
// association added in this commit. The rebalancer sends a flat list
// of (partition, range) entries; the readcache must demux them into
// the matching partitionState's currentRanges and then echo them back
// with the partition id intact.
func TestReadcache_SetAndGetHashRanges_PerPartition(t *testing.T) {
	cfg := newTestConfig(t, true, 4)
	cfg.OwnedPartitions = "0,1"

	limits := validation.NewOverrides(validation.Limits{}, nil)
	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, r) }()

	// Rebalancer sends partition 0 → [0, 99], partition 1 → [100,
	// 199], and a stray entry for unowned partition 9 (must be
	// ignored).
	req := &ingester_client.SetHashRangesRequest{
		Ranges: []ingester_client.HashRangeEntry{
			{Lo: 100, Hi: 199, PartitionId: 1},
			{Lo: 0, Hi: 99, PartitionId: 0},
			{Lo: 500, Hi: 599, PartitionId: 9},
		},
	}
	_, err = r.setHashRanges(ctx, req)
	require.NoError(t, err)

	getResp, err := r.getHashRanges(ctx, &ingester_client.GetHashRangesRequest{})
	require.NoError(t, err)

	// GetHashRanges should report exactly what the rebalancer sent
	// for the OWNED partitions; the unowned partition 9 entry must
	// have been dropped on the floor.
	sort.Slice(getResp.Ranges, func(i, j int) bool {
		if getResp.Ranges[i].PartitionId != getResp.Ranges[j].PartitionId {
			return getResp.Ranges[i].PartitionId < getResp.Ranges[j].PartitionId
		}
		return getResp.Ranges[i].Lo < getResp.Ranges[j].Lo
	})
	require.Equal(t, []ingester_client.HashRangeEntry{
		{Lo: 0, Hi: 99, PartitionId: 0},
		{Lo: 100, Hi: 199, PartitionId: 1},
	}, getResp.Ranges)
}

// TestReadcache_HashRangeStats_ResidueOnFormerOwner verifies the
// design goal of this commit: after a hash range moves from one
// partition to another, the previous owner's TSDB head still has the
// series until compaction clears them. HashRangeStats reports the
// residue against the previous owner's partition id — separately from
// any growth on the new owner — so the rebalancer attributes load
// correctly instead of summing residue onto whoever currently owns
// the range. This is the per-partition L_pid accounting from the
// design notes.
func TestReadcache_HashRangeStats_ResidueOnFormerOwner(t *testing.T) {
	cfg := newTestConfig(t, true, 4)
	cfg.OwnedPartitions = "0,1"

	limits := validation.NewOverrides(validation.Limits{}, nil)
	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	// Block the background series walker for the duration of the
	// test. running() spawns refreshSeriesStats in a goroutine at
	// startup; if it lands between setHashRanges and applyWalkResult
	// below it walks an empty head, sees count=0 for [0,99] in P0's
	// historical, GCs the entry, and invalidates the snapshot we
	// capture. Acquiring before StartAndAwaitRunning is the only
	// way to win that race — refreshSeriesStats TryLocks and exits
	// immediately when it fails.
	r.seriesWalkMu.Lock()
	defer r.seriesWalkMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, r) }()

	// First round: partition 0 owns [0, 99], partition 1 owns [100,
	// 199].
	_, err = r.setHashRanges(ctx, &ingester_client.SetHashRangesRequest{
		Ranges: []ingester_client.HashRangeEntry{
			{Lo: 0, Hi: 99, PartitionId: 0},
			{Lo: 100, Hi: 199, PartitionId: 1},
		},
	})
	require.NoError(t, err)

	// Simulate a slicer round that moves [0, 99] from partition 0
	// to partition 1.
	_, err = r.setHashRanges(ctx, &ingester_client.SetHashRangesRequest{
		Ranges: []ingester_client.HashRangeEntry{
			{Lo: 100, Hi: 199, PartitionId: 1},
			{Lo: 0, Hi: 99, PartitionId: 1},
		},
	})
	require.NoError(t, err)

	// Inject synthetic walker results: partition 0 has 1234 residue
	// series in [0, 99], partition 1 has 56 fresh series in [0, 99]
	// (just started growing) and 78 in [100, 199] (steady state).
	r.partitionMu.RLock()
	p0 := r.partitions[0]
	p1 := r.partitions[1]
	r.partitionMu.RUnlock()
	require.NotNil(t, p0)
	require.NotNil(t, p1)

	p0Snap := p0.ranges.rangesSnapshot()
	require.Equal(t, []assignment.HashRange{hr(0, 99)}, p0Snap)
	require.True(t, p0.ranges.applyWalkResult(p0Snap, []int64{1234}, nil))

	p1Snap := p1.ranges.rangesSnapshot()
	// Sorted by Lo.
	require.Equal(t, []assignment.HashRange{hr(0, 99), hr(100, 199)}, p1Snap)
	require.True(t, p1.ranges.applyWalkResult(p1Snap, []int64{56, 78}, nil))

	resp, err := r.hashRangeStats(ctx, &ingester_client.HashRangeStatsRequest{})
	require.NoError(t, err)

	sort.Slice(resp.Rates, func(i, j int) bool {
		if resp.Rates[i].PartitionId != resp.Rates[j].PartitionId {
			return resp.Rates[i].PartitionId < resp.Rates[j].PartitionId
		}
		return resp.Rates[i].Lo < resp.Rates[j].Lo
	})

	// The same (0, 99) range appears under partition 0 (residue) AND
	// partition 1 (growth) — distinct entries, NOT summed.
	require.Equal(t, []ingester_client.HashRangeRate{
		{Lo: 0, Hi: 99, ActiveSeries: 1234, PartitionId: 0},
		{Lo: 0, Hi: 99, ActiveSeries: 56, PartitionId: 1},
		{Lo: 100, Hi: 199, ActiveSeries: 78, PartitionId: 1},
	}, resp.Rates)
}

func TestReadcache_GetOrOpenTSDB(t *testing.T) {
	cfg := newTestConfig(t, true, 2)
	cfg.OwnedPartitions = "0,1"

	limits := validation.NewOverrides(validation.Limits{}, nil)

	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, r) }()

	// Unknown partition returns nil.
	db, err := r.getOrOpenTSDB("user-1", 999)
	require.NoError(t, err)
	assert.Nil(t, db)

	// Owned partition opens a fresh TSDB.
	db, err = r.getOrOpenTSDB("user-1", 0)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Second call returns the same instance.
	db2, err := r.getOrOpenTSDB("user-1", 0)
	require.NoError(t, err)
	assert.Same(t, db, db2)

	// Different partitions are different TSDBs.
	dbOther, err := r.getOrOpenTSDB("user-1", 1)
	require.NoError(t, err)
	require.NotNil(t, dbOther)
	assert.NotSame(t, db, dbOther)
}

// TestReadcache_Stopping_TearsDownPartitionsInParallel verifies that
// stopping() fans out per-partition teardown across multiple
// goroutines rather than tearing down one partition at a time. Before
// the parallel-stopping change, a pod with N partitions paid O(N) *
// stop-cost even when individual stops were independent; this test
// would have timed out under the old serial path.
//
// The test uses the stopPartitionHook seam to block every
// stopPartition call on a shared barrier until ALL partitions have
// arrived. If the implementation is serial, only the first call
// reaches the barrier, no further calls fire, and the wait times out.
// If parallel, all N reach the barrier within the wait window and
// the test passes.
func TestReadcache_Stopping_TearsDownPartitionsInParallel(t *testing.T) {
	const numPartitions = 8
	cfg := newTestConfig(t, true, numPartitions)
	cfg.OwnedPartitions = "0,1,2,3,4,5,6,7"

	limits := validation.NewOverrides(validation.Limits{}, nil)
	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	startCtx, startCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer startCancel()
	require.NoError(t, services.StartAndAwaitRunning(startCtx, r))

	// Each stopPartition call signals its arrival on `arrived`, then
	// blocks on `release`. The test main goroutine waits to receive
	// `numPartitions` arrivals (proving parallelism), then closes
	// `release` to let all stops proceed.
	arrived := make(chan int32, numPartitions)
	release := make(chan struct{})
	r.stopPartitionHook = func(pid int32) {
		arrived <- pid
		<-release
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- services.StopAndAwaitTerminated(context.Background(), r)
	}()

	// Wait for every partition to reach the hook. If the fan-out is
	// serial, only one will, and this select will time out.
	seen := make(map[int32]struct{}, numPartitions)
	waitDeadline := time.After(15 * time.Second)
	for len(seen) < numPartitions {
		select {
		case pid := <-arrived:
			seen[pid] = struct{}{}
		case <-waitDeadline:
			t.Fatalf("stopping is serial: only %d/%d partitions reached stopPartition before timeout (saw %v)",
				len(seen), numPartitions, seen)
		}
	}

	close(release)

	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("stopping did not complete after releasing the barrier")
	}
}

// TestReadcache_Stopping_DoesNotHoldPartitionMuDuringStopPartition is
// the regression test for the shutdown deadlock: with the old code,
// stopping() held the partitionMu writer lock for the entire
// duration of stopPartition (i.e. while the Kafka reader's stop was
// waiting for in-flight pushers to complete), which meant any
// concurrent getOrOpenTSDB call — including the one inside the
// pusher itself — blocked, producing an A/B deadlock that only k8s
// SIGKILL could resolve.
//
// We verify the fix by installing a stopPartitionHook that, the
// moment a stop is in flight, races a writer-lock probe. If the
// writer is held, TryLock returns false; we want it to succeed,
// proving the lock has already been released by the time
// stopPartition runs.
func TestReadcache_Stopping_DoesNotHoldPartitionMuDuringStopPartition(t *testing.T) {
	cfg := newTestConfig(t, true, 4)
	cfg.OwnedPartitions = "0,1,2,3"

	limits := validation.NewOverrides(validation.Limits{}, nil)
	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	startCtx, startCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer startCancel()
	require.NoError(t, services.StartAndAwaitRunning(startCtx, r))

	var probedHeld bool
	var probedFree bool
	r.stopPartitionHook = func(_ int32) {
		// If stopping() is still holding partitionMu (the bug we
		// fixed), TryLock will fail because the writer is held.
		if r.partitionMu.TryLock() {
			probedFree = true
			r.partitionMu.Unlock()
		} else {
			probedHeld = true
		}
	}

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), r))

	assert.True(t, probedFree, "partitionMu must be released before stopPartition runs")
	assert.False(t, probedHeld, "partitionMu was held while stopPartition ran (deadlock bug regression)")
}

// TestReadcache_RemovePartition_DoesNotHoldPartitionMuDuringStop is
// the equivalent regression test for the removePartition path.
// removePartition is called from applyAssignment whenever the
// rebalancer drops a partition, so the same lock-ordering bug
// applies there: holding the writer lock during the Kafka reader's
// stop would deadlock the in-flight pusher.
func TestReadcache_RemovePartition_DoesNotHoldPartitionMuDuringStop(t *testing.T) {
	cfg := newTestConfig(t, true, 2)
	cfg.OwnedPartitions = "0,1"

	limits := validation.NewOverrides(validation.Limits{}, nil)
	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, r) }()

	var probedFree bool
	var probedHeld bool
	r.stopPartitionHook = func(pid int32) {
		if pid != 0 {
			return
		}
		if r.partitionMu.TryLock() {
			probedFree = true
			r.partitionMu.Unlock()
		} else {
			probedHeld = true
		}
	}

	require.NoError(t, r.removePartition(0))

	assert.True(t, probedFree, "partitionMu must be released before stopPartition runs from removePartition")
	assert.False(t, probedHeld, "partitionMu held by removePartition during stop (deadlock bug regression)")
}
