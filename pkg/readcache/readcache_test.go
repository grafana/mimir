// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
