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
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestReadcache_KafkaConsumption(t *testing.T) {
	const (
		tenantID = "user-1"
		topic    = "test-topic"
	)

	cluster, addr := testkafka.CreateCluster(t, 4, topic)
	_ = cluster

	cfg := newTestConfigNoKafka(t)
	cfg.KafkaTopic = topic
	cfg.OwnedPartitions = "0,1"

	var kafkaCfg ingest.KafkaConfig
	kafkaCfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
	kafkaCfg.Address = flagext.StringSliceCSV{addr}
	kafkaCfg.Topic = topic
	cfg.Kafka = kafkaCfg

	limits := validation.NewOverrides(validation.Limits{}, nil)
	reg := prometheus.NewPedanticRegistry()

	rc, err := New(cfg, limits, nil, log.NewNopLogger(), reg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, services.StartAndAwaitRunning(ctx, rc))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, rc) }()

	// Produce a WriteRequest to partition 0 via the kafka writer.
	writer := makeKafkaWriter(t, kafkaCfg, reg)
	defer func() { _ = services.StopAndAwaitTerminated(ctx, writer) }()

	// Build a write request for tenantID with a single series.
	req := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: mimirpb.FromLabelsToLabelAdapters(
						labels.FromStrings("__name__", "http_requests_total", "job", "test")),
					Samples: []mimirpb.Sample{{TimestampMs: 1, Value: 42.0}},
				},
			},
		},
	}

	wreqCtx := user.InjectOrgID(ctx, tenantID)
	require.NoError(t, writer.WriteSync(wreqCtx, topic, 0, tenantID, req))

	// Wait for the head to receive the series.
	require.Eventually(t, func() bool {
		db, err := rc.getOrOpenTSDB(tenantID, 0)
		if err != nil || db == nil {
			return false
		}
		return db.Head().NumSeries() > 0
	}, 10*time.Second, 100*time.Millisecond, "partition 0 head should contain pushed series")

	// Partition 1 was owned but received no data — its TSDB should
	// still be openable lazily but contain no series.
	db, err := rc.getOrOpenTSDB(tenantID, 1)
	require.NoError(t, err)
	if db != nil {
		assert.Equal(t, uint64(0), db.Head().NumSeries())
	}

	// Partition 2 is not owned: getOrOpenTSDB returns nil.
	db, err = rc.getOrOpenTSDB(tenantID, 2)
	require.NoError(t, err)
	assert.Nil(t, db)
}

// TestReadcache_ResumesFromStoredOffsetAcrossRestart reproduces the
// mimir-dev-15 autoscaler-eviction data hole: a readcache that stops
// (pod restart/eviction) while records keep being produced must
// consume those records after it comes back on the same data volume,
// instead of rejoining at the Kafka live edge and skipping them
// forever. The stored offset file in DataDir is what makes the resume
// possible.
func TestReadcache_ResumesFromStoredOffsetAcrossRestart(t *testing.T) {
	const (
		tenantID = "user-1"
		topic    = "test-topic"
	)

	_, addr := testkafka.CreateCluster(t, 1, topic)

	cfg := newTestConfigNoKafka(t)
	cfg.KafkaTopic = topic
	cfg.OwnedPartitions = "0"

	var kafkaCfg ingest.KafkaConfig
	kafkaCfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
	kafkaCfg.Address = flagext.StringSliceCSV{addr}
	kafkaCfg.Topic = topic
	cfg.Kafka = kafkaCfg

	limits := validation.NewOverrides(validation.Limits{}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	writer := makeKafkaWriter(t, kafkaCfg, prometheus.NewPedanticRegistry())
	defer func() { _ = services.StopAndAwaitTerminated(ctx, writer) }()

	writeSample := func(name string, ts int64) {
		req := &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: mimirpb.FromLabelsToLabelAdapters(
							labels.FromStrings("__name__", name, "job", "test")),
						Samples: []mimirpb.Sample{{TimestampMs: ts, Value: 1.0}},
					},
				},
			},
		}
		require.NoError(t, writer.WriteSync(user.InjectOrgID(ctx, tenantID), topic, 0, tenantID, req))
	}

	// First incarnation: consume one record, which persists the offset
	// file into DataDir.
	rc1, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, rc1))

	writeSample("series_before_restart", 1000)
	require.Eventually(t, func() bool {
		db, err := rc1.getOrOpenTSDB(tenantID, 0)
		return err == nil && db != nil && db.Head().NumSeries() == 1
	}, 10*time.Second, 100*time.Millisecond, "first incarnation should consume the pre-restart record")

	require.NoError(t, services.StopAndAwaitTerminated(ctx, rc1))
	require.FileExists(t, filepath.Join(cfg.DataDir, "partition-0.offset.json"),
		"offset file must survive a process stop so the next incarnation can resume")

	// Produce while the readcache is down: this is the eviction window.
	writeSample("series_during_downtime", 2000)

	// Second incarnation on the same DataDir: must resume from the
	// stored offset and consume the downtime record. With the old
	// always-join-at-end behaviour this record was skipped forever.
	rc2, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, rc2))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, rc2) }()

	require.Eventually(t, func() bool {
		db, err := rc2.getOrOpenTSDB(tenantID, 0)
		if err != nil || db == nil {
			return false
		}
		// WAL replay restores series_before_restart; the resumed reader
		// must add series_during_downtime on top.
		return db.Head().NumSeries() == 2
	}, 20*time.Second, 100*time.Millisecond, "second incarnation must consume the record produced during downtime")
}

// TestReadcache_OwnershipLossDeletesOffsetFile ensures losing a
// partition (rebalancer reassignment -> removePartition/freeze)
// removes the stored offset file, so a later re-acquisition of the
// same partition joins at the live edge instead of replaying records
// an intermediate owner already ingested and still serves from its
// frozen epoch.
func TestReadcache_OwnershipLossDeletesOffsetFile(t *testing.T) {
	const (
		tenantID = "user-1"
		topic    = "test-topic"
	)

	_, addr := testkafka.CreateCluster(t, 1, topic)

	cfg := newTestConfigNoKafka(t)
	cfg.KafkaTopic = topic
	cfg.OwnedPartitions = "0"

	var kafkaCfg ingest.KafkaConfig
	kafkaCfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
	kafkaCfg.Address = flagext.StringSliceCSV{addr}
	kafkaCfg.Topic = topic
	cfg.Kafka = kafkaCfg

	limits := validation.NewOverrides(validation.Limits{}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	writer := makeKafkaWriter(t, kafkaCfg, prometheus.NewPedanticRegistry())
	defer func() { _ = services.StopAndAwaitTerminated(ctx, writer) }()

	rc, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, rc))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, rc) }()

	req := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: mimirpb.FromLabelsToLabelAdapters(
						labels.FromStrings("__name__", "some_series", "job", "test")),
					Samples: []mimirpb.Sample{{TimestampMs: 1000, Value: 1.0}},
				},
			},
		},
	}
	require.NoError(t, writer.WriteSync(user.InjectOrgID(ctx, tenantID), topic, 0, tenantID, req))

	offsetFile := filepath.Join(cfg.DataDir, "partition-0.offset.json")
	require.Eventually(t, func() bool {
		_, err := os.Stat(offsetFile)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond, "offset file should be written once consumption starts")

	require.NoError(t, rc.removePartition(0))
	assert.NoFileExists(t, offsetFile, "losing ownership must delete the offset file")
}

// TestReadcache_FreshAcquisitionIgnoresStaleOffsetFile reproduces the
// mimir-dev-15 backlog-replay incident: a pod goes down while owning a
// partition (leaving its offset file on the PVC), the rebalancer
// reassigns the partition elsewhere while the pod is down (so the
// freeze-time file deletion never runs), and hours later the partition
// is moved back. The stale file must NOT be trusted: a partition
// acquired after the initial startup reconciliation joins at the live
// edge, and the stale file is deleted.
func TestReadcache_FreshAcquisitionIgnoresStaleOffsetFile(t *testing.T) {
	const (
		tenantID = "user-1"
		topic    = "test-topic"
	)

	_, addr := testkafka.CreateCluster(t, 2, topic)

	cfg := newTestConfigNoKafka(t)
	cfg.KafkaTopic = topic
	cfg.OwnedPartitions = "0"

	var kafkaCfg ingest.KafkaConfig
	kafkaCfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
	kafkaCfg.Address = flagext.StringSliceCSV{addr}
	kafkaCfg.Topic = topic
	cfg.Kafka = kafkaCfg

	limits := validation.NewOverrides(validation.Limits{}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	writer := makeKafkaWriter(t, kafkaCfg, prometheus.NewPedanticRegistry())
	defer func() { _ = services.StopAndAwaitTerminated(ctx, writer) }()

	writeSample := func(name string, ts int64) {
		req := &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: mimirpb.FromLabelsToLabelAdapters(
							labels.FromStrings("__name__", name, "job", "test")),
						Samples: []mimirpb.Sample{{TimestampMs: ts, Value: 1.0}},
					},
				},
			},
		}
		require.NoError(t, writer.WriteSync(user.InjectOrgID(ctx, tenantID), topic, 1, tenantID, req))
	}

	// Records produced to partition 1 before this pod (re-)acquires it:
	// offsets 0 and 1. A trusted stale file (below) would replay offset 1.
	writeSample("stale_series_1", 1000)
	writeSample("stale_series_2", 2000)

	// Plant a stale offset file from a previous ownership stint, claiming
	// offset 0 was the last consumed. The format mirrors
	// pkg/storage/ingest's offsetFileData.
	staleOffsetFile := filepath.Join(cfg.DataDir, "partition-1.offset.json")
	require.NoError(t, os.MkdirAll(cfg.DataDir, 0o755))
	require.NoError(t, os.WriteFile(staleOffsetFile, []byte(`{"version":1,"partition_id":1,"offset":0}`), 0o644))

	// Start owning only partition 0: the startup reconciliation completes
	// without partition 1, so a later acquisition of it is fresh.
	rc, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, rc))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, rc) }()

	// The rebalancer moves partition 1 onto this pod.
	require.NoError(t, rc.addPartition(ctx, 1))
	assert.NoFileExists(t, staleOffsetFile,
		"a fresh acquisition must delete the stale offset file instead of resuming from it")

	// Only records produced from now on are consumed.
	writeSample("fresh_series", 3000)
	require.Eventually(t, func() bool {
		db, err := rc.getOrOpenTSDB(tenantID, 1)
		return err == nil && db != nil && db.Head().NumSeries() > 0
	}, 20*time.Second, 100*time.Millisecond, "the fresh acquisition must consume records produced after it joined")

	db, err := rc.getOrOpenTSDB(tenantID, 1)
	require.NoError(t, err)
	require.NotNil(t, db)
	assert.Equal(t, uint64(1), db.Head().NumSeries(),
		"records produced before the fresh acquisition must not be replayed from the stale offset")
}

// makeKafkaWriter is a small helper that constructs an ingest writer
// for tests. It hides the boilerplate of NewWriter (which expects a
// flag-derived config and a registerer).
func makeKafkaWriter(t *testing.T, cfg ingest.KafkaConfig, reg prometheus.Registerer) *ingest.Writer {
	t.Helper()
	w := ingest.NewWriter(cfg, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), w))
	return w
}

// newTestConfigNoKafka is like newTestConfig(t, false, 0) but inlined
// to make the dependency between the test and the surrounding fixture
// explicit.
func newTestConfigNoKafka(t *testing.T) Config {
	t.Helper()
	return newTestConfig(t, false, 0)
}

func TestUnregisterPartitionMetricsRegistry(t *testing.T) {
	main := prometheus.NewPedanticRegistry()
	partitionReg := prometheus.NewRegistry()
	require.NoError(t, main.Register(partitionReg))

	rc := &Readcache{reg: main, logger: log.NewNopLogger()}
	rc.unregisterPartitionMetricsRegistry(partitionReg, 1)

	partitionReg2 := prometheus.NewRegistry()
	require.NoError(t, main.Register(partitionReg2))
}

func TestStartKafkaReader_RollsBackMetricsRegistryOnFailure(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	cfg := newTestConfigNoKafka(t)

	var kafkaCfg ingest.KafkaConfig
	kafkaCfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
	kafkaCfg.Address = flagext.StringSliceCSV{"127.0.0.1:1"}
	kafkaCfg.Topic = "test-topic"
	cfg.Kafka = kafkaCfg

	rc := &Readcache{
		cfg:    cfg,
		reg:    reg,
		logger: log.NewNopLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := newPartitionState(0)
	require.Error(t, rc.startKafkaReader(ctx, p))
	assert.Nil(t, p.reader)
	assert.Nil(t, p.readerMetrics)

	readerPartitionSeries := 0
	metrics, err := reg.Gather()
	if err == nil {
		for _, fam := range metrics {
			for _, m := range fam.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "reader_partition" {
						readerPartitionSeries++
					}
				}
			}
		}
	}
	assert.Zero(t, readerPartitionSeries, "failed start must unregister per-partition metrics from the main registerer")
	assert.NoError(t, err, "failed start must not leave duplicate per-partition collectors on the main registerer")
}
