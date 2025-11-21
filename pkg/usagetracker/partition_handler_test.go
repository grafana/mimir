// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
	utiltest "github.com/grafana/mimir/pkg/util/test"
)

const eventsTopic = "usage-tracker-events"
const snapshotsMetadataTopic = "usage-tracker-snapshots-metadata"

var noRejected = []uint64{}

func TestPartitionHandler(t *testing.T) {
	const tenantID = "tenant-1"
	oneSeriesPerShard := func() []uint64 {
		series := make([]uint64, shards)
		for s := range series {
			series[s] = uint64(s)
		}
		return series
	}

	startPartitionHandlerTrackTwoSeriesAndShutDown := func(t *testing.T, h *partitionHandlerTestHelper) *partitionHandler {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		p := h.newHandler(t)
		require.NoError(t, services.StartAndAwaitRunning(ctx, p))
		requireTrackSeries(t, p, tenantID, []uint64{1, 2}, noRejected)
		h.expectEvents(t, expectedSeriesCreatedEvent{tenantID, []uint64{1, 2}})
		require.NoError(t, p.publishSnapshot(ctx))
		h.expectEvents(t, expectedSnapshotEvent{})
		require.NoError(t, services.StopAndAwaitTerminated(ctx, p))
		return p
	}

	t.Run("track series and publish events", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		h := newPartitionHandlerTestHelper(t)
		h.limiter[tenantID] = 4
		ph := h.newHandler(t)
		require.NoError(t, services.StartAndAwaitRunning(ctx, ph))

		requirePerTenantSeries(t, ph, map[string]uint64{})
		requireTrackSeries(t, ph, tenantID, []uint64{1, 2, 3}, []uint64{3}) // limit is 4, but current limit is 2 (because it's halfway)
		requirePerTenantSeries(t, ph, map[string]uint64{tenantID: 2})
		h.expectEvents(t, expectedSeriesCreatedEvent{tenantID, []uint64{1, 2}})

		require.NoError(t, testutil.CollectAndCompare(h.reg, strings.NewReader(`
			# HELP cortex_usage_tracker_active_series Number of active series tracker for each user.
        	# TYPE cortex_usage_tracker_active_series gauge
			cortex_usage_tracker_active_series{partition="0",pidx="0",user="tenant-1"} 2
		`), "cortex_usage_tracker_active_series"))

		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph))

		// No snapshot was published, yet the new partitionHandler consumes last events on creation.
		ph = h.newHandler(t)
		require.NoError(t, services.StartAndAwaitRunning(ctx, ph))
		requirePerTenantSeries(t, ph, map[string]uint64{tenantID: 2})
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph))
	})

	t.Run("events are only published when new series are tracked", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		h := newPartitionHandlerTestHelper(t)
		h.limiter[tenantID] = 4
		ph := h.newHandler(t)
		require.NoError(t, services.StartAndAwaitRunning(ctx, ph))

		requireTrackSeries(t, ph, tenantID, []uint64{1}, nil)
		requirePerTenantSeries(t, ph, map[string]uint64{tenantID: 1})
		h.expectEvents(t, expectedSeriesCreatedEvent{tenantID, []uint64{1}})

		// Track 1 again, this should not publish an event.
		requireTrackSeries(t, ph, tenantID, []uint64{1}, nil)

		// Track 2, this should publish an event.
		requireTrackSeries(t, ph, tenantID, []uint64{2}, nil)
		h.expectEvents(t, expectedSeriesCreatedEvent{tenantID, []uint64{2}})
	})

	t.Run("create and load snapshots", func(t *testing.T) {
		for maxFileSize, expectedSnapshotFiles := range map[int]int{
			10e3: 1,
			100:  4, // If the snapshot file change, we need to update this value.
		} {
			t.Run("maxFileSize="+strconv.Itoa(maxFileSize), func(t *testing.T) {
				t.Parallel()

				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()

				h := newPartitionHandlerTestHelper(t)
				h.limiter[tenantID] = 0 // no limit.

				// First partitionHandler is created, tracks some series and creates a snapshot.
				ph1 := h.newHandler(t, func(cfg *Config) {
					cfg.TargetSnapshotFileSizeBytes = maxFileSize
				})
				require.NoError(t, services.StartAndAwaitRunning(ctx, ph1))
				requireTrackSeries(t, ph1, tenantID, oneSeriesPerShard(), noRejected)
				h.expectEvents(t, expectedSeriesCreatedEvent{tenantID, oneSeriesPerShard()})
				require.NoError(t, ph1.publishSnapshot(ctx))
				for i := 0; i < expectedSnapshotFiles; i++ {
					h.expectEvents(t, expectedSnapshotEvent{})
				}
				require.NoError(t, services.StopAndAwaitTerminated(ctx, ph1))

				snapshotFiles := 0
				require.NoError(t, h.snapshotsBucket.Iter(context.Background(), "", func(name string) error {
					snapshotFiles++
					return nil
				}))
				require.Equal(t, expectedSnapshotFiles, snapshotFiles, "There should be exactly %d snapshot files created, got %d", expectedSnapshotFiles, snapshotFiles)

				// New partitionHandler is created, loads the snapshot and should contain the same series.
				// Since we generated one series per shard, we're checking here that all shards are loaded.
				ph2 := h.newHandler(t)
				require.NoError(t, services.StartAndAwaitRunning(ctx, ph2))
				requirePerTenantSeries(t, ph2, map[string]uint64{tenantID: shards})
				requireTrackSeries(t, ph2, tenantID, []uint64{shards / 2, shards * 2}, noRejected)

				// The series shards/2 is already tracked, so it should not be published again.
				h.expectEvents(t, expectedSeriesCreatedEvent{tenantID, []uint64{shards * 2}})
				requirePerTenantSeries(t, ph2, map[string]uint64{tenantID: shards + 1})

				require.NoError(t, services.StopAndAwaitTerminated(ctx, ph2))
			})
		}
	})

	t.Run("snapshot is loaded for the correct partition", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		h := newPartitionHandlerTestHelper(t)
		h.limiter[tenantID] = 0 // no limit.

		// First partitionHandler is created, tracks some series and creates a snapshot.
		ph1 := h.newHandler(t)
		require.NoError(t, services.StartAndAwaitRunning(ctx, ph1))
		requireTrackSeries(t, ph1, tenantID, oneSeriesPerShard(), noRejected)
		require.NoError(t, ph1.publishSnapshot(ctx))
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph1))

		// Different partition, it should not load the snapshot.
		differentPartitionHandler := h.newHandlerForPartitionID(t, 1)
		require.NoError(t, services.StartAndAwaitRunning(ctx, differentPartitionHandler))
		requirePerTenantSeries(t, differentPartitionHandler, map[string]uint64{})
		// Track series for a different tenant in this partition.
		differentSeriesSet := make([]uint64, 10*shards)
		for i := range differentSeriesSet {
			differentSeriesSet[i] = uint64(i + 1000) // Different series set.
		}
		requireTrackSeries(t, differentPartitionHandler, "different-tenant", differentSeriesSet, noRejected)
		requirePerTenantSeries(t, differentPartitionHandler, map[string]uint64{"different-tenant": 10 * shards})
		// Publish a snapshot for this partition.
		require.NoError(t, differentPartitionHandler.publishSnapshot(ctx))
		require.NoError(t, services.StopAndAwaitTerminated(ctx, differentPartitionHandler))

		snapshotFiles := 0
		require.NoError(t, h.snapshotsBucket.Iter(context.Background(), "", func(name string) error {
			snapshotFiles++
			return nil
		}))
		require.Equal(t, 2, snapshotFiles, "There should be exactly 2 snapshot files created, got %d", snapshotFiles)

		// New partitionHandler is created, it should load the snapshot for the correct partition.
		ph2 := h.newHandler(t)
		require.NoError(t, services.StartAndAwaitRunning(ctx, ph2))
		requirePerTenantSeries(t, ph2, map[string]uint64{tenantID: shards})
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph2))
	})

	t.Run("snapshot events are loaded continuously", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		h := newPartitionHandlerTestHelper(t)
		h.limiter[tenantID] = 10

		// First partitionHandler is created, tracks some series and creates a snapshot.
		ph1 := h.newHandler(t)
		require.NoError(t, services.StartAndAwaitRunning(ctx, ph1))
		requireTrackSeries(t, ph1, tenantID, []uint64{1, 2}, noRejected)
		h.expectEvents(t, expectedSeriesCreatedEvent{tenantID, []uint64{1, 2}})

		// New partitionHandler is created it loads the events.
		// We need to create this partitionHandler on a different instance because otherwise own events are ignored.
		ph2 := h.newHandler(t, func(cfg *Config) {
			cfg.InstanceRing.InstanceID = "usage-tracker-zone-b-1"
			cfg.InstanceRing.InstanceZone = "zone-b" // Doesn't necessarily need to be different.
		})
		consumedEvents := make(chan string, 10)
		ph2.onConsumeEvent = func(typ string) { consumedEvents <- typ }
		require.NoError(t, services.StartAndAwaitRunning(ctx, ph2))
		requirePerTenantSeries(t, ph2, map[string]uint64{tenantID: 2})
		// We artificially reset its state, let's say it didn't receive any requests for series 1 and 2 for a while.
		ph2.store.cleanup(time.Now().Add(2 * ph2.cfg.IdleTimeout))
		requirePerTenantSeries(t, ph2, map[string]uint64{})

		// ph2 publishes a snapshot of its current state. ph2 should sync.
		require.NoError(t, ph1.publishSnapshot(ctx))
		h.expectEvents(t, expectedSnapshotEvent{})

		// Wait until ph2 consumes the snapshot event.
		select {
		case typ := <-consumedEvents:
			require.Equal(t, eventTypeSnapshot, typ, "expected ph2 to consume snapshot event, got %s", typ)
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for ph2 to consume snapshot event")
		}
		requirePerTenantSeries(t, ph2, map[string]uint64{tenantID: 2})

		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph1))
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph2))
	})

	t.Run("events published after snapshot are correctly consumed", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		h := newPartitionHandlerTestHelper(t)
		h.limiter[tenantID] = 10

		// First partitionHandler is created, tracks some series and creates a snapshot.
		ph := h.newHandler(t)
		require.NoError(t, services.StartAndAwaitRunning(ctx, ph))
		requireTrackSeries(t, ph, tenantID, []uint64{1, 2}, noRejected)
		// Consume the series created events.
		h.expectEvents(t, expectedSeriesCreatedEvent{tenantID, []uint64{1, 2}})

		// Publish a snapshot manually.
		require.NoError(t, ph.publishSnapshot(ctx))

		// Expect a snapshot event, consume it.
		h.expectEvents(t, expectedSnapshotEvent{})

		// Track one more series after the snapshot.
		requireTrackSeries(t, ph, tenantID, []uint64{3}, noRejected)
		h.expectEvents(t, expectedSeriesCreatedEvent{tenantID, []uint64{3}})
		requirePerTenantSeries(t, ph, map[string]uint64{tenantID: 3})

		// Shutdown this partitionHandler (not really needed to be done yet).
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph))

		// New partitionHandler is created it should load 2 series from the snapshot and 1 series from the event.
		ph = h.newHandler(t)
		require.NoError(t, services.StartAndAwaitRunning(ctx, ph))
		requirePerTenantSeries(t, ph, map[string]uint64{tenantID: 3})
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph))
	})

	t.Run("only last snapshot and the events after it are processed", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		h := newPartitionHandlerTestHelper(t)
		h.limiter[tenantID] = 10

		// First partitionHandler is created, tracks some series and creates a snapshot.
		ph := h.newHandler(t)
		require.NoError(t, services.StartAndAwaitRunning(ctx, ph))
		requireTrackSeries(t, ph, tenantID, []uint64{1}, noRejected)
		requireTrackSeries(t, ph, tenantID, []uint64{2}, noRejected)
		requireTrackSeries(t, ph, tenantID, []uint64{3}, noRejected)
		// Consume the series created events.
		h.expectEvents(t,
			expectedSeriesCreatedEvent{tenantID, []uint64{1}},
			expectedSeriesCreatedEvent{tenantID, []uint64{2}},
			expectedSeriesCreatedEvent{tenantID, []uint64{3}},
		)
		// Publish a snapshot manually and consume the event.
		require.NoError(t, ph.publishSnapshot(ctx))
		h.expectEvents(t, expectedSnapshotEvent{})
		// Publish another event.
		requireTrackSeries(t, ph, tenantID, []uint64{4, 5}, noRejected)
		h.expectEvents(t, expectedSeriesCreatedEvent{tenantID, []uint64{4, 5}})
		requirePerTenantSeries(t, ph, map[string]uint64{tenantID: 5})

		// *** Reset the partitionHandler ***
		// This simulates some time passing and series expired.
		ph.store.cleanup(time.Now().Add(2 * ph.cfg.IdleTimeout))
		// We should now have 0 series again (tenant is fully deleted).
		requirePerTenantSeries(t, ph, map[string]uint64{})

		// Track different series.
		requireTrackSeries(t, ph, tenantID, []uint64{100, 200, 300}, noRejected)
		h.expectEvents(t, expectedSeriesCreatedEvent{tenantID, []uint64{100, 200, 300}})

		// Publish a a snapshot and consume the event.
		require.NoError(t, ph.publishSnapshot(ctx))
		h.expectEvents(t, expectedSnapshotEvent{})

		// Track some more series.
		requireTrackSeries(t, ph, tenantID, []uint64{400, 500}, noRejected)
		h.expectEvents(t, expectedSeriesCreatedEvent{tenantID, []uint64{400, 500}})

		// Shutdown this partitionHandler (not really needed to be done yet).
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph))

		// New partitionHandler is created it should load 3 series {100, 200, 300} from the snapshot and 2 series {300, 400} from the event.
		// We don't load the series {1, 2, 3, 4, 5} from the previous snapshot/events.
		ph = h.newHandler(t)
		require.NoError(t, services.StartAndAwaitRunning(ctx, ph))
		requirePerTenantSeries(t, ph, map[string]uint64{tenantID: 5})
		// Since the limit is 10 and currentLimit is 8 (after loading 5 series), by sending series {1, 2, 3, 4, 5, 100, 200, 300, 400, 500} we can accept 3 more series (up to 8 total), so we accept {1, 2, 3} and reject {4, 5}.
		requireTrackSeries(t, ph, tenantID, []uint64{1, 2, 3, 4, 5, 100, 200, 300, 400, 500}, []uint64{4, 5})

		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph))
	})

	t.Run("partition handler startup waits until snapshot is loaded", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		h := newPartitionHandlerTestHelper(t)
		h.limiter[tenantID] = 3

		// First partitionHandler is created, tracks some series and creates a snapshot and it's shut down.
		startPartitionHandlerTrackTwoSeriesAndShutDown(t, h)

		// New partitionHandler is created, loads the snapshot and should contain the same series.
		ph := h.newHandler(t)
		// This partitionHandler has a slow bucket reader.
		unblock := make(chan struct{})
		running := make(chan struct{})
		ph.snapshotsBucket = &slowBucket{ph.snapshotsBucket, unblock}

		go func() {
			assert.NoError(t, services.StartAndAwaitRunning(ctx, ph))
			close(running)
		}()
		// Wait a little bit.
		time.Sleep(500 * time.Millisecond)

		// While partition handler is starting the per-tenant metrics should not be registered yet.
		descs := make(chan *prometheus.Desc)
		go func() {
			h.reg.Describe(descs)
			close(descs)
		}()
		for desc := range descs {
			assert.NotContains(t, desc.String(), activeSeriesMetricName, "partition handler should not register per-tenant metrics while starting")
		}

		// Unblock the bucket reader, so it can load the snapshot.
		close(unblock)

		select {
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for partition handler to start")
		case <-running:
			requirePerTenantSeries(t, ph, map[string]uint64{tenantID: 2})
			require.NoError(t, testutil.CollectAndCompare(h.reg, strings.NewReader(`
			# HELP cortex_usage_tracker_active_series Number of active series tracker for each user.
			# TYPE cortex_usage_tracker_active_series gauge
			cortex_usage_tracker_active_series{partition="0",pidx="1",user="tenant-1"} 2
		`), "cortex_usage_tracker_active_series"))
		}

		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph))
	})

	t.Run("snapshot is loaded only once at startup (event is ignored)", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		h := newPartitionHandlerTestHelper(t)
		h.limiter[tenantID] = 3

		// First partitionHandler is created, tracks some series and creates a snapshot and it's shut down.
		startPartitionHandlerTrackTwoSeriesAndShutDown(t, h)

		// New partitionHandler is created, loads the snapshot and should contain the same series.
		ph := h.newHandler(t)
		// This partitionHandler has a slow bucket reader.
		getCalls := atomic.NewInt64(0)
		ph.snapshotsBucket = &getCounterBucketReader{ph.snapshotsBucket, getCalls}

		require.NoError(t, services.StartAndAwaitRunning(ctx, ph))
		requirePerTenantSeries(t, ph, map[string]uint64{tenantID: 2})
		require.Equal(t, int64(1), getCalls.Load(), "snapshot should be loaded only once at startup")

		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph))
	})

	t.Run("can skip snapshot loading through config", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		h := newPartitionHandlerTestHelper(t)
		h.limiter[tenantID] = 3

		// First partitionHandler is created, tracks some series and creates a snapshot and it's shut down.
		startPartitionHandlerTrackTwoSeriesAndShutDown(t, h)

		// New partitionHandler is created that skips snapshot loading at startup.
		ph := h.newHandler(t, func(cfg *Config) {
			cfg.SkipSnapshotLoadingAtStartup = true
		})

		getCalls := atomic.NewInt64(0)
		ph.snapshotsBucket = &getCounterBucketReader{ph.snapshotsBucket, getCalls}

		require.NoError(t, services.StartAndAwaitRunning(ctx, ph))
		requirePerTenantSeries(t, ph, map[string]uint64{tenantID: 2})
		require.Equal(t, int64(0), getCalls.Load(), "snapshot should not be loaded")

		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph))
	})

	t.Run("snapshot and events are not loaded if they are too old", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		h := newPartitionHandlerTestHelper(t)
		h.limiter[tenantID] = 3

		// First partitionHandler is created, tracks some series and creates a snapshot and it's shut down.
		startPartitionHandlerTrackTwoSeriesAndShutDown(t, h)

		// Wait 1 second for the snapshot to expire.
		time.Sleep(time.Second)

		// New partitionHandler is created, it has IdleTimeout set to 1 second, which already passed, so old snapshot should not be loaded.
		ph := h.newHandler(t, func(cfg *Config) { cfg.IdleTimeout = time.Second })
		// This partitionHandler has a slow bucket reader.
		getCalls := atomic.NewInt64(0)
		ph.snapshotsBucket = &getCounterBucketReader{ph.snapshotsBucket, getCalls}

		require.NoError(t, services.StartAndAwaitRunning(ctx, ph))
		requirePerTenantSeries(t, ph, map[string]uint64{}) // Should be empty.
		require.Equal(t, int64(0), getCalls.Load(), "snapshot should not be loaded because it's too old")

		require.NoError(t, services.StopAndAwaitTerminated(ctx, ph))
	})
}

func requireTrackSeries(t *testing.T, p *partitionHandler, id string, series, rejected []uint64) {
	t.Helper()
	actualRejected, err := p.store.trackSeries(t.Context(), id, series, time.Now())
	require.NoError(t, err, "failed to track series for tenant %s in partition %d", id, p.partitionID)
	if len(rejected) == 0 && len(actualRejected) == 0 {
		return // No series tracked, no rejected series.
	}
	require.ElementsMatch(t, rejected, actualRejected, "wrong rejected series for tenant %s in partition %d", id, p.partitionID)
}

func requirePerTenantSeries(t *testing.T, p *partitionHandler, expected map[string]uint64) {
	t.Helper()
	actual := make(map[string]uint64)
	p.store.mtx.RLock()
	for tenantID, tenant := range p.store.tenants {
		actual[tenantID] = tenant.series.Load()
	}
	p.store.mtx.RUnlock()
	require.Equal(t, expected, actual, "unexpected series counts in partition %d", p.partitionID)
}

func newPartitionHandlerTestHelper(t *testing.T) *partitionHandlerTestHelper {
	consulConfig := consul.Config{
		MaxCasRetries: 100,
		CasRetryDelay: 10 * time.Millisecond,
	}
	ikv, instanceKVCloser := consul.NewInMemoryClientWithConfig(ring.GetCodec(), consulConfig, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, instanceKVCloser.Close()) })
	pkv, partitionKVCloser := consul.NewInMemoryClientWithConfig(ring.GetPartitionRingCodec(), consulConfig, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, partitionKVCloser.Close()) })
	cluster := fakeKafkaCluster(t, eventsTopic, snapshotsMetadataTopic)

	cfg := newTestUsageTrackerConfig(t, "usage-tracker-zone-a-0", "zone-a", ikv, pkv, cluster)
	logger := utiltest.NewTestingLogger(t)

	reg := prometheus.NewRegistry()

	// Create Kafka writer for events storage.
	eventsKafkaWriter, err := ingest.NewKafkaWriterClient(cfg.EventsStorageWriter, 20, logger, prometheus.WrapRegistererWith(prometheus.Labels{"component": eventsKafkaWriterComponent}, reg))
	require.NoError(t, err)
	t.Cleanup(eventsKafkaWriter.Close)

	// Create Kafka writer for snapshots metadata storage.
	snapshotsKafkaWriter, err := ingest.NewKafkaWriterClient(cfg.SnapshotsMetadataWriter, 20, logger, prometheus.WrapRegistererWith(prometheus.Labels{"component": snapshotsKafkaWriterComponent}, reg))
	require.NoError(t, err)
	t.Cleanup(snapshotsKafkaWriter.Close)

	snapshotsBucket, err := bucket.NewClient(context.Background(), cfg.SnapshotsStorage, "usage-tracker-snapshots", logger, reg)
	require.NoError(t, err)

	eventsKafkaReader, err := ingest.NewKafkaReaderClient(cfg.EventsStorageReader, ingest.NewKafkaReaderClientMetrics(ingest.ReaderMetricsPrefix, "test-helper", prometheus.NewRegistry()), logger)
	require.NoError(t, err)
	eventsKafkaReader.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		cfg.EventsStorageWriter.Topic: {0: kgo.NewOffset().AtStart()},
	})
	t.Cleanup(eventsKafkaReader.Close)

	return &partitionHandlerTestHelper{
		cfg: cfg,

		ikv:                  ikv,
		pkv:                  pkv,
		eventsKafkaWriter:    eventsKafkaWriter,
		snapshotsKafkaWriter: snapshotsKafkaWriter,
		snapshotsBucket:      snapshotsBucket,
		limiter:              limiterMock{},
		logger:               logger,
		reg:                  reg,

		// Needed to run test helpers.
		eventsKafkaReader: eventsKafkaReader,
	}
}

type partitionHandlerTestHelper struct {
	cfg Config

	ikv                  *consul.Client
	pkv                  *consul.Client
	eventsKafkaWriter    *kgo.Client
	snapshotsKafkaWriter *kgo.Client
	snapshotsBucket      objstore.InstrumentedBucket
	limiter              limiterMock
	logger               *utiltest.TestingLogger
	reg                  *prometheus.Registry

	// Needed to run test helpers.
	eventsKafkaReader *kgo.Client

	pidx int // Identify each partitionHandler in the logs.
}

func (h *partitionHandlerTestHelper) newHandler(t *testing.T, maybeConfigure ...func(cfg *Config)) *partitionHandler {
	t.Helper()
	return h.newHandlerForPartitionID(t, 0, maybeConfigure...)
}

func (h *partitionHandlerTestHelper) newHandlerForPartitionID(t *testing.T, partitionID int32, maybeConfigure ...func(cfg *Config)) *partitionHandler {
	t.Helper()
	// Wrap logger to understand test logs easier.
	logger := log.With(h.logger, "pidx", h.pidx)
	// Wrap the registry to allow running two partitionHandler handlers for the same partitionHandler ID (this doesn't happen in usage-tracker).
	reg := prometheus.WrapRegistererWith(prometheus.Labels{"pidx": strconv.Itoa(h.pidx)}, h.reg)
	h.pidx++

	cfg := h.cfg
	for _, configure := range maybeConfigure {
		configure(&cfg)
	}
	instanceRing, err := NewInstanceRingClient(cfg.InstanceRing, logger, reg)
	require.NoError(t, err)
	startServiceAndStopOnCleanup(t, instanceRing)

	p, err := newPartitionHandler(partitionID, cfg, h.pkv, h.eventsKafkaWriter, h.snapshotsKafkaWriter, h.snapshotsBucket, h.limiter, logger, reg)
	require.NoError(t, err)
	return p
}

func (h *partitionHandlerTestHelper) expectEvents(t *testing.T, expectedEvents ...any) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	i := 0
	for ctx.Err() == nil && i < len(expectedEvents) {
		fetches := h.eventsKafkaReader.PollRecords(t.Context(), max(0, len(expectedEvents)-i))
		require.NoError(t, fetches.Err(), "could not fetch events from Kafka, current: %d, expected: %d", i, len(expectedEvents))
		fetches.EachRecord(func(record *kgo.Record) {
			switch expected := expectedEvents[i].(type) {
			case expectedSeriesCreatedEvent:
				typ, ok := recordHeader(record, eventTypeRecordHeader)
				require.True(t, ok, "expected a snapshot event at %d, got no type header", i)
				require.Equal(t, eventTypeSeriesCreated, typ, "expected a series created event at %d, got %s on offset %d", i, typ, record.Offset)

				var ev usagetrackerpb.SeriesCreatedEvent
				require.NoError(t, ev.Unmarshal(record.Value))
				got := expectedSeriesCreatedEvent{
					userID: ev.UserID,
					series: ev.SeriesHashes,
				}
				// Order doesn't matter, so sort the slices.
				slices.Sort(got.series)
				slices.Sort(expected.series)
				require.Equal(t, expected, got, "wrong snapshot event at %d offset %d", i, record.Offset)
			case expectedSnapshotEvent:
				typ, ok := recordHeader(record, eventTypeRecordHeader)
				require.True(t, ok, "expected a snapshot event at %d, got no type header", i)
				require.Equal(t, eventTypeSnapshot, typ, "expected a snapshot event at %d, got %s on offset %d", i, typ, record.Offset)
			}

			i++
		})
	}
}

type expectedSeriesCreatedEvent struct {
	userID string
	series []uint64
}

type expectedSnapshotEvent struct{}

type slowBucket struct {
	objstore.Bucket
	unblock chan struct{}
}

func (s *slowBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	select {
	case <-s.unblock:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.Bucket.Get(ctx, name)
}

type getCounterBucketReader struct {
	objstore.Bucket

	getCalls *atomic.Int64
}

func (s *getCounterBucketReader) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	s.getCalls.Inc()
	return s.Bucket.Get(ctx, name)
}
