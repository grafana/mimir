// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/ingest"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestIngester_Compartments_RegistersPartitionInReadCompartmentRing(t *testing.T) {
	ctx := context.Background()

	kvStore, closer := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { require.NoError(t, closer.Close()) })

	const readCompartmentID = 1

	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.InstanceID = "ingester-zone-a-0" // Maps to partition 0.
	cfg.IngesterPartitionRing.KVStore.Mock = kvStore
	cfg.Compartments.Enabled = true
	cfg.Compartments.Read.NumCompartments = 2
	cfg.Compartments.Write.NumCompartments = 1
	cfg.ReadCompartmentID = readCompartmentID

	overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	ingester, _, _ := createTestCompartmentsIngester(t, &cfg, overrides, nil, util_test.NewTestingLogger(t))

	require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
	})

	countPartitions := func(key string) int {
		val, err := kvStore.Get(ctx, key)
		require.NoError(t, err)
		if val == nil {
			return 0
		}
		return len(val.(*ring.PartitionRingDesc).Partitions)
	}

	// The partition is registered under the read compartment's ring key.
	compartmentKey := compartments.ReadCompartmentRingKey(readCompartmentID, PartitionRingKey)
	test.Poll(t, 5*time.Second, 1, func() interface{} {
		return countPartitions(compartmentKey)
	})

	// Nothing is registered under the legacy (non-compartment) key.
	assert.Zero(t, countPartitions(PartitionRingKey))
}

// TestIngester_Compartments_ShouldConsumeReadCompartmentTopicAtStartup is the compartments counterpart of
// TestIngester_ShouldReplayPartitionAtStartupAndThenJoinTheRings: it replicates the same behaviour (the
// ingester replays its partition at startup after a restart and then joins the ingesters and partitions
// ring), but the ingester consumes its read compartment's topic from one Kafka cluster per write
// compartment, so the catch up only completes once it has replayed every write compartment's cluster.
func TestIngester_Compartments_ShouldConsumeReadCompartmentTopicAtStartup(t *testing.T) {
	const (
		readCompartmentID    = 1
		numWriteCompartments = 2
	)

	var (
		ctx                = context.Background()
		cfg                = defaultIngesterTestConfig(t)
		reg                = prometheus.NewRegistry()
		fetchRequestsCount = atomic.NewInt64(0)
		series1            = mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:  mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "series_1")),
			Samples: []mimirpb.Sample{{TimestampMs: 1000, Value: 10}},
		}}
		series2 = mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:  mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "series_2")),
			Samples: []mimirpb.Sample{{TimestampMs: 1000, Value: 10}},
		}}
	)

	const expectedSeriesToReplay = 2

	cfg.Compartments.Enabled = true
	cfg.Compartments.Read.NumCompartments = 2
	cfg.Compartments.Write.NumCompartments = numWriteCompartments
	cfg.ReadCompartmentID = readCompartmentID

	// Configure the ingester to frequently run its internal services.
	cfg.UpdateIngesterOwnedSeries = true
	cfg.OwnedSeriesUpdateInterval = 100 * time.Millisecond
	cfg.ActiveSeriesMetrics.UpdatePeriod = 100 * time.Millisecond
	cfg.limitMetricsUpdatePeriod = 100 * time.Millisecond

	// Configure the TSDB Head compaction interval to be greater than the high frequency
	// expected while starting up.
	const headCompactionIntervalWhileStarting = 100 * time.Millisecond
	const headCompactionIntervalWhileRunning = time.Minute
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = headCompactionIntervalWhileRunning
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalWhileStarting = headCompactionIntervalWhileStarting
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled = false

	// Create the ingester, consuming the read compartment's topic from one Kafka cluster per write compartment.
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	ingester, kafkaClusters, watcher := createTestCompartmentsIngester(t, &cfg, overrides, reg, util_test.NewTestingLogger(t))
	require.Len(t, kafkaClusters, numWriteCompartments)

	consumedTopic := compartments.ReplaceReadCompartment(cfg.IngestStorageConfig.KafkaConfig.Topic, cfg.ReadCompartmentID)

	// Mock every write compartment's Kafka cluster to:
	// - Count the Fetch requests.
	// - Mock the ListOffsets response, returning the offset expected once the ingester can be
	//   considered having successfully caught up with that cluster.
	for _, cluster := range kafkaClusters {
		cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			fetchRequestsCount.Inc()

			return nil, nil, false
		})

		cluster.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			// Mock only requests for the partition "end" offset (identified by special timestamp -1).
			req := kreq.(*kmsg.ListOffsetsRequest)
			for _, topic := range req.Topics {
				for _, partition := range topic.Partitions {
					if partition.Timestamp != -1 {
						return nil, nil, false
					}
				}
			}

			res := req.ResponseKind().(*kmsg.ListOffsetsResponse)
			res.Topics = []kmsg.ListOffsetsResponseTopic{{
				Topic: consumedTopic,
				Partitions: []kmsg.ListOffsetsResponseTopicPartition{{
					Partition: ingester.ingestPartitionID,
					ErrorCode: 0,
					Offset:    expectedSeriesToReplay,
				}},
			}}

			return res, nil, true
		})
	}

	// Create a Kafka writer per write compartment and then write a series to each cluster.
	partitionID, err := ingest.IngesterPartitionID(cfg.IngesterRing.InstanceID)
	require.NoError(t, err)

	writers := make([]*ingest.Writer, numWriteCompartments)
	for writeCompartmentID, cluster := range kafkaClusters {
		writers[writeCompartmentID] = startClusterWriter(ctx, t, cfg.IngestStorageConfig.KafkaConfig, cluster)
		require.NoError(t, writers[writeCompartmentID].WriteSync(ctx, consumedTopic, partitionID, userID, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{series1}, Source: mimirpb.API}))
	}

	// Add the ingester in LEAVING state in the ring, in order to simulate an ingester restart.
	// This will make the owned series tracker to correctly work at ingester startup.
	require.NoError(t, cfg.IngesterRing.KVStore.Mock.CAS(context.Background(), IngesterRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := ring.GetOrCreateRingDesc(in)
		desc.AddIngester(
			cfg.IngesterRing.InstanceID,
			cfg.IngesterRing.InstanceAddr,
			cfg.IngesterRing.InstanceZone,
			cfg.IngesterRing.customTokenGenerator().GenerateTokens(512, nil),
			ring.LEAVING,
			time.Now(),
			false,
			time.Time{},
			nil)

		return desc, true, nil
	}))

	// Add the partition and owner in the read compartment's partition ring, to simulate an ingester restart.
	require.NoError(t, cfg.IngesterPartitionRing.KVStore.Mock.CAS(context.Background(), compartments.ReadCompartmentRingKey(readCompartmentID, PartitionRingKey), func(in interface{}) (out interface{}, retry bool, err error) {
		desc := ring.GetOrCreatePartitionRingDesc(in)
		desc.AddPartition(partitionID, ring.PartitionActive, time.Now())
		desc.AddOrUpdateOwner(cfg.IngesterRing.InstanceID, ring.OwnerDeleted, partitionID, time.Now())

		return desc, true, nil
	}))

	// Start the ingester.
	require.NoError(t, ingester.StartAsync(ctx))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
	})

	// Wait until every write compartment's Kafka cluster received a Fetch request.
	test.Poll(t, 5*time.Second, true, func() interface{} {
		return fetchRequestsCount.Load() >= int64(numWriteCompartments)
	})

	// At this point we expect the ingester is stuck in starting state while catching up
	// replaying the partition from every write compartment's cluster. The catchup will not
	// succeed until we'll write the next series to each cluster because we've mocked Kafka to
	// return a "last produced offset" in the future.

	// We expect the following internal services to be already running at this point.
	assert.Equal(t, services.Running, ingester.compactionService.State())
	assert.Equal(t, services.Running, ingester.ownedSeriesService.State())
	assert.Equal(t, services.Running, ingester.metricsUpdaterService.State())
	assert.Equal(t, services.Running, ingester.metadataPurgerService.State())

	// We expect the TSDB Head compaction to run while replaying from Kafka.
	assert.Eventually(t, func() bool {
		return testutil.ToFloat64(ingester.metrics.compactionsTriggered) > 0
	}, time.Second, 10*time.Millisecond)

	// We expect metrics to be updated. Every write compartment's cluster replays the same series, so
	// the ingester has a single active (and owned) series.
	test.Poll(t, time.Second, nil, func() interface{} {
		return testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="%s"} 1

				# HELP cortex_ingester_owned_series Number of currently owned series per user.
				# TYPE cortex_ingester_owned_series gauge
				cortex_ingester_owned_series{user="%s"} 1
			`, userID, userID)), "cortex_ingester_active_series", "cortex_ingester_owned_series")
	})

	// We expect the ingester run TSDB Head compaction at higher frequency while catching up.
	firstInterval, standardInterval := ingester.compactionServiceInterval(time.Now(), ingester.instanceRing.Zones())
	assert.Equal(t, headCompactionIntervalWhileStarting, firstInterval)
	assert.Equal(t, headCompactionIntervalWhileStarting, standardInterval)

	assert.Eventually(t, func() bool {
		return testutil.ToFloat64(ingester.metrics.compactionsTriggered) > 0
	}, 5*time.Second, 10*time.Millisecond)

	// Since the ingester is still replaying the partition we expect it to be in starting state.
	assert.Equal(t, services.Starting, ingester.State())
	assert.Equal(t, services.New, ingester.lifecycler.State())

	// Write one more request to each write compartment's cluster. This will cause the ingester to
	// consume up until the "last produced offset" returned by the mocked Kafka on every cluster, and so
	// consider the catch up complete.
	for writeCompartmentID := range kafkaClusters {
		require.NoError(t, writers[writeCompartmentID].WriteSync(ctx, consumedTopic, partitionID, userID, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{series2}, Source: mimirpb.API}))
	}

	// We expect the ingester to catch up, and then switch to Running state.
	test.Poll(t, 5*time.Second, services.Running, func() interface{} {
		return ingester.State()
	})
	assert.Equal(t, services.Running, ingester.lifecycler.State())

	assert.Eventually(t, func() bool {
		// Use the ring to check the instance state instead of the lifecycler interface.
		rs, err := ingester.instanceRing.GetAllHealthy(ring.Write)
		if err != nil || len(rs.Instances) == 0 {
			return false
		}
		return rs.Instances[0].State == ring.ACTIVE
	}, time.Second, 10*time.Millisecond)

	assert.Eventually(t, func() bool {
		return slices.Equal(
			watcher.PartitionRing().PartitionOwnerIDs(partitionID),
			[]string{ingester.cfg.IngesterRing.InstanceID})
	}, time.Second, 10*time.Millisecond)
}

// TestIngester_Compartments_QueryStream_ReadConsistency is the compartments counterpart of
// TestIngester_QueryStream_IngestStorageReadConsistency. It writes a distinct series to each write
// compartment's Kafka cluster and checks the read consistency guarantees across all of them.
func TestIngester_Compartments_QueryStream_ReadConsistency(t *testing.T) {
	const numWriteCompartments = 2

	tests := map[string]struct {
		readConsistencyLevel   string
		readConsistencyOffsets map[int32]int64
		expectedQueriedSeries  int
	}{
		"eventual read consistency": {
			readConsistencyLevel: api.ReadConsistencyEventual,

			// We expect no series because the query didn't wait for the read consistency to be guaranteed.
			expectedQueriedSeries: 0,
		},
		"strong read consistency without offsets": {
			readConsistencyLevel: api.ReadConsistencyStrong,

			// We expect the query to wait for read consistency across every write compartment's cluster.
			expectedQueriedSeries: numWriteCompartments,
		},
		"strong read consistency with offsets": {
			readConsistencyLevel: api.ReadConsistencyStrong,

			// With compartments enabled the encoded offsets are ignored (per-compartment offsets are not
			// propagated by the query path yet), so the negative-offset trick does not short-circuit the
			// wait: the query waits until the last produced offset of every cluster and returns all series.
			readConsistencyOffsets: map[int32]int64{0: -2},
			expectedQueriedSeries:  numWriteCompartments,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			var (
				cfg    = defaultIngesterTestConfig(t)
				limits = defaultLimitsTestConfig()
				reg    = prometheus.NewRegistry()
				ctx    = context.Background()
			)

			limits.IngestStorageReadConsistency = testData.readConsistencyLevel

			cfg.Compartments.Enabled = true
			cfg.Compartments.Read.NumCompartments = 2
			cfg.Compartments.Write.NumCompartments = numWriteCompartments
			cfg.ReadCompartmentID = 1

			// Create the ingester.
			overrides := validation.NewOverrides(limits, nil)
			ingester, kafkaClusters, _ := createTestCompartmentsIngester(t, &cfg, overrides, reg, util_test.NewTestingLogger(t))
			require.Len(t, kafkaClusters, numWriteCompartments)

			// Mock every write compartment's Kafka cluster to fail the Fetch operation until we unblock it
			// later in the test. If read consistency is "eventual" then these failures shouldn't affect
			// queries, but if it's "strong" then a query shouldn't succeed until the Fetch requests succeed.
			failFetch := atomic.NewBool(true)
			for _, cluster := range kafkaClusters {
				cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
					cluster.KeepControl()

					if failFetch.Load() {
						return nil, errors.New("mocked error"), true
					}

					return nil, nil, false
				})
			}

			// Start the ingester (after the Kafka clusters have been mocked).
			require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
			})

			// Wait until the ingester is healthy.
			test.Poll(t, 1*time.Second, 1, func() interface{} {
				return healthyInstancesCount(ingester.instanceRing)
			})

			// Write a distinct series to each write compartment's Kafka cluster.
			consumedTopic := compartments.ReplaceReadCompartment(cfg.IngestStorageConfig.KafkaConfig.Topic, cfg.ReadCompartmentID)
			partitionID, err := ingest.IngesterPartitionID(cfg.IngesterRing.InstanceID)
			require.NoError(t, err)
			for writeCompartmentID, cluster := range kafkaClusters {
				writer := startClusterWriter(ctx, t, cfg.IngestStorageConfig.KafkaConfig, cluster)
				series := mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
					Labels:  mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, fmt.Sprintf("series_wc_%d", writeCompartmentID))),
					Samples: []mimirpb.Sample{{TimestampMs: 1000, Value: 10}},
				}}
				require.NoError(t, writer.WriteSync(ctx, consumedTopic, partitionID, userID, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{series}, Source: mimirpb.API}))
			}

			// Run a query in a separate goroutine and collect the result.
			var (
				queryRes    model.Matrix
				queryWg     = sync.WaitGroup{}
				queryIssued = make(chan struct{})
			)

			queryWg.Add(1)
			go func() {
				defer queryWg.Done()

				// Ensure the query will eventually terminate.
				queryCtx, cancel := context.WithTimeout(user.InjectOrgID(ctx, userID), 5*time.Second)
				defer cancel()

				// Inject the requested offsets (if any).
				if len(testData.readConsistencyOffsets) > 0 {
					queryCtx = api.ContextWithReadConsistencyEncodedOffsets(queryCtx, api.EncodeOffsetsV1(testData.readConsistencyOffsets))
				}

				close(queryIssued)
				queryRes, _, err = runTestQuery(queryCtx, t, ingester, labels.MatchRegexp, model.MetricNameLabel, "series_wc_.*")
				require.NoError(t, err)
			}()

			// Wait until the query is issued in the dedicated goroutine and then unblock the Fetch.
			<-queryIssued
			failFetch.Store(false)

			// Wait until the query returns.
			queryWg.Wait()

			assert.Len(t, queryRes, testData.expectedQueriedSeries)
		})
	}
}

// createTestCompartmentsIngester creates an ingester configured for compartments, consuming its read
// compartment's topic from one Kafka cluster per write compartment. It returns the ingester, the
// per-write-compartment Kafka clusters (indexed by write compartment ID), and the read compartment
// partition ring watcher.
func createTestCompartmentsIngester(t *testing.T, ingesterCfg *Config, overrides *validation.Overrides, reg prometheus.Registerer, logger log.Logger) (*Ingester, []*kfake.Cluster, *ring.PartitionRingWatcher) {
	ctx := context.Background()
	defaultConfig := defaultIngesterTestConfig(t)

	require.True(t, ingesterCfg.Compartments.Enabled, "compartments must be enabled")
	if logger == nil {
		logger = log.NewNopLogger()
	}

	ingestersRing := createAndStartRing(t, ingesterCfg.IngesterRing.ToRingConfig())

	ingesterCfg.PushGrpcMethodEnabled = false
	ingesterCfg.IngestStorageConfig.Enabled = true
	// The topic is parameterised by read compartment ID; the ingester consumes its own read compartment's
	// topic from every write compartment's cluster.
	ingesterCfg.IngestStorageConfig.KafkaConfig.Topic = "mimir-rc-" + compartments.ReadCompartmentIDPlaceholder
	ingesterCfg.IngestStorageConfig.KafkaConfig.LastProducedOffsetPollInterval = 100 * time.Millisecond

	if ingesterCfg.IngesterRing.InstanceID == "" || ingesterCfg.IngesterRing.InstanceID == defaultConfig.IngesterRing.InstanceID {
		// The ingest storage requires the ingester ID to have a well known format.
		ingesterCfg.IngesterRing.InstanceID = "ingester-zone-a-0"
	}

	// Create the partition ring store.
	kv := ingesterCfg.IngesterPartitionRing.KVStore.Mock
	if kv == nil {
		var closer io.Closer
		kv, closer = consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { assert.NoError(t, closer.Close()) })
		ingesterCfg.IngesterPartitionRing.KVStore.Mock = kv
	}
	ingesterCfg.IngesterPartitionRing.MinOwnersDuration = 0
	ingesterCfg.IngesterPartitionRing.lifecyclerPollingInterval = 10 * time.Millisecond

	if ingesterCfg.BlocksStorageConfig.TSDB.Dir == "" || ingesterCfg.BlocksStorageConfig.TSDB.Dir == defaultConfig.BlocksStorageConfig.TSDB.Dir {
		ingesterCfg.BlocksStorageConfig.TSDB.Dir = t.TempDir()
	}
	ingesterCfg.BlocksStorageConfig.Bucket.Backend = "filesystem"
	ingesterCfg.BlocksStorageConfig.Bucket.Filesystem.Directory = t.TempDir()
	ingesterCfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled = false

	// Run one Kafka cluster per write compartment, all seeded with the read compartment's topic the
	// ingester consumes. The clusters use consecutive ports so a single templated address reaches them all.
	consumedTopic := compartments.ReplaceReadCompartment(ingesterCfg.IngestStorageConfig.KafkaConfig.Topic, ingesterCfg.ReadCompartmentID)
	addressTemplate, clusters := startTemplatableKafkaClusters(t, 10, ingesterCfg.Compartments.Write.NumCompartments, consumedTopic)
	ingesterCfg.IngestStorageConfig.KafkaConfig.Address = flagext.StringSliceCSV{addressTemplate}

	// Create and start the partition ring watcher on the ingester's read compartment ring.
	prw := ring.NewPartitionRingWatcher(
		compartments.ReadCompartmentRingName(ingesterCfg.ReadCompartmentID, PartitionRingName),
		compartments.ReadCompartmentRingKey(ingesterCfg.ReadCompartmentID, PartitionRingKey),
		kv, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, prw))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, prw))
	})

	ingester, err := New(*ingesterCfg, overrides, ingestersRing, prw, nil, nil, reg, logger)
	require.NoError(t, err)

	return ingester, clusters, prw
}

// startTemplatableKafkaClusters starts numClusters single-broker fake Kafka clusters seeded with the
// given topic, each listening on a consecutive port (base, base+1, ...) so that a single
// "<write-compartment-id>"-templated address resolves to all of them. It returns the address template
// and the clusters indexed by write compartment ID.
func startTemplatableKafkaClusters(t *testing.T, numPartitions int32, numClusters int, topic string) (addressTemplate string, clusters []*kfake.Cluster) {
	require.GreaterOrEqual(t, numClusters, 1)
	require.LessOrEqual(t, numClusters, 10, "templatable ports support up to 10 clusters")

	for attempt := 0; ; attempt++ {
		require.Less(t, attempt, 100, "could not find free consecutive ports for the Kafka clusters")

		// Derive a candidate base port that is a multiple of 10, so the cluster ports differ only in
		// their last digit and a single "<write-compartment-id>" template resolves to all of them.
		seed, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		base := (seed.Addr().(*net.TCPAddr).Port / 10) * 10
		require.NoError(t, seed.Close())
		if base < 1024 || base+numClusters > 65535 {
			continue
		}

		// Pre-bind a listener per cluster so we own the ports (no bind race); the clusters accept on them.
		listeners := make([]net.Listener, 0, numClusters)
		for w := 0; w < numClusters; w++ {
			ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+w))
			if err != nil {
				break
			}
			listeners = append(listeners, ln)
		}
		if len(listeners) < numClusters {
			for _, ln := range listeners {
				_ = ln.Close()
			}
			continue
		}

		clusters = make([]*kfake.Cluster, numClusters)
		for w := 0; w < numClusters; w++ {
			cluster, addr := testkafka.CreateCluster(t, numPartitions, topic, testkafka.WithListener(listeners[w]))
			require.Equal(t, fmt.Sprintf("127.0.0.1:%d", base+w), addr)
			clusters[w] = cluster
		}

		return fmt.Sprintf("127.0.0.1:%d%s", base/10, compartments.WriteCompartmentIDPlaceholder), clusters
	}
}

// startClusterWriter starts a Kafka writer that inherits baseKafkaCfg but targets the given cluster.
func startClusterWriter(ctx context.Context, t *testing.T, baseKafkaCfg ingest.KafkaConfig, cluster *kfake.Cluster) *ingest.Writer {
	kafkaCfg := baseKafkaCfg
	kafkaCfg.Address = flagext.StringSliceCSV{cluster.ListenAddrs()[0]}

	writer := ingest.NewWriter(kafkaCfg, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, writer))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, writer))
	})
	return writer
}
