// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
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

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/shutdownmarker"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestIngester_Start(t *testing.T) {
	t.Run("should replay the partition at startup and then join the ingesters and partitions ring", func(t *testing.T) {
		var (
			ctx                = context.Background()
			cfg                = defaultIngesterTestConfig(t)
			reg                = prometheus.NewRegistry()
			fetchRequestsCount = atomic.NewInt64(0)
			fetchShouldFail    = atomic.NewBool(true)
			series1            = mimirpb.PreallocTimeseries{
				TimeSeries: &mimirpb.TimeSeries{
					Labels:  mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "test")),
					Samples: []mimirpb.Sample{{TimestampMs: 1000, Value: 10}},
				},
			}
		)

		// Create the ingester.
		overrides, err := validation.NewOverrides(defaultLimitsTestConfig(), nil)
		require.NoError(t, err)
		ingester, kafkaCluster, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, reg)

		// Mock the Kafka cluster to fail the Fetch operation until we unblock it later in the test.
		kafkaCluster.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			kafkaCluster.KeepControl()
			fetchRequestsCount.Inc()

			if fetchShouldFail.Load() {
				return nil, errors.New("mocked error"), true
			}

			return nil, nil, false
		})

		// Create a Kafka writer and then write a series.
		writer := ingest.NewWriter(cfg.IngestStorageConfig.KafkaConfig, log.NewNopLogger(), nil)
		require.NoError(t, services.StartAndAwaitRunning(ctx, writer))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, writer))
		})

		partitionID, err := ingest.IngesterPartitionID(cfg.IngesterRing.InstanceID)
		require.NoError(t, err)
		require.NoError(t, writer.WriteSync(ctx, partitionID, userID, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{series1}, Source: mimirpb.API}))

		// Start the ingester.
		require.NoError(t, ingester.StartAsync(ctx))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
		})

		// Wait until the Kafka cluster received few Fetch requests.
		test.Poll(t, 5*time.Second, true, func() interface{} {
			return fetchRequestsCount.Load() > 2
		})

		// Since the mocked Kafka cluster is configured to fail any Fetch we expect the ingester hasn't
		// catched up yet, and it's still in Starting state.
		assert.Equal(t, services.Starting, ingester.State())
		assert.Empty(t, ingester.ingestPartitionWatcher.PartitionRing().PartitionOwnerIDs(partitionID))
		assert.Equal(t, services.New, ingester.lifecycler.State())

		// Unblock the Fetch requests. Now they will succeed.
		fetchShouldFail.Store(false)

		// We expect the ingester to catch up, and then switch to Running state.
		test.Poll(t, 5*time.Second, services.Running, func() interface{} {
			return ingester.State()
		})

		assert.Equal(t, services.Running, ingester.lifecycler.State())

		assert.Eventually(t, func() bool {
			return ingester.lifecycler.GetState() == ring.ACTIVE
		}, time.Second, 10*time.Millisecond)

		assert.Eventually(t, func() bool {
			return slices.Equal(
				ingester.ingestPartitionWatcher.PartitionRing().PartitionOwnerIDs(partitionID),
				[]string{ingester.cfg.IngesterRing.InstanceID})
		}, time.Second, 10*time.Millisecond)
	})
}

func TestIngester_QueryStream_IngestStorageReadConsistency(t *testing.T) {
	const (
		metricName = "series_1"
	)

	for _, readConsistency := range []string{api.ReadConsistencyEventual, api.ReadConsistencyStrong} {
		t.Run(fmt.Sprintf("read consistency = %s", readConsistency), func(t *testing.T) {
			var (
				cfg     = defaultIngesterTestConfig(t)
				limits  = defaultLimitsTestConfig()
				reg     = prometheus.NewRegistry()
				ctx     = context.Background()
				series1 = mimirpb.PreallocTimeseries{
					TimeSeries: &mimirpb.TimeSeries{
						Labels:  mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, metricName)),
						Samples: []mimirpb.Sample{{TimestampMs: 1000, Value: 10}},
					},
				}
			)

			limits.IngestStorageReadConsistency = readConsistency

			// Create the ingester.
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)
			ingester, kafkaCluster, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, reg)

			// Mock the Kafka cluster to fail the Fetch operation until we unblock it later in the test.
			// If read consistency is "eventual" then these failures shouldn't affect queries, but if it's set
			// to "strong" then a query shouldn't succeed until the Fetch requests succeed.
			failFetch := atomic.NewBool(true)

			kafkaCluster.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
				kafkaCluster.KeepControl()

				if failFetch.Load() {
					return nil, errors.New("mocked error"), true
				}

				return nil, nil, false
			})

			// Start the ingester (after the Kafka cluster has been mocked).
			require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
			})

			// Wait until the ingester is healthy.
			test.Poll(t, 1*time.Second, 1, func() interface{} {
				return ingester.lifecycler.HealthyInstancesCount()
			})

			// Create a Kafka writer and then write a series.
			writer := ingest.NewWriter(cfg.IngestStorageConfig.KafkaConfig, log.NewNopLogger(), nil)
			require.NoError(t, services.StartAndAwaitRunning(ctx, writer))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, writer))
			})

			partitionID, err := ingest.IngesterPartitionID(cfg.IngesterRing.InstanceID)
			require.NoError(t, err)
			require.NoError(t, writer.WriteSync(ctx, partitionID, userID, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{series1}, Source: mimirpb.API}))

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

				close(queryIssued)
				queryRes, _, err = runTestQuery(queryCtx, t, ingester, labels.MatchEqual, labels.MetricName, metricName)
				require.NoError(t, err)
			}()

			// Wait until the query is issued in the dedicated goroutine and then unblock the Fetch.
			<-queryIssued
			failFetch.Store(false)

			// Wait until the query returns.
			queryWg.Wait()

			switch readConsistency {
			case api.ReadConsistencyEventual:
				// We expect no series because the query didn't wait for the read consistency to be guaranteed.
				assert.Len(t, queryRes, 0)

			case api.ReadConsistencyStrong:
				// We expect query did wait for the read consistency to be guaranteed.
				assert.Len(t, queryRes, 1)
			}
		})
	}
}

func TestIngester_PrepareShutdownHandler_IngestStorageSupport(t *testing.T) {
	ctx := context.Background()

	reg := prometheus.NewPedanticRegistry()
	overrides, err := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	require.NoError(t, err)

	// Start ingester.
	cfg := defaultIngesterTestConfig(t)
	ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return ingester.lifecycler.HealthyInstancesCount()
	})

	t.Run("should not allow to cancel the prepare shutdown, because unsupported by the ingest storage", func(t *testing.T) {
		res := httptest.NewRecorder()
		ingester.PrepareShutdownHandler(res, httptest.NewRequest(http.MethodDelete, "/ingester/prepare-shutdown", nil))
		require.Equal(t, http.StatusMethodNotAllowed, res.Code)

		require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP cortex_ingester_prepare_shutdown_requested If the ingester has been requested to prepare for shutdown via endpoint or marker file.
			# TYPE cortex_ingester_prepare_shutdown_requested gauge
			cortex_ingester_prepare_shutdown_requested 0
		`), "cortex_ingester_prepare_shutdown_requested"))
	})

	t.Run("should remove the ingester from partition owners on a prepared shutdown", func(t *testing.T) {
		res := httptest.NewRecorder()
		ingester.PrepareShutdownHandler(res, httptest.NewRequest(http.MethodPost, "/ingester/prepare-shutdown", nil))
		require.Equal(t, 204, res.Code)

		require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP cortex_ingester_prepare_shutdown_requested If the ingester has been requested to prepare for shutdown via endpoint or marker file.
			# TYPE cortex_ingester_prepare_shutdown_requested gauge
			cortex_ingester_prepare_shutdown_requested 1
		`), "cortex_ingester_prepare_shutdown_requested"))

		// Pre-condition: the ingester should be registered as owner in the ring.
		require.Eventually(t, func() bool {
			return slices.Equal(ingester.ingestPartitionWatcher.PartitionRing().PartitionOwnerIDs(0), []string{"ingester-zone-a-0"})
		}, time.Second, 10*time.Millisecond)

		// Shutdown ingester.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))

		// We expect the ingester to be removed from partition owners.
		require.Eventually(t, func() bool {
			return slices.Equal(ingester.ingestPartitionWatcher.PartitionRing().PartitionOwnerIDs(0), []string{})
		}, time.Second, 10*time.Millisecond)
	})
}

func TestIngester_PreparePartitionDownscaleHandler(t *testing.T) {
	ctx := context.Background()

	overrides, err := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	require.NoError(t, err)

	setup := func(t *testing.T, cfg Config) *Ingester {
		// Start ingester.
		ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, prometheus.NewPedanticRegistry())
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
		})

		// Wait until it's healthy
		test.Poll(t, 1*time.Second, 1, func() interface{} {
			return ingester.lifecycler.HealthyInstancesCount()
		})

		return ingester
	}

	t.Run("POST request should switch the partition state to INACTIVE", func(t *testing.T) {
		t.Parallel()

		ingester := setup(t, defaultIngesterTestConfig(t))

		// Pre-condition: the partition is ACTIVE.
		require.Eventually(t, func() bool {
			return slices.Equal(ingester.ingestPartitionWatcher.PartitionRing().ActivePartitionIDs(), []int32{0})
		}, time.Second, 10*time.Millisecond)

		res := httptest.NewRecorder()
		ingester.PreparePartitionDownscaleHandler(res, httptest.NewRequest(http.MethodPost, "/ingester/prepare-partition-downscale", nil))
		require.Equal(t, http.StatusOK, res.Code)

		// We expect the partition to switch to INACTIVE.
		require.Eventually(t, func() bool {
			return slices.Equal(ingester.ingestPartitionWatcher.PartitionRing().InactivePartitionIDs(), []int32{0})
		}, time.Second, 10*time.Millisecond)
	})

	t.Run("DELETE request after a POST request should switch the partition back to ACTIVE state", func(t *testing.T) {
		t.Parallel()

		ingester := setup(t, defaultIngesterTestConfig(t))

		// Pre-condition: the partition is ACTIVE.
		require.Eventually(t, func() bool {
			return slices.Equal(ingester.ingestPartitionWatcher.PartitionRing().ActivePartitionIDs(), []int32{0})
		}, time.Second, 10*time.Millisecond)

		res := httptest.NewRecorder()
		ingester.PreparePartitionDownscaleHandler(res, httptest.NewRequest(http.MethodPost, "/ingester/prepare-partition-downscale", nil))
		require.Equal(t, http.StatusOK, res.Code)

		// We expect the partition to switch to INACTIVE.
		require.Eventually(t, func() bool {
			return slices.Equal(ingester.ingestPartitionWatcher.PartitionRing().InactivePartitionIDs(), []int32{0})
		}, time.Second, 10*time.Millisecond)

		res = httptest.NewRecorder()
		ingester.PreparePartitionDownscaleHandler(res, httptest.NewRequest(http.MethodDelete, "/ingester/prepare-partition-downscale", nil))
		require.Equal(t, http.StatusOK, res.Code)

		// We expect the partition to switch to ACTIVE.
		require.Eventually(t, func() bool {
			return slices.Equal(ingester.ingestPartitionWatcher.PartitionRing().ActivePartitionIDs(), []int32{0})
		}, time.Second, 10*time.Millisecond)
	})

	t.Run("POST request should be rejected if the partition is in PENDING state", func(t *testing.T) {
		t.Parallel()

		// To keep the partition in PENDING state we set a minimum number of owners
		// higher than the actual number of ingesters we're going to run.
		cfg := defaultIngesterTestConfig(t)
		cfg.IngesterPartitionRing.MinOwnersCount = 2

		ingester := setup(t, cfg)

		// Pre-condition: the partition is PENDING.
		require.Eventually(t, func() bool {
			return slices.Equal(ingester.ingestPartitionWatcher.PartitionRing().PendingPartitionIDs(), []int32{0})
		}, time.Second, 10*time.Millisecond)

		res := httptest.NewRecorder()
		ingester.PreparePartitionDownscaleHandler(res, httptest.NewRequest(http.MethodPost, "/ingester/prepare-partition-downscale", nil))
		require.Equal(t, http.StatusConflict, res.Code)

		// We expect the partition to be in PENDING state.
		require.Eventually(t, func() bool {
			return slices.Equal(ingester.ingestPartitionWatcher.PartitionRing().PendingPartitionIDs(), []int32{0})
		}, time.Second, 10*time.Millisecond)
	})

	t.Run("DELETE is ignored if the partition is in PENDING state", func(t *testing.T) {
		t.Parallel()

		// To keep the partition in PENDING state we set a minimum number of owners
		// higher than the actual number of ingesters we're going to run.
		cfg := defaultIngesterTestConfig(t)
		cfg.IngesterPartitionRing.MinOwnersCount = 2

		ingester := setup(t, cfg)

		// Pre-condition: the partition is PENDING.
		require.Eventually(t, func() bool {
			return slices.Equal(ingester.ingestPartitionWatcher.PartitionRing().PendingPartitionIDs(), []int32{0})
		}, time.Second, 10*time.Millisecond)

		res := httptest.NewRecorder()
		ingester.PreparePartitionDownscaleHandler(res, httptest.NewRequest(http.MethodDelete, "/ingester/prepare-partition-downscale", nil))
		require.Equal(t, http.StatusOK, res.Code)

		// We expect the partition to be in PENDING state.
		require.Eventually(t, func() bool {
			return slices.Equal(ingester.ingestPartitionWatcher.PartitionRing().PendingPartitionIDs(), []int32{0})
		}, time.Second, 10*time.Millisecond)
	})
}

func TestIngester_ShouldNotCreatePartitionIfThereIsShutdownMarker(t *testing.T) {
	ctx := context.Background()

	overrides, err := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	require.NoError(t, err)

	cfg := defaultIngesterTestConfig(t)
	ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, prometheus.NewPedanticRegistry())

	// Create the shutdown marker.
	require.NoError(t, os.MkdirAll(cfg.BlocksStorageConfig.TSDB.Dir, os.ModePerm))
	require.NoError(t, shutdownmarker.Create(shutdownmarker.GetPath(cfg.BlocksStorageConfig.TSDB.Dir)))

	// Start ingester.
	require.NoError(t, err)
	require.NoError(t, ingester.StartAsync(ctx))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(ctx, ingester)
	})

	// No matter how long we wait, we expect the ingester service to hung in the starting state
	// given it's not allowed to create the partition and the partition doesn't exist in the ring.
	time.Sleep(10 * cfg.IngesterPartitionRing.lifecyclerPollingInterval)

	assert.Equal(t, services.Starting, ingester.State())
	assert.Empty(t, ingester.ingestPartitionWatcher.PartitionRing().PartitionIDs())
	assert.Empty(t, ingester.ingestPartitionWatcher.PartitionRing().PartitionOwnerIDs(ingester.ingestPartitionID))
}

// Returned ingester and ring watcher are NOT started.
func createTestIngesterWithIngestStorage(t testing.TB, ingesterCfg *Config, overrides *validation.Overrides, reg prometheus.Registerer) (*Ingester, *kfake.Cluster, *ring.PartitionRingWatcher) {
	var (
		ctx                   = context.Background()
		defaultIngesterConfig = defaultIngesterTestConfig(t)
	)

	// Always disable gRPC Push API when testing ingest store.
	ingesterCfg.PushGrpcMethodEnabled = false

	ingesterCfg.IngestStorageConfig.Enabled = true
	ingesterCfg.IngestStorageConfig.KafkaConfig.Topic = "mimir"
	ingesterCfg.IngestStorageConfig.KafkaConfig.LastProducedOffsetPollInterval = 100 * time.Millisecond

	// Create the partition ring store.
	kv := ingesterCfg.IngesterPartitionRing.kvMock
	if kv == nil {
		var closer io.Closer
		kv, closer = consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { assert.NoError(t, closer.Close()) })
		ingesterCfg.IngesterPartitionRing.kvMock = kv
	}

	ingesterCfg.IngesterPartitionRing.MinOwnersDuration = 0
	ingesterCfg.IngesterPartitionRing.lifecyclerPollingInterval = 10 * time.Millisecond

	// Create a fake Kafka cluster.
	kafkaCluster, kafkaAddr := testkafka.CreateCluster(t, 10, ingesterCfg.IngestStorageConfig.KafkaConfig.Topic)
	ingesterCfg.IngestStorageConfig.KafkaConfig.Address = kafkaAddr

	if ingesterCfg.IngesterRing.InstanceID == "" || ingesterCfg.IngesterRing.InstanceID == defaultIngesterConfig.IngesterRing.InstanceID {
		// The ingest storage requires the ingester ID to have a well known format.
		ingesterCfg.IngesterRing.InstanceID = "ingester-zone-a-0"
	}

	if ingesterCfg.BlocksStorageConfig.TSDB.Dir == "" || ingesterCfg.BlocksStorageConfig.TSDB.Dir == defaultIngesterConfig.BlocksStorageConfig.TSDB.Dir { // Overwrite default values to temp dir.
		ingesterCfg.BlocksStorageConfig.TSDB.Dir = t.TempDir()
	}
	ingesterCfg.BlocksStorageConfig.Bucket.Backend = "filesystem"
	ingesterCfg.BlocksStorageConfig.Bucket.Filesystem.Directory = t.TempDir()

	// Disable TSDB head compaction jitter to have predictable tests.
	ingesterCfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled = false

	prw := ring.NewPartitionRingWatcher(PartitionRingName, PartitionRingKey, kv, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, prw))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, prw))
	})

	ingester, err := New(*ingesterCfg, overrides, nil, prw, nil, reg, util_test.NewTestingLogger(t))
	require.NoError(t, err)

	return ingester, kafkaCluster, prw
}
