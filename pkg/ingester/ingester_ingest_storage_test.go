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
	"strings"
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
	util_test.VerifyNoLeak(t)

	t.Run("should replay the partition at startup (after a restart) and then join the ingesters and partitions ring", func(t *testing.T) {
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

		// Create the ingester.
		overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
		ingester, kafkaCluster, watcher := createTestIngesterWithIngestStorage(t, &cfg, overrides, reg)

		// Mock the Kafka cluster to:
		// - Count the Fetch requests.
		// - Mock the ListOffsets response, returning the offset expected once the ingester can be
		//   considered having successfully caught up.
		kafkaCluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
			kafkaCluster.KeepControl()
			fetchRequestsCount.Inc()

			return nil, nil, false
		})

		kafkaCluster.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			kafkaCluster.KeepControl()

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
				Topic: cfg.IngestStorageConfig.KafkaConfig.Topic,
				Partitions: []kmsg.ListOffsetsResponseTopicPartition{{
					Partition: ingester.ingestPartitionID,
					ErrorCode: 0,
					Offset:    expectedSeriesToReplay,
				}},
			}}

			return res, nil, true
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

		// Add the partition and owner in the ring, in order to simulate an ingester restart.
		require.NoError(t, cfg.IngesterPartitionRing.KVStore.Mock.CAS(context.Background(), PartitionRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
			partitionID, err := ingest.IngesterPartitionID(cfg.IngesterRing.InstanceID)
			if err != nil {
				return nil, false, err
			}

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

		// Wait until the Kafka cluster received the 1st Fetch request.
		test.Poll(t, 5*time.Second, true, func() interface{} {
			return fetchRequestsCount.Load() > 0
		})

		// At this point we expect the ingester is stuck in starting state while catching up
		// replaying the partition. The catchup will not succeed until we'll write the next
		// series to Kafka because we've mocked the Kafka to return a "last produced offset"
		// in the future.

		// We expect the following internal services to be already running at this point.
		assert.Equal(t, services.Running, ingester.compactionService.State())
		assert.Equal(t, services.Running, ingester.ownedSeriesService.State())
		assert.Equal(t, services.Running, ingester.metricsUpdaterService.State())
		assert.Equal(t, services.Running, ingester.metadataPurgerService.State())

		// We expect the TSDB Head compaction to run while replaying from Kafka.
		assert.Eventually(t, func() bool {
			return testutil.ToFloat64(ingester.metrics.compactionsTriggered) > 0
		}, time.Second, 10*time.Millisecond)

		// We expect metrics to be updated.
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
		firstInterval, standardInterval := ingester.compactionServiceInterval(time.Now(), ingester.lifecycler.Zones())
		assert.Equal(t, headCompactionIntervalWhileStarting, firstInterval)
		assert.Equal(t, headCompactionIntervalWhileStarting, standardInterval)

		assert.Eventually(t, func() bool {
			return testutil.ToFloat64(ingester.metrics.compactionsTriggered) > 0
		}, 5*time.Second, 10*time.Millisecond)

		// Since the ingester it still replaying the partition we expect it to be in starting state.
		assert.Equal(t, services.Starting, ingester.State())
		assert.Equal(t, services.New, ingester.lifecycler.State())

		// Write one more request to Kafka. This will cause the ingester to consume up until the
		// "last produced offset" returned by the mocked Kafka, and so consider the catch up complete.
		require.NoError(t, writer.WriteSync(ctx, partitionID, userID, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{series2}, Source: mimirpb.API}))

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
				watcher.PartitionRing().PartitionOwnerIDs(partitionID),
				[]string{ingester.cfg.IngesterRing.InstanceID})
		}, time.Second, 10*time.Millisecond)
	})
}

func TestIngester_QueryStream_IngestStorageReadConsistency(t *testing.T) {
	const (
		metricName = "series_1"
	)

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

			// We expect query did wait for the read consistency to be guaranteed.
			expectedQueriedSeries: 1,
		},
		"strong read consistency with offsets": {
			readConsistencyLevel: api.ReadConsistencyStrong,

			// To keep the test simple, use a trick passing a negative offset so the query will return immediately.
			// In this test we just want to check that the passed offset is honored.
			readConsistencyOffsets: map[int32]int64{0: -2},
			expectedQueriedSeries:  0,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			var (
				cfg     = defaultIngesterTestConfig(t)
				limits  = defaultLimitsTestConfig()
				reg     = prometheus.NewRegistry()
				ctx     = context.Background()
				series1 = mimirpb.PreallocTimeseries{
					TimeSeries: &mimirpb.TimeSeries{
						Labels:  mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, metricName)),
						Samples: []mimirpb.Sample{{TimestampMs: 1000, Value: 10}},
					},
				}
			)

			limits.IngestStorageReadConsistency = testData.readConsistencyLevel

			// Create the ingester.
			overrides := validation.NewOverrides(limits, nil)
			ingester, kafkaCluster, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, reg)

			// Mock the Kafka cluster to fail the Fetch operation until we unblock it later in the test.
			// If read consistency is "eventual" then these failures shouldn't affect queries, but if it's set
			// to "strong" then a query shouldn't succeed until the Fetch requests succeed.
			failFetch := atomic.NewBool(true)

			kafkaCluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
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

				// Inject the requested offsets (if any).
				if len(testData.readConsistencyOffsets) > 0 {
					queryCtx = api.ContextWithReadConsistencyEncodedOffsets(queryCtx, api.EncodeOffsets(testData.readConsistencyOffsets))
				}

				close(queryIssued)
				queryRes, _, err = runTestQuery(queryCtx, t, ingester, labels.MatchEqual, model.MetricNameLabel, metricName)
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

func TestIngester_PrepareShutdownHandler_IngestStorageSupport(t *testing.T) {
	ctx := context.Background()

	reg := prometheus.NewPedanticRegistry()
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)

	// Start ingester.
	cfg := defaultIngesterTestConfig(t)
	ingester, _, watcher := createTestIngesterWithIngestStorage(t, &cfg, overrides, reg)
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
			return slices.Equal(watcher.PartitionRing().PartitionOwnerIDs(0), []string{"ingester-zone-a-0"})
		}, time.Second, 10*time.Millisecond)

		// Shutdown ingester.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))

		// We expect the ingester to be removed from partition owners.
		require.Eventually(t, func() bool {
			return slices.Equal(watcher.PartitionRing().PartitionOwnerIDs(0), []string{})
		}, time.Second, 10*time.Millisecond)
	})
}

func TestIngester_PreparePartitionDownscaleHandler(t *testing.T) {
	ctx := context.Background()

	overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	setup := func(t *testing.T, cfg Config) (*Ingester, *ring.PartitionRingWatcher) {
		// Start ingester.
		ingester, _, watcher := createTestIngesterWithIngestStorage(t, &cfg, overrides, prometheus.NewPedanticRegistry())
		require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
		})

		// Wait until it's healthy
		test.Poll(t, 5*time.Second, 1, func() interface{} {
			return ingester.lifecycler.HealthyInstancesCount()
		})

		return ingester, watcher
	}

	t.Run("POST request should switch the partition state to INACTIVE", func(t *testing.T) {
		t.Parallel()

		ingester, watcher := setup(t, defaultIngesterTestConfig(t))

		// Pre-condition: the partition is ACTIVE.
		test.Poll(t, 5*time.Second, []int32{0}, func() interface{} {
			return watcher.PartitionRing().ActivePartitionIDs()
		})

		res := httptest.NewRecorder()
		ingester.PreparePartitionDownscaleHandler(res, httptest.NewRequest(http.MethodPost, "/ingester/prepare-partition-downscale", nil))
		require.Equal(t, http.StatusOK, res.Code)

		// We expect the partition to switch to INACTIVE.
		test.Poll(t, 5*time.Second, []int32{0}, func() interface{} {
			return watcher.PartitionRing().InactivePartitionIDs()
		})
	})

	t.Run("DELETE request after a POST request should switch the partition back to ACTIVE state", func(t *testing.T) {
		t.Parallel()

		ingester, watcher := setup(t, defaultIngesterTestConfig(t))

		// Pre-condition: the partition is ACTIVE.
		test.Poll(t, 5*time.Second, []int32{0}, func() interface{} {
			return watcher.PartitionRing().ActivePartitionIDs()
		})

		res := httptest.NewRecorder()
		ingester.PreparePartitionDownscaleHandler(res, httptest.NewRequest(http.MethodPost, "/ingester/prepare-partition-downscale", nil))
		require.Equal(t, http.StatusOK, res.Code)

		// We expect the partition to switch to INACTIVE.
		test.Poll(t, 5*time.Second, []int32{0}, func() interface{} {
			return watcher.PartitionRing().InactivePartitionIDs()
		})

		res = httptest.NewRecorder()
		ingester.PreparePartitionDownscaleHandler(res, httptest.NewRequest(http.MethodDelete, "/ingester/prepare-partition-downscale", nil))
		require.Equal(t, http.StatusOK, res.Code)

		// We expect the partition to switch to ACTIVE.
		test.Poll(t, 5*time.Second, []int32{0}, func() interface{} {
			return watcher.PartitionRing().ActivePartitionIDs()
		})
	})

	t.Run("POST request should be rejected if the partition is in PENDING state", func(t *testing.T) {
		t.Parallel()

		// To keep the partition in PENDING state we set a minimum number of owners
		// higher than the actual number of ingesters we're going to run.
		cfg := defaultIngesterTestConfig(t)
		cfg.IngesterPartitionRing.MinOwnersCount = 2

		ingester, watcher := setup(t, cfg)

		// Pre-condition: the partition is PENDING.
		test.Poll(t, 5*time.Second, []int32{0}, func() interface{} {
			return watcher.PartitionRing().PendingPartitionIDs()
		})

		res := httptest.NewRecorder()
		ingester.PreparePartitionDownscaleHandler(res, httptest.NewRequest(http.MethodPost, "/ingester/prepare-partition-downscale", nil))
		require.Equal(t, http.StatusConflict, res.Code)

		// We expect the partition to be in PENDING state.
		test.Poll(t, 5*time.Second, []int32{0}, func() interface{} {
			return watcher.PartitionRing().PendingPartitionIDs()
		})
	})

	t.Run("DELETE is ignored if the partition is in PENDING state", func(t *testing.T) {
		t.Parallel()

		// To keep the partition in PENDING state we set a minimum number of owners
		// higher than the actual number of ingesters we're going to run.
		cfg := defaultIngesterTestConfig(t)
		cfg.IngesterPartitionRing.MinOwnersCount = 2

		ingester, watcher := setup(t, cfg)

		// Pre-condition: the partition is PENDING.
		test.Poll(t, 5*time.Second, []int32{0}, func() interface{} {
			return watcher.PartitionRing().PendingPartitionIDs()
		})

		res := httptest.NewRecorder()
		ingester.PreparePartitionDownscaleHandler(res, httptest.NewRequest(http.MethodDelete, "/ingester/prepare-partition-downscale", nil))
		require.Equal(t, http.StatusOK, res.Code)

		// We expect the partition to be in PENDING state.
		test.Poll(t, 5*time.Second, []int32{0}, func() interface{} {
			return watcher.PartitionRing().PendingPartitionIDs()
		})
	})
}

func TestIngester_ShouldNotCreatePartitionIfThereIsShutdownMarker(t *testing.T) {
	ctx := context.Background()

	overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)

	cfg := defaultIngesterTestConfig(t)
	ingester, _, watcher := createTestIngesterWithIngestStorage(t, &cfg, overrides, prometheus.NewPedanticRegistry())

	// Create the shutdown marker.
	require.NoError(t, os.MkdirAll(cfg.BlocksStorageConfig.TSDB.Dir, os.ModePerm))
	require.NoError(t, shutdownmarker.Create(shutdownmarker.GetPath(cfg.BlocksStorageConfig.TSDB.Dir)))

	// Start ingester.
	require.NoError(t, ingester.StartAsync(ctx))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(ctx, ingester)
	})

	// No matter how long we wait, we expect the ingester service to hung in the starting state
	// given it's not allowed to create the partition and the partition doesn't exist in the ring.
	time.Sleep(10 * cfg.IngesterPartitionRing.lifecyclerPollingInterval)

	assert.Equal(t, services.Starting, ingester.State())
	assert.Empty(t, watcher.PartitionRing().PartitionIDs())
	assert.Empty(t, watcher.PartitionRing().PartitionOwnerIDs(ingester.ingestPartitionID))
}

func TestIngester_compactionServiceInterval(t *testing.T) {
	fakeNow := time.Date(2025, 7, 21, 10, 27, 0, 0, time.UTC)

	tc := map[string]struct {
		state         services.State
		interval      time.Duration
		jitterEnabled bool
		zones         []string
		expected      func(t *testing.T, first, standard time.Duration)
		instanceZone  string
	}{
		"when starting without jittering enabled": {
			state:         services.Starting,
			interval:      time.Minute,
			jitterEnabled: false,
			expected: func(t *testing.T, first, standard time.Duration) {
				assert.Equal(t, 30*time.Second, first)
				assert.Equal(t, 30*time.Second, standard)
			},
		},
		"when starting with jittering enabled": {
			state:         services.Starting,
			interval:      time.Minute,
			jitterEnabled: true,
			expected: func(t *testing.T, first, standard time.Duration) {
				assert.Less(t, first, 30*time.Second)
				assert.Equal(t, 30*time.Second, standard)
			},
		},
		"for zone 1 out 2 with jittering and zone awareness enabled at a 10m interval": {
			state:         services.Running,
			interval:      20 * time.Minute,
			jitterEnabled: true,
			zones:         []string{"zone-a", "zone-b"},
			instanceZone:  "zone-a",
			expected: func(t *testing.T, first, standard time.Duration) {
				// It's 10:27:00, so the next compaction interval for zone-a is 10:40:00, which means we need a minimum of
				// 13 minutes and then a maximum of 18 minutes to reach the next compaction interval.
				assert.GreaterOrEqual(t, first, 13*time.Minute)
				assert.LessOrEqual(t, first, 18*time.Minute)
				assert.Equal(t, 20*time.Minute, standard)
			},
		},
		"for zone 2 out 2 with jittering and zone awareness enabled at a 10m interval": {
			state:         services.Running,
			interval:      20 * time.Minute,
			jitterEnabled: true,
			zones:         []string{"zone-a", "zone-b"},
			instanceZone:  "zone-b",
			expected: func(t *testing.T, first, standard time.Duration) {
				// It's 10:27:00, so the next compaction interval for zone-b is 10:30:00, which means we need a minimum of
				// 3 minutes and then a maximum of 8 minutes to reach the next compaction interval.
				assert.GreaterOrEqual(t, first, 3*time.Minute)
				assert.LessOrEqual(t, first, 8*time.Minute)
				assert.Equal(t, 20*time.Minute, standard)
			},
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			cfg := defaultIngesterTestConfig(t)
			overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
			ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil)

			ingester.cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = tt.interval
			ingester.cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled = tt.jitterEnabled
			ingester.cfg.IngesterRing.InstanceZone = tt.instanceZone
			if len(tt.zones) > 0 {
				ingester.cfg.IngesterRing.ZoneAwarenessEnabled = true
			}

			switch tt.state {
			case services.Starting:
				// force the ingester to be in a starting state by creating a shutdown marker.
				require.NoError(t, os.MkdirAll(cfg.BlocksStorageConfig.TSDB.Dir, os.ModePerm))
				require.NoError(t, shutdownmarker.Create(shutdownmarker.GetPath(cfg.BlocksStorageConfig.TSDB.Dir)))
				require.NoError(t, ingester.StartAsync(context.Background()))
				t.Cleanup(func() {
					_ = services.StopAndAwaitTerminated(context.Background(), ingester)
				})
			case services.Running:
			default:
				t.Fatalf("unsupported state %s", tt.state)
			}

			firstInterval, standardInterval := ingester.compactionServiceInterval(fakeNow, tt.zones)
			tt.expected(t, firstInterval, standardInterval)
		})
	}
}

func TestIngester_timeToNextZoneAwareCompaction(t *testing.T) {
	defaultBaseHeadCompactionInterval := 15 * time.Minute
	// July 21st 2025, 10:00 AM
	fakeNow := time.Date(2025, 7, 21, 10, 27, 0, 0, time.UTC)

	tc := map[string]struct {
		zoneAwarenessEnabled bool
		instanceZone         string
		zones                []string
		expected             time.Duration
	}{
		"zone awareness disabled": {
			zoneAwarenessEnabled: false,
			instanceZone:         "zone-a",
			zones:                []string{"zone-a", "zone-b"},
			expected:             defaultBaseHeadCompactionInterval,
		},
		"empty instance zone": {
			zoneAwarenessEnabled: true,
			instanceZone:         "",
			zones:                []string{"zone-a", "zone-b"},
			expected:             defaultBaseHeadCompactionInterval,
		},
		"zone awareness disabled and empty instance zone": {
			zoneAwarenessEnabled: false,
			instanceZone:         "",
			zones:                []string{"zone-a", "zone-b"},
			expected:             defaultBaseHeadCompactionInterval,
		},
		"single zone": {
			zoneAwarenessEnabled: true,
			instanceZone:         "zone-a",
			zones:                []string{"zone-a"},
			expected:             defaultBaseHeadCompactionInterval,
		},
		"empty zones list": {
			zoneAwarenessEnabled: true,
			instanceZone:         "zone-a",
			zones:                []string{},
			expected:             defaultBaseHeadCompactionInterval,
		},
		"current zone not found in zones list": {
			zoneAwarenessEnabled: true,
			instanceZone:         "zone-missing",
			zones:                []string{"zone-a", "zone-b"},
			expected:             defaultBaseHeadCompactionInterval,
		},
		"two zones - first zone": {
			zoneAwarenessEnabled: true,
			instanceZone:         "zone-a",
			zones:                []string{"zone-a", "zone-b"},
			expected:             3 * time.Minute, // It's 00:27:00, so the next compaction interval is 00:30:00, which is 3 minutes from now.
		},
		"two zones - second zone": {
			zoneAwarenessEnabled: true,
			instanceZone:         "zone-b",
			zones:                []string{"zone-a", "zone-b"},
			expected:             10*time.Minute + 30*time.Second, // It's 00:27:00, so the next compaction interval is 00:37:30, which is 10 minutes and 30 seconds from now.
		},
		"three zones - first zone": {
			zoneAwarenessEnabled: true,
			instanceZone:         "zone-a",
			zones:                []string{"zone-a", "zone-b", "zone-c"},
			expected:             3 * time.Minute, // It's 00:27:00, so the next compaction interval for zone a is 00:30:00, which is 3 minutes from now.
		},
		"three zones - middle zone": {
			zoneAwarenessEnabled: true,
			instanceZone:         "zone-b",
			zones:                []string{"zone-a", "zone-b", "zone-c"},
			expected:             8 * time.Minute, // It's 00:27:00, so the next compaction interval for zone b is 00:35:00, which is 8 minutes from now.
		},
		"three zones - last zone": {
			zoneAwarenessEnabled: true,
			instanceZone:         "zone-c",
			zones:                []string{"zone-a", "zone-b", "zone-c"},
			expected:             13 * time.Minute, // It's 00:27:00, so the next compaction interval for zone c is 00:40:00, which is 13 minutes from now.
		},
		"many zones - verify staggering by picking one random": {
			zoneAwarenessEnabled: true,
			instanceZone:         "zone-c",
			zones:                []string{"zone-a", "zone-b", "zone-c", "zone-d", "zone-e"},
			expected:             9 * time.Minute, // It's 00:27:00, so the next compaction interval for zone-3 is 00:36:00, which is 9 minutes from now.
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var (
				cfg       = defaultIngesterTestConfig(t)
				overrides = validation.NewOverrides(defaultLimitsTestConfig(), nil)
			)

			cfg.IngesterRing.ZoneAwarenessEnabled = tt.zoneAwarenessEnabled
			cfg.IngesterRing.InstanceZone = tt.instanceZone
			cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = defaultBaseHeadCompactionInterval

			ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil)

			headCompactionInterval := ingester.timeToNextZoneAwareCompaction(fakeNow, tt.zones)
			require.Equal(t, tt.expected, headCompactionInterval)
		})
	}
}

func TestIngester_timeUntilCompaction(t *testing.T) {
	// July 21st 2025, 10:00 AM
	fakeNow := time.Date(2025, 7, 21, 10, 0, 0, 0, time.UTC)

	tc := map[string]struct {
		now        time.Time
		interval   time.Duration
		zoneOffset time.Duration
		expected   time.Duration
	}{
		"no offset zone and starting at :00 gives us interval as-is": {
			now:        fakeNow,
			interval:   15 * time.Minute,
			zoneOffset: 0,
			expected:   15 * time.Minute, // Next interval at 10:15
		},
		"with offset zone of 7m30s and starting at :00 gives interval of 7m30s": {
			now:        fakeNow,
			interval:   15 * time.Minute,
			zoneOffset: 7*time.Minute + 30*time.Second,
			expected:   7*time.Minute + 30*time.Second, // Next interval at 10:07:30
		},
		"with offset zone of 15m and equal to interval gives us offset interval as-is": {
			now:        fakeNow,
			interval:   15 * time.Minute,
			zoneOffset: 15 * time.Minute,
			expected:   15 * time.Minute, // Offset wraps around to the beginning of the interval
		},
		"with offset zone of 7m30s and starting at :10 gives us offset 12m30s": {
			now:        fakeNow.Add(10 * time.Minute), // 10:10:00
			interval:   15 * time.Minute,
			zoneOffset: 7*time.Minute + 30*time.Second,
			expected:   12*time.Minute + 30*time.Second, // Next interval at 10:22:30
		},
		"with offset zone of 7m30s, current time around the second interval": {
			now:        fakeNow.Add(26 * time.Minute), // 10:26:00
			interval:   15 * time.Minute,
			zoneOffset: 7*time.Minute + 30*time.Second,
			expected:   11*time.Minute + 30*time.Second, // Next interval at 10:37:30, 11m30s to the next interval.
		},
		"with offset zone of 7m30s, current time around the third interval": {
			now:        fakeNow.Add(39*time.Minute + 13*time.Second), // 10:39:13
			interval:   15 * time.Minute,
			zoneOffset: 7*time.Minute + 30*time.Second,
			expected:   13*time.Minute + 17*time.Second, // Next interval at 10:52:30, 13m17s to the next interval.
		},
		"with offset zone of 7m30s, current time around the fourth interval": {
			now:        fakeNow.Add(52*time.Minute + 29*time.Second), // 10:52:29
			interval:   15 * time.Minute,
			zoneOffset: 7*time.Minute + 30*time.Second,
			expected:   1 * time.Second, // Next interval at 10:52:30, 1s to the next interval.
		},
		"with offset zone of 7m30s, current time around past the fourth interval": {
			now:        fakeNow.Add(58*time.Minute + 40*time.Second), // 10:58:40
			interval:   15 * time.Minute,
			zoneOffset: 7*time.Minute + 30*time.Second,
			expected:   8*time.Minute + 50*time.Second, // Next interval at 11:07:30, 1s to the next interval.
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			result := timeUntilCompaction(tt.now, tt.interval, tt.zoneOffset)
			require.Equal(t, tt.expected, result)
		})
	}
}

// Returned ingester is NOT started.
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
	kv := ingesterCfg.IngesterPartitionRing.KVStore.Mock
	if kv == nil {
		var closer io.Closer
		kv, closer = consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { assert.NoError(t, closer.Close()) })
		ingesterCfg.IngesterPartitionRing.KVStore.Mock = kv
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

	// Create and start the partition ring watcher. Since it's a dependency which is passed
	// to ingester New() function, we start the watcher beforehand.
	prw := ring.NewPartitionRingWatcher(PartitionRingName, PartitionRingKey, kv, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, prw))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, prw))
	})

	ingester, err := New(*ingesterCfg, overrides, nil, prw, nil, nil, reg, util_test.NewTestingLogger(t))
	require.NoError(t, err)

	return ingester, kafkaCluster, prw
}
