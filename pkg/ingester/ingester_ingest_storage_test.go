// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	"github.com/twmb/franz-go/pkg/kgo"
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

func TestIngester_Startup_PartitionRingActiveBlocksOnInstanceRingActive(t *testing.T) {
	util_test.VerifyNoLeak(t)
	var err error

	cfg := defaultIngesterTestConfig(t)

	// To simulate a stuck instance ring join process,
	// we can use a TokenGenerator which will block on CanJoin.
	cfg.IngesterRing.TokenGenerationStrategy = tokenGenerationSpreadMinimizing
	cfg.IngesterRing.InstanceZone = "zone-a"
	cfg.IngesterRing.SpreadMinimizingZones = []string{"zone-a"}
	// This configures the spread-minimizing TokenGenerator to block in CanJoin
	// until lower-index ingesters to join the ring and claim tokens.
	cfg.IngesterRing.SpreadMinimizingJoinRingInOrder = true

	cfg0 := cfg
	cfg0.IngesterRing.InstanceID = "ingester-zone-a-0" // spread-minimizing TokenGenerator instanceID=0

	cfg1 := cfg
	cfg1.IngesterRing.InstanceID = "ingester-zone-a-1" // spread-minimizing TokenGenerator instanceID=1, same zone

	overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)

	ctx := context.Background()

	// Start ingester instance ring & wait
	ingesterRing, err := ring.New(cfg1.IngesterRing.ToRingConfig(), "ingester", IngesterRingKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, ingesterRing))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingesterRing))
	})

	// Get ingester instance ring desc
	var ringDesc *ring.Desc
	err = ingesterRing.KVClient.CAS(ctx, IngesterRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		ringDesc = ring.GetOrCreateRingDesc(in)
		return ringDesc, true, nil
	})
	require.NoError(t, err)

	// Create an ingester and manually add to the ring;
	// needed to emulate an ingester which is delayed in claiming tokens.
	i0, _, _ := createTestIngesterWithIngestStorage(
		t, &cfg0, overrides, ingesterRing, nil, util_test.NewTestingLogger(t),
	)
	t.Cleanup(func() {
		// Some services are started when the ingester is created with New()
		// and only closed when defer is triggered by error cases in ingester.starting();
		// Call ingester.starting(), with a canceled context to guarantee failure and trigger that goroutine cleanup.
		i0Ctx, i0Cancel := context.WithCancel(context.Background())
		i0Cancel()
		require.NoError(t, i0.StartAsync(i0Ctx))
		err := services.StopAndAwaitTerminated(ctx, i0)
		// Service failure case error is propagated from error returned by ingester.starting().
		require.ErrorIs(t, err, context.Canceled)
	})
	i0Ro, i0RoTs := i0.lifecycler.GetReadOnlyState()
	ringDesc.AddIngester(
		i0.ingesterID,
		cfg0.IngesterRing.InstanceAddr,
		cfg0.IngesterRing.InstanceZone,
		[]uint32{}, // Empty tokens; higher-indexed ingesters will block until lower-indexed ingesters claim tokens
		i0.lifecycler.GetState(),
		time.Now(),
		i0Ro,
		i0RoTs,
		nil,
	)
	// Ensure updated instance ring state is committed
	err = ingesterRing.KVClient.CAS(ctx, IngesterRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		return ringDesc, true, nil
	})
	require.NoError(t, err)

	// Ensure addition of ingester-zone-a-0 is reflected in top-level instance ring state
	require.NoError(t, ring.WaitInstanceState(ctx, ingesterRing, i0.cfg.IngesterRing.InstanceID, ring.PENDING))

	// Create ingester-zone-a-1, which will block on joining
	// while it waits for the lower-indexed ingester-zone-a-0 to claim its tokens.
	// It will proceed after the currently-hardcoded ring.LifeCycler.canJoinTimeout deadline of 5 minutes,
	// which will not trigger before the end of the test.
	i1, _, _ := createTestIngesterWithIngestStorage(
		t, &cfg1, overrides, ingesterRing, nil, util_test.NewTestingLogger(t),
	)
	require.NoError(t, i1.StartAsync(ctx))
	t.Cleanup(func() {
		err := services.StopAndAwaitTerminated(ctx, i1)
		require.ErrorContains(t, err, "failed to wait for instance to be active in ring")
	})

	awaitJoinCtx, awaitJoinCancel := context.WithTimeout(context.Background(), 1*time.Second)
	t.Cleanup(func() { awaitJoinCancel() })
	err = i1.AwaitRunning(awaitJoinCtx)
	assert.ErrorIs(t, err, context.DeadlineExceeded) // i1 will not start as it blocks on i0 claiming tokens

	// Confirm ingester-zone-a-1 pending instance state
	i1InstanceState, err := ingesterRing.GetInstanceState(i1.cfg.IngesterRing.InstanceID)
	require.NoError(t, err)
	assert.Equal(t, ring.PENDING, i1InstanceState)

	// Confirm ingester-zone-a-1 partition lifecycler ring has not been started
	i1PartitionLifecyclerState := i1.ingestPartitionLifecycler.State()
	assert.Equal(t, services.New, i1PartitionLifecyclerState)

	// Confirm partition 1 does not exist in partition ring
	partitionState, _, err := i1.ingestPartitionLifecycler.GetPartitionState(ctx)
	require.ErrorIs(t, err, ring.ErrPartitionDoesNotExist)
	require.Equal(t, ring.PartitionUnknown, partitionState)
}

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
		ingester, kafkaCluster, watcher := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, reg, util_test.NewTestingLogger(t))

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
		firstInterval, standardInterval := ingester.compactionServiceInterval(time.Now(), ingester.instanceRing.Zones())
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
			ingester, kafkaCluster, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, reg, util_test.NewTestingLogger(t))

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
				return healthyInstancesCount(ingester.instanceRing)
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
	// Test with both classic Lifecycler (NumTokens > 0) and tokenless mode (NumTokens == 0).
	for _, numTokens := range []int{512, 0} {
		t.Run(fmt.Sprintf("num ingester ring tokens: %d", numTokens), func(t *testing.T) {
			ctx := context.Background()
			reg := prometheus.NewPedanticRegistry()
			overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)

			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.NumTokens = numTokens

			// Start ingester.
			ingester, _, watcher := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, reg, util_test.NewTestingLogger(t))
			require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
			})

			// Wait until it's healthy.
			test.Poll(t, 1*time.Second, 1, func() interface{} {
				return healthyInstancesCount(ingester.instanceRing)
			})

			// Verify token count matches expected for this mode.
			tokenCount, exists := getInstanceTokenCount(t, cfg.IngesterRing.KVStore.Mock, IngesterRingKey, ingester.ingesterID)
			require.True(t, exists)
			require.Equal(t, numTokens, tokenCount)

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
				// Pre-condition: instance should be ACTIVE in the ingesters ring.
				state, exists := getInstanceStateInRing(t, cfg.IngesterRing.KVStore.Mock, IngesterRingKey, ingester.ingesterID)
				require.True(t, exists, "instance should exist in ring before prepare shutdown")
				require.Equal(t, ring.ACTIVE, state, "instance should be ACTIVE before prepare shutdown")

				res := httptest.NewRecorder()
				ingester.PrepareShutdownHandler(res, httptest.NewRequest(http.MethodPost, "/ingester/prepare-shutdown", nil))
				require.Equal(t, 204, res.Code)

				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_prepare_shutdown_requested If the ingester has been requested to prepare for shutdown via endpoint or marker file.
					# TYPE cortex_ingester_prepare_shutdown_requested gauge
					cortex_ingester_prepare_shutdown_requested 1
				`), "cortex_ingester_prepare_shutdown_requested"))

				// Pre-condition: the ingester should still be ACTIVE and registered as owner in the partition ring.
				state, exists = getInstanceStateInRing(t, cfg.IngesterRing.KVStore.Mock, IngesterRingKey, ingester.ingesterID)
				require.True(t, exists, "instance should exist in ring after prepare shutdown POST")
				require.Equal(t, ring.ACTIVE, state, "instance should still be ACTIVE after prepare shutdown POST")
				require.Eventually(t, func() bool {
					return slices.Equal(watcher.PartitionRing().PartitionOwnerIDs(0), []string{"ingester-zone-a-0"})
				}, time.Second, 10*time.Millisecond)

				// Shutdown ingester.
				require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))

				// We expect the ingester to be removed from partition owners.
				require.Eventually(t, func() bool {
					return slices.Equal(watcher.PartitionRing().PartitionOwnerIDs(0), []string{})
				}, time.Second, 10*time.Millisecond)

				// After shutdown: instance should be removed from the ingesters ring.
				_, exists = getInstanceStateInRing(t, cfg.IngesterRing.KVStore.Mock, IngesterRingKey, ingester.ingesterID)
				require.False(t, exists, "instance should be removed from ring after shutdown with prepare-shutdown")
			})
		})
	}
}

func TestIngester_PreparePartitionDownscaleHandler(t *testing.T) {
	// Test with both classic Lifecycler (NumTokens > 0) and tokenless mode (NumTokens == 0).
	for _, numTokens := range []int{512, 0} {
		t.Run(fmt.Sprintf("num ingester ring tokens: %d", numTokens), func(t *testing.T) {
			ctx := context.Background()
			overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)

			setup := func(t *testing.T, cfg Config) (*Ingester, *ring.PartitionRingWatcher, *ring.PartitionRingEditor) {
				cfg.IngesterRing.NumTokens = numTokens

				// Start ingester.
				ingester, _, watcher := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, prometheus.NewPedanticRegistry(), util_test.NewTestingLogger(t))
				require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
				t.Cleanup(func() {
					require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
				})

				editor := ring.NewPartitionRingEditor(PartitionRingKey, cfg.IngesterPartitionRing.KVStore.Mock)

				// Wait until it's healthy
				test.Poll(t, 5*time.Second, 1, func() interface{} {
					return healthyInstancesCount(ingester.instanceRing)
				})

				// Verify token count matches expected for this mode.
				tokenCount, exists := getInstanceTokenCount(t, cfg.IngesterRing.KVStore.Mock, IngesterRingKey, ingester.ingesterID)
				require.True(t, exists)
				require.Equal(t, numTokens, tokenCount)

				return ingester, watcher, editor
			}

			t.Run("POST request should switch the partition state to INACTIVE", func(t *testing.T) {
				t.Parallel()

				ingester, watcher, _ := setup(t, defaultIngesterTestConfig(t))

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

				ingester, watcher, _ := setup(t, defaultIngesterTestConfig(t))

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

				ingester, watcher, _ := setup(t, cfg)

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

				ingester, watcher, _ := setup(t, cfg)

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

			t.Run("POST request should be rejected if the partition state change is locked", func(t *testing.T) {
				t.Parallel()

				ingester, watcher, editor := setup(t, defaultIngesterTestConfig(t))

				// Pre-condition: the partition is ACTIVE.
				test.Poll(t, 5*time.Second, []int32{0}, func() interface{} {
					return watcher.PartitionRing().ActivePartitionIDs()
				})

				// Lock the partition state change.
				require.NoError(t, editor.SetPartitionStateChangeLock(ctx, 0, true))
				test.Poll(t, 5*time.Second, true, func() interface{} {
					for _, partition := range watcher.PartitionRing().Partitions() {
						if partition.Id == 0 {
							return partition.GetStateChangeLocked()
						}
					}

					return false
				})

				res := httptest.NewRecorder()
				ingester.PreparePartitionDownscaleHandler(res, httptest.NewRequest(http.MethodPost, "/ingester/prepare-partition-downscale", nil))
				require.Equal(t, http.StatusConflict, res.Code)

				// We expect the partition to be in the ACTIVE state.
				test.Poll(t, 5*time.Second, []int32{0}, func() interface{} {
					return watcher.PartitionRing().ActivePartitionIDs()
				})
			})

			t.Run("DELETE request should be rejected if the partition state change is locked", func(t *testing.T) {
				t.Parallel()

				ingester, watcher, editor := setup(t, defaultIngesterTestConfig(t))

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

				// Lock the partition state change.
				require.NoError(t, editor.SetPartitionStateChangeLock(ctx, 0, true))
				test.Poll(t, 5*time.Second, true, func() interface{} {
					for _, partition := range watcher.PartitionRing().Partitions() {
						if partition.Id == 0 {
							return partition.GetStateChangeLocked()
						}
					}

					return false
				})

				res = httptest.NewRecorder()
				ingester.PreparePartitionDownscaleHandler(res, httptest.NewRequest(http.MethodDelete, "/ingester/prepare-partition-downscale", nil))
				require.Equal(t, http.StatusConflict, res.Code)

				// We expect the partition to be INACTIVE.
				test.Poll(t, 5*time.Second, []int32{0}, func() interface{} {
					return watcher.PartitionRing().InactivePartitionIDs()
				})
			})
		})
	}
}

func TestIngester_ShouldNotCreatePartitionIfThereIsShutdownMarker(t *testing.T) {
	ctx := context.Background()

	overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)

	cfg := defaultIngesterTestConfig(t)
	ingester, _, watcher := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, prometheus.NewPedanticRegistry(), util_test.NewTestingLogger(t))

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
			ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, nil, util_test.NewTestingLogger(t))

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

			ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, nil, util_test.NewTestingLogger(t))

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
func createTestIngesterWithIngestStorage(
	t testing.TB,
	ingesterCfg *Config,
	overrides *validation.Overrides,
	ingestersRing ring.ReadRing,
	reg prometheus.Registerer,
	logger log.Logger,
) (*Ingester, *kfake.Cluster, *ring.PartitionRingWatcher) {
	var (
		ctx                   = context.Background()
		defaultIngesterConfig = defaultIngesterTestConfig(t)
	)

	if logger == nil {
		logger = log.NewNopLogger()
	}

	if ingestersRing == nil {
		ingestersRing = createAndStartRing(t, ingesterCfg.IngesterRing.ToRingConfig())
	}

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

	ingester, err := New(*ingesterCfg, overrides, ingestersRing, prw, nil, nil, reg, logger)
	require.NoError(t, err)

	return ingester, kafkaCluster, prw
}

// BenchmarkIngester_ReplayFromKafka tests the ingester replaying records from Kafka at startup.
// Each scenario is derived from analysis of 1 partition in some production clusters in Grafana Cloud,
// using "kafkatool dump analyse". The fixture generator simulates the WriteRequest patterns
// matching the observed tenant distribution.
func BenchmarkIngester_ReplayFromKafka(b *testing.B) {
	const partitionID int32 = 0

	scenarios := map[string]struct {
		numTenants int
		cfg        ingest.FixtureConfig
	}{
		"1 large tenant": {
			numTenants: 1,
			cfg: ingest.FixtureConfig{
				SmallTenants:           ingest.FixtureTenantClassConfig{TenantPercent: 0, TimeseriesPercent: 0, AvgTimeseriesPerReq: 0},
				MediumTenants:          ingest.FixtureTenantClassConfig{TenantPercent: 0, TimeseriesPercent: 0, AvgTimeseriesPerReq: 0},
				LargeTenants:           ingest.FixtureTenantClassConfig{TenantPercent: 100, TimeseriesPercent: 100, AvgTimeseriesPerReq: 350},
				AvgLabelsPerTimeseries: 19,
				AvgLabelNameLength:     9,
				AvgLabelValueLength:    17,
				AvgMetricNameLength:    40,
				NumUniqueLabelNames:    2_500,
				NumUniqueLabelValues:   1_000_000,
			},
		},
		"100 mixed tenants": {
			numTenants: 100,
			cfg: ingest.FixtureConfig{
				SmallTenants:           ingest.FixtureTenantClassConfig{TenantPercent: 52, TimeseriesPercent: 5, AvgTimeseriesPerReq: 6},
				MediumTenants:          ingest.FixtureTenantClassConfig{TenantPercent: 44, TimeseriesPercent: 67, AvgTimeseriesPerReq: 5},
				LargeTenants:           ingest.FixtureTenantClassConfig{TenantPercent: 4, TimeseriesPercent: 28, AvgTimeseriesPerReq: 7},
				AvgLabelsPerTimeseries: 13,
				AvgLabelNameLength:     10,
				AvgLabelValueLength:    19,
				AvgMetricNameLength:    35,
				NumUniqueLabelNames:    5_000,
				NumUniqueLabelValues:   1_500_000,
			},
		},
		"350 mixed tenants": {
			numTenants: 350,
			cfg: ingest.FixtureConfig{
				SmallTenants:           ingest.FixtureTenantClassConfig{TenantPercent: 92, TimeseriesPercent: 14, AvgTimeseriesPerReq: 40},
				MediumTenants:          ingest.FixtureTenantClassConfig{TenantPercent: 7, TimeseriesPercent: 44, AvgTimeseriesPerReq: 100},
				LargeTenants:           ingest.FixtureTenantClassConfig{TenantPercent: 1, TimeseriesPercent: 42, AvgTimeseriesPerReq: 30},
				AvgLabelsPerTimeseries: 10,
				AvgLabelNameLength:     9,
				AvgLabelValueLength:    21,
				AvgMetricNameLength:    35,
				NumUniqueLabelNames:    1_700,
				NumUniqueLabelValues:   280_000,
			},
		},
	}

	for scenarioName, scenario := range scenarios {
		b.Run(scenarioName, func(b *testing.B) {
			ctx := context.Background()

			// Configure the ingester with Kafka settings using the same settings used in Grafana Cloud.
			cfg := defaultIngesterTestConfig(b)
			cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyMax = 8
			cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyBatchSize = 150
			cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyEstimatedBytesPerSample = 200
			cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyQueueCapacity = 3
			cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyTargetFlushesPerShard = 40
			cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencySequentialPusherEnabled = false

			// Disable TSDB WAL to reduce variance in test executions.
			cfg.BlocksStorageConfig.TSDB.WALSegmentSizeBytes = -1

			// Start consuming from the beginning so the ingester replays all records.
			cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"

			// Create the ingester (not started yet).
			overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
			ingester, _, _ := createTestIngesterWithIngestStorage(b, &cfg, overrides, nil, nil, nil)
			b.Cleanup(func() {
				require.NoError(b, services.StopAndAwaitTerminated(ctx, ingester))
			})

			// Create a fixture generator with realistic patterns from production data.
			fixtureCfg := scenario.cfg
			fixtureCfg.TotalUniqueTimeseries = b.N
			fixtureCfg.TotalSamples = b.N * 2 // 2 samples per series in this benchmark.
			gen, err := ingest.NewFixtureGenerator(fixtureCfg, scenario.numTenants, 0)
			require.NoError(b, err)

			// Produce all WriteRequests to Kafka before starting the ingester.
			// We produce all records upfront (before starting the ingester) to ensure that
			// Kafka production is not a bottleneck during the benchmark. This way we measure
			// only the ingester's replay performance, not the combined produce+consume throughput.
			numRecordsProduced, err := gen.ProduceWriteRequests(ctx, cfg.IngestStorageConfig.KafkaConfig.Address, cfg.IngestStorageConfig.KafkaConfig.Topic, partitionID)
			require.NoError(b, err)

			targetOffset := int64(numRecordsProduced - 1)

			// Start the timer and measure the time to replay all records.
			// StartAndAwaitRunning waits for the ingester to finish replaying.
			b.ResetTimer()

			require.NoError(b, services.StartAndAwaitRunning(ctx, ingester))
			require.NoError(b, ingester.ingestReader.WaitReadConsistencyUntilOffset(ctx, targetOffset))

			// Stop the timer so cleanup functions don't affect the measurement.
			b.StopTimer()
		})
	}
}

// BenchmarkIngester_ReplayFromKafka_RealData benchmarks the ingester replaying real production
// data from dump files. This benchmark ignores b.N and runs once per dump file.
//
// Dump files should be placed in the repository root (../../ relative to this test file) with
// the .dump extension. Generate dump files from a Kafka topic using:
//
//		kafkatool dump export \
//		 --kafka-address=<addr> --kafka-sasl-username=<user> --kafka-sasl-password=<pass> --kafka-client-id=ws_pt=proxy-consume \
//	  --topic <topic> --partition <partition> --offset <start> --export-max-records=<num> --file <name>.dump
func BenchmarkIngester_ReplayFromKafka_Dump(b *testing.B) {
	const partitionID int32 = 0

	ctx := context.Background()

	// List all *.dump files in the repository root.
	dumpFiles, err := filepath.Glob("../../*.dump")
	require.NoError(b, err)

	// Skip the benchmark if no dump files found.
	if len(dumpFiles) == 0 {
		b.Skip("no *.dump files found in repository root, skipping benchmark")
	}

	for _, dumpFile := range dumpFiles {
		dumpFileName := filepath.Base(dumpFile)
		b.Run(dumpFileName, func(b *testing.B) {
			b.StopTimer()

			// Configure the ingester with Kafka settings using the same settings used in Grafana Cloud.
			cfg := defaultIngesterTestConfig(b)
			cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyMax = 8
			cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyBatchSize = 150
			cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyEstimatedBytesPerSample = 200
			cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyQueueCapacity = 3
			cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyTargetFlushesPerShard = 40
			cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencySequentialPusherEnabled = false

			// Disable TSDB WAL to reduce variance in benchmark executions.
			cfg.BlocksStorageConfig.TSDB.WALSegmentSizeBytes = -1

			// Start consuming from the beginning so the ingester replays all records.
			cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"

			// Create the ingester (not started yet).
			overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
			ingester, _, _ := createTestIngesterWithIngestStorage(b, &cfg, overrides, nil, nil, nil)
			b.Cleanup(func() {
				require.NoError(b, services.StopAndAwaitTerminated(ctx, ingester))
			})

			// Create a Kafka writer client.
			writerCfg := ingest.KafkaConfig{}
			flagext.DefaultValues(&writerCfg)
			writerCfg.Topic = cfg.IngestStorageConfig.KafkaConfig.Topic
			writerCfg.Address = cfg.IngestStorageConfig.KafkaConfig.Address
			writerCfg.DisableLinger = true

			client, err := ingest.NewKafkaWriterClient(writerCfg, 20, log.NewNopLogger(), nil)
			require.NoError(b, err)
			b.Cleanup(func() { client.Close() })

			// Stream records from the dump file directly to Kafka.
			file, err := os.Open(dumpFile)
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, file.Close()) })

			var numRecords, totalTimeseries int
			var minRecordTimestamp, maxRecordTimestamp time.Time
			tenantIDs := make(map[string]struct{})

			scanner := bufio.NewScanner(file)
			scanner.Buffer(make([]byte, 10_000_000), 10_000_000)
			for scanner.Scan() {
				record := &kgo.Record{}
				err := json.Unmarshal(scanner.Bytes(), record)
				require.NoError(b, err)

				// Track min/max record timestamps.
				if minRecordTimestamp.IsZero() || record.Timestamp.Before(minRecordTimestamp) {
					minRecordTimestamp = record.Timestamp
				}
				if maxRecordTimestamp.IsZero() || record.Timestamp.After(maxRecordTimestamp) {
					maxRecordTimestamp = record.Timestamp
				}

				// Deserialize the WriteRequest to count timeseries.
				prealloc := mimirpb.PreallocWriteRequest{}
				version := ingest.ParseRecordVersion(record)
				err = ingest.DeserializeRecordContent(record.Value, &prealloc, version)
				require.NoError(b, err)

				tenantIDs[string(record.Key)] = struct{}{}
				totalTimeseries += len(prealloc.Timeseries)

				// Produce to Kafka asynchronously (using the original record value).
				client.Produce(ctx, &kgo.Record{
					Key:       record.Key,
					Value:     record.Value,
					Headers:   record.Headers,
					Partition: partitionID,
				}, nil)
				numRecords++
			}
			require.NoError(b, scanner.Err())

			// Wait for all records to be sent.
			err = client.Flush(ctx)
			require.NoError(b, err)

			recordTimeSpan := maxRecordTimestamp.Sub(minRecordTimestamp)
			b.Logf("Wrote %d records with %d timeseries to Kafka from dump file with %d unique tenants (time span: %s)", numRecords, totalTimeseries, len(tenantIDs), recordTimeSpan)

			// Measure the time to start the ingester and replay all records.
			targetOffset := int64(numRecords - 1)

			b.StartTimer()
			require.NoError(b, services.StartAndAwaitRunning(ctx, ingester))
			require.NoError(b, ingester.ingestReader.WaitReadConsistencyUntilOffset(ctx, targetOffset))
			b.StopTimer()

			b.ReportMetric(float64(numRecords)/b.Elapsed().Seconds(), "records/sec")
			b.ReportMetric(float64(totalTimeseries)/b.Elapsed().Seconds(), "timeseries/sec")
		})
	}
}
