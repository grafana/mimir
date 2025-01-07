// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/mtime"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

// kafkaTopic is the Kafka topic used for ingest storage tests.
const kafkaTopic = "test"

func TestDistributor_Push_ShouldSupportIngestStorage(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	// Mock distributor current time (used to get stable metrics assertion).
	now := time.Now()
	mtime.NowForce(now)
	t.Cleanup(mtime.NowReset)

	// To keep assertions simple, all tests send the same request.
	createRequest := func() *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{model.MetricNameLabel, "series_one"}, makeSamples(now.UnixMilli(), 1), nil, makeExemplars([]string{"trace_id", "xxx"}, now.UnixMilli(), 1)),
				makeTimeseries([]string{model.MetricNameLabel, "series_two"}, makeSamples(now.UnixMilli(), 2), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_three"}, makeSamples(now.UnixMilli(), 3), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_four"}, makeSamples(now.UnixMilli(), 4), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_five"}, makeSamples(now.UnixMilli(), 5), nil, nil),
			},
			Metadata: []*mimirpb.MetricMetadata{
				{MetricFamilyName: "series_one", Type: mimirpb.COUNTER, Help: "Series one description"},
				{MetricFamilyName: "series_two", Type: mimirpb.COUNTER, Help: "Series two description"},
			},
		}
	}

	tests := map[string]struct {
		shardSize                    int
		kafkaPartitionCustomResponse map[int32]*kmsg.ProduceResponse
		expectedErr                  error
		expectedSeriesByPartition    map[int32][]string
	}{
		"should shard series across all partitions when shuffle sharding is disabled": {
			shardSize: 0,
			expectedSeriesByPartition: map[int32][]string{
				0: {"series_four", "series_one", "series_three"},
				1: {"series_two"},
				2: {"series_five"},
			},
		},
		"should shard series across the number of configured partitions when shuffle sharding is enabled": {
			shardSize: 2,
			expectedSeriesByPartition: map[int32][]string{
				1: {"series_one", "series_three", "series_two"},
				2: {"series_five", "series_four"},
			},
		},
		"should return gRPC error if writing to 1 out of N partitions fail with a non-retryable error": {
			shardSize: 0,
			kafkaPartitionCustomResponse: map[int32]*kmsg.ProduceResponse{
				// Non-retryable error.
				1: testkafka.CreateProduceResponseError(0, kafkaTopic, 1, kerr.InvalidTopicException),
			},
			expectedErr: fmt.Errorf("%s 1", failedPushingToPartitionMessage),
			expectedSeriesByPartition: map[int32][]string{
				// Partition 1 is missing because it failed.
				0: {"series_four", "series_one", "series_three"},
				2: {"series_five"},
			},
		},

		// This test case simulate the case the request timeout is < than the Kafka writer timeout and producing
		// the message to Kafka fails consistently for a partition. In this case, the request will timeout before
		// Kafka writer and so the client will get a context.DeadlineExceeded.
		"should return context.DeadlineExceeded error if writing to 1 out of N partitions times out because of a retryable error": {
			shardSize: 0,
			kafkaPartitionCustomResponse: map[int32]*kmsg.ProduceResponse{
				// Retryable error.
				1: testkafka.CreateProduceResponseError(0, kafkaTopic, 1, kerr.LeaderNotAvailable),
			},
			expectedErr: context.DeadlineExceeded,
			expectedSeriesByPartition: map[int32][]string{
				// Partition 1 is missing because it failed.
				0: {"series_four", "series_one", "series_three"},
				2: {"series_five"},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			limits := prepareDefaultLimits()
			limits.IngestionPartitionsTenantShardSize = testData.shardSize
			limits.MaxGlobalExemplarsPerUser = 1000

			testConfig := prepConfig{
				numDistributors:         1,
				ingestStorageEnabled:    true,
				ingestStoragePartitions: 3,
				limits:                  limits,
				configure: func(cfg *Config) {
					// Run a number of clients equal to the number of partitions, so that each partition
					// has its own client, as requested by some test cases.
					cfg.IngestStorageConfig.KafkaConfig.WriteClients = 3
				},
			}

			distributors, _, regs, kafkaCluster := prepare(t, testConfig)
			require.Len(t, distributors, 1)
			require.Len(t, regs, 1)

			// Mock Kafka to fail specific partitions (if configured).
			kafkaCluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
				kafkaCluster.KeepControl()

				for _, topic := range request.(*kmsg.ProduceRequest).Topics {
					// For this test to work correctly we expect each request to write only to 1 partition,
					// because we'll fail the entire request.
					require.Len(t, topic.Partitions, 1)

					if res := testData.kafkaPartitionCustomResponse[topic.Partitions[0].Partition]; res != nil {
						res.SetVersion(request.GetVersion())
						return res, nil, true
					}
				}

				return nil, nil, false
			})

			// Send write request.
			res, err := distributors[0].Push(ctx, createRequest())

			if testData.expectedErr != nil {
				require.Error(t, err)
				assert.Nil(t, res)

				if errors.Is(testData.expectedErr, context.DeadlineExceeded) {
					// The context.DeadlineExceeded is not expected to be wrapped in a gRPC error.
					assert.ErrorIs(t, err, testData.expectedErr)
				} else {
					// We expect a gRPC error.
					errStatus, ok := grpcutil.ErrorToStatus(err)
					require.True(t, ok)
					assert.Equal(t, codes.Internal, errStatus.Code())
					assert.ErrorContains(t, errStatus.Err(), testData.expectedErr.Error())
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, emptyResponse, res)
			}

			// Ensure series has been sharded as expected.
			actualSeriesByPartition := readAllMetricNamesByPartitionFromKafka(t, kafkaCluster.ListenAddrs(), testConfig.ingestStoragePartitions, time.Second)
			assert.Equal(t, testData.expectedSeriesByPartition, actualSeriesByPartition)

			// Asserts on tracked metrics.
			assert.NoError(t, testutil.GatherAndCompare(regs[0], strings.NewReader(fmt.Sprintf(`
					# HELP cortex_distributor_requests_in_total The total number of requests that have come in to the distributor, including rejected or deduped requests.
					# TYPE cortex_distributor_requests_in_total counter
					cortex_distributor_requests_in_total{user="user"} 1

					# HELP cortex_distributor_received_requests_total The total number of received requests, excluding rejected and deduped requests.
					# TYPE cortex_distributor_received_requests_total counter
					cortex_distributor_received_requests_total{user="user"} 1

					# HELP cortex_distributor_samples_in_total The total number of samples that have come in to the distributor, including rejected or deduped samples.
					# TYPE cortex_distributor_samples_in_total counter
					cortex_distributor_samples_in_total{user="user"} 5

					# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected and deduped samples.
					# TYPE cortex_distributor_received_samples_total counter
					cortex_distributor_received_samples_total{user="user"} 5

					# HELP cortex_distributor_metadata_in_total The total number of metadata the have come in to the distributor, including rejected.
					# TYPE cortex_distributor_metadata_in_total counter
					cortex_distributor_metadata_in_total{user="user"} 2

					# HELP cortex_distributor_received_metadata_total The total number of received metadata, excluding rejected.
					# TYPE cortex_distributor_received_metadata_total counter
					cortex_distributor_received_metadata_total{user="user"} 2

					# HELP cortex_distributor_exemplars_in_total The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.
					# TYPE cortex_distributor_exemplars_in_total counter
					cortex_distributor_exemplars_in_total{user="user"} 1

					# HELP cortex_distributor_received_exemplars_total The total number of received exemplars, excluding rejected and deduped exemplars.
					# TYPE cortex_distributor_received_exemplars_total counter
					cortex_distributor_received_exemplars_total{user="user"} 1

					# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
					# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
					cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} %f
				`, float64(now.UnixMilli())/1000.)),
				"cortex_distributor_received_requests_total",
				"cortex_distributor_received_samples_total",
				"cortex_distributor_received_exemplars_total",
				"cortex_distributor_received_metadata_total",
				"cortex_distributor_requests_in_total",
				"cortex_distributor_samples_in_total",
				"cortex_distributor_exemplars_in_total",
				"cortex_distributor_metadata_in_total",
				"cortex_distributor_latest_seen_sample_timestamp_seconds",
			))
		})
	}
}

func TestDistributor_Push_ShouldReturnErrorMappedTo4xxStatusCodeIfWriteRequestContainsTimeseriesBiggerThanLimit(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	hugeLabelValueLength := 100 * 1024 * 1024

	createWriteRequest := func() *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{model.MetricNameLabel, strings.Repeat("x", hugeLabelValueLength)}, makeSamples(now.UnixMilli(), 1), nil, nil),
			},
		}
	}

	limits := prepareDefaultLimits()
	limits.MaxLabelValueLength = hugeLabelValueLength

	overrides, err := validation.NewOverrides(*limits, nil)
	require.NoError(t, err)

	testConfig := prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		limits:                  limits,
	}

	distributors, _, regs, _ := prepare(t, testConfig)
	require.Len(t, distributors, 1)
	require.Len(t, regs, 1)

	t.Run("Push()", func(t *testing.T) {
		// Send write request.
		res, err := distributors[0].Push(ctx, createWriteRequest())
		require.Error(t, err)
		require.Nil(t, res)

		// We expect a gRPC error.
		errStatus, ok := grpcutil.ErrorToStatus(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, errStatus.Code())
		assert.ErrorContains(t, errStatus.Err(), ingest.ErrWriteRequestDataItemTooLarge.Error())

		// We expect the gRPC error to be detected as client error.
		assert.True(t, mimirpb.IsClientError(err))
	})

	t.Run("Handler()", func(t *testing.T) {
		marshalledReq, err := createWriteRequest().Marshal()
		require.NoError(t, err)

		maxRecvMsgSize := hugeLabelValueLength * 2
		resp := httptest.NewRecorder()
		sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)", false)

		// Send write request through the HTTP handler.
		h := Handler(maxRecvMsgSize, nil, sourceIPs, false, false, overrides, RetryConfig{}, distributors[0].PushWithMiddlewares, nil, log.NewNopLogger())
		h.ServeHTTP(resp, createRequest(t, marshalledReq))
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestDistributor_Push_ShouldSupportWriteBothToIngestersAndPartitions(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	// To keep assertions simple, all tests send the same request.
	createRequest := func() *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{model.MetricNameLabel, "series_one"}, makeSamples(now.UnixMilli(), 1), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_two"}, makeSamples(now.UnixMilli(), 2), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_three"}, makeSamples(now.UnixMilli(), 3), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_four"}, makeSamples(now.UnixMilli(), 4), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_five"}, makeSamples(now.UnixMilli(), 5), nil, nil),
			},
		}
	}

	tests := map[string]struct {
		shardSize                     int
		shouldFailWritingToPartitions bool
		shouldFailWritingToIngesters  bool
		expectedErr                   string
		expectedMetricsByPartition    map[int32][]string
		expectedMetricsByIngester     map[string][]string
	}{
		"should shard series across all partitions when shuffle sharding is disabled": {
			shardSize: 0,
			expectedMetricsByPartition: map[int32][]string{
				0: {"series_four", "series_one", "series_three"},
				1: {"series_two"},
				2: {"series_five"},
			},
			expectedMetricsByIngester: map[string][]string{
				"ingester-0": {"series_four", "series_five"},
				"ingester-1": {"series_one", "series_two", "series_three"},
				"ingester-2": {},
			},
		},
		"should shard series across the number of configured partitions / ingesters when shuffle sharding is enabled": {
			shardSize: 2,
			expectedMetricsByPartition: map[int32][]string{
				1: {"series_one", "series_three", "series_two"},
				2: {"series_five", "series_four"},
			},
			expectedMetricsByIngester: map[string][]string{
				"ingester-0": {"series_four", "series_five"},
				"ingester-1": {"series_one", "series_two", "series_three"},
			},
		},
		"should return gRPC error if fails to write to ingesters": {
			shouldFailWritingToIngesters: true,
			expectedErr:                  failedPushingToIngesterMessage,
		},
		"should return gRPC error if fails to write to partitions": {
			shouldFailWritingToPartitions: true,
			expectedErr:                   failedPushingToPartitionMessage,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			// Pre-condition: ensure that sharding is different between ingesters and partitions.
			// This is required to ensure series are correctly sharded based on ingesters and partitions ring.
			// If the sharding is the same, then we have no guarantee it's actually working as expected.
			if len(testData.expectedMetricsByIngester) > 0 && len(testData.expectedMetricsByPartition) > 0 {
				actualPartitionsSharding := map[string][]string{}
				actualIngestersSharding := map[string][]string{}

				for partitionID, partitionMetrics := range testData.expectedMetricsByPartition {
					actualPartitionsSharding[strconv.Itoa(int(partitionID))] = slices.Clone(partitionMetrics)
					slices.Sort(actualPartitionsSharding[strconv.Itoa(int(partitionID))])
				}
				for ingesterID, ingesterMetrics := range testData.expectedMetricsByIngester {
					partitionID, err := ingest.IngesterPartitionID(ingesterID)
					require.NoError(t, err)

					actualIngestersSharding[strconv.Itoa(int(partitionID))] = slices.Clone(ingesterMetrics)
					slices.Sort(actualIngestersSharding[strconv.Itoa(int(partitionID))])
				}

				require.NotEqual(t, actualPartitionsSharding, actualIngestersSharding)
			}

			// Setup distributors and ingesters.
			limits := prepareDefaultLimits()
			limits.IngestionPartitionsTenantShardSize = testData.shardSize
			limits.IngestionTenantShardSize = testData.shardSize

			testConfig := prepConfig{
				numDistributors:         1,
				numIngesters:            3,
				happyIngesters:          3,
				replicationFactor:       1,
				ingesterIngestionType:   ingesterIngestionTypeGRPC, // Do not consume from Kafka. Partitions are asserted directly checking Kafka.
				ingestStorageEnabled:    true,
				ingestStoragePartitions: 3,
				limits:                  limits,
				configure: func(cfg *Config) {
					cfg.IngestStorageConfig.Migration.DistributorSendToIngestersEnabled = true
				},
			}

			distributors, ingesters, regs, kafkaCluster := prepare(t, testConfig)
			require.Len(t, distributors, 1)
			require.Len(t, ingesters, 3)
			require.Len(t, regs, 1)

			if testData.shouldFailWritingToPartitions {
				kafkaCluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
					kafkaCluster.KeepControl()

					partitionID := req.(*kmsg.ProduceRequest).Topics[0].Partitions[0].Partition
					res := testkafka.CreateProduceResponseError(req.GetVersion(), kafkaTopic, partitionID, kerr.InvalidTopicException)

					return res, nil, true
				})
			}

			if testData.shouldFailWritingToIngesters {
				for _, ingester := range ingesters {
					ingester.happy = false
				}
			}

			// Send write request.
			_, err := distributors[0].Push(ctx, createRequest())

			if testData.expectedErr != "" {
				require.Error(t, err)

				// We expect a gRPC error.
				errStatus, ok := grpcutil.ErrorToStatus(err)
				require.True(t, ok)
				assert.Equal(t, codes.Internal, errStatus.Code())
				assert.ErrorContains(t, errStatus.Err(), testData.expectedErr)

				// End the test here.
				return
			}

			require.NoError(t, err)

			// Ensure series has been correctly sharded to partitions.
			actualSeriesByPartition := readAllMetricNamesByPartitionFromKafka(t, kafkaCluster.ListenAddrs(), testConfig.ingestStoragePartitions, time.Second)
			if !assert.Equal(t, testData.expectedMetricsByPartition, actualSeriesByPartition, "please report this failure in https://github.com/grafana/mimir/issues/9299") {
				// This test is sometimes flaky. Add a log line to help debug it.
				// Inspect the offsets of partitions in Kafka. There may be records, but we couldn't fetch them in the 1s timeout above.
				kafkaClient, err := kgo.NewClient(kgo.SeedBrokers(kafkaCluster.ListenAddrs()...))
				assert.NoError(t, err)
				offsets, err := kadm.NewClient(kafkaClient).ListEndOffsets(context.Background(), kafkaTopic)
				assert.NoError(t, err)
				t.Logf("Kafka topic %s end offsets: %#v", kafkaTopic, offsets)
			}

			// Ensure series have been correctly sharded to ingesters.
			for _, ingester := range ingesters {
				assert.ElementsMatchf(t, testData.expectedMetricsByIngester[ingester.instanceID()], ingester.metricNames(), "ingester ID: %s", ingester.instanceID())
			}
		})
	}
}

func TestDistributor_Push_ShouldCleanupWriteRequestAfterWritingBothToIngestersAndPartitions(t *testing.T) {
	t.Parallel()

	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	testConfig := prepConfig{
		numDistributors:         1,
		numIngesters:            3,
		happyIngesters:          3,
		replicationFactor:       3,
		ingesterIngestionType:   ingesterIngestionTypeGRPC, // Do not consume from Kafka in this test.
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		limits:                  prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.Migration.DistributorSendToIngestersEnabled = true
		},
	}

	distributors, ingesters, regs, kafkaCluster := prepare(t, testConfig)
	require.Len(t, distributors, 1)
	require.Len(t, ingesters, 3)
	require.Len(t, regs, 1)

	// In this test ingesters have been configured with RF=3. This means that the write request will succeed
	// once written to at least 2 out of 3 ingesters. We configure 1 ingester to block the Push() request, and
	// then we control when unblocking it.
	releaseSlowIngesterPush := make(chan struct{})
	ingesters[0].registerBeforePushHook(func(_ context.Context, _ *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error, bool) {
		<-releaseSlowIngesterPush
		return nil, nil, false
	})

	// Wrap the distributor Push() to inject a custom cleanup function, so that we can track when it gets called.
	pushCleanupCallsCount := atomic.NewInt64(0)
	origPushWithMiddlewares := distributors[0].PushWithMiddlewares
	distributors[0].PushWithMiddlewares = func(ctx context.Context, req *Request) error {
		req.AddCleanup(func() {
			pushCleanupCallsCount.Inc()
		})

		return origPushWithMiddlewares(ctx, req)
	}

	// Send write request.
	_, err := distributors[0].Push(ctx, &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries([]string{model.MetricNameLabel, "series_one"}, makeSamples(now.UnixMilli(), 1), nil, nil),
		},
	})
	require.NoError(t, err)

	// Since there's still 1 ingester in-flight request, we expect the cleanup function not being called yet.
	require.Equal(t, int64(0), pushCleanupCallsCount.Load())
	time.Sleep(time.Second)
	require.Equal(t, int64(0), pushCleanupCallsCount.Load())

	// Unblock the slow ingester.
	close(releaseSlowIngesterPush)

	// Now we expect the cleanup function being called as soon as the request to the slow ingester completes.
	test.Poll(t, time.Second, int64(1), func() interface{} {
		return pushCleanupCallsCount.Load()
	})

	// Ensure series has been correctly written to partitions.
	actualSeriesByPartition := readAllMetricNamesByPartitionFromKafka(t, kafkaCluster.ListenAddrs(), testConfig.ingestStoragePartitions, time.Second)
	assert.Equal(t, map[int32][]string{0: {"series_one"}}, actualSeriesByPartition)

	// Ensure series have been correctly sharded to ingesters.
	for _, ingester := range ingesters {
		assert.Equal(t, []string{"series_one"}, ingester.metricNames(), "ingester ID: %s", ingester.instanceID())
	}
}

func TestDistributor_Push_ShouldGivePrecedenceToPartitionsErrorWhenWritingBothToIngestersAndPartitions(t *testing.T) {
	t.Parallel()

	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	testConfig := prepConfig{
		numDistributors:         1,
		numIngesters:            1,
		happyIngesters:          1,
		replicationFactor:       1,
		ingesterIngestionType:   ingesterIngestionTypeGRPC, // Do not consume from Kafka in this test.
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		limits:                  prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.Migration.DistributorSendToIngestersEnabled = true
		},
	}

	distributors, ingesters, regs, kafkaCluster := prepare(t, testConfig)
	require.Len(t, distributors, 1)
	require.Len(t, ingesters, 1)
	require.Len(t, regs, 1)

	// Mock Kafka to return an hard error.
	releaseProduceRequest := make(chan struct{})
	kafkaCluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		kafkaCluster.KeepControl()

		// Wait until released, then add an extra sleep to increase the likelihood this error
		// will be returned after the ingester one.
		<-releaseProduceRequest
		time.Sleep(time.Second)

		partitionID := req.(*kmsg.ProduceRequest).Topics[0].Partitions[0].Partition
		res := testkafka.CreateProduceResponseError(req.GetVersion(), kafkaTopic, partitionID, kerr.InvalidTopicException)

		return res, nil, true
	})

	// Mock ingester to return a soft error.
	ingesters[0].registerBeforePushHook(func(_ context.Context, _ *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error, bool) {
		// Release the Kafka produce request once the push to ingester has been received.
		close(releaseProduceRequest)

		ingesterErr := httpgrpc.Errorf(http.StatusBadRequest, "ingester error")
		return &mimirpb.WriteResponse{}, ingesterErr, true
	})

	// Send write request.
	_, err := distributors[0].Push(ctx, &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries([]string{model.MetricNameLabel, "series_one"}, makeSamples(now.UnixMilli(), 1), nil, nil),
		},
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, "send data to partitions")
}

func TestDistributor_UserStats_ShouldSupportIngestStorage(t *testing.T) {
	const preferredZone = "zone-a"

	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		ingesterDataByZone  map[string][]*mimirpb.WriteRequest
		shardSize           int
		expectedSeries      uint64
		expectedErr         error
	}{
		"partitions RF=1 (1 zone), 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 3, happyIngesters: 3},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1"),
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3"),
				},
			},
			expectedSeries: 3,
		},
		"partitions RF=1 (1 zone), 6 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1"),
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_7"),
					makeWriteRequest(0, 1, 0, false, false, "series_8", "series_9"),
				},
			},
			expectedSeries: 9,
		},
		"partitions RF=1 (1 zone), 6 ingesters, 1 ingester in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_7"),
					makeWriteRequest(0, 1, 0, false, false, "series_8", "series_9"),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"partitions RF=1 (1 zone), 6 ingesters, 1 ingester is UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1"),
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_7"),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"partitions RF=2 (2 zones), 4 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the preferred zone (zone-a) are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the non-preferred zone (zone-b) are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning different partitions are in LEAVING state across both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are in LEAVING state in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the preferred zone (zone-a) are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 0},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the non-preferred zone (zone-b) are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 0},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					nil,
					nil,
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning different partitions are UNHEALTHY across both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are UNHEALTHY in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are UNHEALTHY in both zones but the partition is not part of the tenant's shard": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
				"zone-b": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
			},
			shardSize:      1, // Tenant's shard made of: partition 1.
			expectedSeries: 3,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for _, minimizeIngesterRequests := range []bool{false, true} {
				t.Run(fmt.Sprintf("minimize ingester requests: %t", minimizeIngesterRequests), func(t *testing.T) {
					t.Parallel()

					// Create distributor
					distributors, _, _, _ := prepare(t, prepConfig{
						numDistributors:      1,
						ingesterStateByZone:  testData.ingesterStateByZone,
						ingesterDataByZone:   testData.ingesterDataByZone,
						ingestStorageEnabled: true,
						configure: func(config *Config) {
							config.PreferAvailabilityZone = preferredZone
							config.MinimizeIngesterRequests = minimizeIngesterRequests
						},
						limits: func() *validation.Limits {
							limits := prepareDefaultLimits()
							limits.IngestionPartitionsTenantShardSize = testData.shardSize
							return limits
						}(),
					})

					// Fetch user stats.
					ctx := user.InjectOrgID(context.Background(), "test")
					res, err := distributors[0].UserStats(ctx, cardinality.InMemoryMethod)

					if testData.expectedErr != nil {
						require.ErrorIs(t, err, testData.expectedErr)
						return
					}

					require.NoError(t, err)
					assert.Equal(t, testData.expectedSeries, res.NumSeries)
				})
			}
		})
	}
}

func TestDistributor_LabelValuesCardinality_AvailabilityAndConsistencyWithIngestStorage(t *testing.T) {
	const preferredZone = "zone-a"

	var (
		// Define fixtures used in tests.
		series1 = makeTimeseries([]string{labels.MetricName, "series_1", "job", "job-a", "service", "service-1"}, makeSamples(0, 0), nil, nil)
		series2 = makeTimeseries([]string{labels.MetricName, "series_2", "job", "job-b", "service", "service-1"}, makeSamples(0, 0), nil, nil)
		series3 = makeTimeseries([]string{labels.MetricName, "series_3", "job", "job-c", "service", "service-1"}, makeSamples(0, 0), nil, nil)
		series4 = makeTimeseries([]string{labels.MetricName, "series_4", "job", "job-a", "service", "service-1"}, makeSamples(0, 0), nil, nil)
		series5 = makeTimeseries([]string{labels.MetricName, "series_5", "job", "job-a", "service", "service-2"}, makeSamples(0, 0), nil, nil)
		series6 = makeTimeseries([]string{labels.MetricName, "series_6", "job", "job-b" /* no service label */}, makeSamples(0, 0), nil, nil)

		// To keep assertions simple, all tests push all series, and then request the cardinality of the same label names,
		// so we expect the same response from each successful test.
		reqLabelNames = []model.LabelName{"job", "service"}
		expectedRes   = []*client.LabelValueSeriesCount{
			{
				LabelName:        "job",
				LabelValueSeries: map[string]uint64{"job-a": 3, "job-b": 2, "job-c": 1},
			}, {
				LabelName:        "service",
				LabelValueSeries: map[string]uint64{"service-1": 4, "service-2": 1},
			},
		}
	)

	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		ingesterDataByZone  map[string][]*mimirpb.WriteRequest
		shardSize           int
		expectedErr         error
	}{
		"partitions RF=1 (1 zone), 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 3, happyIngesters: 3},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequestWith(series1, series2),
					makeWriteRequestWith(series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
		},
		"partitions RF=1 (1 zone), 6 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequestWith(series1),
					makeWriteRequestWith(series2),
					makeWriteRequestWith(series3),
					makeWriteRequestWith(series4),
					makeWriteRequestWith(series5),
					makeWriteRequestWith(series6),
				},
			},
		},
		"partitions RF=1 (1 zone), 6 ingesters, 1 ingester in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					nil,
					makeWriteRequestWith(series2),
					makeWriteRequestWith(series3),
					makeWriteRequestWith(series4),
					makeWriteRequestWith(series5),
					makeWriteRequestWith(series6),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"partitions RF=1 (1 zone), 6 ingesters, 1 ingester is UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequestWith(series1),
					makeWriteRequestWith(series2),
					makeWriteRequestWith(series3),
					makeWriteRequestWith(series4),
					makeWriteRequestWith(series5),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"partitions RF=2 (2 zones), 4 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the preferred zone (zone-a) are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the non-preferred zone (zone-b) are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning different partitions are in LEAVING state across both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are in LEAVING state in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the preferred zone (zone-a) are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 0},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					nil,
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the non-preferred zone (zone-b) are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 0},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					nil,
					nil,
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning different partitions are UNHEALTHY across both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					nil,
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are UNHEALTHY in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					nil,
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are UNHEALTHY in both zones but the partition is not part of the tenant's shard": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
				},
				"zone-b": {
					nil,
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
				},
			},
			shardSize: 1, // Tenant's shard made of: partition 1.
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for _, minimizeIngesterRequests := range []bool{false, true} {
				t.Run(fmt.Sprintf("minimize ingester requests: %t", minimizeIngesterRequests), func(t *testing.T) {
					t.Parallel()

					// Create distributor
					distributors, _, _, _ := prepare(t, prepConfig{
						numDistributors:      1,
						ingesterStateByZone:  testData.ingesterStateByZone,
						ingesterDataByZone:   testData.ingesterDataByZone,
						ingestStorageEnabled: true,
						configure: func(config *Config) {
							config.PreferAvailabilityZone = preferredZone
							config.MinimizeIngesterRequests = minimizeIngesterRequests
						},
						limits: func() *validation.Limits {
							limits := prepareDefaultLimits()
							limits.IngestionPartitionsTenantShardSize = testData.shardSize
							return limits
						}(),
					})

					// Fetch label values cardinality.
					ctx := user.InjectOrgID(context.Background(), "test")
					_, res, err := distributors[0].LabelValuesCardinality(ctx, reqLabelNames, nil, cardinality.InMemoryMethod)

					if testData.expectedErr != nil {
						require.ErrorIs(t, err, testData.expectedErr)
						return
					}

					require.NoError(t, err)
					assert.ElementsMatch(t, expectedRes, res.Items)
				})
			}
		})
	}
}

func TestDistributor_ActiveSeries_AvailabilityAndConsistencyWithIngestStorage(t *testing.T) {
	const preferredZone = "zone-a"

	// In this test we run all queries with a matcher which matches all series.
	reqMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")}

	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		ingesterDataByZone  map[string][]*mimirpb.WriteRequest
		shardSize           int
		expectedSeriesCount int
		expectedErr         error
	}{
		"partitions RF=1 (1 zone), 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 3, happyIngesters: 3},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1"),
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3"),
				},
			},
			expectedSeriesCount: 3,
		},
		"partitions RF=1 (1 zone), 6 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1"),
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_7"),
					makeWriteRequest(0, 1, 0, false, false, "series_8", "series_9"),
				},
			},
			expectedSeriesCount: 9,
		},
		"partitions RF=1 (1 zone), 6 ingesters, 1 ingester in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_7"),
					makeWriteRequest(0, 1, 0, false, false, "series_8", "series_9"),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"partitions RF=1 (1 zone), 6 ingesters, 1 ingester is UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1"),
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_7"),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"partitions RF=2 (2 zones), 4 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the preferred zone (zone-a) are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the non-preferred zone (zone-b) are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning different partitions are in LEAVING state across both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are in LEAVING state in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the preferred zone (zone-a) are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 0},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the non-preferred zone (zone-b) are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 0},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					nil,
					nil,
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning different partitions are UNHEALTHY across both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are UNHEALTHY in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are UNHEALTHY in both zones but the partition is not part of the tenant's shard": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
				"zone-b": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
			},
			shardSize:           1, // Tenant's shard made of: partition 1.
			expectedSeriesCount: 3,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for _, minimizeIngesterRequests := range []bool{false, true} {
				t.Run(fmt.Sprintf("minimize ingester requests: %t", minimizeIngesterRequests), func(t *testing.T) {
					t.Parallel()

					// Create distributor.
					distributors, _, _, _ := prepare(t, prepConfig{
						ingesterStateByZone:  testData.ingesterStateByZone,
						ingesterDataByZone:   testData.ingesterDataByZone,
						numDistributors:      1,
						ingestStorageEnabled: true,
						configure: func(config *Config) {
							config.MinimizeIngesterRequests = minimizeIngesterRequests
							config.PreferAvailabilityZone = preferredZone
						},
						limits: func() *validation.Limits {
							limits := prepareDefaultLimits()
							limits.IngestionPartitionsTenantShardSize = testData.shardSize
							return limits
						}(),
					})

					ctx := user.InjectOrgID(context.Background(), "test")
					qStats, ctx := stats.ContextWithEmptyStats(ctx)

					// Query active series.
					series, err := distributors[0].ActiveSeries(ctx, reqMatchers)
					if testData.expectedErr != nil {
						require.ErrorIs(t, err, testData.expectedErr)
						return
					}

					require.NoError(t, err)
					assert.Equal(t, testData.expectedSeriesCount, len(series))

					// Check that query stats are set correctly.
					assert.Equal(t, testData.expectedSeriesCount, int(qStats.GetFetchedSeriesCount()))
				})
			}
		})
	}
}

func readAllRecordsFromKafka(t testing.TB, kafkaAddresses []string, numPartitions int32, timeout time.Duration) []*kgo.Record {
	// Read all partitions from the beginning.
	offsets := make(map[int32]kgo.Offset, numPartitions)
	for partitionID := int32(0); partitionID < numPartitions; partitionID++ {
		offsets[partitionID] = kgo.NewOffset().AtStart()
	}

	// Init the client.
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAddresses...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			kafkaTopic: offsets,
		}))
	require.NoError(t, err)
	t.Cleanup(kafkaClient.Close)

	var records []*kgo.Record

	// Read all records until no data has been received for at least the timeout period.
	// We don't stop reading as soon as the expected number of entries has been found
	// because we also want to make sure no more than expected entries are written to Kafka.
	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		fetches := kafkaClient.PollRecords(ctx, 1000)
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}

			t.Fatal(err)
		}

		fetches.EachRecord(func(record *kgo.Record) {
			records = append(records, record)
		})
	}

	return records
}

func readAllRequestsByPartitionFromKafka(t testing.TB, kafkaAddresses []string, numPartitions int32, timeout time.Duration) map[int32][]*mimirpb.WriteRequest {
	requestsByPartition := make(map[int32][]*mimirpb.WriteRequest, numPartitions)
	records := readAllRecordsFromKafka(t, kafkaAddresses, numPartitions, timeout)

	for _, record := range records {
		req := &mimirpb.WriteRequest{}
		require.NoError(t, req.Unmarshal(record.Value))

		requestsByPartition[record.Partition] = append(requestsByPartition[record.Partition], req)
	}

	return requestsByPartition
}

func readAllMetricNamesByPartitionFromKafka(t testing.TB, kafkaAddresses []string, numPartitions int32, timeout time.Duration) map[int32][]string {
	requestsByPartition := readAllRequestsByPartitionFromKafka(t, kafkaAddresses, numPartitions, timeout)
	actualSeriesByPartition := map[int32][]string{}

	for partitionID, requests := range requestsByPartition {
		for _, req := range requests {
			for _, series := range req.Timeseries {
				metricName, _ := extract.UnsafeMetricNameFromLabelAdapters(series.Labels)
				actualSeriesByPartition[partitionID] = append(actualSeriesByPartition[partitionID], metricName)
			}
		}

		slices.Sort(actualSeriesByPartition[partitionID])
	}

	return actualSeriesByPartition
}
