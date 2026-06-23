// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

type readConsistencyRoundTripperTestCase struct {
	limits          Limits
	reqConsistency  string
	expectedOffsets bool
}

// readConsistencyRoundTripperTestCases returns the consistency-injection cases shared by the single-cluster
// and multi-cluster round tripper tests, so both exercise the exact same behaviour.
func readConsistencyRoundTripperTestCases() map[string]readConsistencyRoundTripperTestCase {
	return map[string]readConsistencyRoundTripperTestCase{
		"should not inject offsets if default read consistency is 'eventual' and request has explicitly requested any consistency level": {
			limits:          mockLimits{ingestStorageReadConsistency: querierapi.ReadConsistencyEventual},
			expectedOffsets: false,
		},
		"should not inject offsets if default read consistency is 'strong' and request has explicitly requested 'eventual' consistency": {
			limits:          mockLimits{ingestStorageReadConsistency: querierapi.ReadConsistencyStrong},
			reqConsistency:  querierapi.ReadConsistencyEventual,
			expectedOffsets: false,
		},
		"should inject offsets if default read consistency is 'eventual' but request has explicitly requested 'strong' consistency": {
			limits:          mockLimits{ingestStorageReadConsistency: querierapi.ReadConsistencyEventual},
			reqConsistency:  querierapi.ReadConsistencyStrong,
			expectedOffsets: true,
		},
		"should inject offsets if default read consistency is 'strong' and request has not explicitly requested any consistency level": {
			limits:          mockLimits{ingestStorageReadConsistency: querierapi.ReadConsistencyStrong},
			expectedOffsets: true,
		},
	}
}

func TestReadConsistencyRoundTripper_SingleCluster(t *testing.T) {
	const (
		topic         = "test"
		numPartitions = 10
		tenantID      = "user-1"
	)

	for testName, testData := range readConsistencyRoundTripperTestCases() {
		t.Run(testName, func(t *testing.T) {
			// Capture the downstream HTTP request.
			var downstreamReq *http.Request
			downstream := RoundTripFunc(func(req *http.Request) (*http.Response, error) {
				downstreamReq = req
				return nil, nil
			})

			ctx := context.Background()
			logger := log.NewNopLogger()

			_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topic)

			// Write some records to different partitions.
			expectedOffsets := produceKafkaRecords(t, clusterAddr, topic,
				&kgo.Record{Partition: 0},
				&kgo.Record{Partition: 0},
				&kgo.Record{Partition: 0},
				&kgo.Record{Partition: 1},
				&kgo.Record{Partition: 1},
				&kgo.Record{Partition: 2},
			)

			// Create the topic offsets reader.
			kafkaCfg := createKafkaConfig(clusterAddr, topic)
			kafkaCfg.LastProducedOffsetPollInterval = 100 * time.Millisecond
			reader, err := ingest.NewSingleClusterTopicOffsetsReader(kafkaCfg, topic, allTopicPartitionIDs(numPartitions), "query-frontend", prometheus.NewPedanticRegistry(), logger)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
			})

			// Send an HTTP request through the roundtripper.
			req := httptest.NewRequest("GET", "/", nil)
			req = req.WithContext(user.InjectOrgID(req.Context(), tenantID))

			if testData.reqConsistency != "" {
				req = req.WithContext(querierapi.ContextWithReadConsistencyLevel(req.Context(), testData.reqConsistency))
			}

			offsetsReader := NewSingleClusterReadConsistencyOffsetsReader(reader)

			reg := prometheus.NewPedanticRegistry()
			rt := newReadConsistencyRoundTripper(downstream, offsetsReader, testData.limits, log.NewNopLogger(), newReadConsistencyMetrics(reg, offsetsReader))
			_, err = rt.RoundTrip(req)
			require.NoError(t, err)

			require.NotNil(t, downstreamReq)

			if testData.expectedOffsets {
				offsets := querierapi.EncodedOffsets(downstreamReq.Header.Get(querierapi.ReadConsistencyOffsetsHeader))

				for partitionID, expectedOffset := range expectedOffsets {
					actual, ok := offsets.Lookup(0, partitionID)
					assert.True(t, ok)
					assert.Equal(t, expectedOffset, actual.ForKafkaCluster(0))
				}
			} else {
				assert.Empty(t, downstreamReq.Header.Get(querierapi.ReadConsistencyOffsetsHeader))
			}

			// Metrics should be tracked only if the strong consistency is enforced.
			expectedRequests := 0
			if testData.expectedOffsets {
				expectedRequests = 1
			}

			assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested. The metric distinguishes between requests with an offset specified and requests requesting to enforce strong consistency up until the last produced offset.
				# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
				cortex_ingest_storage_strong_consistency_requests_total{component="query-frontend", topic="%s", with_offset="false"} %d
				cortex_ingest_storage_strong_consistency_requests_total{component="query-frontend", topic="%s", with_offset="true"} 0

				# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
				# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
				cortex_ingest_storage_strong_consistency_failures_total{component="query-frontend", topic="%s"} 0
			`, topic, expectedRequests, topic, topic)),
				"cortex_ingest_storage_strong_consistency_requests_total",
				"cortex_ingest_storage_strong_consistency_failures_total"))
		})
	}
}

// TestReadConsistencyRoundTripper_MultiCluster runs the same consistency-injection cases as
// TestReadConsistencyRoundTripper_SingleCluster, but against a multi-cluster offsets reader spanning two
// topics across two Kafka clusters. When offsets are injected, it verifies the single v2-encoded header
// carries one real offset per cluster for every topic and partition.
func TestReadConsistencyRoundTripper_MultiCluster(t *testing.T) {
	const (
		topic0        = "test-0"
		topic1        = "test-1"
		numPartitions = 10
		tenantID      = "user-1"
	)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// The Kafka clusters, the produced records and the offsets reader are shared across all test cases: none
	// of them changes between cases (only the request's consistency and the tenant limits do), and the
	// started reader is only read from.
	_, addr0 := testkafka.CreateCluster(t, numPartitions, topic0)
	createReadConsistencyTestTopic(t, addr0, topic1, numPartitions)
	_, addr1 := testkafka.CreateCluster(t, numPartitions, topic0)
	createReadConsistencyTestTopic(t, addr1, topic1, numPartitions)

	// Write some records to different partitions, producing a distinct offset matrix per cluster and topic
	// (including partitions present on one cluster but not the other).
	cluster0Topic0 := produceKafkaRecords(t, addr0, topic0,
		&kgo.Record{Partition: 0},
		&kgo.Record{Partition: 0},
		&kgo.Record{Partition: 0},
		&kgo.Record{Partition: 1},
		&kgo.Record{Partition: 1},
		&kgo.Record{Partition: 2},
	)
	cluster0Topic1 := produceKafkaRecords(t, addr0, topic1, &kgo.Record{Partition: 0})
	cluster1Topic0 := produceKafkaRecords(t, addr1, topic0, &kgo.Record{Partition: 0})
	// Cluster 1, topic 1: no records, so every partition is empty (-1).

	cfg0 := createKafkaConfig(addr0, topic0)
	cfg0.LastProducedOffsetPollInterval = 100 * time.Millisecond
	cfg1 := createKafkaConfig(addr1, topic0)
	cfg1.LastProducedOffsetPollInterval = 100 * time.Millisecond

	reader, err := ingest.NewMultiClusterOffsetsReader(
		[]ingest.KafkaConfig{cfg0, cfg1},
		[]string{topic0, topic1},
		[]ingest.GetPartitionIDsFunc{allTopicPartitionIDs(numPartitions), allTopicPartitionIDs(numPartitions)},
		"query-frontend",
		prometheus.NewPedanticRegistry(),
		logger,
	)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, reader)) })

	// The v2 encoding is keyed by topic index (topic 0 → 0, topic 1 → 1); each entry carries one offset per
	// cluster, with -1 where a partition has no records on a cluster.
	expectedOffsetsByTopic := map[int]map[int32][]int64{
		0: mergeClusterOffsets(cluster0Topic0, cluster1Topic0),
		1: mergeClusterOffsets(cluster0Topic1, nil),
	}

	for testName, testData := range readConsistencyRoundTripperTestCases() {
		t.Run(testName, func(t *testing.T) {
			// Capture the downstream HTTP request.
			var downstreamReq *http.Request
			downstream := RoundTripFunc(func(req *http.Request) (*http.Response, error) {
				downstreamReq = req
				return nil, nil
			})

			// Send an HTTP request through the roundtripper.
			req := httptest.NewRequest("GET", "/", nil)
			req = req.WithContext(user.InjectOrgID(req.Context(), tenantID))

			if testData.reqConsistency != "" {
				req = req.WithContext(querierapi.ContextWithReadConsistencyLevel(req.Context(), testData.reqConsistency))
			}

			offsetsReader := NewMultiClusterReadConsistencyOffsetsReader(reader)

			reg := prometheus.NewPedanticRegistry()
			rt := newReadConsistencyRoundTripper(downstream, offsetsReader, testData.limits, logger, newReadConsistencyMetrics(reg, offsetsReader))
			_, err := rt.RoundTrip(req)
			require.NoError(t, err)

			require.NotNil(t, downstreamReq)

			if testData.expectedOffsets {
				headers := downstreamReq.Header.Values(querierapi.ReadConsistencyOffsetsHeader)
				require.Len(t, headers, 1)
				offsets := querierapi.EncodedOffsets(headers[0])

				for topicIdx, partitions := range expectedOffsetsByTopic {
					for partitionID, clusterOffsets := range partitions {
						actual, ok := offsets.Lookup(topicIdx, partitionID)
						assert.True(t, ok)
						assert.Equal(t, kmeta.NewMultiClusterPartitionOffsets(clusterOffsets), actual)
					}
				}
			} else {
				assert.Empty(t, downstreamReq.Header.Get(querierapi.ReadConsistencyOffsetsHeader))
			}

			// Metrics should be tracked only if the strong consistency is enforced. The reader monitors
			// multiple topics, so the "topic" label is "mixed".
			expectedRequests := 0
			if testData.expectedOffsets {
				expectedRequests = 1
			}

			assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested. The metric distinguishes between requests with an offset specified and requests requesting to enforce strong consistency up until the last produced offset.
				# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
				cortex_ingest_storage_strong_consistency_requests_total{component="query-frontend", topic="mixed", with_offset="false"} %d
				cortex_ingest_storage_strong_consistency_requests_total{component="query-frontend", topic="mixed", with_offset="true"} 0

				# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
				# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
				cortex_ingest_storage_strong_consistency_failures_total{component="query-frontend", topic="mixed"} 0
			`, expectedRequests)),
				"cortex_ingest_storage_strong_consistency_requests_total",
				"cortex_ingest_storage_strong_consistency_failures_total"))
		})
	}
}

func createReadConsistencyTestTopic(t *testing.T, clusterAddr, topic string, numPartitions int32) {
	client, err := ingest.NewKafkaReaderClient(createKafkaConfig(clusterAddr, topic), nil, log.NewNopLogger())
	require.NoError(t, err)
	t.Cleanup(client.Close)

	_, err = kadm.NewClient(client).CreateTopic(context.Background(), numPartitions, 1, nil, topic)
	require.NoError(t, err)
}

func TestGetDefaultReadConsistency(t *testing.T) {
	defaults := validation.Limits{IngestStorageReadConsistency: querierapi.ReadConsistencyEventual}
	tenantLimits := map[string]*validation.Limits{
		// tenant-a has no overrides
		"tenant-b": {IngestStorageReadConsistency: querierapi.ReadConsistencyEventual},
		"tenant-c": {IngestStorageReadConsistency: querierapi.ReadConsistencyStrong},
	}

	ov := validation.NewOverrides(defaults, validation.NewMockTenantLimits(tenantLimits))

	tests := []struct {
		tenantIDs []string
		expected  string
	}{
		// Single tenant.
		{tenantIDs: []string{"tenant-a"}, expected: querierapi.ReadConsistencyEventual},
		{tenantIDs: []string{"tenant-b"}, expected: querierapi.ReadConsistencyEventual},
		{tenantIDs: []string{"tenant-c"}, expected: querierapi.ReadConsistencyStrong},

		// Multi tenant.
		{tenantIDs: []string{"tenant-a", "tenant-b"}, expected: querierapi.ReadConsistencyEventual},
		{tenantIDs: []string{"tenant-a", "tenant-c"}, expected: querierapi.ReadConsistencyStrong},
		{tenantIDs: []string{"tenant-b", "tenant-c"}, expected: querierapi.ReadConsistencyStrong},
	}

	for testID, testData := range tests {
		t.Run(fmt.Sprintf("Test case #%d", testID), func(t *testing.T) {
			assert.Equal(t, testData.expected, getDefaultReadConsistency(testData.tenantIDs, ov))
		})
	}
}

func createKafkaConfig(clusterAddr, topic string) ingest.KafkaConfig {
	cfg := ingest.KafkaConfig{}
	flagext.DefaultValues(&cfg)
	cfg.Address = flagext.StringSliceCSV{clusterAddr}
	cfg.Topic = topic

	return cfg
}

// allTopicPartitionIDs returns a GetPartitionIDsFunc listing the partitions [0, numPartitions).
func allTopicPartitionIDs(numPartitions int) ingest.GetPartitionIDsFunc {
	return func(context.Context) ([]int32, error) {
		ids := make([]int32, numPartitions)
		for i := range ids {
			ids[i] = int32(i)
		}
		return ids, nil
	}
}

// mergeClusterOffsets builds the expected per-partition offsets across two Kafka clusters from each
// cluster's per-partition highest produced offsets, defaulting to -1 for a partition with no records on a
// cluster. Only partitions present on at least one cluster are returned.
func mergeClusterOffsets(cluster0, cluster1 map[int32]int64) map[int32][]int64 {
	merged := make(map[int32][]int64)
	ensure := func(partitionID int32) []int64 {
		if _, ok := merged[partitionID]; !ok {
			merged[partitionID] = []int64{-1, -1}
		}
		return merged[partitionID]
	}
	for partitionID, offset := range cluster0 {
		ensure(partitionID)[0] = offset
	}
	for partitionID, offset := range cluster1 {
		ensure(partitionID)[1] = offset
	}
	return merged
}

// produceKafkaRecords produces the input records to Kafka and returns the highest produced offset
// for each partition.
func produceKafkaRecords(t *testing.T, clusterAddr, topic string, records ...*kgo.Record) map[int32]int64 {
	cfg := createKafkaConfig(clusterAddr, topic)
	reg := prometheus.NewPedanticRegistry()

	writeClient, err := ingest.NewKafkaWriterClient(cfg, 1, log.NewNopLogger(), reg)
	require.NoError(t, err)
	t.Cleanup(writeClient.Close)

	writeRes := writeClient.ProduceSync(context.Background(), records...)
	require.NoError(t, writeRes.FirstErr())

	// Collect the highest produced offset for each partition.
	offsets := make(map[int32]int64)
	for _, res := range writeRes {
		partition := res.Record.Partition
		offset := res.Record.Offset

		if prev, ok := offsets[partition]; !ok || prev < offset {
			offsets[partition] = offset
		}
	}

	return offsets
}
