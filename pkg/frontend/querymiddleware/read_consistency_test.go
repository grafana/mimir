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
	"github.com/twmb/franz-go/pkg/kgo"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestReadConsistencyRoundTripper(t *testing.T) {
	const (
		topic         = "test"
		numPartitions = 10
		tenantID      = "user-1"
	)

	tests := map[string]struct {
		limits          Limits
		reqConsistency  string
		expectedOffsets bool
	}{
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

	for testName, testData := range tests {
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
			readClient, err := ingest.NewKafkaReaderClient(createKafkaConfig(clusterAddr, topic), nil, logger)
			require.NoError(t, err)
			t.Cleanup(readClient.Close)

			reader := ingest.NewTopicOffsetsReaderForAllPartitions(readClient, topic, 100*time.Millisecond, nil, logger)
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

			reg := prometheus.NewPedanticRegistry()
			rt := newReadConsistencyRoundTripper(downstream, map[string]*ingest.TopicOffsetsReader{querierapi.ReadConsistencyOffsetsHeader: reader}, testData.limits, log.NewNopLogger(), newReadConsistencyMetrics(reg))
			_, err = rt.RoundTrip(req)
			require.NoError(t, err)

			require.NotNil(t, downstreamReq)

			if testData.expectedOffsets {
				offsets := querierapi.EncodedOffsets(downstreamReq.Header.Get(querierapi.ReadConsistencyOffsetsHeader))

				for partitionID, expectedOffset := range expectedOffsets {
					actual, ok := offsets.Lookup(partitionID)
					assert.True(t, ok)
					assert.Equal(t, expectedOffset, actual)
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
				cortex_ingest_storage_strong_consistency_requests_total{component="query-frontend", with_offset="false"} %d
				cortex_ingest_storage_strong_consistency_requests_total{component="query-frontend", with_offset="true"} 0

				# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
				# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
				cortex_ingest_storage_strong_consistency_failures_total{component="query-frontend"} 0
			`, expectedRequests)),
				"cortex_ingest_storage_strong_consistency_requests_total",
				"cortex_ingest_storage_strong_consistency_failures_total"))
		})
	}
}

func TestGetDefaultReadConsistency(t *testing.T) {
	defaults := validation.Limits{IngestStorageReadConsistency: querierapi.ReadConsistencyEventual}
	tenantLimits := map[string]*validation.Limits{
		// tenant-a has no overrides
		"tenant-b": {IngestStorageReadConsistency: querierapi.ReadConsistencyEventual},
		"tenant-c": {IngestStorageReadConsistency: querierapi.ReadConsistencyStrong},
	}

	ov, err := validation.NewOverrides(defaults, validation.NewMockTenantLimits(tenantLimits))
	require.NoError(t, err)

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
	cfg.Address = clusterAddr
	cfg.Topic = topic

	return cfg
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
