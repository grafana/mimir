// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestIngester_FlushHandler_WhileStarting(t *testing.T) {
	var (
		ctx                = context.Background()
		cfg                = defaultIngesterTestConfig(t)
		reg                = prometheus.NewRegistry()
		fetchRequestsCount = atomic.NewInt64(0)
		series1            = mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:  mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "series_1")),
			Samples: []mimirpb.Sample{{TimestampMs: 1000, Value: 10}},
		}}
	)

	const expectedSeriesToReplay = 2

	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Minute
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalWhileStarting = 100 * time.Millisecond
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled = false

	overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	ingester, kafkaCluster, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, reg, util_test.NewTestingLogger(t))

	// Mock the Kafka cluster to count Fetch requests and return a "last produced offset" in the future,
	// so that the ingester will be stuck replaying the partition.
	kafkaCluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		kafkaCluster.KeepControl()
		fetchRequestsCount.Inc()
		return nil, nil, false
	})

	kafkaCluster.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		kafkaCluster.KeepControl()

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

	// Write a series to Kafka.
	writer := ingest.NewWriter(cfg.IngestStorageConfig.KafkaConfig, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, writer))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, writer))
	})

	partitionID, err := ingest.IngesterPartitionID(cfg.IngesterRing.InstanceID)
	require.NoError(t, err)
	require.NoError(t, writer.WriteSync(ctx, cfg.IngestStorageConfig.KafkaConfig.Topic, partitionID, userID, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{series1}, Source: mimirpb.API}))

	// Start the ingester.
	require.NoError(t, ingester.StartAsync(ctx))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
	})

	// Wait until the ingester is stuck in Starting state replaying the partition.
	test.Poll(t, 5*time.Second, true, func() interface{} {
		return fetchRequestsCount.Load() > 0
	})
	assert.Equal(t, services.Starting, ingester.State())
	assert.Equal(t, services.Running, ingester.compactionService.State())

	// Call FlushHandler while the ingester is starting. It should succeed.
	rec := httptest.NewRecorder()
	ingester.FlushHandler(rec, httptest.NewRequest("POST", "/ingester/flush?wait=true", nil))
	assert.Equal(t, http.StatusNoContent, rec.Result().StatusCode)

	// Unblock the ingester by writing the remaining series to Kafka.
	series2 := mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
		Labels:  mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "series_2")),
		Samples: []mimirpb.Sample{{TimestampMs: 1000, Value: 10}},
	}}
	require.NoError(t, writer.WriteSync(ctx, cfg.IngestStorageConfig.KafkaConfig.Topic, partitionID, userID, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{series2}, Source: mimirpb.API}))

	// Wait for the ingester to transition to Running state.
	test.Poll(t, 5*time.Second, services.Running, func() interface{} {
		return ingester.State()
	})
}
