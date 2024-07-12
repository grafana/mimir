package ingest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestKafkaWriterClient_ShouldTrackBufferedProduceBytes(t *testing.T) {
	const (
		numPartitions = 1
		topicName     = "test"
	)

	cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

	// Configure Kafka to block on Produce requests until the test unblocks it.
	unblockProduceRequests := make(chan struct{})
	cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
		<-unblockProduceRequests
		return nil, nil, false
	})

	ctx := context.Background()
	cfg := createTestKafkaConfig(clusterAddr, topicName)
	reg := prometheus.NewPedanticRegistry()
	client, err := newKafkaWriterClient(1, cfg, 1, log.NewNopLogger(), reg)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	wg := sync.WaitGroup{}

	// At the beginning, the buffered produced bytes metric should be 0.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.InDelta(collect, 0, getSummaryQuantileValue(collect, reg, "cortex_ingest_storage_writer_buffered_produce_bytes", 1), 0.0001)
	}, time.Second, 100*time.Millisecond)

	// Produce a 1st record.
	wg.Add(1)
	client.Produce(ctx, &kgo.Record{Key: []byte("test"), Value: []byte("message 1")}, func(_ *kgo.Record, err error) {
		defer wg.Done()
		require.NoError(t, err)
	})

	// Get the expected record size directly from Kafka client.
	expectedRecordSize := client.BufferedProduceBytes()
	require.Greater(t, expectedRecordSize, int64(0))
	t.Logf("expected record size bytes: %d", expectedRecordSize)

	// At this point, the buffered produced bytes metric should have tracked 1 record.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.InDelta(collect, expectedRecordSize, getSummaryQuantileValue(collect, reg, "cortex_ingest_storage_writer_buffered_produce_bytes", 1), 0.0001)
	}, time.Second, 100*time.Millisecond)

	// Produce a 2nd record, while the 1st is still in-flight (because in this test Produce requests are blocked on Kafka side).
	wg.Add(1)
	client.Produce(ctx, &kgo.Record{Key: []byte("test"), Value: []byte("message 1")}, func(_ *kgo.Record, err error) {
		defer wg.Done()
		require.NoError(t, err)
	})

	// At this point, the buffered produced bytes metric should have tracked 2 records.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.InDelta(collect, 2*expectedRecordSize, getSummaryQuantileValue(collect, reg, "cortex_ingest_storage_writer_buffered_produce_bytes", 1), 0.0001)
	}, time.Second, 100*time.Millisecond)

	// Release Produce requests and wait until done.
	close(unblockProduceRequests)
	wg.Wait()
}

func getSummaryQuantileValue(t require.TestingT, reg *prometheus.Registry, metricName string, quantile float64) float64 {
	const delta = 0.0001

	metrics, err := reg.Gather()
	require.NoError(t, err)

	for _, family := range metrics {
		if family.GetName() != metricName || family.GetType() != dto.MetricType_SUMMARY {
			continue
		}

		require.Len(t, family.Metric, 1)
		require.NotNil(t, family.Metric[0].Summary)

		for _, q := range family.Metric[0].Summary.Quantile {
			if q.GetQuantile() > quantile-delta && q.GetQuantile() < quantile+delta {
				return q.GetValue()
			}
		}
	}

	require.Failf(t, "summary metric %s or quantile %f not found", metricName, quantile)
	return 0
}
