package ingest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestWriter_WriteSync(t *testing.T) {
	const (
		topicName     = "test"
		numPartitions = 1
		tenantID      = "user-1"
	)

	var (
		ctx        = context.Background()
		reg        = prometheus.NewPedanticRegistry()
		timeseries = []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1"), mockPreallocTimeseries("series_2")}
	)

	// Create fake Kafka cluster.
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(numPartitions, topicName))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	addrs := cluster.ListenAddrs()
	require.Len(t, addrs, 1)

	// Init writer.
	writer := NewWriter(addrs[0], topicName, log.NewNopLogger(), reg)

	t.Run("should block until data has been committed to storage", func(t *testing.T) {
		const partitionID = 0

		err := writer.WriteSync(ctx, partitionID, tenantID, timeseries, nil, mimirpb.API)
		require.NoError(t, err)

		// Read back from Kafka.
		consumer, err := kgo.NewClient(kgo.SeedBrokers(addrs[0]), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topicName: {int32(partitionID): kgo.NewOffset().AtStart()}}))
		require.NoError(t, err)
		t.Cleanup(consumer.Close)

		fetchCtx, cancel := context.WithTimeout(ctx, time.Second)
		t.Cleanup(cancel)

		fetches := consumer.PollFetches(fetchCtx)
		require.NoError(t, fetches.Err())
		require.Len(t, fetches.Records(), 1)
		assert.Equal(t, []byte(tenantID), fetches.Records()[0].Key)

		received := mimirpb.WriteRequest{}
		require.NoError(t, received.Unmarshal(fetches.Records()[0].Value))
		require.Len(t, received.Timeseries, len(timeseries))

		for idx, expected := range timeseries {
			assert.Equal(t, expected.Labels, received.Timeseries[idx].Labels)
			assert.Equal(t, expected.Samples, received.Timeseries[idx].Samples)
		}

		// Check metrics.
		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_ingest_storage_writer_sent_bytes_total Total number of bytes sent to the ingest storage.
			# TYPE cortex_ingest_storage_writer_sent_bytes_total counter
			cortex_ingest_storage_writer_sent_bytes_total %d
		`, len(fetches.Records()[0].Value))), "cortex_ingest_storage_writer_sent_bytes_total"))
	})

	t.Run("should return error on failure committing data to storage", func(t *testing.T) {
		// Write to a non-existing partition.
		err := writer.WriteSync(ctx, 100, tenantID, timeseries, nil, mimirpb.API)
		require.Error(t, err)
	})
}

func mockPreallocTimeseries(metricName string) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: metricName}},
			Samples: []mimirpb.Sample{{TimestampMs: 1, Value: 2}},
		},
	}
}
