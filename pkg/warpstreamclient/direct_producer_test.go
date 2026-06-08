// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestKafkaDirectProducer_ImplementsDirectProducer(t *testing.T) {
	var _ DirectProducer = (*KafkaDirectProducer)(nil)
}

func TestKafkaDirectProducer_Produce(t *testing.T) {
	const (
		topicName     = "test-topic"
		numPartitions = int32(1)
		partition     = int32(0)
		// kfake assigns NodeID 0 to the single broker in a one-broker cluster.
		brokerNodeID = int32(0)
	)

	t.Run("produces a record successfully and round-trips via consumer", func(t *testing.T) {
		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

		client, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr))
		require.NoError(t, err)
		t.Cleanup(client.Close)

		// Fetch topic metadata to obtain the topic UUID. Both Topic (name) and TopicID
		// (UUID) are set in the ProduceRequest so the request is valid regardless of
		// which API version the connection negotiates: v0-v12 use the name, v13+ the UUID.
		metaResp, err := client.Request(context.Background(), &kmsg.MetadataRequest{
			Topics: []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(topicName)}},
		})
		require.NoError(t, err)
		meta := metaResp.(*kmsg.MetadataResponse)
		require.Len(t, meta.Topics, 1)
		topicID := meta.Topics[0].TopicID

		reg := prometheus.NewPedanticRegistry()
		topicIDFn := func(name string) ([16]byte, bool) {
			if name == topicName {
				return topicID, true
			}
			return [16]byte{}, false
		}
		m := newMetrics(reg)
		producer := NewKafkaDirectProducer(client, topicIDFn, 9, KafkaDirectProducerConfig{
			ProduceRequestTimeout:         time.Second,
			ProduceRequestTimeoutOverhead: time.Second,
		}, m)

		records := []*kgo.Record{{
			Topic:     topicName,
			Partition: partition,
			Key:       []byte("test-key"),
			Value:     []byte("test-value"),
			Timestamp: time.Now(),
		}}

		resp, err := producer.ProduceSync(context.Background(), brokerNodeID, partitionsForTest(records))
		require.NoError(t, err)
		require.NoError(t, parseProduceResponse(resp))

		// Verify the record was actually stored by consuming it back.
		consumer, err := kgo.NewClient(
			kgo.SeedBrokers(clusterAddr),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topicName: {partition: kgo.NewOffset().AtStart()},
			}),
		)
		require.NoError(t, err)
		t.Cleanup(consumer.Close)

		fetchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)

		fetches := consumer.PollFetches(fetchCtx)
		require.NoError(t, fetches.Err())
		require.Len(t, fetches.Records(), 1)
		assert.Equal(t, []byte("test-key"), fetches.Records()[0].Key)
		assert.Equal(t, []byte("test-value"), fetches.Records()[0].Value)

		// Counter ticked once on the successful Produce; no failures.
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP produce_requests_total Total number of Produce requests issued to a Warpstream agent. Each retry counts as a separate request.
			# TYPE produce_requests_total counter
			produce_requests_total 1
		`), "produce_requests_total", "produce_requests_failed_total"))

		// Per-attempt latency recorded once under outcome=success; no failure
		// series exists (only the success series is collected).
		successCount, _ := histogramCountSum(t, m.produceRequestLatencySuccess.(prometheus.Histogram))
		assert.Equal(t, uint64(1), successCount)
		assert.Equal(t, 1, testutil.CollectAndCount(reg, "produce_request_latency_seconds"))
	})

	t.Run("applies per-attempt timeout (TimeoutMillis on wire + client-side ctx deadline)", func(t *testing.T) {
		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

		// Capture every Produce request that reaches the broker.
		type captured struct {
			timeoutMillis int32
		}
		var (
			seenMu sync.Mutex
			seen   []captured
			hold   = make(chan struct{})
			holdCh = sync.OnceFunc(func() { close(hold) })
		)
		t.Cleanup(holdCh)

		cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
			pr := req.(*kmsg.ProduceRequest)
			seenMu.Lock()
			seen = append(seen, captured{timeoutMillis: pr.TimeoutMillis})
			seenMu.Unlock()
			// Block to force the client-side deadline to fire.
			<-hold
			return nil, nil, false
		})

		client, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr))
		require.NoError(t, err)
		t.Cleanup(client.Close)

		metaResp, err := client.Request(context.Background(), &kmsg.MetadataRequest{
			Topics: []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(topicName)}},
		})
		require.NoError(t, err)
		topicID := metaResp.(*kmsg.MetadataResponse).Topics[0].TopicID

		const (
			produceTimeout         = 250 * time.Millisecond
			produceTimeoutOverhead = 250 * time.Millisecond
		)
		reg := prometheus.NewPedanticRegistry()
		topicIDFn := func(name string) ([16]byte, bool) {
			if name == topicName {
				return topicID, true
			}
			return [16]byte{}, false
		}
		producer := NewKafkaDirectProducer(client, topicIDFn, 9, KafkaDirectProducerConfig{
			ProduceRequestTimeout:         produceTimeout,
			ProduceRequestTimeoutOverhead: produceTimeoutOverhead,
		}, newMetrics(reg))

		startedAt := time.Now()
		_, err = producer.ProduceSync(context.Background(), brokerNodeID, partitionsForTest([]*kgo.Record{{
			Topic: topicName, Partition: partition, Value: []byte("v"), Timestamp: time.Now(),
		}}))
		elapsed := time.Since(startedAt)

		// Verify (a) per-attempt deadline fires on the client side as
		// context.DeadlineExceeded, and (b) it does so within the configured
		// budget plus a small slack.
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Less(t, elapsed, produceTimeout+produceTimeoutOverhead+time.Second)

		holdCh()

		// Verify req.TimeoutMillis was set from cfg.ProduceRequestTimeout (not
		// the kmsg default 15000).
		seenMu.Lock()
		var firstTimeoutMillis int32
		if len(seen) > 0 {
			firstTimeoutMillis = seen[0].timeoutMillis
		}
		seenMu.Unlock()
		assert.Equal(t, int32(produceTimeout.Milliseconds()), firstTimeoutMillis, "req.TimeoutMillis must reflect cfg.ProduceRequestTimeout")

		// Failure was an attempt-timeout (DeadlineExceeded from inner ctx).
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP produce_requests_total Total number of Produce requests issued to a Warpstream agent. Each retry counts as a separate request.
			# TYPE produce_requests_total counter
			produce_requests_total 1
			# HELP produce_requests_failed_total Total number of Produce requests issued to a Warpstream agent that failed, by failure reason. Each retry counts as a separate request.
			# TYPE produce_requests_failed_total counter
			produce_requests_failed_total{reason="timeout"} 1
		`), "produce_requests_total", "produce_requests_failed_total"))
	})
}

// partitionsForTest groups records by (topic, partition) into a slice of
// routedTopicPartitionRecords. done is left nil because callers in this file
// (DirectProducer-level tests) don't go through the buffer.
func partitionsForTest(records []*kgo.Record) []topicPartitionRecords {
	groups := make(map[topicPartition]*topicPartitionRecords)
	var order []topicPartition
	for _, r := range records {
		key := topicPartition{topic: r.Topic, partition: r.Partition}
		g, ok := groups[key]
		if !ok {
			g = &topicPartitionRecords{topic: r.Topic, partition: r.Partition}
			groups[key] = g
			order = append(order, key)
		}
		g.records = append(g.records, r)
	}
	out := make([]topicPartitionRecords, 0, len(order))
	for _, k := range order {
		out = append(out, *groups[k])
	}
	return out
}
