// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

// newTestWarpstreamClient brings up a kfake cluster and wires a
// WarpstreamClient against it. The background metadata refresh goroutine
// runs but its ticker (MetadataRefreshInterval) is well above any individual
// test's runtime, so it never fires; t.Cleanup ensures Close cancels the
// refresh ctx and joins the goroutine.
func newTestWarpstreamClient(t *testing.T, topic string, numPartitions int32) (*WarpstreamClient, string) {
	t.Helper()

	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topic)

	cfg := Config{
		Address:                 []string{clusterAddr},
		Topic:                   topic,
		ClientID:                "warpstream-test",
		DialTimeout:             2 * time.Second,
		WriteTimeout:            5 * time.Second,
		Linger:                  10 * time.Millisecond,
		MaxBatchBytes:           1 << 20,
		MaxBufferedBytes:        1 << 24,
		HedgeSlowMultiplier:     2.0,
		HedgeMaxSlowFraction:    0.3,
		HedgeFaultyThreshold:    0.05,
		HedgeMaxFaultyFraction:  0.3,
		HedgeMinDelay:           10 * time.Millisecond,
		ClusterStatsTTL:         time.Second,
		MetadataRefreshInterval: 10 * time.Second,
	}

	c, err := NewWarpstreamClient(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = c.Close()
	})
	return c, clusterAddr
}

func TestWarpstreamClient_ProduceSync(t *testing.T) {
	const topic = "test-topic"

	t.Run("single record produces and is consumable", func(t *testing.T) {
		c, clusterAddr := newTestWarpstreamClient(t, topic, 1)

		results := c.ProduceSync(context.Background(), []*kgo.Record{
			{Topic: topic, Partition: 0, Key: []byte("k"), Value: []byte("v"), Timestamp: time.Now()},
		})
		require.Len(t, results, 1)
		require.NoError(t, results[0].Err)

		// Verify the record landed by consuming it back.
		consumer, err := kgo.NewClient(
			kgo.SeedBrokers(clusterAddr),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart()},
			}),
		)
		require.NoError(t, err)
		t.Cleanup(consumer.Close)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		fetches := consumer.PollFetches(ctx)
		require.NoError(t, fetches.Err())
		require.Len(t, fetches.Records(), 1)
		assert.Equal(t, []byte("v"), fetches.Records()[0].Value)
	})

	t.Run("records across two partitions both succeed", func(t *testing.T) {
		c, _ := newTestWarpstreamClient(t, topic, 2)

		results := c.ProduceSync(context.Background(), []*kgo.Record{
			{Topic: topic, Partition: 0, Value: []byte("a"), Timestamp: time.Now()},
			{Topic: topic, Partition: 1, Value: []byte("b"), Timestamp: time.Now()},
		})
		require.Len(t, results, 2)
		assert.NoError(t, results[0].Err)
		assert.NoError(t, results[1].Err)
	})

	t.Run("results preserve input record order", func(t *testing.T) {
		c, _ := newTestWarpstreamClient(t, topic, 1)

		records := []*kgo.Record{
			{Topic: topic, Partition: 0, Value: []byte("0"), Timestamp: time.Now()},
			{Topic: topic, Partition: 0, Value: []byte("1"), Timestamp: time.Now()},
			{Topic: topic, Partition: 0, Value: []byte("2"), Timestamp: time.Now()},
		}
		results := c.ProduceSync(context.Background(), records)
		require.Len(t, results, 3)
		for i, r := range results {
			assert.Same(t, records[i], r.Record, "result[%d] must reference input record %d", i, i)
		}
	})

	t.Run("empty input returns nil", func(t *testing.T) {
		c, _ := newTestWarpstreamClient(t, topic, 1)
		results := c.ProduceSync(context.Background(), nil)
		assert.Nil(t, results)
	})

	t.Run("record for unknown topic-partition fails fast at the resolver", func(t *testing.T) {
		c, _ := newTestWarpstreamClient(t, topic, 1)

		results := c.ProduceSync(context.Background(), []*kgo.Record{
			{Topic: "does-not-exist", Partition: 0, Value: []byte("v"), Timestamp: time.Now()},
		})
		require.Len(t, results, 1)
		require.Error(t, results[0].Err)
		assert.ErrorContains(t, results[0].Err, "no agent assigned")
	})

	t.Run("canceled ctx ends the wait", func(t *testing.T) {
		c, _ := newTestWarpstreamClient(t, topic, 1)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		results := c.ProduceSync(ctx, []*kgo.Record{
			{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()},
		})
		require.Len(t, results, 1)
		require.ErrorIs(t, results[0].Err, context.Canceled)
	})
}

func TestWarpstreamClient_Close(t *testing.T) {
	t.Run("close is idempotent", func(t *testing.T) {
		c, _ := newTestWarpstreamClient(t, "test-topic", 1)
		require.NoError(t, c.Close())
		require.NoError(t, c.Close())
	})

	t.Run("close flushes pending records", func(t *testing.T) {
		const topic = "test-topic"
		c, clusterAddr := newTestWarpstreamClient(t, topic, 1)

		// Produce without waiting for the linger.
		doneCh := make(chan error, 1)
		c.buffer.Add(context.Background(), []*kgo.Record{
			{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()},
		}, func(err error) { doneCh <- err })

		require.NoError(t, c.Close())
		require.NoError(t, <-doneCh, "pending record must be flushed by Close")

		// Verify the record landed.
		consumer, err := kgo.NewClient(
			kgo.SeedBrokers(clusterAddr),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart()},
			}),
		)
		require.NoError(t, err)
		t.Cleanup(consumer.Close)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		fetches := consumer.PollFetches(ctx)
		require.NoError(t, fetches.Err())
		require.Len(t, fetches.Records(), 1)
	})
}
