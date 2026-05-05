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
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

// newTestWarpstreamClient brings up a kfake cluster and wires a
// WarpstreamClient against it. The background metadata refresh goroutine
// runs but its ticker (MetadataRefreshInterval) is well above any individual
// test's runtime, so it never fires; t.Cleanup ensures Close cancels the
// refresh ctx and joins the goroutine.
func newTestWarpstreamClient(t *testing.T, topic string, numPartitions int32) (*WarpstreamClient, *kfake.Cluster, string) {
	t.Helper()

	cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topic)

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
		c.Close()
	})
	return c, cluster, clusterAddr
}

func TestWarpstreamClient_ProduceSync(t *testing.T) {
	const topic = "test-topic"

	t.Run("single record produces and is consumable", func(t *testing.T) {
		c, _, clusterAddr := newTestWarpstreamClient(t, topic, 1)

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
		c, _, _ := newTestWarpstreamClient(t, topic, 2)

		results := c.ProduceSync(context.Background(), []*kgo.Record{
			{Topic: topic, Partition: 0, Value: []byte("a"), Timestamp: time.Now()},
			{Topic: topic, Partition: 1, Value: []byte("b"), Timestamp: time.Now()},
		})
		require.Len(t, results, 2)
		assert.NoError(t, results[0].Err)
		assert.NoError(t, results[1].Err)
	})

	t.Run("results preserve input record order", func(t *testing.T) {
		c, _, _ := newTestWarpstreamClient(t, topic, 1)

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
		c, _, _ := newTestWarpstreamClient(t, topic, 1)
		results := c.ProduceSync(context.Background(), nil)
		assert.Nil(t, results)
	})

	t.Run("record for unknown topic-partition fails fast at the resolver", func(t *testing.T) {
		c, _, _ := newTestWarpstreamClient(t, topic, 1)

		results := c.ProduceSync(context.Background(), []*kgo.Record{
			{Topic: "does-not-exist", Partition: 0, Value: []byte("v"), Timestamp: time.Now()},
		})
		require.Len(t, results, 1)
		require.Error(t, results[0].Err)
		assert.ErrorContains(t, results[0].Err, "no agent assigned")
	})

	t.Run("canceled ctx ends the wait", func(t *testing.T) {
		c, _, _ := newTestWarpstreamClient(t, topic, 1)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		results := c.ProduceSync(ctx, []*kgo.Record{
			{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()},
		})
		require.Len(t, results, 1)
		require.ErrorIs(t, results[0].Err, context.Canceled)
	})
}

func TestWarpstreamClient_Produce(t *testing.T) {
	const topic = "test-topic"

	t.Run("invokes promise once with the same record on success", func(t *testing.T) {
		c, _, _ := newTestWarpstreamClient(t, topic, 1)

		input := &kgo.Record{Topic: topic, Partition: 0, Key: []byte("k"), Value: []byte("v"), Timestamp: time.Now()}
		done := make(chan struct {
			r   *kgo.Record
			err error
		}, 1)
		c.Produce(context.Background(), input, func(r *kgo.Record, err error) {
			done <- struct {
				r   *kgo.Record
				err error
			}{r, err}
		})

		select {
		case got := <-done:
			require.NoError(t, got.err)
			assert.Same(t, input, got.r, "promise must receive the same record pointer")
		case <-time.After(time.Second):
			t.Fatal("promise did not fire")
		}
	})

	t.Run("invokes promise with error when topic is unknown to the agent pool", func(t *testing.T) {
		c, _, _ := newTestWarpstreamClient(t, topic, 1)

		input := &kgo.Record{Topic: "does-not-exist", Partition: 0, Value: []byte("v"), Timestamp: time.Now()}
		done := make(chan error, 1)
		c.Produce(context.Background(), input, func(_ *kgo.Record, err error) {
			done <- err
		})

		select {
		case err := <-done:
			require.Error(t, err)
		case <-time.After(time.Second):
			t.Fatal("promise did not fire")
		}
	})
}

func TestWarpstreamClient_BufferedProduceBytes(t *testing.T) {
	const topic = "test-topic"

	t.Run("zero before any record is buffered", func(t *testing.T) {
		c, _, _ := newTestWarpstreamClient(t, topic, 1)
		assert.Equal(t, int64(0), c.BufferedProduceBytes())
	})

	t.Run("reflects in-flight bytes while produce is held, drops to zero once it completes", func(t *testing.T) {
		c, cluster, _ := newTestWarpstreamClient(t, topic, 1)

		// Block the broker's Produce handler so the in-flight state is
		// observable deterministically rather than racing against the linger
		// timer.
		release := make(chan struct{})
		cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
			<-release
			return nil, nil, false
		})

		const value = "in-flight value"
		records := []*kgo.Record{
			{Topic: topic, Partition: 0, Value: []byte(value), Timestamp: time.Now()},
		}
		produced := make(chan struct{})
		go func() {
			defer close(produced)
			c.ProduceSync(context.Background(), records)
		}()

		// While the broker is holding the Produce request, the bytes must
		// be reflected as in-flight.
		require.Eventually(t, func() bool {
			return c.BufferedProduceBytes() == int64(len(value))
		}, time.Second, 5*time.Millisecond)

		// Release the broker; the counter drops back to zero once the
		// produce completes.
		close(release)
		<-produced
		require.Eventually(t, func() bool { return c.BufferedProduceBytes() == 0 },
			time.Second, 5*time.Millisecond)
	})
}

func TestWarpstreamClient_BufferedProduceRecords(t *testing.T) {
	const topic = "test-topic"

	t.Run("zero before any record is buffered", func(t *testing.T) {
		c, _, _ := newTestWarpstreamClient(t, topic, 1)
		assert.Equal(t, int64(0), c.BufferedProduceRecords())
	})

	t.Run("reflects in-flight records while produce is held, drops to zero once it completes", func(t *testing.T) {
		c, cluster, _ := newTestWarpstreamClient(t, topic, 1)

		release := make(chan struct{})
		cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
			<-release
			return nil, nil, false
		})

		records := []*kgo.Record{
			{Topic: topic, Partition: 0, Value: []byte("a"), Timestamp: time.Now()},
			{Topic: topic, Partition: 0, Value: []byte("b"), Timestamp: time.Now()},
			{Topic: topic, Partition: 0, Value: []byte("c"), Timestamp: time.Now()},
		}
		produced := make(chan struct{})
		go func() {
			defer close(produced)
			c.ProduceSync(context.Background(), records)
		}()

		require.Eventually(t, func() bool {
			return c.BufferedProduceRecords() == int64(len(records))
		}, time.Second, 5*time.Millisecond)

		close(release)
		<-produced
		require.Eventually(t, func() bool { return c.BufferedProduceRecords() == 0 },
			time.Second, 5*time.Millisecond)
	})
}

func TestWarpstreamClient_Close(t *testing.T) {
	t.Run("close is idempotent", func(t *testing.T) {
		c, _, _ := newTestWarpstreamClient(t, "test-topic", 1)
		c.Close()
		c.Close()
	})

	t.Run("close flushes pending records", func(t *testing.T) {
		const topic = "test-topic"
		c, _, clusterAddr := newTestWarpstreamClient(t, topic, 1)

		// Produce without waiting for the linger.
		doneCh := make(chan error, 1)
		c.buffer.Add(context.Background(), []*kgo.Record{
			{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()},
		}, func(err error) { doneCh <- err })

		c.Close()
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
