// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestWriter_MultiWriteSync_ProducerFallback(t *testing.T) {
	const (
		topic = "test"
		delay = time.Second
	)

	// The primary cluster has two brokers: partition 0 is led by broker 0, partition 1 by broker 1.
	setup := func(t *testing.T) (primary *kfake.Cluster, primaryAddr, fallbackAddr string, fetch func(addr string, partition int32) int, writer *Writer) {
		vnet := &kfake.VirtualNetwork{}
		ports := func(p ...int) testkafka.Opt { return func() []kfake.Opt { return []kfake.Opt{kfake.Ports(p...)} } }
		primary, primaryAddr = testkafka.CreateCluster(t, 2, topic, testkafka.WithVirtualNetwork(vnet), ports(19092, 19093))
		_, fallbackAddr = testkafka.CreateCluster(t, 2, topic, testkafka.WithVirtualNetwork(vnet), ports(19094))

		cfg := createTestKafkaConfig(primaryAddr, topic)
		cfg.Dialer = vnet.DialContext
		cfg.ProducerFallbackAddress = flagext.StringSliceCSV{fallbackAddr}
		cfg.ProducerFallbackDelay = delay
		writer, _ = createTestWriter(t, cfg)

		fetch = func(addr string, partition int32) int {
			consumer, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.Dialer(vnet.DialContext),
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {partition: kgo.NewOffset().AtStart()}}))
			require.NoError(t, err)
			defer consumer.Close()

			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			fetches := consumer.PollFetches(ctx)
			if err := fetches.Err(); err != nil {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			return len(fetches.Records())
		}
		return primary, primaryAddr, fallbackAddr, fetch, writer
	}

	requests := []PartitionWriteRequest{
		{PartitionID: 0, WriteRequest: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}, Source: mimirpb.API}},
		{PartitionID: 1, WriteRequest: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2")}, Source: mimirpb.API}},
	}

	t.Run("should produce the records not acknowledged within the delay through the fallback client", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			primary, primaryAddr, fallbackAddr, fetch, writer := setup(t)

			// Hold the produce requests to broker 1 until the end of the test.
			stuck := make(chan struct{})
			t.Cleanup(func() { close(stuck) })
			primary.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
				primary.KeepControl()
				if primary.CurrentNode() == 1 {
					primary.SleepControl(func() { <-stuck })
				}
				return nil, nil, false
			})

			start := time.Now()
			require.NoError(t, writer.MultiWriteSync(t.Context(), topic, "user-1", requests))
			assert.GreaterOrEqual(t, time.Since(start), delay)

			// Only the stuck partition has been produced through the fallback client.
			assert.Equal(t, 1, fetch(primaryAddr, 0))
			assert.Equal(t, 0, fetch(fallbackAddr, 0))
			assert.Equal(t, 1, fetch(fallbackAddr, 1))
		})
	})

	t.Run("should not use the fallback client when the records are acknowledged within the delay", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			_, primaryAddr, fallbackAddr, fetch, writer := setup(t)

			require.NoError(t, writer.MultiWriteSync(t.Context(), topic, "user-1", requests))

			for _, partition := range []int32{0, 1} {
				assert.Equal(t, 1, fetch(primaryAddr, partition))
				assert.Equal(t, 0, fetch(fallbackAddr, partition))
			}
		})
	})
}
