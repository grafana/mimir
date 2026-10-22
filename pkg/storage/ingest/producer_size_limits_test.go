// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

type produceRequestSizes struct {
	mu       sync.Mutex
	requests []int
	batches  []int
}

type produceRequestSizeConn struct {
	net.Conn
	hook *produceRequestSizes
}

func (c *produceRequestSizeConn) Write(p []byte) (int, error) {
	if len(p) >= 6 && int16(binary.BigEndian.Uint16(p[4:6])) == int16(kmsg.Produce) {
		c.hook.mu.Lock()
		c.hook.requests = append(c.hook.requests, len(p))
		c.hook.mu.Unlock()
	}
	return c.Conn.Write(p)
}

func TestNewKafkaWriterClient_ProduceRequestSizeLimit(t *testing.T) {
	var vnet kfake.VirtualNetwork
	cluster, addr := testkafka.CreateCluster(t, 2, "size-limit", testkafka.WithVirtualNetwork(&vnet))
	cfg := createTestKafkaConfig(addr, "size-limit")
	cfg.ProducerMaxRecordSizeBytes = 900000
	cfg.ProducerMaxBatchSizeBytes = 950000
	cfg.ProducerMaxRequestSizeBytes = 1000000
	cfg.ProducerCompression = kafkaCompressionNone
	hook := &produceRequestSizes{}
	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		hook.mu.Lock()
		defer hook.mu.Unlock()
		for _, topic := range req.(*kmsg.ProduceRequest).Topics {
			for _, partition := range topic.Partitions {
				hook.batches = append(hook.batches, len(partition.Records))
			}
		}
		cluster.KeepControl()
		return nil, nil, false
	})
	cfg.Dialer = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := vnet.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &produceRequestSizeConn{Conn: conn, hook: hook}, nil
	}
	require.NoError(t, cfg.Validate())

	client, err := NewKafkaWriterClient(cfg, defaultMaxInflightProduceRequests, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// The combined payload exceeds the request limit even though each record fits within the batch limit. The limit must apply across partitions.
	records := make([]*kgo.Record, 100)
	for i := range records {
		records[i] = &kgo.Record{Partition: int32(i % 2), Value: make([]byte, 32768)}
	}
	results := client.ProduceSync(t.Context(), records...)
	require.NoError(t, results.FirstErr())
	require.Len(t, results, len(records))

	hook.mu.Lock()
	defer hook.mu.Unlock()
	require.NotEmpty(t, hook.requests)
	for _, request := range hook.requests {
		require.LessOrEqual(t, request, cfg.ProducerMaxRequestSizeBytes)
	}
	require.NotEmpty(t, hook.batches)
	for _, batch := range hook.batches {
		require.LessOrEqual(t, batch, cfg.ProducerMaxBatchSizeBytes)
	}

}

func TestRecordSerializer_SubMegabyteSizeLimit(t *testing.T) {
	for _, version := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("version_%d", version), func(t *testing.T) {
			cfg := createTestKafkaConfig("localhost:9092", "size-limit")
			cfg.ProducerRecordVersion = version
			cfg.ProducerMaxRecordSizeBytes = 4096
			cfg.ProducerMaxBatchSizeBytes = 4096 + producerSizeOverheadBytes
			cfg.ProducerMaxRequestSizeBytes = cfg.ProducerMaxBatchSizeBytes + producerSizeOverheadBytes
			require.NoError(t, cfg.Validate())

			request := &mimirpb.WriteRequest{}
			for i := range 40 {
				series := mockPreallocTimeseries(fmt.Sprintf("metric_%d_%s", i, strings.Repeat("a", 512)))
				request.Timeseries = append(request.Timeseries, series)
			}
			records, _, err := recordSerializerFromCfg(cfg).ToRecords(cfg.Topic, 0, "tenant", request, cfg.ProducerMaxRecordSizeBytes)
			require.NoError(t, err)
			require.Greater(t, len(records), 1)
			var seriesCount int
			for _, record := range records {
				require.LessOrEqual(t, len(record.Value), cfg.ProducerMaxRecordSizeBytes)
				seriesCount += len(deserializeRecord(t, record).Timeseries)
			}
			require.Equal(t, len(request.Timeseries), seriesCount)
		})
	}
}
