// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestWriter_ProduceAfterInputReleased(t *testing.T) {
	runForEachKafkaBackend(t, func(t *testing.T, backend string) {
		for _, version := range []int{0, 1, 2} {
			t.Run(fmt.Sprintf("version-%d", version), func(t *testing.T) {
				_, addr := testkafka.CreateCluster(t, 1, "test")
				cfg := createTestKafkaConfigForBackend(backend, addr, "test")
				cfg.ProducerRecordVersion = version
				writer, reg := createTestWriter(t, cfg)

				expected := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("release_test")}, Source: mimirpb.API}
				wire, err := expected.Marshal()
				require.NoError(t, err)
				var decoded mimirpb.PreallocWriteRequest
				require.NoError(t, decoded.Unmarshal(wire))
				inputSize := decoded.Size()

				write, err := writer.Serialize("test", "user", []PartitionWriteRequest{{PartitionID: 0, WriteRequest: &decoded.WriteRequest}})
				require.NoError(t, err)

				mimirpb.ReuseSlice(decoded.Timeseries)
				decoded = mimirpb.PreallocWriteRequest{}
				for i := range wire {
					wire[i] = 0xff
				}

				require.NoError(t, writer.Produce(t.Context(), write))

				consumer, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"test": {0: kgo.NewOffset().AtStart()}}))
				require.NoError(t, err)
				defer consumer.Close()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				fetches := consumer.PollFetches(ctx)
				require.NoError(t, fetches.Err())
				records := fetches.Records()
				require.Len(t, records, 1)
				got := deserializeRecord(t, records[0])
				require.Equal(t, expected.Timeseries[0].Labels, got.Timeseries[0].Labels)
				require.Equal(t, expected.Timeseries[0].Samples, got.Timeseries[0].Samples)
				require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
# HELP cortex_ingest_storage_writer_input_bytes_total Total number of bytes in write requests before conversion to the Kafka record format.
# TYPE cortex_ingest_storage_writer_input_bytes_total counter
cortex_ingest_storage_writer_input_bytes_total %d
`, inputSize)), "cortex_ingest_storage_writer_input_bytes_total"))
			})
		}
	})
}

func TestWriter_SerializeAndProduceWithoutRunningClient(t *testing.T) {
	writer := NewWriter(KafkaConfig{ProducerMaxRecordSizeBytes: 1 << 20}, log.NewNopLogger(), prometheus.NewRegistry())
	nonEmpty := []PartitionWriteRequest{{PartitionID: 0, WriteRequest: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("release_test")}}}}
	empty := []PartitionWriteRequest{{PartitionID: 0, WriteRequest: &mimirpb.WriteRequest{}}}

	_, err := writer.Serialize("test", "user", nonEmpty)
	require.ErrorIs(t, err, ErrWriterNotRunning)

	writer.client.Store(&KafkaProducer{})
	write, err := writer.Serialize("test", "user", nonEmpty)
	require.NoError(t, err)
	emptyWrite, err := writer.Serialize("test", "user", empty)
	require.NoError(t, err)

	writer.client.Store(nil)
	require.ErrorIs(t, writer.Produce(t.Context(), write), ErrWriterNotRunning)
	require.NoError(t, writer.Produce(t.Context(), emptyWrite))
}

type failingRecordSerializer struct{ err error }

func (s failingRecordSerializer) ToRecords(string, int32, string, *mimirpb.WriteRequest, int) ([]*kgo.Record, int, error) {
	return nil, 0, s.err
}

func TestWriter_SerializeError(t *testing.T) {
	writer := NewWriter(KafkaConfig{ProducerMaxRecordSizeBytes: 1 << 20}, log.NewNopLogger(), prometheus.NewRegistry())
	// The zero-value producer can't produce, so a nil error below would mean records were handed to it.
	writer.client.Store(&KafkaProducer{})
	wantErr := errors.New("serialization failed")
	writer.serializer = failingRecordSerializer{wantErr}
	requests := []PartitionWriteRequest{{PartitionID: 0, WriteRequest: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("release_test")}}}}

	write, err := writer.Serialize("test", "user", requests)
	require.ErrorIs(t, err, wantErr)
	require.Empty(t, write.records)

	require.ErrorIs(t, writer.MultiWriteSync(t.Context(), "test", "user", requests), wantErr)
}
