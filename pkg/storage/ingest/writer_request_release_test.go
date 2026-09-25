// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestWriter_ReleaseDecodedRequestBeforeAcknowledgement(t *testing.T) {
	runForEachKafkaBackend(t, func(t *testing.T, backend string) {
		for _, version := range []int{0, 1, 2} {
			t.Run(fmt.Sprintf("version-%d", version), func(t *testing.T) {
				cluster, addr := testkafka.CreateCluster(t, 1, "test")
				cfg := createTestKafkaConfigForBackend(backend, addr, "test")
				cfg.ProducerRecordVersion = version
				writer, reg := createTestWriter(t, cfg)
				allowProduce := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(allowProduce) })
				t.Cleanup(unblock)
				cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
					<-allowProduce
					return nil, nil, false
				})
				expected := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("release_test")}, Source: mimirpb.API}
				wire, err := expected.Marshal()
				require.NoError(t, err)
				var decoded mimirpb.PreallocWriteRequest
				require.NoError(t, decoded.Unmarshal(wire))
				inputSize := decoded.Size()
				released := make(chan struct{})
				done := make(chan error, 1)
				go func() {
					done <- writer.MultiWriteSyncWithRequestRelease(t.Context(), "test", "user", []PartitionWriteRequest{{PartitionID: 0, WriteRequest: &decoded.WriteRequest}}, func() {
						mimirpb.ReuseSlice(decoded.Timeseries)
						decoded = mimirpb.PreallocWriteRequest{}
						for i := range wire {
							wire[i] = 0xff
						}
						close(released)
					})
				}()
				select {
				case <-released:
				case <-time.After(5 * time.Second):
					t.Fatal("input not released before acknowledgement")
				}
				select {
				case err := <-done:
					t.Fatalf("write completed before acknowledgement: %v", err)
				default:
				}
				unblock()
				require.NoError(t, <-done)
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

type failingRequestSerializer struct{ err error }

func (s failingRequestSerializer) ToRecords(string, int32, string, *mimirpb.WriteRequest, int) ([]*kgo.Record, int, error) {
	return nil, 0, s.err
}

func TestWriter_ReleaseDecodedRequestOnEarlyReturn(t *testing.T) {
	for _, scenario := range []string{"not-running", "empty", "serialization-error"} {
		t.Run(scenario, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			writer := NewWriter(KafkaConfig{}, log.NewNopLogger(), reg)
			request := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("release_test")}}
			wantErr := error(nil)
			if scenario == "not-running" {
				wantErr = ErrWriterNotRunning
			} else {
				writer.client.Store(&KafkaProducer{})
			}
			if scenario == "empty" {
				request = &mimirpb.WriteRequest{}
			}
			if scenario == "serialization-error" {
				wantErr = errors.New("serialization failed")
				writer.serializer = failingRequestSerializer{wantErr}
			}
			calls := 0
			err := writer.MultiWriteSyncWithRequestRelease(t.Context(), "test", "user", []PartitionWriteRequest{{PartitionID: 0, WriteRequest: request}}, func() { calls++ })
			require.ErrorIs(t, err, wantErr)
			require.Equal(t, 1, calls)
		})
	}
}
