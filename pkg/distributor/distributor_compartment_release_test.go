// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

type gatedCompartmentWriter struct {
	ingestStorageWriter
	blockedTopic    string
	blocked         chan struct{}
	proceed         <-chan struct{}
	otherSerialized chan struct{}
	notifyOther     sync.Once
	failure         error
}

func (w *gatedCompartmentWriter) MultiWriteSyncWithRequestRelease(ctx context.Context, topic, userID string, requests []ingest.PartitionWriteRequest, release func()) error {
	if topic == w.blockedTopic {
		close(w.blocked)
		<-w.proceed
		if w.failure != nil {
			return w.failure
		}
	}
	return w.ingestStorageWriter.MultiWriteSyncWithRequestRelease(ctx, topic, userID, requests, func() {
		release()
		if topic != w.blockedTopic {
			w.notifyOther.Do(func() { close(w.otherSerialized) })
		}
	})
}

func TestDistributor_CompartmentReleaseWaitsForAllSerializers(t *testing.T) {
	for _, failBlocked := range []bool{false, true} {
		t.Run(fmt.Sprintf("serialization-failure-%t", failBlocked), func(t *testing.T) {
			d, cluster, topics := prepareCompartmentsTestDistributor(t, 3)
			acknowledge := make(chan struct{})
			unblockAcks := sync.OnceFunc(func() { close(acknowledge) })
			t.Cleanup(unblockAcks)
			cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
				cluster.SleepControl(func() { <-acknowledge })
				return nil, nil, false
			})
			source := &mimirpb.WriteRequest{}
			var expectedNames []string
			expectedSamples := map[string][]mimirpb.Sample{}
			for i := 0; i < 64; i++ {
				name := fmt.Sprintf("release_metric_%d", i)
				expectedNames = append(expectedNames, name)
				source.Timeseries = append(source.Timeseries, mockWriteRequest([]mimirpb.LabelAdapter{{Name: "__name__", Value: name}}, float64(i), time.Now().UnixMilli()).Timeseries...)
				expectedSamples[name] = source.Timeseries[len(source.Timeseries)-1].Samples
			}
			wire, err := source.Marshal()
			require.NoError(t, err)
			var decoded mimirpb.PreallocWriteRequest
			require.NoError(t, decoded.Unmarshal(wire))
			cts, _ := getCompartmentTokensForWriteRequest(d.compartmentRouter, "user", &decoded.WriteRequest)
			require.Len(t, cts, 3)
			proceed := make(chan struct{})
			unblockSerialization := sync.OnceFunc(func() { close(proceed) })
			t.Cleanup(unblockSerialization)
			gate := &gatedCompartmentWriter{ingestStorageWriter: d.ingestStorageWriter, blockedTopic: cts[0].topic, blocked: make(chan struct{}), proceed: proceed, otherSerialized: make(chan struct{})}
			if failBlocked {
				gate.failure = errors.New("compartment failed before serialization")
			}
			d.ingestStorageWriter = gate
			var releaseCount atomic.Int32
			released := make(chan struct{})
			inputSize := decoded.Size()
			request := newRequest(func() (*mimirpb.WriteRequest, func(), int, error) {
				return &decoded.WriteRequest, func() {
					releaseCount.Add(1)
					mimirpb.ReuseSlice(decoded.Timeseries)
					decoded = mimirpb.PreallocWriteRequest{}
					for i := range wire {
						wire[i] = 0xff
					}
					close(released)
				}, inputSize, nil
			})
			done := make(chan error, 1)
			go func() { done <- d.PushWithMiddlewares(user.InjectOrgID(t.Context(), "user"), request) }()
			for _, signal := range []<-chan struct{}{gate.blocked, gate.otherSerialized} {
				select {
				case <-signal:
				case <-time.After(5 * time.Second):
					t.Fatal("compartment did not reach serialization gate")
				}
			}
			require.Zero(t, releaseCount.Load(), "a slow serializer still owns the decoded input")
			unblockSerialization()
			select {
			case <-released:
			case <-time.After(5 * time.Second):
				t.Fatal("decoded input was not released before acknowledgements")
			}
			if !failBlocked {
				require.Equal(t, int64(1), d.inflightPushRequests.Load())
				select {
				case err := <-done:
					t.Fatalf("push completed before acknowledgement: %v", err)
				default:
				}
			}
			unblockAcks()
			err = <-done
			if failBlocked {
				require.ErrorIs(t, err, gate.failure)
			} else {
				require.NoError(t, err)
				records := readAllRecordsFromKafkaTopics(t, cluster.ListenAddrs(), topics, 3, time.Second)
				require.ElementsMatch(t, expectedNames, metricNamesFromRecords(t, records))
				seen := map[string]bool{}
				for _, record := range records {
					seen[record.Topic] = true
					var stored mimirpb.PreallocWriteRequest
					require.NoError(t, ingest.DeserializeRecordContent(record.Value, &stored, ingest.ParseRecordVersion(record)))
					for _, series := range stored.Timeseries {
						require.Len(t, series.Labels, 1)
						require.Equal(t, expectedSamples[series.Labels[0].Value], series.Samples)
					}
				}
				require.Len(t, seen, 3)
			}
			require.Equal(t, int32(1), releaseCount.Load())
			require.Zero(t, d.inflightPushRequests.Load())
		})
	}
}
