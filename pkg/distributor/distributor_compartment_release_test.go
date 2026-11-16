// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
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

func TestDistributor_CompartmentsReleaseDecodedRequestBeforeAcknowledgement(t *testing.T) {
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

	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("decoded input was not released before acknowledgements")
	}
	require.Equal(t, int64(1), d.inflightPushRequests.Load())
	select {
	case err := <-done:
		t.Fatalf("push completed before acknowledgement: %v", err)
	default:
	}

	unblockAcks()
	require.NoError(t, <-done)
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
	require.Equal(t, int32(1), releaseCount.Load())
	require.Zero(t, d.inflightPushRequests.Load())
}
