// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestDistributor_ReleaseDecodedRequestPreservesAdmission(t *testing.T) {
	cluster, _ := testkafka.CreateCluster(t, 1, kafkaTopic)
	ds, _, _, _ := prepare(t, prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		ingestStorageKafka:      cluster,
		limits:                  prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.DefaultLimits.MaxInflightPushRequests = 1
			cfg.RemoteTimeout = 10 * time.Second
		},
	})
	d := ds[0]
	allowProduce := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(allowProduce) })
	t.Cleanup(unblock)
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		<-allowProduce
		return nil, nil, false
	})

	ctx := user.InjectOrgID(t.Context(), "user")
	body := mockWriteRequest([]mimirpb.LabelAdapter{{Name: "__name__", Value: "release_test"}}, 1, time.Now().UnixMilli())
	inputBytes := body.Size()
	released := make(chan struct{})
	request := newRequest(func() (*mimirpb.WriteRequest, func(), int, error) {
		return body, func() {
			mimirpb.ReuseSlice(body.Timeseries)
			*body = mimirpb.WriteRequest{}
			close(released)
		}, inputBytes, nil
	})
	done := make(chan error, 1)
	go func() { done <- d.PushWithMiddlewares(ctx, request) }()
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("decoded request was not released while Kafka was blocked")
	}
	require.Equal(t, int64(1), d.inflightPushRequests.Load())
	require.Equal(t, int64(inputBytes), d.inflightPushRequestsBytes.Load())
	select {
	case err := <-done:
		t.Fatalf("push returned before acknowledgement: %v", err)
	default:
	}

	_, err := d.Push(ctx, mockWriteRequest([]mimirpb.LabelAdapter{{Name: "__name__", Value: "second"}}, 1, time.Now().UnixMilli()))
	require.ErrorContains(t, err, "inflight")
	unblock()
	require.NoError(t, <-done)
	require.Zero(t, d.inflightPushRequests.Load())
	require.Zero(t, d.inflightPushRequestsBytes.Load())
	_, err = request.WriteRequest()
	require.EqualError(t, err, "decoded write request has been released")
}
