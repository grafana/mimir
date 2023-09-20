// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestGrpcPushCheck(t *testing.T) {
	const limit = 3
	ingServer := &ingesterServer{finishRequest: make(chan struct{}), inflightLimit: limit}

	c := setupGrpcServerWithCheckAndClient(t, ingServer)

	// start push requests in background goroutines
	started := sync.WaitGroup{}
	finished := sync.WaitGroup{}
	for i := 0; i < limit; i++ {
		started.Add(1)
		finished.Add(1)

		go func() {
			started.Done()
			defer finished.Done()

			_, err := c.Push(context.Background(), &mimirpb.WriteRequest{})
			require.NoError(t, err)
		}()
	}

	// Wait until all goroutines start.
	started.Wait()

	// Wait until all requests are inflight.
	test.Poll(t, 1*time.Second, int64(limit), func() interface{} {
		return ingServer.inflight.Load()
	})

	// Try another request. This one should fail with too many inflight requests error.
	_, err := c.Push(context.Background(), &mimirpb.WriteRequest{})
	require.Error(t, err)
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(123), s.Code())
	require.Equal(t, "too many requests", s.Message())

	require.Equal(t, int64(limit), ingServer.inflight.Load())

	// unblock all requests
	for i := 0; i < limit; i++ {
		ingServer.finishRequest <- struct{}{}
	}
	finished.Wait()

	require.Equal(t, int64(0), ingServer.inflight.Load())

	// unblock subsequent requests immediately
	close(ingServer.finishRequest)

	// another push request should succeed.
	_, err = c.Push(context.Background(), &mimirpb.WriteRequest{})
	require.NoError(t, err)
}

func setupGrpcServerWithCheckAndClient(t *testing.T, ingServer *ingesterServer) ingester_client.IngesterClient {
	g := newGrpcInflightMethodLimiter(func() pushReceiver {
		return ingServer
	})

	server := grpc.NewServer(grpc.InTapHandle(g.TapHandle), grpc.StatsHandler(g))
	ingester_client.RegisterIngesterServer(server, ingServer)

	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		_ = server.Serve(l)
	}()

	t.Cleanup(func() {
		_ = l.Close()
	})

	cc, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = cc.Close()
	})

	return ingester_client.NewIngesterClient(cc)
}

type ingesterServer struct {
	ingester_client.IngesterServer
	inflightLimit int64
	finishRequest chan struct{}
	inflight      atomic.Int64
}

func (i *ingesterServer) Push(_ context.Context, _ *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	<-i.finishRequest
	return &mimirpb.WriteResponse{}, nil
}

func (i *ingesterServer) StartPushRequest() error {
	v := i.inflight.Inc()
	if v > i.inflightLimit {
		i.inflight.Dec()
		return status.Error(123, "too many requests")
	}
	return nil
}

func (i *ingesterServer) FinishPushRequest() {
	i.inflight.Dec()
}
