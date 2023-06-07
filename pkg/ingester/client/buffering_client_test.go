package client

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/mimirpb"
	pool2 "github.com/grafana/mimir/pkg/util/pool"
)

func setupGrpc(t testing.TB) (*mockServer, *grpc.ClientConn) {
	server := grpc.NewServer()
	l, err := net.Listen("tcp", "127.0.0.1:")
	require.NoError(t, err)

	ingServ := &mockServer{}

	RegisterIngesterServer(server, ingServ)

	go func() {
		_ = server.Serve(l)
	}()

	t.Cleanup(func() {
		_ = l.Close()
	})

	c, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	return ingServ, c
}

func TestWriteRequestBufferingClient_Push(t *testing.T) {
	serv, conn := setupGrpc(t)

	bufferingClient := newWriteRequestBufferingClient(NewIngesterClient(conn), conn)

	var requestsToSends []*mimirpb.WriteRequest
	for i := 0; i < 10; i++ {
		requestsToSends = append(requestsToSends, createRequest("test", 100+10*i))
	}

	t.Run("push without pooling", func(t *testing.T) {
		serv.clearRequests()

		for _, r := range requestsToSends {
			_, err := bufferingClient.Push(context.Background(), r)
			require.NoError(t, err)
		}

		reqs := serv.requests()
		require.Equal(t, reqs, requestsToSends)
	})

	t.Run("push with pooling", func(t *testing.T) {
		serv.clearRequests()

		pool := &pool2.TrackedPool{Parent: &sync.Pool{}}
		slabPool := pool2.NewFastReleasingSlabPool[byte](pool, 512*1024)

		ctx := WithSlabPool(context.Background(), slabPool)

		for _, r := range requestsToSends {
			_, err := bufferingClient.Push(ctx, r)
			require.NoError(t, err)
		}

		reqs := serv.requests()
		require.Equal(t, reqs, requestsToSends)

		// Verify that pool was used.
		require.Greater(t, pool.Gets.Load(), int64(0))
		require.Equal(t, int64(0), pool.Balance.Load())
	})
}

func TestWriteRequestBufferingClient_Push_WithMultipleMarshalCalls(t *testing.T) {
	serv, conn := setupGrpc(t)

	bufferingClient := newWriteRequestBufferingClient(NewIngesterClient(conn), conn)
	bufferingClient.pushRawFn = func(ctx context.Context, conn *grpc.ClientConn, msg interface{}, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
		// Call Marshal several times. We are testing if all buffers from the pool are returned.
		_, _ = msg.(*wrappedRequest).Marshal()
		_, _ = msg.(*wrappedRequest).Marshal()
		_, _ = msg.(*wrappedRequest).Marshal()

		return pushRaw(ctx, conn, msg, opts...)
	}

	req := createRequest("test", 100)

	pool := &pool2.TrackedPool{Parent: &sync.Pool{}}
	slabPool := pool2.NewFastReleasingSlabPool[byte](pool, 512*1024)

	ctx := WithSlabPool(context.Background(), slabPool)

	_, err := bufferingClient.Push(ctx, req)
	require.NoError(t, err)

	require.Equal(t, serv.requests(), []*mimirpb.WriteRequest{req})

	// Verify that all buffers from the pool were returned.
	require.Greater(t, pool.Gets.Load(), int64(0))
	require.Equal(t, int64(0), pool.Balance.Load())
}

func BenchmarkWriteRequestBufferingClient_Push(b *testing.B) {
	bufferingClient := newWriteRequestBufferingClient(&dummyIngesterClient{}, nil)
	bufferingClient.pushRawFn = func(ctx context.Context, conn *grpc.ClientConn, msg interface{}, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
		_, err := msg.(proto.Marshaler).Marshal()
		return nil, err
	}

	req := createRequest("test", 100)

	b.Run("push without pooling", func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			_, err := bufferingClient.Push(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("push with pooling", func(b *testing.B) {
		ctx := WithSlabPool(context.Background(), pool2.NewFastReleasingSlabPool[byte](&sync.Pool{}, 512*1024))
		for i := 0; i < b.N; i++ {
			_, err := bufferingClient.Push(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestWriteRequestBufferingClient_PushConcurrent(t *testing.T) {
	serv, conn := setupGrpc(t)

	bufferingClient := newWriteRequestBufferingClient(NewIngesterClient(conn), conn)
	serv.trackSamples = true

	pool := &pool2.TrackedPool{Parent: &sync.Pool{}}
	slabPool := pool2.NewFastReleasingSlabPool[byte](pool, 512*1024)

	ctx := WithSlabPool(context.Background(), slabPool)

	wg := sync.WaitGroup{}

	const concurrency = 50
	const requestsPerGoroutine = 100

	for i := 0; i < concurrency; i++ {
		metricName := fmt.Sprintf("test_%d", i)

		wg.Add(1)
		go func() {
			defer wg.Done()

			sentSamples := map[string][]mimirpb.Sample{}

			req := createRequest(metricName, 100)

			for i := 0; i < requestsPerGoroutine; i++ {
				for _, ts := range req.Timeseries {
					ser := mimirpb.FromLabelAdaptersToLabelsWithCopy(ts.Labels).String()
					sentSamples[ser] = append(sentSamples[ser], ts.Samples...)
				}

				_, err := bufferingClient.Push(ctx, req)
				require.NoError(t, err)
			}

			// Verify that mock server has all sent samples.
			for ser, samples := range sentSamples {
				receivedSamples := serv.getAndRemoveSamplesForSeries(ser)
				// Each concurrent client is sending samples for its series in order, so we can do this equality check.
				require.Equal(t, samples, receivedSamples)
			}
		}()
	}

	wg.Wait()

	// Verify that pool usage.
	require.Greater(t, pool.Gets.Load(), int64(0))
	require.Equal(t, int64(0), pool.Balance.Load())
}

func createRequest(metricName string, seriesPerRequest int) *mimirpb.WriteRequest {
	metrics := make([]labels.Labels, 0, seriesPerRequest)
	samples := make([]mimirpb.Sample, 0, seriesPerRequest)
	for i := 0; i < seriesPerRequest; i++ {
		metrics = append(metrics, labels.FromStrings(labels.MetricName, metricName, "cardinality", strconv.Itoa(i)))
		samples = append(samples, mimirpb.Sample{Value: float64(i), TimestampMs: time.Now().UnixMilli()})
	}

	req := mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API)
	return req
}

type mockServer struct {
	IngesterServer

	trackSamples bool

	mu               sync.Mutex
	reqs             []*mimirpb.WriteRequest
	samplesPerSeries map[string][]mimirpb.Sample
}

func (ms *mockServer) Push(_ context.Context, r *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Clear unmarshal data. We don't need it and it breaks equality check in test.
	r.ClearTimeseriesUnmarshalData()
	ms.reqs = append(ms.reqs, r)

	if ms.trackSamples {
		if ms.samplesPerSeries == nil {
			ms.samplesPerSeries = map[string][]mimirpb.Sample{}
		}

		for _, ts := range r.Timeseries {
			ser := mimirpb.FromLabelAdaptersToLabelsWithCopy(ts.Labels).String()
			ms.samplesPerSeries[ser] = append(ms.samplesPerSeries[ser], ts.Samples...)
		}
	}

	return &mimirpb.WriteResponse{}, nil
}

func (ms *mockServer) clearRequests() {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.reqs = nil
}

func (ms *mockServer) requests() []*mimirpb.WriteRequest {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	return ms.reqs
}

func (ms *mockServer) getAndRemoveSamplesForSeries(series string) []mimirpb.Sample {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	res := ms.samplesPerSeries[series]
	delete(ms.samplesPerSeries, series)
	return res
}

type dummyIngesterClient struct {
	IngesterClient
}

func (d *dummyIngesterClient) Push(ctx context.Context, in *mimirpb.WriteRequest, _ ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	_, err := in.Marshal()
	return nil, err
}
