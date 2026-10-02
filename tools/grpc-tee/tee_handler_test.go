// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

const (
	seriesMethod     = "/gatewaypb.StoreGateway/Series"
	labelNamesMethod = "/gatewaypb.StoreGateway/LabelNames"
)

func TestTeeHandler_ForwardsPrimaryAndCollectsBothBackends(t *testing.T) {
	primary := newFakeStoreGateway("primary", seriesResponses("up"))
	secondary := newFakeStoreGateway("secondary", seriesResponses("up"))
	client, calls := startTee(t, primary, secondary)

	var header, trailer metadata.MD
	stream, err := client.Series(t.Context(), seriesRequest(), grpc.Header(&header), grpc.Trailer(&trailer))
	require.NoError(t, err)
	received, err := receiveAll(stream)
	require.NoError(t, err)
	require.Len(t, received, 3)

	// The client gets the headers and trailers of the primary backend only.
	require.Equal(t, []string{"primary"}, header.Get("x-backend"))
	require.Equal(t, []string{"primary"}, trailer.Get("x-backend"))

	call := calls.waitForOne(t)
	require.Equal(t, seriesMethod, call.FullMethod)
	require.Len(t, call.Requests, 1)
	require.Equal(t, int64(10), call.Requests[0].Decoded.(*storepb.SeriesRequest).MinTime)
	require.Positive(t, call.RequestBytes)

	require.Equal(t, "primary", call.Primary.Backend)
	require.NoError(t, call.Primary.Err)
	require.Len(t, call.Primary.Responses, 3)
	require.Positive(t, call.Primary.Duration)
	require.Positive(t, call.Primary.TimeToFirstResponse)

	require.NotNil(t, call.Secondary)
	require.Equal(t, "secondary", call.Secondary.Backend)
	require.NoError(t, call.Secondary.Err)
	require.Len(t, call.Secondary.Responses, 3)
	require.Positive(t, call.Secondary.Duration)

	result, err := compareCall(storeGatewayComparator{}, call)
	require.NoError(t, err)
	require.Equal(t, ComparisonMatch, result)
	require.Equal(t, 1, secondary.seriesCalls())
}

func TestTeeHandler_ClientGetsPrimaryResponsesWhenSecondaryDiffers(t *testing.T) {
	primary := newFakeStoreGateway("primary", seriesResponses("up"))
	secondary := newFakeStoreGateway("secondary", seriesResponses("down"))
	client, calls := startTee(t, primary, secondary)

	stream, err := client.Series(t.Context(), seriesRequest())
	require.NoError(t, err)
	received, err := receiveAll(stream)
	require.NoError(t, err)
	require.Equal(t, []mimirpb.LabelAdapter{{Name: "__name__", Value: "up"}}, received[0].GetStreamingSeries().Series[0].Labels)

	result, err := compareCall(storeGatewayComparator{}, calls.waitForOne(t))
	require.Equal(t, ComparisonMismatch, result)
	require.EqualError(t, err, `series 0: the primary backend returned {__name__="up"} but the secondary backend returned {__name__="down"}`)
}

func TestTeeHandler_SecondaryFailureDoesNotChangeTheClientCall(t *testing.T) {
	primary := newFakeStoreGateway("primary", seriesResponses("up"))
	secondary := newFakeStoreGateway("secondary", seriesResponses("up"))
	secondary.seriesErr = status.Error(codes.Unavailable, "secondary is not available")
	client, calls := startTee(t, primary, secondary)

	stream, err := client.Series(t.Context(), seriesRequest())
	require.NoError(t, err)
	received, err := receiveAll(stream)
	require.NoError(t, err)
	require.Len(t, received, 3)

	call := calls.waitForOne(t)
	require.Equal(t, codes.Unavailable, status.Code(call.Secondary.Err))
	result, err := compareCall(storeGatewayComparator{}, call)
	require.Equal(t, ComparisonMismatch, result)
	require.EqualError(t, err, "the primary backend returned status OK but the secondary backend returned status Unavailable")
}

func TestTeeHandler_PrimaryFailureIsReturnedToTheClient(t *testing.T) {
	primary := newFakeStoreGateway("primary", seriesResponses("up"))
	primary.seriesErr = status.Error(codes.ResourceExhausted, "too many chunks")
	secondary := newFakeStoreGateway("secondary", seriesResponses("up"))
	secondary.seriesErr = status.Error(codes.ResourceExhausted, "too many chunks")
	client, calls := startTee(t, primary, secondary)

	stream, err := client.Series(t.Context(), seriesRequest())
	require.NoError(t, err)
	received, err := receiveAll(stream)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Len(t, received, 3)

	// Both backends failed with the same status code.
	result, err := compareCall(storeGatewayComparator{}, calls.waitForOne(t))
	require.NoError(t, err)
	require.Equal(t, ComparisonMatch, result)
}

func TestTeeHandler_ClientDoesNotWaitForTheSecondary(t *testing.T) {
	primary := newFakeStoreGateway("primary", seriesResponses("up"))
	secondary := newFakeStoreGateway("secondary", seriesResponses("up"))
	secondary.release = make(chan struct{})
	client, calls := startTee(t, primary, secondary)

	stream, err := client.Series(t.Context(), seriesRequest())
	require.NoError(t, err)
	received, err := receiveAll(stream)
	require.NoError(t, err)
	require.Len(t, received, 3)

	// The client call is complete, but the secondary call is still waiting, so the call is not finished.
	require.Never(t, func() bool { return len(calls.all()) > 0 }, 100*time.Millisecond, 10*time.Millisecond)

	close(secondary.release)
	call := calls.waitForOne(t)
	require.NoError(t, call.Secondary.Err)
	require.Len(t, call.Secondary.Responses, 3)
	require.Greater(t, call.Secondary.Duration, call.Primary.Duration)
}

func TestTeeHandler_ClientCancelStopsThePrimaryButNotTheSecondary(t *testing.T) {
	primary := newFakeStoreGateway("primary", seriesResponses("up"))
	primary.release = make(chan struct{}) // Never closed: the primary call ends only when it is canceled.
	secondary := newFakeStoreGateway("secondary", seriesResponses("up"))
	secondary.release = make(chan struct{})
	client, calls := startTee(t, primary, secondary)

	ctx, cancel := context.WithCancel(t.Context())
	stream, err := client.Series(ctx, seriesRequest())
	require.NoError(t, err)
	// Wait until both backends received the call.
	require.Eventually(t, func() bool { return primary.seriesCalls() == 1 && secondary.seriesCalls() == 1 }, time.Second, 10*time.Millisecond)
	cancel()
	_, err = receiveAll(stream)
	require.Equal(t, codes.Canceled, status.Code(err))

	// The primary backend sees the cancellation.
	select {
	case <-primary.canceled:
	case <-time.After(time.Second):
		t.Fatal("the primary call was not canceled")
	}

	// The secondary call continues, and completes after the release.
	close(secondary.release)
	call := calls.waitForOne(t)
	require.True(t, call.Primary.Aborted)
	require.NoError(t, call.Secondary.Err)
	require.Len(t, call.Secondary.Responses, 3)

	result, err := compareCall(storeGatewayComparator{}, call)
	require.Equal(t, ComparisonSkipped, result)
	require.EqualError(t, err, "the primary call is incomplete because the client side failed")
}

func TestTeeHandler_WithoutSecondary(t *testing.T) {
	primary := newFakeStoreGateway("primary", seriesResponses("up"))
	client, calls := startTee(t, primary, nil)

	labelNames, err := client.LabelNames(t.Context(), &storepb.LabelNamesRequest{Start: 10, End: 20})
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "job"}, labelNames.Names)

	call := calls.waitForOne(t)
	require.Equal(t, labelNamesMethod, call.FullMethod)
	require.Nil(t, call.Secondary)
	require.Len(t, call.Primary.Responses, 1)
	require.Equal(t, []string{"__name__", "job"}, call.Primary.Responses[0].Decoded.(*storepb.LabelNamesResponse).Names)
}

func TestTeeHandler_UnknownMethodGoesToThePrimaryOnly(t *testing.T) {
	primary := newFakeStoreGateway("primary", nil)
	secondary := newFakeStoreGateway("secondary", nil)
	backendType, err := newBackendType(backendTypeStoreGateway)
	require.NoError(t, err)
	calls := &callRecorder{}
	conn := dial(t, startTeeServer(t, backendType, startBackend(t, primary), startBackend(t, secondary), calls.record))

	resp, err := grpc_health_v1.NewHealthClient(conn).Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)

	require.Equal(t, 1, primary.healthChecks())
	require.Never(t, func() bool { return secondary.healthChecks() > 0 || len(calls.all()) > 0 }, 100*time.Millisecond, 10*time.Millisecond)
}

func TestTeeHandler_OpaqueClientStreamingCall(t *testing.T) {
	backendType, err := newBackendType(backendTypeOpaque)
	require.NoError(t, err)
	calls := &callRecorder{}
	conn := dial(t, startTeeServer(t, backendType, startCountingBackend(t, "primary"), startCountingBackend(t, "secondary"), calls.record))

	// The counting backends answer each client-streaming call with the number of request messages.
	stream, err := conn.NewStream(t.Context(), &grpc.StreamDesc{ClientStreams: true}, "/test.Counter/Count", grpc.ForceCodecV2(newFrameCodec()))
	require.NoError(t, err)
	for _, payload := range []string{"a", "b", "c"} {
		require.NoError(t, stream.SendMsg(&frame{payload: []byte(payload)}))
	}
	require.NoError(t, stream.CloseSend()) //nolint:forbidigo // A client-streaming call must half-close before it receives the response.
	resp := &frame{}
	require.NoError(t, stream.RecvMsg(resp))
	require.Equal(t, "primary:3", string(resp.payload))

	call := calls.waitForOne(t)
	require.Len(t, call.Requests, 3)
	require.Equal(t, []byte("b"), call.Requests[1].Payload)
	require.Nil(t, call.Requests[1].Decoded)
	require.Equal(t, 3, call.RequestBytes)
	require.Equal(t, []byte("secondary:3"), call.Secondary.Responses[0].Payload)

	result, err := compareCall(opaqueComparator{}, call)
	require.Equal(t, ComparisonMismatch, result)
	require.EqualError(t, err, "response message 0 is different")
}

func TestNewBackendType(t *testing.T) {
	bt, err := newBackendType(backendTypeOpaque)
	require.NoError(t, err)
	require.Nil(t, bt.codec)
	require.Equal(t, opaqueComparator{}, bt.comparator)

	bt, err = newBackendType(backendTypeStoreGateway)
	require.NoError(t, err)
	require.True(t, bt.codec.Knows(seriesMethod))
	require.False(t, bt.codec.Knows("/grpc.health.v1.Health/Check"))
	require.Equal(t, storeGatewayComparator{}, bt.comparator)

	_, err = newBackendType("unknown")
	require.EqualError(t, err, `unknown backend type "unknown"`)
}

func seriesRequest() *storepb.SeriesRequest {
	blockMatcher := storepb.LabelMatcher{Type: storepb.LabelMatcher_RE, Name: "__block_id", Value: "block-1|block-2"}
	return &storepb.SeriesRequest{
		MinTime:      10,
		MaxTime:      20,
		Matchers:     []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}},
		RequestHints: &storepb.SeriesRequestHints{BlockMatchers: []storepb.LabelMatcher{blockMatcher}},
	}
}

// seriesResponses returns the responses of a Series call that returns one series with the given metric name.
func seriesResponses(metricName string) []*storepb.SeriesResponse {
	return []*storepb.SeriesResponse{
		storepb.NewStreamingSeriesResponse(&storepb.StreamingSeriesBatch{
			Series:              []*storepb.StreamingSeries{{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: metricName}}}},
			IsEndOfSeriesStream: true,
		}),
		storepb.NewStatsResponse(1),
		storepb.NewHintsSeriesResponse(&storepb.SeriesResponseHints{QueriedBlocks: []storepb.Block{{Id: "block-1"}}}),
	}
}

// fakeStoreGateway is a store-gateway that returns fixed responses.
type fakeStoreGateway struct {
	storegatewaypb.UnimplementedStoreGatewayServer
	grpc_health_v1.UnimplementedHealthServer

	name      string
	responses []*storepb.SeriesResponse
	// seriesErr is the error that Series returns after it sent the responses.
	seriesErr error
	// release, if not nil, makes Series wait until release is closed or the call is canceled.
	release chan struct{}
	// canceled receives a value when a Series call is canceled while it waits for release.
	canceled chan struct{}

	mtx             sync.Mutex
	seriesCallCount int
	healthCount     int
}

func newFakeStoreGateway(name string, responses []*storepb.SeriesResponse) *fakeStoreGateway {
	return &fakeStoreGateway{name: name, responses: responses, canceled: make(chan struct{}, 1)}
}

func (g *fakeStoreGateway) Series(_ *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) error {
	g.mtx.Lock()
	g.seriesCallCount++
	g.mtx.Unlock()

	md := metadata.Pairs("x-backend", g.name)
	if err := srv.SendHeader(md); err != nil {
		return err
	}
	srv.SetTrailer(md)

	if g.release != nil {
		select {
		case <-g.release:
		case <-srv.Context().Done():
			g.canceled <- struct{}{}
			return srv.Context().Err()
		}
	}

	for _, resp := range g.responses {
		if err := srv.Send(resp); err != nil {
			return err
		}
	}
	return g.seriesErr
}

func (g *fakeStoreGateway) LabelNames(context.Context, *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	return &storepb.LabelNamesResponse{Names: []string{"__name__", "job"}}, nil
}

func (g *fakeStoreGateway) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	g.healthCount++
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func (g *fakeStoreGateway) seriesCalls() int {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	return g.seriesCallCount
}

func (g *fakeStoreGateway) healthChecks() int {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	return g.healthCount
}

// testBackend is a teeBackend with a fixed connection.
type testBackend struct {
	name string
	conn *grpc.ClientConn
}

func (b testBackend) Name() string           { return b.name }
func (b testBackend) Conn() *grpc.ClientConn { return b.conn }

// startBackend starts a gRPC server for the fake store-gateway, and returns a tee backend that connects to it.
func startBackend(t *testing.T, g *fakeStoreGateway) teeBackend {
	server := grpc.NewServer()
	storegatewaypb.RegisterStoreGatewayServer(server, g)
	grpc_health_v1.RegisterHealthServer(server, g)
	return dialBackend(t, g.name, serve(t, server))
}

// startCountingBackend starts a backend that answers each call with "<name>:<number of request messages>".
func startCountingBackend(t *testing.T, name string) teeBackend {
	server := grpc.NewServer(
		grpc.ForceServerCodecV2(newFrameCodec()),
		grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
			count := 0
			for {
				if err := stream.RecvMsg(&frame{}); err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					return err
				}
				count++
			}
			return stream.SendMsg(&frame{payload: []byte(name + ":" + strconv.Itoa(count))})
		}),
	)
	return dialBackend(t, name, serve(t, server))
}

func dialBackend(t *testing.T, name, addr string) teeBackend {
	conn, err := grpc.NewClient(addr,
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(newFrameCodec())),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return testBackend{name: name, conn: conn}
}

// startTee starts the fake store-gateways and a tee in front of them. The secondary can be nil.
// It returns a store-gateway client that connects to the tee, and the recorder of the finished calls.
func startTee(t *testing.T, primary, secondary *fakeStoreGateway) (storegatewaypb.StoreGatewayClient, *callRecorder) {
	backendType, err := newBackendType(backendTypeStoreGateway)
	require.NoError(t, err)

	var secondaryBackend teeBackend
	if secondary != nil {
		secondaryBackend = startBackend(t, secondary)
	}
	calls := &callRecorder{}
	addr := startTeeServer(t, backendType, startBackend(t, primary), secondaryBackend, calls.record)
	return storegatewaypb.NewCustomStoreGatewayClient(dial(t, addr)), calls
}

func startTeeServer(t *testing.T, backendType backendType, primary, secondary teeBackend, onFinish func(teeCall)) string {
	handler := &teeHandler{
		backendType:      backendType,
		primary:          primary,
		secondary:        secondary,
		secondaryTimeout: time.Minute,
		onFinish:         onFinish,
	}
	server := grpc.NewServer(
		grpc.ForceServerCodecV2(newFrameCodec()),
		grpc.UnknownServiceHandler(handler.handle),
	)
	return serve(t, server)
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

func serve(t *testing.T, server *grpc.Server) string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-serveErr)
	})
	return lis.Addr().String()
}

// receiveAll receives the messages of a Series stream until the stream ends.
// It returns the received messages, and the error that ended the stream, or nil for io.EOF.
func receiveAll(stream storegatewaypb.StoreGateway_SeriesClient) ([]*storepb.SeriesResponse, error) {
	var received []*storepb.SeriesResponse
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return received, nil
		}
		if err != nil {
			return received, err
		}
		received = append(received, resp)
	}
}

// callRecorder records the finished calls.
type callRecorder struct {
	mtx   sync.Mutex
	calls []teeCall
}

func (r *callRecorder) record(call teeCall) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.calls = append(r.calls, call)
}

func (r *callRecorder) all() []teeCall {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return append([]teeCall(nil), r.calls...)
}

// waitForOne waits until the recorder has exactly one call, and returns it.
func (r *callRecorder) waitForOne(t *testing.T) teeCall {
	require.Eventually(t, func() bool { return len(r.all()) == 1 }, 5*time.Second, 10*time.Millisecond)
	return r.all()[0]
}
