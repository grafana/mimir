// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/siderolabs/grpc-proxy/proxy"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

const (
	seriesMethod     = "/gatewaypb.StoreGateway/Series"
	labelNamesMethod = "/gatewaypb.StoreGateway/LabelNames"
)

func TestDecodingStreamInterceptor_SeriesCallCollectsAllResponses(t *testing.T) {
	client, calls := startStoreGatewayProxy(t)

	blockMatcher := storepb.LabelMatcher{Type: storepb.LabelMatcher_RE, Name: "__block_id", Value: "block-1|block-2"}
	stream, err := client.Series(t.Context(), &storepb.SeriesRequest{
		MinTime:      10,
		MaxTime:      20,
		Matchers:     []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}},
		RequestHints: &storepb.SeriesRequestHints{BlockMatchers: []storepb.LabelMatcher{blockMatcher}},
	})
	require.NoError(t, err)
	received, err := receiveAll(stream)
	require.NoError(t, err)
	require.Equal(t, 3, received)

	// The proxy collects the request and all the responses of the call in one proxiedCall.
	recorded := calls.all()
	require.Len(t, recorded, 1)
	call := recorded[0]
	require.Equal(t, seriesMethod, call.FullMethod)
	require.NoError(t, call.Err)
	require.NoError(t, call.DecodeErr)

	require.Len(t, call.Requests, 1)
	request := call.Requests[0].(*storepb.SeriesRequest)
	require.Equal(t, int64(10), request.MinTime)
	require.Equal(t, []storepb.LabelMatcher{blockMatcher}, request.RequestHints.BlockMatchers)
	require.Positive(t, call.RequestBytes)

	require.Len(t, call.Responses, 3)
	series := call.Responses[0].(*storepb.SeriesResponse).GetStreamingSeries()
	require.NotNil(t, series)
	require.True(t, series.IsEndOfSeriesStream)
	require.Equal(t, []mimirpb.LabelAdapter{{Name: "__name__", Value: "up"}}, series.Series[0].Labels)
	require.Equal(t, uint64(1), call.Responses[1].(*storepb.SeriesResponse).GetStats().FetchedIndexBytes)
	require.Equal(t, []storepb.Block{{Id: "block-1"}}, call.Responses[2].(*storepb.SeriesResponse).GetResponseHints().QueriedBlocks)
	require.Positive(t, call.ResponseBytes)
}

func TestDecodingStreamInterceptor_UnaryCall(t *testing.T) {
	client, calls := startStoreGatewayProxy(t)

	labelNames, err := client.LabelNames(t.Context(), &storepb.LabelNamesRequest{Start: 10, End: 20})
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "job"}, labelNames.Names)

	recorded := calls.all()
	require.Len(t, recorded, 1)
	call := recorded[0]
	require.Equal(t, labelNamesMethod, call.FullMethod)
	require.NoError(t, call.Err)
	require.Len(t, call.Requests, 1)
	require.Equal(t, int64(10), call.Requests[0].(*storepb.LabelNamesRequest).Start)
	require.Len(t, call.Responses, 1)
	require.Equal(t, []string{"__name__", "job"}, call.Responses[0].(*storepb.LabelNamesResponse).Names)
}

func TestDecodingStreamInterceptor_FailedCallKeepsResponsesBeforeTheError(t *testing.T) {
	client, calls := startStoreGatewayProxy(t)

	// A negative MinTime makes the fake store-gateway fail after it sends one response.
	stream, err := client.Series(t.Context(), &storepb.SeriesRequest{MinTime: -1, MaxTime: 20})
	require.NoError(t, err)
	received, err := receiveAll(stream)
	require.Equal(t, 1, received)
	require.Equal(t, codes.Unavailable, status.Code(err))

	recorded := calls.all()
	require.Len(t, recorded, 1)
	call := recorded[0]
	require.Equal(t, codes.Unavailable, status.Code(call.Err))
	require.NoError(t, call.DecodeErr)
	require.Len(t, call.Responses, 1)
}

func TestDecodingStreamInterceptor_UnknownMethodIsNotCollected(t *testing.T) {
	backendAddr := startFakeStoreGateway(t)
	calls := &callRecorder{}
	conn := dial(t, startDecodingProxy(t, backendAddr, newStoreGatewayCodec(), calls.record))

	resp, err := grpc_health_v1.NewHealthClient(conn).Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)
	require.Empty(t, calls.all())
}

func TestDecodingStreamInterceptor_ClientStreamingCallCollectsAllRequests(t *testing.T) {
	const method = "/test.Service/ClientStream"
	codec := methodTableCodec{methods: map[string]methodTypes{
		method: {
			newRequest:  func() proto.Message { return &storepb.LabelNamesRequest{} },
			newResponse: func() proto.Message { return &storepb.LabelNamesResponse{} },
		},
	}}

	var requests [][]byte
	for _, start := range []int64{1, 2, 3} {
		data, err := (&storepb.LabelNamesRequest{Start: start}).Marshal()
		require.NoError(t, err)
		requests = append(requests, data)
	}
	stream := &fakeServerStream{ctx: t.Context(), requests: requests}

	calls := &callRecorder{}
	interceptor := decodingStreamInterceptor(codec, calls.record)
	// The handler receives the request frames until the client closes the stream, the same way as the proxy.
	handler := func(_ any, ss grpc.ServerStream) error {
		for {
			if err := ss.RecvMsg(proxy.NewFrame(nil)); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
		}
	}
	require.NoError(t, interceptor(nil, stream, &grpc.StreamServerInfo{FullMethod: method, IsClientStream: true}, handler))

	recorded := calls.all()
	require.Len(t, recorded, 1)
	call := recorded[0]
	require.NoError(t, call.DecodeErr)
	require.Len(t, call.Requests, 3)
	for i, req := range call.Requests {
		require.Equal(t, int64(i+1), req.(*storepb.LabelNamesRequest).Start)
	}
	require.Equal(t, len(requests[0])+len(requests[1])+len(requests[2]), call.RequestBytes)
}

func TestNewMessageCodec(t *testing.T) {
	codec, err := newMessageCodec(backendTypeOpaque)
	require.NoError(t, err)
	require.Nil(t, codec)

	codec, err = newMessageCodec(backendTypeStoreGateway)
	require.NoError(t, err)
	require.NotNil(t, codec)

	_, err = newMessageCodec("unknown")
	require.EqualError(t, err, `unknown backend type "unknown"`)
}

type fakeStoreGateway struct {
	storegatewaypb.UnimplementedStoreGatewayServer
}

func (fakeStoreGateway) Series(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) error {
	if req.MinTime < 0 {
		if err := srv.Send(storepb.NewStatsResponse(1)); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "store-gateway is not available")
	}

	responses := []*storepb.SeriesResponse{
		storepb.NewStreamingSeriesResponse(&storepb.StreamingSeriesBatch{
			Series:              []*storepb.StreamingSeries{{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "up"}}}},
			IsEndOfSeriesStream: true,
		}),
		storepb.NewStatsResponse(1),
		storepb.NewHintsSeriesResponse(&storepb.SeriesResponseHints{QueriedBlocks: []storepb.Block{{Id: "block-1"}}}),
	}
	for _, resp := range responses {
		if err := srv.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func (fakeStoreGateway) LabelNames(context.Context, *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	return &storepb.LabelNamesResponse{Names: []string{"__name__", "job"}}, nil
}

type fakeHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (fakeHealthServer) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func startFakeStoreGateway(t *testing.T) string {
	server := grpc.NewServer()
	storegatewaypb.RegisterStoreGatewayServer(server, &fakeStoreGateway{})
	grpc_health_v1.RegisterHealthServer(server, &fakeHealthServer{})
	return serve(t, server)
}

// startStoreGatewayProxy starts a fake store-gateway and a decoding proxy in front of it.
// It returns a store-gateway client that connects to the proxy, and the recorder of the proxied calls.
func startStoreGatewayProxy(t *testing.T) (storegatewaypb.StoreGatewayClient, *callRecorder) {
	backendAddr := startFakeStoreGateway(t)
	calls := &callRecorder{}
	conn := dial(t, startDecodingProxy(t, backendAddr, newStoreGatewayCodec(), calls.record))
	return storegatewaypb.NewCustomStoreGatewayClient(conn), calls
}

func startDecodingProxy(t *testing.T, backendAddr string, codec MessageCodec, onCall func(proxiedCall)) string {
	conn, err := grpc.NewClient(
		backendAddr,
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(proxy.Codec())),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	backend := &proxy.SingleBackend{
		GetConn: func(ctx context.Context) (context.Context, *grpc.ClientConn, error) {
			return ctx, conn, nil
		},
	}
	director := func(context.Context, string) (proxy.Mode, []proxy.Backend, error) {
		return proxy.One2One, []proxy.Backend{backend}, nil
	}

	server := grpc.NewServer(
		grpc.ForceServerCodecV2(proxy.Codec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
		grpc.StreamInterceptor(decodingStreamInterceptor(codec, onCall)),
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
// It returns the number of received messages, and the error that ended the stream, or nil for io.EOF.
func receiveAll(stream storegatewaypb.StoreGateway_SeriesClient) (int, error) {
	received := 0
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return received, nil
		}
		if err != nil {
			return received, err
		}
		received++
	}
}

// fakeServerStream is a server stream that returns the given request frames, then io.EOF.
type fakeServerStream struct {
	grpc.ServerStream

	ctx      context.Context
	requests [][]byte
}

func (s *fakeServerStream) Context() context.Context {
	return s.ctx
}

func (s *fakeServerStream) RecvMsg(m any) error {
	if len(s.requests) == 0 {
		return io.EOF
	}
	data := s.requests[0]
	s.requests = s.requests[1:]
	// The proxy codec fills the frame with the payload, the same way as gRPC does for the proxy.
	return proxy.Codec().Unmarshal(mem.BufferSlice{mem.SliceBuffer(data)}, m)
}

// callRecorder records the proxied calls.
type callRecorder struct {
	mtx   sync.Mutex
	calls []proxiedCall
}

func (r *callRecorder) record(call proxiedCall) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.calls = append(r.calls, call)
}

func (r *callRecorder) all() []proxiedCall {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return append([]proxiedCall(nil), r.calls...)
}
