// SPDX-License-Identifier: AGPL-3.0-only

package ingesterquerytee

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

type testIngester struct {
	client.UnimplementedIngesterServer
	labelNames func(context.Context, *client.LabelNamesRequest) (*client.LabelNamesResponse, error)
	query      func(*client.QueryRequest, client.Ingester_QueryStreamServer) error
	pushes     atomic.Int32
}

func (s *testIngester) LabelNames(ctx context.Context, req *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	if s.labelNames != nil {
		return s.labelNames(ctx, req)
	}
	return &client.LabelNamesResponse{LabelNames: []string{"a"}}, nil
}
func (s *testIngester) QueryStream(req *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
	return s.query(req, stream)
}
func (s *testIngester) Push(context.Context, *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	s.pushes.Add(1)
	return &mimirpb.WriteResponse{}, nil
}

func testConnection(t *testing.T, srv *grpc.Server) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	done := make(chan error, 1)
	go func() { done <- srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop(); require.NoError(t, <-done); require.NoError(t, lis.Close()) })
	conn, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

func testBackend(t *testing.T, backend *testIngester) *grpc.ClientConn {
	t.Helper()
	srv := grpc.NewServer()
	client.RegisterIngesterServer(srv, backend)
	return testConnection(t, srv)
}

func testProxy(t *testing.T, primary, shadow *testIngester, cfg Config) (*Proxy, client.IngesterClient) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	p, err := New(ctx, cfg, testBackend(t, primary), testBackend(t, shadow), prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.ForceServerCodec(WireCodec{}), grpc.UnknownServiceHandler(p.Handler))
	return p, client.NewIngesterClient(testConnection(t, srv))
}

func awaitOutcome(t *testing.T, p *Proxy, method, result string) {
	t.Helper()
	require.Eventually(t, func() bool { return testutil.ToFloat64(p.comparisons.WithLabelValues(method, result)) == 1 }, 5*time.Second, time.Millisecond)
}

func TestProxyUnaryMetadataAndAsyncComparison(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	seen := make(chan metadata.MD, 1)
	primary := &testIngester{labelNames: func(ctx context.Context, _ *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		seen <- md
		if err := grpc.SendHeader(ctx, metadata.Pairs("primary-header", "yes")); err != nil {
			return nil, err
		}
		if err := grpc.SetTrailer(ctx, metadata.Pairs("primary-trailer", "yes")); err != nil {
			return nil, err
		}
		return &client.LabelNamesResponse{LabelNames: []string{"a", "b"}}, nil
	}}
	shadowSeen := make(chan metadata.MD, 1)
	shadow := &testIngester{labelNames: func(ctx context.Context, _ *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		shadowSeen <- md
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &client.LabelNamesResponse{LabelNames: []string{"b", "a"}}, nil
	}}
	p, cli := testProxy(t, primary, shadow, testConfig())
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("x-scope-orgid", "tenant", "__consistency_level__", "strong", "__consistency_offsets__", "v1=0:123", "__only_replica__", "true"))
	var header, trailer metadata.MD
	resp, err := cli.LabelNames(ctx, &client.LabelNamesRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, resp.LabelNames)
	require.Equal(t, []string{"yes"}, header.Get("primary-header"))
	require.Equal(t, []string{"yes"}, trailer.Get("primary-trailer"))
	for _, md := range []metadata.MD{<-seen, <-shadowSeen} {
		require.Equal(t, []string{"tenant"}, md.Get("x-scope-orgid"))
		require.Equal(t, []string{"strong"}, md.Get("__consistency_level__"))
		require.Equal(t, []string{"v1=0:123"}, md.Get("__consistency_offsets__"))
		require.Equal(t, []string{"true"}, md.Get("__only_replica__"))
	}
	// Returning from the caller cancels its server context; the shadow must finish independently.
	release <- struct{}{}
	awaitOutcome(t, p, "LabelNames", "match")
}

func TestProxyShadowTimeoutAndMismatch(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		cfg := testConfig()
		cfg.ShadowTimeout = 20 * time.Millisecond
		shadow := &testIngester{labelNames: func(ctx context.Context, _ *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		p, cli := testProxy(t, &testIngester{}, shadow, cfg)
		resp, err := cli.LabelNames(t.Context(), &client.LabelNamesRequest{})
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, resp.LabelNames)
		awaitOutcome(t, p, "LabelNames", "shadow_error")
	})
	t.Run("mismatch", func(t *testing.T) {
		shadow := &testIngester{labelNames: func(context.Context, *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
			return &client.LabelNamesResponse{LabelNames: []string{"b"}}, nil
		}}
		p, cli := testProxy(t, &testIngester{}, shadow, testConfig())
		_, err := cli.LabelNames(t.Context(), &client.LabelNamesRequest{})
		require.NoError(t, err)
		awaitOutcome(t, p, "LabelNames", "mismatch")
	})
	t.Run("primary status and trailer", func(t *testing.T) {
		primary := &testIngester{labelNames: func(ctx context.Context, _ *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
			if err := grpc.SetTrailer(ctx, metadata.Pairs("reason", "primary")); err != nil {
				return nil, err
			}
			return nil, status.Error(codes.ResourceExhausted, "primary limit")
		}}
		p, cli := testProxy(t, primary, &testIngester{}, testConfig())
		var trailer metadata.MD
		_, err := cli.LabelNames(t.Context(), &client.LabelNamesRequest{}, grpc.Trailer(&trailer))
		require.Equal(t, codes.ResourceExhausted, status.Code(err))
		require.Equal(t, "primary limit", status.Convert(err).Message())
		require.Equal(t, []string{"primary"}, trailer.Get("reason"))
		awaitOutcome(t, p, "LabelNames", "primary_error")
	})
}

func TestProxyStreamingReturnsBeforeShadow(t *testing.T) {
	frames := streamFrames(t, floatChunk(t, []int64{100, 200}, []float64{1, 2}))
	send := func(stream client.Ingester_QueryStreamServer) error {
		if err := stream.SendHeader(metadata.Pairs("stream-header", "yes")); err != nil {
			return err
		}
		stream.SetTrailer(metadata.Pairs("stream-trailer", "yes"))
		for _, f := range frames {
			var msg client.QueryStreamResponse
			if err := msg.Unmarshal(f); err != nil {
				return err
			}
			if err := stream.Send(&msg); err != nil {
				return err
			}
		}
		return nil
	}
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	primary := &testIngester{query: func(_ *client.QueryRequest, stream client.Ingester_QueryStreamServer) error { return send(stream) }}
	shadow := &testIngester{query: func(_ *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
		select {
		case <-release:
			return send(stream)
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}}
	p, cli := testProxy(t, primary, shadow, testConfig())
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	s, err := cli.QueryStream(ctx, &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 200})
	require.NoError(t, err)
	for _, f := range frames {
		msg, err := s.Recv()
		require.NoError(t, err)
		require.Equal(t, f, wire(t, msg))
	}
	_, err = s.Recv()
	require.ErrorIs(t, err, io.EOF)
	header, err := s.Header()
	require.NoError(t, err)
	require.Equal(t, []string{"yes"}, header.Get("stream-header"))
	require.Equal(t, []string{"yes"}, s.Trailer().Get("stream-trailer"))
	release <- struct{}{}
	awaitOutcome(t, p, "QueryStream", "match")
}

func TestProxyLimitsAndWrites(t *testing.T) {
	t.Run("capture limit", func(t *testing.T) {
		cfg := testConfig()
		cfg.MaxResponseBytes = 1
		p, cli := testProxy(t, &testIngester{}, &testIngester{}, cfg)
		resp, err := cli.LabelNames(t.Context(), &client.LabelNamesRequest{})
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, resp.LabelNames)
		awaitOutcome(t, p, "LabelNames", "limit")
	})
	t.Run("saturation", func(t *testing.T) {
		cfg := testConfig()
		cfg.MaxConcurrent = 1
		p, cli := testProxy(t, &testIngester{}, &testIngester{}, cfg)
		p.slots <- struct{}{}
		_, err := cli.LabelNames(t.Context(), &client.LabelNamesRequest{})
		require.NoError(t, err)
		awaitOutcome(t, p, "LabelNames", "skipped")
		<-p.slots
	})
	t.Run("sample rate zero", func(t *testing.T) {
		cfg := testConfig()
		cfg.SampleRate = 0
		p, cli := testProxy(t, &testIngester{}, &testIngester{}, cfg)
		_, err := cli.LabelNames(t.Context(), &client.LabelNamesRequest{})
		require.NoError(t, err)
		awaitOutcome(t, p, "LabelNames", "skipped")
	})
	t.Run("push primary only", func(t *testing.T) {
		primary, shadow := &testIngester{}, &testIngester{}
		_, cli := testProxy(t, primary, shadow, testConfig())
		_, err := cli.Push(t.Context(), &mimirpb.WriteRequest{})
		require.NoError(t, err)
		require.Equal(t, int32(1), primary.pushes.Load())
		require.Zero(t, shadow.pushes.Load())
	})
}

func TestWireCodecTypedFallbackAndOwnership(t *testing.T) {
	c := WireCodec{}
	msg := &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}
	data, err := c.Marshal(msg)
	require.NoError(t, err)
	var decoded grpc_health_v1.HealthCheckResponse
	require.NoError(t, c.Unmarshal(data, &decoded))
	require.Equal(t, msg.Status, decoded.Status)
	var f frame
	require.NoError(t, c.Unmarshal(data, &f))
	data[0] = 0
	require.NotEqual(t, data, []byte(f))
}

func TestProxyCallerCancellation(t *testing.T) {
	primaryStarted := make(chan struct{})
	primaryCanceled := make(chan struct{})
	primary := &testIngester{labelNames: func(ctx context.Context, _ *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
		close(primaryStarted)
		<-ctx.Done()
		close(primaryCanceled)
		return nil, ctx.Err()
	}}
	p, cli := testProxy(t, primary, &testIngester{}, testConfig())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := cli.LabelNames(ctx, &client.LabelNamesRequest{}); done <- err }()
	select {
	case <-primaryStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("primary was not called")
	}
	cancel()
	select {
	case err := <-done:
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("caller was not canceled")
	}
	select {
	case <-primaryCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("primary was not canceled")
	}
	awaitOutcome(t, p, "LabelNames", "primary_error")
	require.Eventually(t, func() bool { return len(p.slots) == 0 }, time.Second, time.Millisecond)
}
