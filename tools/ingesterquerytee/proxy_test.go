// SPDX-License-Identifier: AGPL-3.0-only

package ingesterquerytee

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"
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

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
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
	return testProxyWithLogger(t, primary, shadow, cfg, log.NewNopLogger())
}

func testProxyWithLogger(t *testing.T, primary, shadow *testIngester, cfg Config, logger log.Logger) (*Proxy, client.IngesterClient) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	p, err := New(ctx, cfg, testBackend(t, primary), testBackend(t, shadow), prometheus.NewRegistry(), logger)
	require.NoError(t, err)
	rpcLog := middleware.GRPCServerLog{Log: log.NewNopLogger(), DisableRequestSuccessLog: true}
	srv := grpc.NewServer(grpc.ForceServerCodec(WireCodec{}), grpc.UnknownServiceHandler(p.Handler), grpc.StreamInterceptor(rpcLog.StreamServerInterceptor))
	return p, client.NewIngesterClient(testConnection(t, srv))
}

func awaitOutcome(t *testing.T, p *Proxy, method, result string) {
	t.Helper()
	require.Eventually(t, func() bool { return testutil.ToFloat64(p.comparisons.WithLabelValues(method, result)) == 1 }, 5*time.Second, time.Millisecond)
}

func testMetadata(ctx context.Context) metadata.MD {
	md := metadata.MD{}
	for _, key := range []string{"x-scope-orgid", "__consistency_level__", "__consistency_offsets__", "__only_replica__", "x-tee-test"} {
		md[key] = metadata.ValueFromIncomingContext(ctx, key)
	}
	return md
}

func TestProxyUnaryMetadataAndAsyncComparison(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	seen := make(chan metadata.MD, 1)
	primary := &testIngester{labelNames: func(ctx context.Context, _ *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
		seen <- testMetadata(ctx)
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
		shadowSeen <- testMetadata(ctx)
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
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("x-scope-orgid", "tenant", "__consistency_level__", "strong", "__consistency_offsets__", "v1=0:123", "__only_replica__", "true", "x-tee-test", "custom-value"))
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
		require.Equal(t, []string{"custom-value"}, md.Get("x-tee-test"))
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
		awaitOutcome(t, p, "LabelNames", "unavailable")
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

func TestUnavailableIsMetricOnlyAndNeverReturnsShadow(t *testing.T) {
	for _, test := range []struct {
		name            string
		primary, shadow error
	}{
		{"shadow unavailable", nil, status.Error(codes.Unavailable, "spot instance gone")},
		{"primary unavailable", status.Error(codes.Unavailable, "spot instance gone"), nil},
		{"both unavailable", status.Error(codes.Unavailable, "primary gone"), status.Error(codes.Unavailable, "shadow gone")},
		{"shadow timed out", nil, status.Error(codes.DeadlineExceeded, "shadow timed out")},
		{"primary timed out", status.Error(codes.DeadlineExceeded, "primary timed out"), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			primary := &testIngester{labelNames: func(context.Context, *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
				if test.primary != nil {
					return nil, test.primary
				}
				return &client.LabelNamesResponse{LabelNames: []string{"primary"}}, nil
			}}
			shadow := &testIngester{labelNames: func(context.Context, *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
				if test.shadow != nil {
					return nil, test.shadow
				}
				return &client.LabelNamesResponse{LabelNames: []string{"shadow"}}, nil
			}}
			var logs logBuffer
			p, cli := testProxyWithLogger(t, primary, shadow, testConfig(), log.NewLogfmtLogger(log.NewSyncWriter(&logs)))
			resp, err := cli.LabelNames(t.Context(), &client.LabelNamesRequest{})
			if test.primary != nil {
				require.Nil(t, resp)
				require.Equal(t, status.Code(test.primary), status.Code(err))
				require.Equal(t, status.Convert(test.primary).Message(), status.Convert(err).Message())
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{"primary"}, resp.LabelNames)
			}
			awaitOutcome(t, p, "LabelNames", "unavailable")
			require.Eventually(t, func() bool { return len(p.slots) == 0 }, time.Second, time.Millisecond)
			require.Empty(t, logs.String())
		})
	}
}

func TestUnavailableStreamNeverFallsBackToShadow(t *testing.T) {
	frames := streamFrames(t, floatChunk(t, []int64{100}, []float64{1}))
	primary := &testIngester{query: func(_ *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
		var msg client.QueryStreamResponse
		if err := msg.Unmarshal(frames[0]); err != nil {
			return err
		}
		if err := stream.Send(&msg); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "primary disappeared mid-stream")
	}}
	shadow := &testIngester{query: func(_ *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
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
	}}
	var logs logBuffer
	p, cli := testProxyWithLogger(t, primary, shadow, testConfig(), log.NewLogfmtLogger(log.NewSyncWriter(&logs)))
	s, err := cli.QueryStream(t.Context(), &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 100})
	require.NoError(t, err)
	msg, err := s.Recv()
	require.NoError(t, err)
	require.Equal(t, frames[0], wire(t, msg))
	msg, err = s.Recv()
	require.Nil(t, msg)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, "primary disappeared mid-stream", status.Convert(err).Message())
	awaitOutcome(t, p, "QueryStream", "unavailable")
	require.Eventually(t, func() bool { return len(p.slots) == 0 }, time.Second, time.Millisecond)
	require.Empty(t, logs.String())
}

func TestBackendErrorDetailsAreNotLogged(t *testing.T) {
	const sensitive = "tenant-secret\nforged-log-entry"
	shadow := &testIngester{labelNames: func(context.Context, *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
		return nil, status.Error(codes.Internal, sensitive)
	}}
	var logs logBuffer
	p, cli := testProxyWithLogger(t, &testIngester{}, shadow, testConfig(), log.NewLogfmtLogger(log.NewSyncWriter(&logs)))
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-scope-orgid", "tenant-secret", "authorization", "tenant-secret"))
	resp, err := cli.LabelNames(ctx, &client.LabelNamesRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, resp.LabelNames)
	awaitOutcome(t, p, "LabelNames", "shadow_error")
	require.Eventually(t, func() bool { return len(p.slots) == 0 }, time.Second, time.Millisecond)
	require.Contains(t, logs.String(), "result=shadow_error")
	require.NotContains(t, logs.String(), "tenant-secret")
	require.NotContains(t, logs.String(), "forged-log-entry")
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
		var calls atomic.Int32
		shadow := &testIngester{labelNames: func(context.Context, *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
			calls.Inc()
			return &client.LabelNamesResponse{LabelNames: []string{"a"}}, nil
		}}
		p, cli := testProxy(t, &testIngester{}, shadow, cfg)
		p.slots <- struct{}{}
		_, err := cli.LabelNames(t.Context(), &client.LabelNamesRequest{})
		require.NoError(t, err)
		awaitOutcome(t, p, "LabelNames", "skipped")
		require.Eventually(t, func() bool { return calls.Load() == 1 }, time.Second, time.Millisecond)
		<-p.slots
	})
	t.Run("sample rate zero", func(t *testing.T) {
		cfg := testConfig()
		cfg.SampleRate = 0
		var calls atomic.Int32
		shadow := &testIngester{labelNames: func(context.Context, *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
			calls.Inc()
			return &client.LabelNamesResponse{LabelNames: []string{"a"}}, nil
		}}
		p, cli := testProxy(t, &testIngester{}, shadow, cfg)
		_, err := cli.LabelNames(t.Context(), &client.LabelNamesRequest{})
		require.NoError(t, err)
		awaitOutcome(t, p, "LabelNames", "skipped")
		require.Eventually(t, func() bool { return calls.Load() == 1 }, time.Second, time.Millisecond)
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

func TestProxyDrainsShadowBeyondComparisonBudget(t *testing.T) {
	for _, compare := range []bool{true, false} {
		t.Run(fmt.Sprint(compare), func(t *testing.T) {
			cfg := testConfig()
			cfg.MaxResponseBytes = 1
			if !compare {
				cfg.SampleRate = 0
			}
			done := make(chan error, 1)
			primary := &testIngester{query: func(_ *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
				return stream.Send(&client.QueryStreamResponse{IsEndOfSeriesStream: true})
			}}
			shadow := &testIngester{query: func(_ *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
				msg := &client.QueryStreamResponse{StreamingSeries: []client.QueryStreamSeries{{Labels: []mimirpb.LabelAdapter{{Name: "test", Value: strings.Repeat("x", 256<<10)}}}}}
				for range 16 {
					if err := stream.Send(msg); err != nil {
						done <- err
						return err
					}
				}
				done <- nil
				return nil
			}}
			p, cli := testProxy(t, primary, shadow, cfg)
			s, err := cli.QueryStream(t.Context(), &client.QueryRequest{})
			require.NoError(t, err)
			_, err = s.Recv()
			require.NoError(t, err)
			_, err = s.Recv()
			require.ErrorIs(t, err, io.EOF)
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("shadow response was not fully drained")
			}
			result := "skipped"
			if compare {
				result = "limit"
			}
			awaitOutcome(t, p, "QueryStream", result)
			require.Eventually(t, func() bool { return testutil.ToFloat64(p.mirrored.WithLabelValues("QueryStream", "success")) == 1 }, time.Second, time.Millisecond)
		})
	}
}

func TestProxyMismatchDiagnostics(t *testing.T) {
	for _, logQuery := range []bool{false, true} {
		t.Run(fmt.Sprint(logQuery), func(t *testing.T) {
			cfg := testConfig()
			cfg.LogMismatchedQueries = logQuery
			cfg.LogMismatchedSamples = logQuery
			logs := &logBuffer{}
			backend := func(values []float64) *testIngester {
				frames := streamFrames(t, floatChunk(t, []int64{100, 200}, values))
				return &testIngester{query: func(_ *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
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
				}}
			}
			p, cli := testProxyWithLogger(t, backend([]float64{1, 2}), backend([]float64{1, 9}), cfg, log.NewLogfmtLogger(logs))
			ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-scope-orgid", "replay-tenant", "__consistency_level__", "strong", "__consistency_offsets__", "offsets-value", "__consistency_max_delay__", "2s", "__only_replica__", "true", "authorization", "Bearer private-token", "private-header", "do-not-log"))
			s, err := cli.QueryStream(ctx, &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 200, Matchers: []*client.LabelMatcher{{Name: "secret_query", Value: "secret_value"}}})
			require.NoError(t, err)
			for {
				_, err = s.Recv()
				if err != nil {
					break
				}
			}
			require.ErrorIs(t, err, io.EOF)
			awaitOutcome(t, p, "QueryStream", "mismatch")
			require.Eventually(t, func() bool { return strings.Contains(logs.String(), "samples_with_different_values=1") }, time.Second, time.Millisecond)
			for _, field := range []string{"primary_series=1", "shadow_series=1", "primary_labels=1", "shadow_labels=1", "primary_samples=2", "shadow_samples=2"} {
				require.Contains(t, logs.String(), field)
			}
			if logQuery {
				require.Contains(t, logs.String(), "secret_query")
				require.Contains(t, logs.String(), "secret_value")
				require.Contains(t, logs.String(), "start_timestamp_ms")
				require.Contains(t, logs.String(), "replay-tenant")
				require.Contains(t, logs.String(), "offsets-value")
				require.Contains(t, logs.String(), "__consistency_max_delay__")
				require.Contains(t, logs.String(), "difference=")
				require.Contains(t, logs.String(), "0x4000000000000000")
				require.Contains(t, logs.String(), "0x4022000000000000")
			} else {
				require.NotContains(t, logs.String(), "secret_query")
				require.NotContains(t, logs.String(), "secret_value")
				require.NotContains(t, logs.String(), "replay-tenant")
				require.NotContains(t, logs.String(), "difference=")
			}
			require.NotContains(t, logs.String(), "private-token")
			require.NotContains(t, logs.String(), "do-not-log")
		})
	}
}

func TestProxyComparisonErrorDiagnostics(t *testing.T) {
	for _, backend := range []string{"primary", "shadow"} {
		for _, logQuery := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", backend, logQuery), func(t *testing.T) {
				cfg := testConfig()
				cfg.LogMismatchedQueries = logQuery
				logs := &logBuffer{}
				good := func(_ *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
					return stream.Send(&client.QueryStreamResponse{IsEndOfSeriesStream: true})
				}
				bad := func(_ *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
					return stream.Send(&client.QueryStreamResponse{StreamingSeriesChunks: []client.QueryStreamSeriesChunks{{SeriesIndex: 5}}})
				}
				primary, shadow := &testIngester{query: good}, &testIngester{query: good}
				if backend == "primary" {
					primary.query = bad
				} else {
					shadow.query = bad
				}
				p, cli := testProxyWithLogger(t, primary, shadow, cfg, log.NewLogfmtLogger(logs))
				s, err := cli.QueryStream(t.Context(), &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 200, Matchers: []*client.LabelMatcher{{Name: "secret_query", Value: "secret_value"}}})
				require.NoError(t, err)
				for {
					_, err = s.Recv()
					if err != nil {
						break
					}
				}
				require.ErrorIs(t, err, io.EOF)
				awaitOutcome(t, p, "QueryStream", "comparison_error")
				require.Eventually(t, func() bool { return strings.Contains(logs.String(), "backend="+backend) }, time.Second, time.Millisecond)
				require.Contains(t, logs.String(), "invalid series index or missing end marker")
				require.Contains(t, logs.String(), "primary_frames=1")
				require.Contains(t, logs.String(), "shadow_frames=1")
				if logQuery {
					require.Contains(t, logs.String(), "secret_query")
					require.Contains(t, logs.String(), "start_timestamp_ms")
				} else {
					require.NotContains(t, logs.String(), "secret_query")
					require.NotContains(t, logs.String(), "secret_value")
				}
			})
		}
	}
}

func TestProxyDetailedDiagnosticRateLimit(t *testing.T) {
	cfg := testConfig()
	cfg.LogMismatchedQueries, cfg.LogMismatchedSamples = true, true
	backend := func(value float64) *testIngester {
		frames := streamFrames(t, floatChunk(t, []int64{100}, []float64{value}))
		return &testIngester{query: func(_ *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
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
		}}
	}
	logs := &logBuffer{}
	p, cli := testProxyWithLogger(t, backend(1), backend(2), cfg, log.NewLogfmtLogger(logs))
	p.diagnosticLogs = rate.NewLimiter(rate.Every(time.Hour), 1)
	for range 2 {
		s, err := cli.QueryStream(t.Context(), &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 100})
		require.NoError(t, err)
		for {
			_, err = s.Recv()
			if err != nil {
				break
			}
		}
		require.ErrorIs(t, err, io.EOF)
	}
	require.Eventually(t, func() bool { return strings.Contains(logs.String(), "diagnostics_suppressed=true") }, time.Second, time.Millisecond)
	require.Equal(t, 1, strings.Count(logs.String(), "difference="))
	require.Equal(t, float64(2), testutil.ToFloat64(p.comparisons.WithLabelValues("QueryStream", "mismatch")))
}

func TestProxyMismatchRetries(t *testing.T) {
	cfg := testConfig()
	cfg.MismatchRetries = 2
	cfg.MismatchRetryDelay = time.Millisecond
	// The shadow's label names on each of its calls, the last one again after.
	shadowNames := func(names ...[]string) *testIngester {
		calls := atomic.NewInt32(0)
		return &testIngester{labelNames: func(context.Context, *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
			call := min(int(calls.Inc())-1, len(names)-1)
			return &client.LabelNamesResponse{LabelNames: names[call]}, nil
		}}
	}
	for name, tc := range map[string]struct {
		shadow *testIngester
		result string
	}{
		"agrees on a retry":                  {shadowNames([]string{"b"}, []string{"a"}), "match_after_retry"},
		"differs by other items each time":   {shadowNames([]string{"b"}, []string{"a", "c"}, []string{"a", "d"}), "match_after_retry"},
		"the same items differ on every try": {shadowNames([]string{"b"}, []string{"a", "b"}, []string{"b"}), "mismatch"},
	} {
		t.Run(name, func(t *testing.T) {
			logs := &logBuffer{}
			p, cli := testProxyWithLogger(t, &testIngester{}, tc.shadow, cfg, log.NewLogfmtLogger(logs))
			_, err := cli.LabelNames(t.Context(), &client.LabelNamesRequest{})
			require.NoError(t, err)
			awaitOutcome(t, p, "LabelNames", tc.result)
			require.Zero(t, testutil.ToFloat64(p.comparisons.WithLabelValues("LabelNames", map[string]string{"mismatch": "match_after_retry", "match_after_retry": "mismatch"}[tc.result])))
			if tc.result == "mismatch" {
				require.Contains(t, logs.String(), "retries=2 persistent_missing_from_shadow=0 persistent_extra_in_shadow=1")
			}
		})
	}
}
