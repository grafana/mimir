// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/siderolabs/grpc-proxy/proxy"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func TestDecodingStreamInterceptor_StoreGateway(t *testing.T) {
	backendAddr := startFakeStoreGateway(t)
	logger := &recordingLogger{}
	proxyAddr := startDecodingProxy(t, backendAddr, newStoreGatewayCodec(), logger)

	conn, err := grpc.NewClient(proxyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := storegatewaypb.NewCustomStoreGatewayClient(conn)

	// The client receives the backend responses unchanged.
	blockMatcher := storepb.LabelMatcher{Type: storepb.LabelMatcher_RE, Name: "__block_id", Value: "block-1|block-2"}
	stream, err := client.Series(t.Context(), &storepb.SeriesRequest{
		MinTime:      10,
		MaxTime:      20,
		Matchers:     []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}},
		RequestHints: &storepb.SeriesRequestHints{BlockMatchers: []storepb.LabelMatcher{blockMatcher}},
	})
	require.NoError(t, err)
	var fetchedIndexBytes []uint64
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		fetchedIndexBytes = append(fetchedIndexBytes, resp.GetStats().FetchedIndexBytes)
	}
	require.Equal(t, []uint64{1, 2}, fetchedIndexBytes)

	labelNames, err := client.LabelNames(t.Context(), &storepb.LabelNamesRequest{Start: 10, End: 20})
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "job"}, labelNames.Names)

	// The proxy logs each decoded message.
	require.Equal(t, []decodedLogEntry{
		{method: "/gatewaypb.StoreGateway/Series", direction: "request", messageType: "thanos.SeriesRequest"},
		{method: "/gatewaypb.StoreGateway/Series", direction: "response", messageType: "thanos.SeriesResponse"},
		{method: "/gatewaypb.StoreGateway/Series", direction: "response", messageType: "thanos.SeriesResponse"},
		{method: "/gatewaypb.StoreGateway/LabelNames", direction: "request", messageType: "thanos.LabelNamesRequest"},
		{method: "/gatewaypb.StoreGateway/LabelNames", direction: "response", messageType: "thanos.LabelNamesResponse"},
	}, logger.decodedEntries())
	require.Contains(t, logger.decodedMessages()[0], "block-1|block-2")
}

func TestDecodingStreamInterceptor_UnknownMethodIsNotLogged(t *testing.T) {
	codec := newStoreGatewayCodec()
	_, err := codec.DecodeRequest("/grpc.health.v1.Health/Check", nil)
	require.ErrorIs(t, err, errUnknownMethod)
	_, err = codec.DecodeResponse("/grpc.health.v1.Health/Check", nil)
	require.ErrorIs(t, err, errUnknownMethod)
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

func (fakeStoreGateway) Series(_ *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) error {
	if err := srv.Send(storepb.NewStatsResponse(1)); err != nil {
		return err
	}
	return srv.Send(storepb.NewStatsResponse(2))
}

func (fakeStoreGateway) LabelNames(context.Context, *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	return &storepb.LabelNamesResponse{Names: []string{"__name__", "job"}}, nil
}

func startFakeStoreGateway(t *testing.T) string {
	server := grpc.NewServer()
	storegatewaypb.RegisterStoreGatewayServer(server, &fakeStoreGateway{})
	return serve(t, server)
}

func startDecodingProxy(t *testing.T, backendAddr string, codec MessageCodec, logger *recordingLogger) string {
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
		grpc.StreamInterceptor(decodingStreamInterceptor(codec, logger)),
	)
	return serve(t, server)
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

type decodedLogEntry struct {
	method      string
	direction   string
	messageType string
}

// recordingLogger records the log entries of decoded messages.
type recordingLogger struct {
	mtx      sync.Mutex
	entries  []decodedLogEntry
	messages []string
}

func (l *recordingLogger) Log(keyvals ...any) error {
	fields := map[any]any{}
	for i := 0; i+1 < len(keyvals); i += 2 {
		fields[keyvals[i]] = keyvals[i+1]
	}
	if fields["msg"] != "proxied message" {
		return nil
	}

	l.mtx.Lock()
	defer l.mtx.Unlock()
	l.entries = append(l.entries, decodedLogEntry{
		method:      fields["method"].(string),
		direction:   fields["direction"].(string),
		messageType: fields["type"].(string),
	})
	l.messages = append(l.messages, fields["message"].(string))
	return nil
}

func (l *recordingLogger) decodedEntries() []decodedLogEntry {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	return append([]decodedLogEntry(nil), l.entries...)
}

func (l *recordingLogger) decodedMessages() []string {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	return append([]string(nil), l.messages...)
}
