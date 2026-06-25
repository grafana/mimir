// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/distributor/distributorpb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/grpcencoding/s2"
)

type mockDistributorServer struct {
	distributorpb.UnimplementedDistributorServer

	mu       sync.Mutex
	requests []*mimirpb.WriteRequest
	userIDs  []string
	errs     []error

	blockUntilContextDone bool
	onPush                func(calls int)
}

func (m *mockDistributorServer) Push(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	if m.blockUntilContextDone {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	userID, _, err := user.ExtractFromGRPCRequest(ctx)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.requests = append(m.requests, req)
	m.userIDs = append(m.userIDs, userID)
	calls := len(m.requests)
	if m.onPush != nil {
		m.onPush(calls)
	}

	if len(m.errs) > 0 {
		err := m.errs[0]
		m.errs = m.errs[1:]
		return nil, err
	}
	return &mimirpb.WriteResponse{}, nil
}

func (m *mockDistributorServer) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

func (m *mockDistributorServer) lastRequest() *mimirpb.WriteRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests[len(m.requests)-1]
}

func (m *mockDistributorServer) lastUserID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.userIDs[len(m.userIDs)-1]
}

func setupDistributorGRPCClient(t *testing.T, srv *mockDistributorServer, logger log.Logger, pushCfg distributorPushConfig) *DistributorGRPCClient {
	t.Helper()

	grpcServer := grpc.NewServer()
	distributorpb.RegisterDistributorServer(grpcServer, srv)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		err := <-serveErrCh
		require.Truef(t, err == nil || errors.Is(err, grpc.ErrServerStopped), "unexpected gRPC server error: %v", err)
	})

	var cfg DistributorConfig
	flagext.DefaultValues(&cfg)
	cfg.Address = listener.Addr().String()

	client, err := newDistributorGRPCClient(cfg, nil, logger, pushCfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}

func testDistributorPushConfig() distributorPushConfig {
	return distributorPushConfig{
		backoff: backoff.Config{
			MinBackoff: 0,
			MaxBackoff: 0,
			MaxRetries: 1,
		},
		timeout: time.Second,
	}
}

func newTestWriteRequest() *mimirpb.WriteRequest {
	return mimirpb.ToWriteRequest(
		[][]mimirpb.LabelAdapter{
			mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "test_metric")),
		},
		[]mimirpb.Sample{{TimestampMs: 123, Value: 456}},
		nil,
		nil,
		mimirpb.RULE,
	)
}

func TestDistributorGRPCClient(t *testing.T) {
	t.Run("push sends request", func(t *testing.T) {
		srv := &mockDistributorServer{}
		client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), testDistributorPushConfig())

		req := newTestWriteRequest()

		_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), req)
		require.NoError(t, err)

		require.Equal(t, 1, srv.calls())
		require.Equal(t, "test-user", srv.lastUserID())
		require.Equal(t, mimirpb.RULE, srv.lastRequest().Source)
	})

	t.Run("push retries", func(t *testing.T) {
		for _, tc := range []struct {
			name         string
			err          error
			expectedCall int
		}{
			{
				name:         "unavailable is retried",
				err:          status.Error(codes.Unavailable, "try again"),
				expectedCall: 2,
			},
			{
				name:         "deadline exceeded is retried",
				err:          status.Error(codes.DeadlineExceeded, "try again"),
				expectedCall: 2,
			},
			{
				name:         "internal is retried",
				err:          status.Error(codes.Internal, "try again"),
				expectedCall: 2,
			},
			{
				name:         "resource exhausted is not retried",
				err:          status.Error(codes.ResourceExhausted, "limited"),
				expectedCall: 1,
			},
			{
				name:         "client error is not retried",
				err:          mustStatusWithDetails(codes.Internal, mimirpb.ERROR_CAUSE_BAD_DATA).Err(),
				expectedCall: 1,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				srv := &mockDistributorServer{errs: []error{tc.err}}
				client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), testDistributorPushConfig())

				_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), newTestWriteRequest())
				if tc.expectedCall == 1 {
					require.Equal(t, status.Convert(tc.err).Proto(), status.Convert(err).Proto())
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tc.expectedCall, srv.calls())
			})
		}
	})

	t.Run("push uses configured max retries and logs", func(t *testing.T) {
		const maxRetries = 3
		srv := &mockDistributorServer{
			errs: []error{
				status.Error(codes.Unavailable, "try again"),
				status.Error(codes.Unavailable, "try again"),
				status.Error(codes.Unavailable, "try again"),
				status.Error(codes.Unavailable, "try again"),
			},
		}
		var logs bytes.Buffer
		client := setupDistributorGRPCClient(t, srv, log.NewLogfmtLogger(&logs), distributorPushConfig{
			backoff: backoff.Config{
				MinBackoff: 0,
				MaxBackoff: 0,
				MaxRetries: maxRetries,
			},
			timeout: time.Second,
		})

		_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), newTestWriteRequest())
		require.Equal(t, codes.Unavailable, status.Code(err))
		require.Contains(t, err.Error(), "try again")
		require.Equal(t, maxRetries+1, srv.calls())

		logOutput := logs.String()
		require.Equal(t, maxRetries, strings.Count(logOutput, "failed to remotely push rule evaluation results"))
		require.Contains(t, logOutput, "attempt=1")
		require.Contains(t, logOutput, "max_retries=3")
		require.Contains(t, logOutput, "retry_delay=0s")
	})

	t.Run("push retries until context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		srv := &mockDistributorServer{
			errs: []error{
				status.Error(codes.Unavailable, "try again"),
				status.Error(codes.Unavailable, "try again"),
				status.Error(codes.Unavailable, "try again"),
			},
			onPush: func(calls int) {
				if calls == 3 {
					cancel()
				}
			},
		}
		client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), distributorPushConfig{
			backoff: backoff.Config{
				MinBackoff: 0,
				MaxBackoff: 0,
				MaxRetries: 0,
			},
			timeout: time.Second,
		})

		_, err := client.Push(user.InjectOrgID(ctx, "test-user"), newTestWriteRequest())
		require.Equal(t, codes.Canceled, status.Code(err))
		require.Equal(t, 3, srv.calls())
	})

	t.Run("push timeout", func(t *testing.T) {
		srv := &mockDistributorServer{blockUntilContextDone: true}
		client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), distributorPushConfig{
			backoff: backoff.Config{
				MinBackoff: 0,
				MaxBackoff: 0,
				MaxRetries: 1,
			},
			timeout: 10 * time.Millisecond,
		})

		start := time.Now()
		_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), newTestWriteRequest())
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
		require.Less(t, time.Since(start), time.Second)
	})

	t.Run("close closes connection", func(t *testing.T) {
		srv := &mockDistributorServer{}
		client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), testDistributorPushConfig())

		require.NoError(t, client.Close())
		require.NoError(t, client.Close())

		_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), newTestWriteRequest())
		require.Equal(t, codes.Canceled, status.Code(err))
		require.Contains(t, err.Error(), "grpc: the client connection is closing")
	})

	t.Run("close returns first close error", func(t *testing.T) {
		conn, err := grpc.NewClient("127.0.0.1:0", grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		require.NoError(t, conn.Close())

		client := &DistributorGRPCClient{conn: conn}
		require.EqualError(t, client.Close(), "rpc error: code = Canceled desc = grpc: the client connection is closing")
		require.EqualError(t, client.Close(), "rpc error: code = Canceled desc = grpc: the client connection is closing")
	})
}

func TestDistributorConfig_Validate(t *testing.T) {
	t.Run("address validation", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			address     string
			expectedErr string
		}{
			{name: "empty address"},
			{name: "plain grpc address", address: "localhost:9095"},
			{name: "dns address", address: "dns:///distributor:9095"},
			{name: "http address", address: "http://localhost:9095", expectedErr: `address must be a gRPC address, got HTTP(S) address: "http://localhost:9095"`},
			{name: "https address", address: "https://localhost:9095", expectedErr: `address must be a gRPC address, got HTTP(S) address: "https://localhost:9095"`},
			{name: "malformed dns address", address: "dns://distributor:9095", expectedErr: `address must have "dns:///" prefix when using gRPC service discovery, got: "dns://distributor:9095"`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var cfg DistributorConfig
				flagext.DefaultValues(&cfg)
				cfg.Address = tc.address

				err := cfg.Validate()
				if tc.expectedErr != "" {
					require.EqualError(t, err, tc.expectedErr)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("accepts s2 compression", func(t *testing.T) {
		var cfg DistributorConfig
		flagext.DefaultValues(&cfg)
		cfg.GRPCClientConfig.GRPCCompression = s2.Name

		require.NoError(t, cfg.Validate())
	})
}
