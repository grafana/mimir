// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"

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

func setupDistributorGRPCClient(t *testing.T, srv *mockDistributorServer, logger log.Logger, configure func(*DistributorConfig)) *DistributorGRPCClient {
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
	cfg.GRPCClientConfig.BackoffConfig.MinBackoff = 0
	cfg.GRPCClientConfig.BackoffConfig.MaxBackoff = 0
	cfg.GRPCClientConfig.BackoffConfig.MaxRetries = 2
	cfg.RemoteTimeout = time.Second
	if configure != nil {
		configure(&cfg)
	}

	client, err := NewDistributorGRPCClient(cfg, nil, logger)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), client))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), client))
	})

	return client
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
	t.Run("push before service start", func(t *testing.T) {
		var cfg DistributorConfig
		flagext.DefaultValues(&cfg)
		client, err := NewDistributorGRPCClient(cfg, nil, log.NewNopLogger())
		require.NoError(t, err)

		req := newTestWriteRequest()
		req.SetBuffer(mem.SliceBuffer([]byte("request buffer")))
		_, err = client.Push(user.InjectOrgID(t.Context(), "test-user"), req)
		require.ErrorIs(t, err, errDistributorClientNotRunning)
		require.Nil(t, req.Buffer())
	})

	t.Run("push sends request", func(t *testing.T) {
		srv := &mockDistributorServer{}
		client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), nil)

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
				client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), nil)

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
		for _, tc := range []struct {
			name                string
			maxRetries          int
			expectedCalls       int
			expectedFailureLogs int
		}{
			{
				name:                "three total attempts",
				maxRetries:          3,
				expectedCalls:       3,
				expectedFailureLogs: 3,
			},
			{
				name:                "one total attempt",
				maxRetries:          1,
				expectedCalls:       1,
				expectedFailureLogs: 1,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				srv := &mockDistributorServer{
					errs: []error{
						status.Error(codes.Unavailable, "try again"),
						status.Error(codes.Unavailable, "try again"),
						status.Error(codes.Unavailable, "try again"),
					},
				}
				var logs bytes.Buffer
				client := setupDistributorGRPCClient(t, srv, log.NewLogfmtLogger(&logs), func(cfg *DistributorConfig) {
					cfg.GRPCClientConfig.BackoffConfig.MaxRetries = tc.maxRetries
				})

				_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), newTestWriteRequest())
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Contains(t, err.Error(), "try again")
				require.Equal(t, tc.expectedCalls, srv.calls())

				logOutput := logs.String()
				require.Equal(t, tc.expectedFailureLogs, strings.Count(logOutput, "failed to write to remote distributor"))
				require.Equal(t, tc.expectedFailureLogs, strings.Count(logOutput, "retryable=true"))
				require.Equal(t, tc.expectedFailureLogs, strings.Count(logOutput, "max_attempts="+strconv.Itoa(tc.maxRetries)))
				for attempt := 1; attempt <= tc.expectedFailureLogs; attempt++ {
					require.Contains(t, logOutput, "attempt="+strconv.Itoa(attempt))
				}
			})
		}
	})

	t.Run("push logs non-retryable failure", func(t *testing.T) {
		srv := &mockDistributorServer{
			errs: []error{
				status.Error(codes.ResourceExhausted, "limited"),
			},
		}
		var logs bytes.Buffer
		client := setupDistributorGRPCClient(t, srv, log.NewLogfmtLogger(&logs), nil)

		_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), newTestWriteRequest())
		require.Equal(t, codes.ResourceExhausted, status.Code(err))
		require.Contains(t, err.Error(), "limited")
		require.Equal(t, 1, srv.calls())

		logOutput := logs.String()
		require.Equal(t, 1, strings.Count(logOutput, "failed to write to remote distributor"))
		require.Contains(t, logOutput, "retryable=false")
		require.Contains(t, logOutput, "attempt=1")
		require.Contains(t, logOutput, "max_attempts=2")
	})

	t.Run("push returns context error when context is canceled before first attempt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(user.InjectOrgID(t.Context(), "test-user"))
		cancel()

		srv := &mockDistributorServer{}
		client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), nil)

		_, err := client.Push(ctx, newTestWriteRequest())
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 0, srv.calls())
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
		client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), func(cfg *DistributorConfig) {
			cfg.GRPCClientConfig.BackoffConfig.MaxRetries = 0
		})

		_, err := client.Push(user.InjectOrgID(ctx, "test-user"), newTestWriteRequest())
		require.Equal(t, codes.Canceled, status.Code(err))
		require.Equal(t, 3, srv.calls())
	})

	t.Run("push timeout", func(t *testing.T) {
		srv := &mockDistributorServer{blockUntilContextDone: true}
		client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), func(cfg *DistributorConfig) {
			cfg.RemoteTimeout = 10 * time.Millisecond
		})

		start := time.Now()
		_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), newTestWriteRequest())
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
		require.Less(t, time.Since(start), time.Second)
	})

	t.Run("stop closes connection", func(t *testing.T) {
		srv := &mockDistributorServer{}
		client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), nil)

		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), client))

		_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), newTestWriteRequest())
		require.ErrorIs(t, err, errDistributorClientNotRunning)
	})

	t.Run("close is idempotent", func(t *testing.T) {
		var cfg DistributorConfig
		flagext.DefaultValues(&cfg)
		client, err := NewDistributorGRPCClient(cfg, nil, log.NewNopLogger())
		require.NoError(t, err)

		require.NoError(t, client.Close())
		require.NoError(t, client.Close())
	})

	t.Run("start failure does not publish connection", func(t *testing.T) {
		var cfg DistributorConfig
		flagext.DefaultValues(&cfg)
		cfg.Address = "127.0.0.1:0"
		cfg.RemoteTimeout = time.Second
		cfg.GRPCClientConfig.GRPCCompression = "unsupported"

		client, err := NewDistributorGRPCClient(cfg, nil, log.NewNopLogger())
		require.NoError(t, err)

		err = services.StartAndAwaitRunning(t.Context(), client)
		require.EqualError(t, err, `ruler's distributor client gRPC settings: unsupported compression type: "unsupported"`)

		client.mu.RLock()
		require.Nil(t, client.conn)
		require.Nil(t, client.client)
		client.mu.RUnlock()
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
			{name: "http address", address: "http://localhost:9095", expectedErr: `ruler's distributor client address must be a gRPC address, got HTTP(S) address: "http://localhost:9095"`},
			{name: "https address", address: "https://localhost:9095", expectedErr: `ruler's distributor client address must be a gRPC address, got HTTP(S) address: "https://localhost:9095"`},
			{name: "malformed dns address", address: "dns://distributor:9095", expectedErr: `ruler's distributor client address must have "dns:///" prefix when using gRPC service discovery, got: "dns://distributor:9095"`},
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

	t.Run("remote timeout validation", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			address     string
			timeout     time.Duration
			expectedErr string
		}{
			{name: "empty address accepts zero timeout"},
			{name: "remote address accepts positive timeout", address: "localhost:9095", timeout: time.Second},
			{name: "remote address rejects zero timeout", address: "localhost:9095", expectedErr: "remote timeout must be greater than 0"},
			{name: "remote address rejects negative timeout", address: "localhost:9095", timeout: -time.Second, expectedErr: "remote timeout must be greater than 0"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var cfg DistributorConfig
				flagext.DefaultValues(&cfg)
				cfg.Address = tc.address
				cfg.RemoteTimeout = tc.timeout

				err := cfg.Validate()
				if tc.expectedErr != "" {
					require.EqualError(t, err, tc.expectedErr)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("wraps grpc client config validation errors", func(t *testing.T) {
		var cfg DistributorConfig
		flagext.DefaultValues(&cfg)
		cfg.GRPCClientConfig.GRPCCompression = "unsupported"

		require.EqualError(t, cfg.Validate(), `ruler's distributor client gRPC settings: unsupported compression type: "unsupported"`)
	})

	t.Run("validate does not mutate custom compressors", func(t *testing.T) {
		var cfg DistributorConfig
		cfg.GRPCClientConfig.GRPCCompression = s2.Name

		require.EqualError(t, cfg.Validate(), `ruler's distributor client gRPC settings: unsupported compression type: "s2"`)
		require.Empty(t, cfg.GRPCClientConfig.CustomCompressors)
	})
}
