// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/flagext"
	grpcsnappy "github.com/grafana/dskit/grpcencoding/snappy"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/mem"

	"github.com/grafana/mimir/pkg/distributor/distributorpb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/grpcencoding/s2"
)

type mockDistributorServer struct {
	distributorpb.UnimplementedDistributorServer

	mu         sync.Mutex
	requests   []*mimirpb.WriteRequest
	userIDs    []string
	errs       []error
	errsByCall map[int]error

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
	if err := m.errsByCall[calls]; err != nil {
		return nil, err
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

func (m *mockDistributorServer) allRequests() []*mimirpb.WriteRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*mimirpb.WriteRequest(nil), m.requests...)
}

func (m *mockDistributorServer) allUserIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.userIDs...)
}

func setupDistributorGRPCClient(t *testing.T, srv *mockDistributorServer, logger log.Logger, configure func(*DistributorConfig), serverOptions ...grpc.ServerOption) *DistributorGRPCClient {
	t.Helper()

	grpcServer := grpc.NewServer(serverOptions...)
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
	return newTestWriteRequestWithSeries(1)
}

func newTestWriteRequestWithSeries(numSeries int) *mimirpb.WriteRequest {
	seriesLabels := make([][]mimirpb.LabelAdapter, 0, numSeries)
	samples := make([]mimirpb.Sample, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		seriesLabels = append(seriesLabels, mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(
			model.MetricNameLabel, "test_metric_"+strconv.Itoa(i),
			"padding", strings.Repeat("x", 32),
		)))
		samples = append(samples, mimirpb.Sample{TimestampMs: 123, Value: float64(i)})
	}

	return mimirpb.ToWriteRequest(
		seriesLabels,
		samples,
		nil,
		nil,
		mimirpb.RULE,
	)
}

func maxWriteRequestSizeForOneSeries(req *mimirpb.WriteRequest) int {
	maxSize := 0
	for idx := range req.Timeseries {
		oneSeriesReq := &mimirpb.WriteRequest{
			Timeseries:          req.Timeseries[idx : idx+1],
			Source:              req.Source,
			SkipLabelValidation: req.SkipLabelValidation,
		}
		maxSize = max(maxSize, oneSeriesReq.Size())
	}
	return maxSize
}

func metricNamesFromWriteRequests(requests []*mimirpb.WriteRequest) []string {
	metricNames := make([]string, 0)
	for _, req := range requests {
		for _, ts := range req.Timeseries {
			for _, lbl := range ts.Labels {
				if lbl.Name == model.MetricNameLabel {
					metricNames = append(metricNames, lbl.Value)
					break
				}
			}
		}
	}
	return metricNames
}

func requireRequestsPerWriteMetric(t *testing.T, client *DistributorGRPCClient, expectedCount uint64, expectedSum float64) {
	t.Helper()

	metric := &dto.Metric{}
	require.NoError(t, client.requestsPerWriteRequest.Write(metric))
	require.Equal(t, expectedCount, metric.GetHistogram().GetSampleCount())
	require.Equal(t, expectedSum, metric.GetHistogram().GetSampleSum())
}

func compressPayload(t *testing.T, compressorName string, payload []byte) int {
	t.Helper()

	compressor := encoding.GetCompressor(compressorName)
	require.NotNil(t, compressor)

	var compressed bytes.Buffer
	writer, err := compressor.Compress(&compressed)
	require.NoError(t, err)
	_, err = writer.Write(payload)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return compressed.Len()
}

func TestMaxUncompressedPayloadSize(t *testing.T) {
	t.Run("uncompressed payload uses the configured limit", func(t *testing.T) {
		for _, limit := range []int{-1, 0, 1, 100, int(^uint(0) >> 1)} {
			actual, err := maxUncompressedPayloadSize(limit, "")
			require.NoError(t, err)
			require.Equal(t, max(limit, 0), actual)
		}
	})

	t.Run("compressed payload reserves framing overhead", func(t *testing.T) {
		for _, tc := range []struct {
			limit    int
			expected int
		}{
			{limit: 0, expected: 0},
			{limit: 31, expected: 0},
			{limit: 32, expected: 1},
			{limit: 16415, expected: 16384},
			{limit: 16423, expected: 16384},
			{limit: 16424, expected: 16385},
		} {
			for _, compressorName := range []string{grpcgzip.Name, grpcsnappy.Name, s2.Name} {
				actual, err := maxUncompressedPayloadSize(tc.limit, compressorName)
				require.NoError(t, err)
				require.Equal(t, tc.expected, actual, "compressor %s", compressorName)
			}
		}
	})

	t.Run("compressed payload calculation does not overflow", func(t *testing.T) {
		maxInt := int(^uint(0) >> 1)
		actual, err := maxUncompressedPayloadSize(maxInt, grpcgzip.Name)
		require.NoError(t, err)
		require.Positive(t, actual)
		require.True(t, compressedPayloadFits(actual, maxInt))
		require.False(t, compressedPayloadFits(actual+1, maxInt))
	})

	t.Run("unsupported compressor fails closed", func(t *testing.T) {
		_, err := maxUncompressedPayloadSize(100, "future-compressor")
		require.EqualError(t, err, `compression type "future-compressor" has no payload-size bound`)
	})
}

func TestCompressedPayloadSizeUpperBound(t *testing.T) {
	random := rand.New(rand.NewSource(12345))
	for _, size := range []int{0, 1, 16383, 16384, 16385, 65534, 65535, 65536, 2 * 65535, 2*65535 + 1, 1<<20 - 1, 1 << 20, 1<<20 + 1} {
		payload := make([]byte, size)
		_, err := random.Read(payload)
		require.NoError(t, err)

		upperBound, ok := compressedPayloadSizeUpperBound(size)
		require.True(t, ok)
		for _, compressorName := range []string{grpcgzip.Name, grpcsnappy.Name, s2.Name} {
			compressedSize := compressPayload(t, compressorName, payload)
			require.LessOrEqual(t, compressedSize, upperBound, "compressor %s, payload size %d", compressorName, size)
		}
	}
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
		requireRequestsPerWriteMetric(t, client, 1, 1)
	})

	t.Run("push splits oversized request", func(t *testing.T) {
		srv := &mockDistributorServer{}
		req := newTestWriteRequestWithSeries(4)
		expectedMetricNames := metricNamesFromWriteRequests([]*mimirpb.WriteRequest{req})
		maxSize := maxWriteRequestSizeForOneSeries(req)
		req.SetBuffer(mem.SliceBuffer([]byte("request buffer")))

		client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), func(cfg *DistributorConfig) {
			cfg.GRPCClientConfig.MaxSendMsgSize = maxSize
		})

		_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), req)
		require.NoError(t, err)
		require.Nil(t, req.Buffer())

		requests := srv.allRequests()
		require.Len(t, requests, 4)
		for _, request := range requests {
			require.LessOrEqual(t, request.Size(), maxSize)
			require.Equal(t, mimirpb.RULE, request.Source)
		}
		require.Equal(t, expectedMetricNames, metricNamesFromWriteRequests(requests))
		require.Equal(t, []string{"test-user", "test-user", "test-user", "test-user"}, srv.allUserIDs())
		requireRequestsPerWriteMetric(t, client, 1, 4)
	})

	for _, compressorName := range []string{grpcgzip.Name, grpcsnappy.Name, s2.Name} {
		t.Run("push splits oversized request with "+compressorName+" compression", func(t *testing.T) {
			srv := &mockDistributorServer{}
			req := newTestWriteRequestWithSeries(4)
			expectedMetricNames := metricNamesFromWriteRequests([]*mimirpb.WriteRequest{req})
			maxUncompressedSize := maxWriteRequestSizeForOneSeries(req)
			maxSendMsgSize, ok := compressedPayloadSizeUpperBound(maxUncompressedSize)
			require.True(t, ok)

			client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), func(cfg *DistributorConfig) {
				cfg.GRPCClientConfig.MaxSendMsgSize = maxSendMsgSize
				cfg.GRPCClientConfig.GRPCCompression = compressorName
			}, grpc.MaxRecvMsgSize(maxSendMsgSize))

			_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), req)
			require.NoError(t, err)

			requests := srv.allRequests()
			require.Len(t, requests, 4)
			for _, request := range requests {
				require.LessOrEqual(t, request.Size(), maxUncompressedSize)
			}
			require.Equal(t, expectedMetricNames, metricNamesFromWriteRequests(requests))
			requireRequestsPerWriteMetric(t, client, 1, 4)
		})
	}

	t.Run("push retries split requests independently", func(t *testing.T) {
		srv := &mockDistributorServer{
			errsByCall: map[int]error{2: status.Error(codes.Unavailable, "try again")},
		}
		req := newTestWriteRequestWithSeries(4)
		maxSize := maxWriteRequestSizeForOneSeries(req)
		client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), func(cfg *DistributorConfig) {
			cfg.GRPCClientConfig.MaxSendMsgSize = maxSize
		})

		_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), req)
		require.NoError(t, err)
		require.Equal(t, []string{"test_metric_0", "test_metric_1", "test_metric_1", "test_metric_2", "test_metric_3"}, metricNamesFromWriteRequests(srv.allRequests()))
		requireRequestsPerWriteMetric(t, client, 1, 4)
	})

	t.Run("push stops after split request failure", func(t *testing.T) {
		srv := &mockDistributorServer{
			errsByCall: map[int]error{2: status.Error(codes.ResourceExhausted, "limited")},
		}
		var logs bytes.Buffer
		req := newTestWriteRequestWithSeries(4)
		maxSize := maxWriteRequestSizeForOneSeries(req)
		client := setupDistributorGRPCClient(t, srv, log.NewLogfmtLogger(&logs), func(cfg *DistributorConfig) {
			cfg.GRPCClientConfig.MaxSendMsgSize = maxSize
		})

		_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), req)
		require.Equal(t, codes.ResourceExhausted, status.Code(err))
		require.Equal(t, []string{"test_metric_0", "test_metric_1"}, metricNamesFromWriteRequests(srv.allRequests()))
		require.Contains(t, logs.String(), "request=2 requests=4")
		requireRequestsPerWriteMetric(t, client, 1, 4)
	})

	t.Run("push reports an unsplittable request", func(t *testing.T) {
		srv := &mockDistributorServer{}
		client := setupDistributorGRPCClient(t, srv, log.NewNopLogger(), func(cfg *DistributorConfig) {
			cfg.GRPCClientConfig.MaxSendMsgSize = 1
		})

		_, err := client.Push(user.InjectOrgID(t.Context(), "test-user"), newTestWriteRequest())
		require.Equal(t, codes.ResourceExhausted, status.Code(err))
		require.Equal(t, 0, srv.calls())
		requireRequestsPerWriteMetric(t, client, 1, 1)
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
		require.Contains(t, logOutput, "request=1")
		require.Contains(t, logOutput, "requests=1")
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

func TestSplitWriteRequest(t *testing.T) {
	t.Run("does not split at the size limit", func(t *testing.T) {
		req := newTestWriteRequest()
		t.Cleanup(func() {
			req.FreeBuffer()
			mimirpb.ReuseSlice(req.Timeseries)
		})

		requests := splitWriteRequest(req, req.Size())
		require.Len(t, requests, 1)
		require.Same(t, req, requests[0])
	})

	t.Run("does not split with a non-positive limit", func(t *testing.T) {
		req := &mimirpb.WriteRequest{}

		for _, maxSize := range []int{0, -1} {
			requests := splitWriteRequest(req, maxSize)
			require.Len(t, requests, 1)
			require.Same(t, req, requests[0])
		}
	})

	t.Run("does not drop an unsupported oversized request", func(t *testing.T) {
		req := newTestWriteRequest()
		req.TimeseriesRW2 = []mimirpb.TimeSeriesRW2{{LabelsRefs: []uint32{1, 2}}}
		t.Cleanup(func() {
			req.FreeBuffer()
			mimirpb.ReuseSlice(req.Timeseries)
		})

		requests := splitWriteRequest(req, 1)
		require.Len(t, requests, 1)
		require.Same(t, req, requests[0])
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

	t.Run("rejects a custom compressor without a payload-size bound", func(t *testing.T) {
		var cfg DistributorConfig
		flagext.DefaultValues(&cfg)
		cfg.GRPCClientConfig.CustomCompressors = append(cfg.GRPCClientConfig.CustomCompressors, "future-compressor")
		cfg.GRPCClientConfig.GRPCCompression = "future-compressor"

		require.EqualError(t, cfg.Validate(), `ruler's distributor client gRPC settings: compression type "future-compressor" has no payload-size bound`)
	})

	t.Run("validate does not mutate custom compressors", func(t *testing.T) {
		var cfg DistributorConfig
		cfg.GRPCClientConfig.GRPCCompression = s2.Name

		require.EqualError(t, cfg.Validate(), `ruler's distributor client gRPC settings: unsupported compression type: "s2"`)
		require.Empty(t, cfg.GRPCClientConfig.CustomCompressors)
	})
}
