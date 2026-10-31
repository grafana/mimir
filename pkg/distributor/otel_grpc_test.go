// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"go.uber.org/atomic"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestOTLPGRPCServer_Export(t *testing.T) {
	series := []prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "__name__", Value: "foo"}, {Name: "bar", Value: "baz"}},
		Samples: []prompb.Sample{{Timestamp: time.Now().UnixMilli(), Value: 1}},
	}}
	exportReq := TimeseriesToOTLPRequest(series, nil)
	payload, err := exportReq.MarshalProto()
	require.NoError(t, err)
	var grpcReq colmetricpb.ExportMetricsServiceRequest
	require.NoError(t, proto.Unmarshal(payload, &grpcReq))

	tests := map[string]struct {
		tenant       string
		pushErr      error
		expectedCode codes.Code
		expectedPush bool
	}{
		"successful push": {
			tenant:       "tenant-1",
			expectedCode: codes.OK,
			expectedPush: true,
		},
		"missing tenant is rejected": {
			expectedCode: codes.Unauthenticated,
		},
		"non-elected HA replica is reported as success": {
			tenant:       "tenant-1",
			pushErr:      newReplicasDidNotMatchError("a", "b"),
			expectedCode: codes.OK,
			expectedPush: true,
		},
		"unknown error maps to unavailable": {
			tenant:       "tenant-1",
			pushErr:      errors.New("unexpected"),
			expectedCode: codes.Unavailable,
			expectedPush: true,
		},
		"deadline exceeded maps to unavailable": {
			tenant:       "tenant-1",
			pushErr:      context.DeadlineExceeded,
			expectedCode: codes.Unavailable,
			expectedPush: true,
		},
		"TSDB unavailable maps to unavailable": {
			tenant:       "tenant-1",
			pushErr:      newIngesterPushError(createStatusWithDetails(t, codes.Internal, "tsdb unavailable", mimirpb.ERROR_CAUSE_TSDB_UNAVAILABLE), "ingester-1"),
			expectedCode: codes.Unavailable,
			expectedPush: true,
		},
		"HTTP 400 maps to invalid argument": {
			tenant:       "tenant-1",
			pushErr:      httpgrpc.Error(400, "bad data"),
			expectedCode: codes.InvalidArgument,
			expectedPush: true,
		},
		"HTTP 429 maps to resource exhausted": {
			tenant:       "tenant-1",
			pushErr:      httpgrpc.Error(429, "slow down"),
			expectedCode: codes.ResourceExhausted,
			expectedPush: true,
		},
		"HTTP 500 maps to unavailable": {
			tenant:       "tenant-1",
			pushErr:      httpgrpc.Error(500, "oops"),
			expectedCode: codes.Unavailable,
			expectedPush: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var pushedTenant string
			var pushedSeries []mimirpb.PreallocTimeseries
			pushed := false
			push := func(ctx context.Context, pushReq *Request) error {
				pushed = true
				wr, err := pushReq.WriteRequest()
				if err != nil {
					return err
				}
				pushedTenant, _ = tenant.TenantID(ctx)
				for _, ts := range wr.Timeseries {
					pushedSeries = append(pushedSeries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
						Labels:  mimirpb.FromLabelsToLabelAdapters(mimirpb.FromLabelAdaptersToLabelsWithCopy(ts.Labels)),
						Samples: append([]mimirpb.Sample(nil), ts.Samples...),
					}})
				}
				pushReq.CleanUp()
				return tc.pushErr
			}

			srv := NewOTLPGRPCServer(
				OTLPGRPCConfig{ListenAddress: "127.0.0.1"}, 10<<20, middleware.ServerUserHeaderInterceptor, nil,
				validation.MockDefaultOverrides(), validation.MockDefaultOverrides(), nil, nil, nil, RetryConfig{}, push, nil, log.NewNopLogger(),
			)
			require.NoError(t, services.StartAndAwaitRunning(t.Context(), srv))
			t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.WithoutCancel(t.Context()), srv)) })

			conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })

			ctx := t.Context()
			if tc.tenant != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, "X-Scope-OrgID", tc.tenant)
			}
			_, err = colmetricpb.NewMetricsServiceClient(conn).Export(ctx, &grpcReq)

			require.Equal(t, tc.expectedCode, status.Code(err), "error: %v", err)
			require.Equal(t, tc.expectedPush, pushed)
			if tc.expectedCode == codes.OK {
				require.Equal(t, tc.tenant, pushedTenant)
				require.Len(t, pushedSeries, 1)
				require.Equal(t, "foo", pushedSeries[0].Labels[0].Value)
				require.Equal(t, float64(1), pushedSeries[0].Samples[0].Value)
			}
		})
	}
}

func TestHTTPStatusToOTLPGRPCCode(t *testing.T) {
	require.Equal(t, codes.InvalidArgument, httpStatusToOTLPGRPCCode(400))
	require.Equal(t, codes.ResourceExhausted, httpStatusToOTLPGRPCCode(429))
	require.Equal(t, codes.Unimplemented, httpStatusToOTLPGRPCCode(501))
	require.Equal(t, codes.Unavailable, httpStatusToOTLPGRPCCode(503))
}

func TestOTLPGRPCServer_RetryInfoAndMetrics(t *testing.T) {
	exportReq := TimeseriesToOTLPRequest([]prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "__name__", Value: "foo"}},
		Samples: []prompb.Sample{{Timestamp: time.Now().UnixMilli(), Value: 1}},
	}}, nil)
	payload, err := exportReq.MarshalProto()
	require.NoError(t, err)
	var grpcReq colmetricpb.ExportMetricsServiceRequest
	require.NoError(t, proto.Unmarshal(payload, &grpcReq))

	tests := map[string]struct {
		pushErr       error
		retryCfg      RetryConfig
		retryInfoOff  bool // Disables retry info for the tenant.
		expectedCode  codes.Code
		expectedDelay time.Duration // 0 means no RetryInfo expected.
	}{
		"resource exhausted includes retry info": {
			pushErr:       httpgrpc.Error(429, "slow down"),
			retryCfg:      RetryConfig{Enabled: true, MinBackoff: 6 * time.Second, MaxBackoff: 6 * time.Second},
			expectedCode:  codes.ResourceExhausted,
			expectedDelay: 6 * time.Second,
		},
		"unavailable includes retry info": {
			pushErr:       httpgrpc.Error(503, "unavailable"),
			retryCfg:      RetryConfig{Enabled: true, MinBackoff: 6 * time.Second, MaxBackoff: 6 * time.Second},
			expectedCode:  codes.Unavailable,
			expectedDelay: 6 * time.Second,
		},
		"retry info is omitted when disabled": {
			pushErr:      httpgrpc.Error(429, "slow down"),
			expectedCode: codes.ResourceExhausted,
		},
		"retry info is omitted when disabled for the tenant": {
			pushErr:      httpgrpc.Error(429, "slow down"),
			retryCfg:     RetryConfig{Enabled: true, MinBackoff: 6 * time.Second, MaxBackoff: 6 * time.Second},
			retryInfoOff: true,
			expectedCode: codes.ResourceExhausted,
		},
		"retry info is omitted for non-retryable errors": {
			pushErr:      httpgrpc.Error(400, "bad data"),
			retryCfg:     RetryConfig{Enabled: true, MinBackoff: 6 * time.Second, MaxBackoff: 6 * time.Second},
			expectedCode: codes.InvalidArgument,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			push := func(_ context.Context, pushReq *Request) error {
				_, err := pushReq.WriteRequest()
				require.NoError(t, err)
				pushReq.CleanUp()
				return tc.pushErr
			}
			limits := validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
				defaults.OTLPGRPCRetryInfoEnabled = !tc.retryInfoOff
			})
			srv := NewOTLPGRPCServer(
				OTLPGRPCConfig{ListenAddress: "127.0.0.1", ListenPort: 0}, 10<<20, middleware.ServerUserHeaderInterceptor, nil,
				limits, limits, nil, nil, nil, tc.retryCfg, push, newPushMetrics(reg), log.NewNopLogger(),
			)
			require.NoError(t, services.StartAndAwaitRunning(t.Context(), srv))
			t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.WithoutCancel(t.Context()), srv)) })

			conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })

			ctx := metadata.AppendToOutgoingContext(t.Context(), "X-Scope-OrgID", "tenant-1")
			_, err = colmetricpb.NewMetricsServiceClient(conn).Export(ctx, &grpcReq)
			require.Equal(t, tc.expectedCode, status.Code(err), "error: %v", err)

			var retryInfo *errdetails.RetryInfo
			for _, detail := range status.Convert(err).Details() {
				if ri, ok := detail.(*errdetails.RetryInfo); ok {
					retryInfo = ri
				}
			}
			if tc.expectedDelay == 0 {
				require.Nil(t, retryInfo)
			} else {
				require.NotNil(t, retryInfo)
				require.Equal(t, tc.expectedDelay, retryInfo.RetryDelay.AsDuration())
			}

			count, err := testutil.GatherAndCount(reg, "cortex_distributor_otlp_grpc_request_duration_seconds")
			require.NoError(t, err)
			require.Equal(t, 1, count)

			// The uncompressed size is tracked, but the compression ratio isn't since the wire size is unknown.
			count, err = testutil.GatherAndCount(reg, "cortex_distributor_uncompressed_request_body_size_bytes")
			require.NoError(t, err)
			require.Equal(t, 1, count)
			count, err = testutil.GatherAndCount(reg, "cortex_distributor_request_body_compression_ratio")
			require.NoError(t, err)
			require.Equal(t, 0, count)
		})
	}
}

func TestOTLPGRPCConfig_Validate(t *testing.T) {
	require.NoError(t, (&OTLPGRPCConfig{}).Validate())
	require.NoError(t, (&OTLPGRPCConfig{TLSCertPath: "c", TLSKeyPath: "k", TLSClientCAPath: "ca"}).Validate())
	require.EqualError(t, (&OTLPGRPCConfig{TLSCertPath: "c"}).Validate(), "the OTLP gRPC receiver TLS certificate and key must be set together")
	require.EqualError(t, (&OTLPGRPCConfig{TLSKeyPath: "k"}).Validate(), "the OTLP gRPC receiver TLS certificate and key must be set together")
	require.EqualError(t, (&OTLPGRPCConfig{TLSClientCAPath: "ca"}).Validate(), "the OTLP gRPC receiver TLS client CA requires the TLS certificate and key to be set")
}

func TestOTLPGRPCServer_TLS(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath, certPEM := writeSelfSignedCert(t, dir)

	srv := NewOTLPGRPCServer(
		OTLPGRPCConfig{ListenAddress: "127.0.0.1", TLSCertPath: certPath, TLSKeyPath: keyPath}, 10<<20, middleware.ServerUserHeaderInterceptor, nil,
		validation.MockDefaultOverrides(), validation.MockDefaultOverrides(), nil, nil, nil, RetryConfig{},
		func(_ context.Context, pushReq *Request) error { pushReq.CleanUp(); return nil },
		nil, log.NewNopLogger(),
	)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), srv))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.WithoutCancel(t.Context()), srv)) })

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(certPEM))
	conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, "localhost")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx := metadata.AppendToOutgoingContext(t.Context(), "X-Scope-OrgID", "tenant-1")
	_, err = colmetricpb.NewMetricsServiceClient(conn).Export(ctx, &colmetricpb.ExportMetricsServiceRequest{})
	require.NoError(t, err)

	// A plaintext client can't talk to a TLS server.
	plain, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, plain.Close()) })
	_, err = colmetricpb.NewMetricsServiceClient(plain).Export(ctx, &colmetricpb.ExportMetricsServiceRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func writeSelfSignedCert(t *testing.T, dir string) (certPath, keyPath string, certPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
	return certPath, keyPath, certPEM
}

func TestOTLPGRPCServer_ShutdownDoesNotHangOnOpenConnection(t *testing.T) {
	srv := NewOTLPGRPCServer(
		OTLPGRPCConfig{ListenAddress: "127.0.0.1"}, 10<<20, middleware.ServerUserHeaderInterceptor, nil,
		validation.MockDefaultOverrides(), validation.MockDefaultOverrides(), nil, nil, nil, RetryConfig{},
		func(_ context.Context, pushReq *Request) error { pushReq.CleanUp(); return nil },
		nil, log.NewNopLogger(),
	)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), srv))

	conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx := metadata.AppendToOutgoingContext(t.Context(), "X-Scope-OrgID", "tenant-1")
	_, err = colmetricpb.NewMetricsServiceClient(conn).Export(ctx, &colmetricpb.ExportMetricsServiceRequest{})
	require.NoError(t, err)

	// An idle client connection doesn't prevent a graceful stop from completing.
	require.NoError(t, services.StopAndAwaitTerminated(t.Context(), srv))
}

func TestOTLPGRPCServer_StoppingWithoutRunningReleasesListener(t *testing.T) {
	srv := NewOTLPGRPCServer(
		OTLPGRPCConfig{ListenAddress: "127.0.0.1"}, 10<<20, middleware.ServerUserHeaderInterceptor, nil,
		validation.MockDefaultOverrides(), validation.MockDefaultOverrides(), nil, nil, nil, RetryConfig{},
		func(_ context.Context, pushReq *Request) error { pushReq.CleanUp(); return nil },
		nil, log.NewNopLogger(),
	)

	// dskit skips running and calls stopping directly when the service is stopped while still Starting.
	require.NoError(t, srv.starting(t.Context()))
	addr := srv.Addr().String()
	require.NoError(t, srv.stopping(nil))

	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, lis.Close())
}

func TestPushMetrics_DeleteUserMetricsRemovesOTLPSeries(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newPushMetrics(reg)
	m.AddOTLPParseErrorDiscardedSamples("user-1", 2)
	m.AddOTLPParseErrorDiscardedSamples("user-2", 3)
	m.ObserveOTLPGRPCRequestDuration("user-1", "OK", time.Second)
	m.ObserveOTLPGRPCRequestDuration("user-2", "OK", time.Second)

	m.deleteUserMetrics("user-1")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_discarded_samples_total The total number of samples that were discarded.
		# TYPE cortex_discarded_samples_total counter
		cortex_discarded_samples_total{group="",reason="otlp_parse_error",user="user-2"} 3
	`), "cortex_discarded_samples_total"))

	families, err := reg.Gather()
	require.NoError(t, err)
	var users []string
	for _, mf := range families {
		if mf.GetName() != "cortex_distributor_otlp_grpc_request_duration_seconds" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, l := range metric.GetLabel() {
				if l.GetName() == "user" {
					users = append(users, l.GetValue())
				}
			}
		}
	}
	require.Equal(t, []string{"user-2"}, users)
}

func TestOTLPGRPCServer_RecoversFromPanic(t *testing.T) {
	shouldPanic := true
	srv := NewOTLPGRPCServer(
		OTLPGRPCConfig{ListenAddress: "127.0.0.1"}, 10<<20, middleware.ServerUserHeaderInterceptor, nil,
		validation.MockDefaultOverrides(), validation.MockDefaultOverrides(), nil, nil, nil, RetryConfig{},
		func(_ context.Context, pushReq *Request) error {
			pushReq.CleanUp()
			if shouldPanic {
				panic("boom")
			}
			return nil
		},
		nil, log.NewNopLogger(),
	)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), srv))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.WithoutCancel(t.Context()), srv)) })

	conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := colmetricpb.NewMetricsServiceClient(conn)
	ctx := metadata.AppendToOutgoingContext(t.Context(), "X-Scope-OrgID", "tenant-1")

	_, err = client.Export(ctx, &colmetricpb.ExportMetricsServiceRequest{})
	require.Equal(t, codes.Internal, status.Code(err), "error: %v", err)
	require.Contains(t, status.Convert(err).Message(), "boom")

	// The server keeps serving requests after recovering.
	shouldPanic = false
	_, err = client.Export(ctx, &colmetricpb.ExportMetricsServiceRequest{})
	require.NoError(t, err)
}

type fakeInflightPushRequestLimiter struct {
	startErr error
	started  atomic.Int64
	finished atomic.Int64
}

func (f *fakeInflightPushRequestLimiter) StartPushRequest(ctx context.Context, _ int64) (context.Context, error) {
	if f.startErr != nil {
		return ctx, f.startErr
	}
	f.started.Inc()
	return ctx, nil
}

func (f *fakeInflightPushRequestLimiter) FinishPushRequest(context.Context) {
	f.finished.Inc()
}

func TestOTLPGRPCServer_InflightLimiter(t *testing.T) {
	exportReq := TimeseriesToOTLPRequest([]prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "__name__", Value: "foo"}},
		Samples: []prompb.Sample{{Timestamp: time.Now().UnixMilli(), Value: 1}},
	}}, nil)
	payload, err := exportReq.MarshalProto()
	require.NoError(t, err)
	var grpcReq colmetricpb.ExportMetricsServiceRequest
	require.NoError(t, proto.Unmarshal(payload, &grpcReq))

	const maxRecvMsgSize = 1 << 10
	tooLargeReq := &colmetricpb.ExportMetricsServiceRequest{ResourceMetrics: []*metricpb.ResourceMetrics{{
		SchemaUrl: strings.Repeat("x", 2*maxRecvMsgSize),
	}}}

	tests := map[string]struct {
		startErr         error
		method           string
		req              *colmetricpb.ExportMetricsServiceRequest
		expectedCode     codes.Code
		expectedPush     bool
		expectedStarted  int64
		expectedFinished int64
	}{
		"request exceeding the limits is rejected before being read": {
			startErr:         errMaxInflightRequestsReached,
			req:              &grpcReq,
			expectedCode:     codes.Unavailable,
			expectedPush:     false,
			expectedStarted:  0,
			expectedFinished: 0,
		},
		"accepted request is released once processed": {
			req:              &grpcReq,
			expectedCode:     codes.OK,
			expectedPush:     true,
			expectedStarted:  1,
			expectedFinished: 1,
		},
		"accepted request rejected by gRPC as too large is released": {
			req:              tooLargeReq,
			expectedCode:     codes.ResourceExhausted,
			expectedPush:     false,
			expectedStarted:  1,
			expectedFinished: 1,
		},
		"other methods are not accounted": {
			method:           "/opentelemetry.proto.collector.metrics.v1.MetricsService/Unknown",
			req:              &grpcReq,
			expectedCode:     codes.Unimplemented,
			expectedPush:     false,
			expectedStarted:  0,
			expectedFinished: 0,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pushed := atomic.NewBool(false)
			push := func(_ context.Context, pushReq *Request) error {
				pushed.Store(true)
				_, err := pushReq.WriteRequest()
				require.NoError(t, err)
				pushReq.CleanUp()
				return nil
			}
			limiter := &fakeInflightPushRequestLimiter{startErr: tc.startErr}

			srv := NewOTLPGRPCServer(
				OTLPGRPCConfig{ListenAddress: "127.0.0.1"}, maxRecvMsgSize, middleware.ServerUserHeaderInterceptor, limiter,
				validation.MockDefaultOverrides(), validation.MockDefaultOverrides(), nil, nil, nil, RetryConfig{}, push, nil, log.NewNopLogger(),
			)
			require.NoError(t, services.StartAndAwaitRunning(t.Context(), srv))
			t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.WithoutCancel(t.Context()), srv)) })

			conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })

			ctx := metadata.AppendToOutgoingContext(t.Context(), "X-Scope-OrgID", "tenant-1")
			if tc.method != "" {
				err = conn.Invoke(ctx, tc.method, tc.req, &colmetricpb.ExportMetricsServiceResponse{})
			} else {
				_, err = colmetricpb.NewMetricsServiceClient(conn).Export(ctx, tc.req)
			}
			require.Equal(t, tc.expectedCode, status.Code(err), "error: %v", err)
			if tc.startErr != nil {
				require.ErrorContains(t, err, tc.startErr.Error())
			}
			require.Equal(t, tc.expectedPush, pushed.Load())
			require.Equal(t, tc.expectedStarted, limiter.started.Load())

			// The request is released by the stats handler, which may run after the response has been sent.
			require.Eventually(t, func() bool { return limiter.finished.Load() == tc.expectedFinished }, time.Second, 10*time.Millisecond)
			require.Equal(t, tc.expectedFinished, limiter.finished.Load())
		})
	}
}
