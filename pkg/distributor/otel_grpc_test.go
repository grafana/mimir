// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

const testOTLPMaxRequestSize = 10 << 20

// startOTLPGRPCTestServer serves the OTLP metrics service on a gRPC server, the way Mimir's main gRPC server does,
// and returns a client connection to it.
func startOTLPGRPCTestServer(t *testing.T, cfg Config, push PushFunc) *grpc.ClientConn {
	t.Helper()

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(middleware.ServerUserHeaderInterceptor))
	RegisterOTLPGRPCService(srv, cfg, validation.MockDefaultOverrides(), push, nil, log.NewNopLogger())

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		require.NoError(t, <-serveErr)
	})

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

// exportOTLPGRPC calls the export method, as an OTLP gRPC client does.
func exportOTLPGRPC(ctx context.Context, conn *grpc.ClientConn, req pmetricotlp.ExportRequest, opts ...grpc.CallOption) (pmetricotlp.ExportResponse, error) {
	resp := pmetricotlp.NewExportResponse()
	err := conn.Invoke(ctx, OTLPGRPCExportMethod, req, &resp, opts...)
	return resp, err
}

func testOTLPGRPCRequest(t *testing.T) pmetricotlp.ExportRequest {
	t.Helper()
	return TimeseriesToOTLPRequest([]prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "__name__", Value: "foo"}, {Name: "bar", Value: "baz"}},
		Samples: []prompb.Sample{{Timestamp: time.Now().UnixMilli(), Value: 1}},
	}}, nil)
}

func tenantContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "X-Scope-OrgID", "tenant-1")
}

func TestOTLPGRPCService_Export(t *testing.T) {
	tests := map[string]struct {
		withoutTenant bool
		pushErr       error
		expectedCode  codes.Code
	}{
		"successful push": {
			expectedCode: codes.OK,
		},
		"missing tenant is rejected like other gRPC services": {
			withoutTenant: true,
			expectedCode:  codes.Unknown,
		},
		"non-elected HA replica is reported as success": {
			pushErr:      newReplicasDidNotMatchError("a", "b"),
			expectedCode: codes.OK,
		},
		"unknown error maps to unavailable": {
			pushErr:      errors.New("unexpected"),
			expectedCode: codes.Unavailable,
		},
		"deadline exceeded maps to unavailable": {
			pushErr:      context.DeadlineExceeded,
			expectedCode: codes.Unavailable,
		},
		"TSDB unavailable maps to unavailable": {
			pushErr:      newIngesterPushError(createStatusWithDetails(t, codes.Internal, "tsdb unavailable", mimirpb.ERROR_CAUSE_TSDB_UNAVAILABLE), "ingester-1"),
			expectedCode: codes.Unavailable,
		},
		"HTTP 400 maps to invalid argument": {
			pushErr:      httpgrpc.Error(400, "bad data"),
			expectedCode: codes.InvalidArgument,
		},
		"HTTP 413 maps to invalid argument": {
			pushErr:      httpgrpc.Error(413, "too large"),
			expectedCode: codes.InvalidArgument,
		},
		"HTTP 429 maps to resource exhausted": {
			pushErr:      httpgrpc.Error(429, "slow down"),
			expectedCode: codes.ResourceExhausted,
		},
		"HTTP 500 maps to unavailable": {
			pushErr:      httpgrpc.Error(500, "oops"),
			expectedCode: codes.Unavailable,
		},
		"HTTP 501 maps to unimplemented": {
			pushErr:      httpgrpc.Error(501, "not implemented"),
			expectedCode: codes.Unimplemented,
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
			conn := startOTLPGRPCTestServer(t, Config{MaxOTLPRequestSize: testOTLPMaxRequestSize}, push)

			ctx := t.Context()
			if !tc.withoutTenant {
				ctx = tenantContext(ctx)
			}
			_, err := exportOTLPGRPC(ctx, conn, testOTLPGRPCRequest(t))

			require.Equal(t, tc.expectedCode, status.Code(err), "error: %v", err)
			require.True(t, pushed != tc.withoutTenant, "push called: %t", pushed)
			if tc.expectedCode == codes.OK && !tc.withoutTenant && tc.pushErr == nil {
				require.Equal(t, "tenant-1", pushedTenant)
				require.Len(t, pushedSeries, 1)
				require.Equal(t, "foo", pushedSeries[0].Labels[0].Value)
				require.Equal(t, float64(1), pushedSeries[0].Samples[0].Value)
			}
		})
	}
}

func TestOTLPGRPCService_GzipCompressedRequest(t *testing.T) {
	pushed := false
	conn := startOTLPGRPCTestServer(t, Config{MaxOTLPRequestSize: testOTLPMaxRequestSize}, func(_ context.Context, pushReq *Request) error {
		pushed = true
		_, err := pushReq.WriteRequest()
		require.NoError(t, err)
		pushReq.CleanUp()
		return nil
	})

	_, err := exportOTLPGRPC(tenantContext(t.Context()), conn, testOTLPGRPCRequest(t), grpc.UseCompressor(gzip.Name))
	require.NoError(t, err)
	require.True(t, pushed)
}

func TestOTLPGRPCService_RecoversFromPanic(t *testing.T) {
	shouldPanic := true
	conn := startOTLPGRPCTestServer(t, Config{MaxOTLPRequestSize: testOTLPMaxRequestSize}, func(_ context.Context, pushReq *Request) error {
		pushReq.CleanUp()
		if shouldPanic {
			panic("boom")
		}
		return nil
	})
	ctx := tenantContext(t.Context())

	_, err := exportOTLPGRPC(ctx, conn, testOTLPGRPCRequest(t))
	require.Equal(t, codes.Internal, status.Code(err), "error: %v", err)
	require.Contains(t, status.Convert(err).Message(), "boom")

	// The server keeps serving requests after recovering.
	shouldPanic = false
	_, err = exportOTLPGRPC(ctx, conn, testOTLPGRPCRequest(t))
	require.NoError(t, err)
}

func TestOTLPGRPCService_RetryInfo(t *testing.T) {
	const delay = 3 * time.Second
	retryCfg := RetryConfig{Enabled: true, MinBackoff: delay, MaxBackoff: delay}

	tests := map[string]struct {
		pushErr       error
		retryCfg      RetryConfig
		expectedCode  codes.Code
		expectedDelay time.Duration // 0 means no RetryInfo expected.
	}{
		"resource exhausted includes retry info": {
			pushErr:       httpgrpc.Error(429, "slow down"),
			retryCfg:      retryCfg,
			expectedCode:  codes.ResourceExhausted,
			expectedDelay: delay,
		},
		"unavailable includes retry info": {
			pushErr:       httpgrpc.Error(503, "unavailable"),
			retryCfg:      retryCfg,
			expectedCode:  codes.Unavailable,
			expectedDelay: delay,
		},
		"retry info is omitted when retry-after header is disabled": {
			pushErr:      httpgrpc.Error(429, "slow down"),
			retryCfg:     RetryConfig{Enabled: false, MinBackoff: delay, MaxBackoff: delay},
			expectedCode: codes.ResourceExhausted,
		},
		"retry info is omitted for non-retryable errors": {
			pushErr:      httpgrpc.Error(400, "bad data"),
			retryCfg:     retryCfg,
			expectedCode: codes.InvalidArgument,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			conn := startOTLPGRPCTestServer(t, Config{MaxOTLPRequestSize: testOTLPMaxRequestSize, RetryConfig: tc.retryCfg}, func(_ context.Context, pushReq *Request) error {
				_, err := pushReq.WriteRequest()
				require.NoError(t, err)
				pushReq.CleanUp()
				return tc.pushErr
			})

			_, err := exportOTLPGRPC(tenantContext(t.Context()), conn, testOTLPGRPCRequest(t))
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
		})
	}
}

func TestOTLPGRPCService_RequestSizeLimit(t *testing.T) {
	pushed := false
	req := testOTLPGRPCRequest(t)
	payload, err := req.MarshalProto()
	require.NoError(t, err)

	conn := startOTLPGRPCTestServer(t, Config{MaxOTLPRequestSize: len(payload) - 1}, func(_ context.Context, pushReq *Request) error {
		pushed = true
		pushReq.CleanUp()
		return nil
	})

	_, err = exportOTLPGRPC(tenantContext(t.Context()), conn, req)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "error: %v", err)
	require.Contains(t, status.Convert(err).Message(), "larger than the allowed limit")
	require.False(t, pushed)
}

func TestOTLPGRPCService_AccountsDecodedSize(t *testing.T) {
	req := testOTLPGRPCRequest(t)
	payload, err := req.MarshalProto()
	require.NoError(t, err)

	var accounted int64
	var decoded bool
	conn := startOTLPGRPCTestServer(t, Config{MaxOTLPRequestSize: testOTLPMaxRequestSize}, func(_ context.Context, pushReq *Request) error {
		accounted = pushReq.contentLength
		decoded = pushReq.contentLengthDecoded
		_, err := pushReq.WriteRequest()
		require.NoError(t, err)
		pushReq.CleanUp()
		return nil
	})

	_, err = exportOTLPGRPC(tenantContext(t.Context()), conn, req)
	require.NoError(t, err)
	// The size of the decoded message is accounted, without the decompression estimate applied to compressed bodies.
	require.Equal(t, int64(len(payload)), accounted)
	require.True(t, decoded)
}

func TestHTTPStatusToOTLPGRPCCode(t *testing.T) {
	require.Equal(t, codes.InvalidArgument, httpStatusToOTLPGRPCCode(400))
	require.Equal(t, codes.ResourceExhausted, httpStatusToOTLPGRPCCode(429))
	require.Equal(t, codes.Unimplemented, httpStatusToOTLPGRPCCode(501))
	require.Equal(t, codes.Unavailable, httpStatusToOTLPGRPCCode(503))
}

func TestOTLPGRPCService_SoftErrorIsPartialSuccess(t *testing.T) {
	conn := startOTLPGRPCTestServer(t, Config{MaxOTLPRequestSize: testOTLPMaxRequestSize}, func(_ context.Context, pushReq *Request) error {
		_, err := pushReq.WriteRequest()
		require.NoError(t, err)
		pushReq.CleanUp()
		return newActiveSeriesLimitedError(10, 3, http.StatusTooManyRequests, 5)
	})

	resp, err := exportOTLPGRPC(tenantContext(t.Context()), conn, testOTLPGRPCRequest(t))
	require.NoError(t, err)
	require.Equal(t, int64(5), resp.PartialSuccess().RejectedDataPoints())
	require.NotEmpty(t, resp.PartialSuccess().ErrorMessage())
}

// TestOTLPGRPCService_ConversionErrorIsClientError checks that a request which can't be converted is reported as a
// client error, like the HTTP receiver does, so that clients don't retry it.
func TestOTLPGRPCService_ConversionErrorIsClientError(t *testing.T) {
	// A metric without a name can't be converted, so all the metrics of the request are dropped.
	req := pmetricotlp.NewExportRequest()
	metric := req.Metrics().ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1)

	pushed := false
	conn := startOTLPGRPCTestServer(t, Config{MaxOTLPRequestSize: testOTLPMaxRequestSize}, func(_ context.Context, pushReq *Request) error {
		pushed = true
		_, err := pushReq.WriteRequest()
		pushReq.CleanUp()
		return err
	})

	_, err := exportOTLPGRPC(tenantContext(t.Context()), conn, req)
	require.True(t, pushed)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "error: %v", err)
}
