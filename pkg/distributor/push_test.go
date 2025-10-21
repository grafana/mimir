// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/push/push_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"

	"slices"

	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	"github.com/golang/snappy"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/httpgrpc/server"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	ring_client "github.com/grafana/dskit/ring/client"
	dskit_server "github.com/grafana/dskit/server"
	"github.com/grafana/dskit/services"
	dskit_test "github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/common/model"
	"github.com/prometheus/otlptranslator"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestOTelMetricsToMetadata(t *testing.T) {
	otelMetrics := pmetric.NewMetrics()
	rs := otelMetrics.ResourceMetrics().AppendEmpty()
	metrics := rs.ScopeMetrics().AppendEmpty().Metrics()

	metricOne := metrics.AppendEmpty()
	metricOne.SetName("name")
	metricOne.SetUnit("Count")
	gaugeMetricOne := metricOne.SetEmptyGauge()
	gaugeDatapoint := gaugeMetricOne.DataPoints().AppendEmpty()
	gaugeDatapoint.Attributes().PutStr("label1", "value1")

	metricTwo := metrics.AppendEmpty()
	metricTwo.SetName("test")
	metricTwo.SetUnit("Count")
	gaugeMetricTwo := metricTwo.SetEmptyGauge()
	gaugeDatapointTwo := gaugeMetricTwo.DataPoints().AppendEmpty()
	gaugeDatapointTwo.Attributes().PutStr("label1", "value2")

	testCases := []struct {
		name           string
		enableSuffixes bool
	}{
		{
			name:           "OTel metric suffixes enabled",
			enableSuffixes: true,
		},
		{
			name:           "OTel metric suffixes disabled",
			enableSuffixes: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			countSfx := ""
			if tc.enableSuffixes {
				countSfx = "_Count"
			}
			sampleMetadata := []*mimirpb.MetricMetadata{
				{
					Help:             "",
					Unit:             "Count",
					Type:             mimirpb.GAUGE,
					MetricFamilyName: "name" + countSfx,
				},
				{
					Help:             "",
					Unit:             "Count",
					Type:             mimirpb.GAUGE,
					MetricFamilyName: "test" + countSfx,
				},
			}
			converter := newOTLPMimirConverter(nil)
			_, res, _, err := otelMetricsToSeriesAndMetadata(context.Background(), converter, otelMetrics, conversionOptions{
				addSuffixes: tc.enableSuffixes,
			}, log.NewNopLogger())
			require.NoError(t, err)
			assert.Equal(t, sampleMetadata, res)
		})
	}
}

func verifyWritePushFunc(t *testing.T, expectSource mimirpb.WriteRequest_SourceEnum) PushFunc {
	t.Helper()
	return func(_ context.Context, pushReq *Request) error {
		request, err := pushReq.WriteRequest()
		require.NoError(t, err)
		t.Cleanup(pushReq.CleanUp)
		require.Len(t, request.Timeseries, 1)
		require.Equal(t, "__name__", request.Timeseries[0].Labels[0].Name)
		require.Equal(t, "foo", request.Timeseries[0].Labels[0].Value)
		require.Equal(t, expectSource, request.Source)
		require.False(t, request.SkipLabelValidation)
		return nil
	}
}

func readBodyPushFunc(t *testing.T) PushFunc {
	t.Helper()
	return func(_ context.Context, req *Request) error {
		_, err := req.WriteRequest()
		return err
	}
}

func createRequest(t testing.TB, protobuf []byte) *http.Request {
	t.Helper()
	inoutBytes := snappy.Encode(nil, protobuf)
	req, err := http.NewRequest("POST", "http://localhost/", bytes.NewReader(inoutBytes))
	require.NoError(t, err)
	req.Header.Add("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	const tenantID = "test"
	req.Header.Set("X-Scope-OrgID", tenantID)
	ctx := user.InjectOrgID(context.Background(), tenantID)
	req = req.WithContext(ctx)

	return req
}

func createMalformedRW1Request(t testing.TB, protobuf []byte) *http.Request {
	t.Helper()
	inoutBytes := snappy.Encode(nil, protobuf)
	req, err := http.NewRequest("POST", "http://localhost/", bytes.NewReader(inoutBytes))
	require.NoError(t, err)
	req.Header.Add("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "text/plain")

	const tenantID = "test"
	req.Header.Set("X-Scope-OrgID", tenantID)
	ctx := user.InjectOrgID(context.Background(), tenantID)
	req = req.WithContext(ctx)

	return req
}

func createPrometheusRemoteWriteProtobuf(t testing.TB, timeseries ...prompb.TimeSeries) []byte {
	t.Helper()

	if len(timeseries) == 0 {
		timeseries = []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "foo"},
				},
				Samples: []prompb.Sample{
					{Value: 1, Timestamp: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
				},
				Histograms: []prompb.Histogram{
					prompb.FromIntHistogram(1337, test.GenerateTestHistogram(1))},
			},
		}
	}

	input := prompb.WriteRequest{
		Timeseries: timeseries,
	}
	inputBytes, err := input.Marshal()
	require.NoError(t, err)
	return inputBytes
}

func createMimirWriteRequestProtobuf(t testing.TB, skipLabelNameValidation, skipLabelCountValidation bool, timeseries ...mimirpb.PreallocTimeseries) []byte {
	t.Helper()
	h := prompb.FromIntHistogram(1337, test.GenerateTestHistogram(1))

	if len(timeseries) == 0 {
		timeseries = []mimirpb.PreallocTimeseries{{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{
					{Name: "__name__", Value: "foo"},
				},
				Samples: []mimirpb.Sample{
					{Value: 1, TimestampMs: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
				},
				Histograms: []mimirpb.Histogram{promToMimirHistogram(&h)},
			},
		}}
	}
	input := mimirpb.WriteRequest{
		Timeseries:               timeseries,
		Source:                   mimirpb.RULE,
		SkipLabelValidation:      skipLabelNameValidation,
		SkipLabelCountValidation: skipLabelCountValidation,
	}
	inoutBytes, err := input.Marshal()
	require.NoError(t, err)
	return inoutBytes
}

func createMimirWriteRequestProtobufWithNonSupportedLabelNames(t *testing.T, skipLabelNameValidation bool) []byte {
	t.Helper()
	ts := mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{
				{Name: "a-label", Value: "value"}, // a-label does not comply with regex [a-zA-Z_:][a-zA-Z0-9_:]*
			},
			Samples: []mimirpb.Sample{
				{Value: 1, TimestampMs: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
			},
		},
	}
	input := mimirpb.WriteRequest{
		Timeseries:          []mimirpb.PreallocTimeseries{ts},
		Source:              mimirpb.RULE,
		SkipLabelValidation: skipLabelNameValidation,
	}
	inoutBytes, err := input.Marshal()
	require.NoError(t, err)
	return inoutBytes
}

func TestNewDistributorMaxWriteMessageSizeErr(t *testing.T) {
	err := distributorMaxWriteMessageSizeErr{actual: 100, limit: 50}
	msg := `the incoming push request has been rejected because its message size of 100 bytes is larger than the allowed limit of 50 bytes (err-mimir-distributor-max-write-message-size). To adjust the related limit, configure -distributor.max-recv-msg-size, or contact your service administrator.`

	assert.Equal(t, msg, err.Error())
}

func TestRetryConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		cfg         RetryConfig
		expectedErr error
	}{
		"should pass with default config": {
			cfg: func() RetryConfig {
				cfg := RetryConfig{}
				flagext.DefaultValues(&cfg)
				return cfg
			}(),
			expectedErr: nil,
		},
		"should pass with min and max backoff equal to 1s": {
			cfg: RetryConfig{
				MinBackoff: 1 * time.Second,
				MaxBackoff: 1 * time.Second,
			},
			expectedErr: nil,
		},
		"should fail if min backoff is 0": {
			cfg: RetryConfig{
				MinBackoff: 0,
				MaxBackoff: 3 * time.Second,
			},
			expectedErr: errNonPositiveMinBackoffDuration,
		},
		"should fail if min backoff is negative": {
			cfg: RetryConfig{
				MinBackoff: -1 * time.Second,
				MaxBackoff: 5 * time.Second,
			},
			expectedErr: errNonPositiveMinBackoffDuration,
		},
		"should fail if max backoff is 0": {
			cfg: RetryConfig{
				MinBackoff: 3 * time.Second,
				MaxBackoff: 0,
			},
			expectedErr: errNonPositiveMaxBackoffDuration,
		},
		"should fail if max backoff is negative": {
			cfg: RetryConfig{
				MinBackoff: 3 * time.Second,
				MaxBackoff: -1,
			},
			expectedErr: errNonPositiveMaxBackoffDuration,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectedErr, testData.cfg.Validate())
		})
	}
}

const MiB = 1024 * 1024

func setupTestDistributor(t *testing.T) (*Distributor, func()) {
	ctx := context.Background()

	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	kvCleanup := func() { require.NoError(t, closer.Close()) }

	err := kvStore.CAS(ctx, ingester.IngesterRingKey,
		func(_ interface{}) (interface{}, bool, error) {
			d := &ring.Desc{}
			d.AddIngester("ingester-1", "127.0.0.1", "", ring.NewRandomTokenGenerator().GenerateTokens(128, nil), ring.ACTIVE, time.Now(), false, time.Time{})
			return d, true, nil
		},
	)
	require.NoError(t, err)

	ingestersRing, err := ring.New(ring.Config{
		KVStore:           kv.Config{Mock: kvStore},
		HeartbeatTimeout:  60 * time.Minute,
		ReplicationFactor: 1,
	}, ingester.IngesterRingKey, ingester.IngesterRingKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, ingestersRing))
	ringCleanup := func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, ingestersRing)) }

	dskit_test.Poll(t, time.Second, 1, func() interface{} {
		return ingestersRing.InstancesCount()
	})

	var distributorConfig Config
	var clientConfig client.Config
	limits := validation.Limits{}
	flagext.DefaultValues(&distributorConfig, &clientConfig, &limits)

	distributorConfig.DefaultLimits = InstanceLimits{MaxInflightPushRequestsBytes: MiB}
	distributorConfig.DistributorRing.Common.KVStore.Store = "inmemory"
	distributorConfig.IngesterClientFactory = ring_client.PoolInstFunc(func(ring.InstanceDesc) (ring_client.PoolClient, error) {
		return &noopIngester{}, nil
	})

	overrides := validation.NewOverrides(limits, nil)
	distributor, err := New(distributorConfig, clientConfig, overrides, nil, nil, ingestersRing, nil, true, nil, nil, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), distributor))

	cleanup := func() {
		ringCleanup()
		kvCleanup()
	}
	return distributor, cleanup
}

func dummyPushFunc(ctx context.Context, pushReq *Request) error {
	return nil
}

func TestOTLPPushHandlerErrorsAreReportedCorrectlyViaHttpgrpc(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := dskit_server.Config{}
	// Set default values
	cfg.RegisterFlags(flag.NewFlagSet("test", flag.ContinueOnError))

	// Configure values for test.
	cfg.HTTPListenAddress = "localhost"
	cfg.HTTPListenPort = 0 // auto-assign
	cfg.GRPCListenAddress = "localhost"
	cfg.GRPCListenPort = 0 // auto-assign
	cfg.Registerer = reg
	cfg.Gatherer = reg
	cfg.ReportHTTP4XXCodesInInstrumentationLabel = true // report 400 as errors.
	cfg.GRPCMiddleware = []grpc.UnaryServerInterceptor{middleware.ServerUserHeaderInterceptor}
	cfg.HTTPMiddleware = []middleware.Interface{middleware.AuthenticateUser}

	srv, err := dskit_server.New(cfg)
	require.NoError(t, err)

	push := func(_ context.Context, req *Request) error {
		// Trigger conversion of incoming request to WriteRequest.
		wr, err := req.WriteRequest()
		if err != nil {
			return err
		}

		if len(wr.Timeseries) > 0 && len(wr.Timeseries[0].Labels) > 0 && wr.Timeseries[0].Labels[0].Name == "__name__" && wr.Timeseries[0].Labels[0].Value == "report_server_error" {
			return errors.New("some random push error")
		}

		return nil
	}
	h := OTLPHandler(200, nil, otlpLimitsMock{}, nil, RetryConfig{}, nil, push, newPushMetrics(reg), reg, log.NewNopLogger())
	srv.HTTP.Handle("/otlp", h)

	// start the server
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = srv.Run() }()
	t.Cleanup(func() {
		srv.Stop()
		wg.Wait()
	})

	// create client
	conn, err := grpc.NewClient(srv.GRPCListenAddr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(middleware.ClientUserHeaderInterceptor))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	type testCase struct {
		request                  *httpgrpc.HTTPRequest
		expectedResponse         *httpgrpc.HTTPResponse
		expectedGrpcErrorMessage string
	}

	emptyReqProto, err := proto.Marshal(&colmetricpb.ExportMetricsServiceRequest{})
	require.NoError(t, err)
	testcases := map[string]testCase{
		"missing content type returns 415": {
			request: &httpgrpc.HTTPRequest{
				Method: "POST",
				Url:    "/otlp",
				Body:   []byte("hello"),
			},
			expectedResponse: &httpgrpc.HTTPResponse{Code: 415,
				Headers: []*httpgrpc.Header{
					{Key: "Content-Length", Values: []string{"86"}},
					{Key: "Content-Type", Values: []string{"application/x-protobuf"}},
					{Key: "X-Content-Type-Options", Values: []string{"nosniff"}},
				},
				Body: protoMarshalStatus(t, 415, "unsupported content type: , supported: [application/json, application/x-protobuf]"),
			},
			expectedGrpcErrorMessage: "rpc error: code = Code(415) desc = unsupported content type: , supported: [application/json, application/x-protobuf]",
		},
		"invalid JSON request returns 400": {
			request: &httpgrpc.HTTPRequest{
				Method: "POST",
				Headers: []*httpgrpc.Header{
					{Key: "Content-Type", Values: []string{"application/json"}},
				},
				Url:  "/otlp",
				Body: []byte("invalid"),
			},
			expectedResponse: &httpgrpc.HTTPResponse{Code: 400,
				Headers: []*httpgrpc.Header{
					{Key: "Content-Length", Values: []string{"140"}},
					{Key: "Content-Type", Values: []string{"application/json"}},
					{Key: "X-Content-Type-Options", Values: []string{"nosniff"}},
				},
				Body: jsonMarshalStatus(t, 400, "ReadObjectCB: expect { or n, but found i, error found in #1 byte of ...|invalid|..., bigger context ...|invalid|..."),
			},
			expectedGrpcErrorMessage: "rpc error: code = Code(400) desc = ReadObjectCB: expect { or n, but found i, error found in #1 byte of ...|invalid|..., bigger context ...|invalid|...",
		},
		"invalid protobuf request returns 400": {
			request: &httpgrpc.HTTPRequest{
				Method: "POST",
				Headers: []*httpgrpc.Header{
					{Key: "Content-Type", Values: []string{"application/x-protobuf"}},
				},
				Url:  "/otlp",
				Body: []byte("invalid"),
			},
			expectedResponse: &httpgrpc.HTTPResponse{Code: 400,
				Headers: []*httpgrpc.Header{
					{Key: "Content-Length", Values: []string{"19"}},
					{Key: "Content-Type", Values: []string{"application/x-protobuf"}},
					{Key: "X-Content-Type-Options", Values: []string{"nosniff"}},
				},
				Body: protoMarshalStatus(t, 400, "unexpected EOF"),
			},
			expectedGrpcErrorMessage: "rpc error: code = Code(400) desc = unexpected EOF",
		},
		"invalid JSON with non-utf8 characters request returns 400": {
			request: &httpgrpc.HTTPRequest{
				Method: "POST",
				Headers: []*httpgrpc.Header{
					{Key: "Content-Type", Values: []string{"application/json"}},
				},
				Url: "/otlp",
				// \xf6 and \xd3 are not valid UTF8 characters, and they should be replaced with \UFFFD in the output.
				Body: []byte("\n\xf6\x16\n\xd3\x02\n\x1d\n\x11container.runtime\x12\x08\n\x06docker\n'\n\x12container.h"),
			},
			expectedResponse: &httpgrpc.HTTPResponse{Code: 400,
				Headers: []*httpgrpc.Header{
					{Key: "Content-Length", Values: []string{"267"}},
					{Key: "Content-Type", Values: []string{"application/json"}},
					{Key: "X-Content-Type-Options", Values: []string{"nosniff"}},
				},
				Body: jsonMarshalStatus(t, 400, "ReadObjectCB: expect { or n, but found \ufffd, error found in #2 byte of ...|\n\ufffd\u0016\n\ufffd\u0002\n\u001d\n\u0011co|..., bigger context ...|\n\ufffd\u0016\n\ufffd\u0002\n\u001d\n\u0011container.runtime\u0012\u0008\n\u0006docker\n'\n\u0012container.h|..."),
			},
			expectedGrpcErrorMessage: "rpc error: code = Code(400) desc = ReadObjectCB: expect { or n, but found \ufffd, error found in #2 byte of ...|\n\ufffd\u0016\n\ufffd\u0002\n\u001d\n\u0011co|..., bigger context ...|\n\ufffd\u0016\n\ufffd\u0002\n\u001d\n\u0011container.runtime\u0012\u0008\n\u0006docker\n'\n\u0012container.h|...",
		},
		"empty JSON is good request, with 200 status code": {
			request: &httpgrpc.HTTPRequest{
				Method: "POST",
				Headers: []*httpgrpc.Header{
					{Key: "Content-Type", Values: []string{"application/json"}},
				},
				Url:  "/otlp",
				Body: []byte("{}"),
			},
			expectedResponse: &httpgrpc.HTTPResponse{Code: 200,
				Headers: []*httpgrpc.Header{
					{Key: "Content-Length", Values: []string{"2"}},
					{Key: "Content-Type", Values: []string{"application/json"}},
					{Key: "X-Content-Type-Options", Values: []string{"nosniff"}},
				},
				Body: []byte("{}"),
			},
			expectedGrpcErrorMessage: "", // No error expected
		},
		"empty protobuf is good request, with 200 status code": {
			request: &httpgrpc.HTTPRequest{
				Method: "POST",
				Headers: []*httpgrpc.Header{
					{Key: "Content-Type", Values: []string{"application/x-protobuf"}},
				},
				Url:  "/otlp",
				Body: emptyReqProto,
			},
			expectedResponse: &httpgrpc.HTTPResponse{Code: 200,
				Headers: []*httpgrpc.Header{
					{Key: "Content-Length", Values: []string{"0"}},
					{Key: "Content-Type", Values: []string{"application/x-protobuf"}},
					{Key: "X-Content-Type-Options", Values: []string{"nosniff"}},
				},
				Body: nil,
			},
			expectedGrpcErrorMessage: "", // No error expected
		},
		"trigger 5xx error by sending special metric": {
			request: &httpgrpc.HTTPRequest{
				Method: "POST",
				Headers: []*httpgrpc.Header{
					{Key: "Content-Type", Values: []string{"application/json"}},
				},
				Url: "/otlp",
				// This is a simple OTLP request, with "report_server_error".
				Body: []byte(`{"resourceMetrics": [{"scopeMetrics": [{"metrics": [{"name": "report_server_error", "gauge": {"dataPoints": [{"timeUnixNano": "1679912463340000000", "asDouble": 10.66}]}}]}]}]}`),
			},
			expectedResponse: &httpgrpc.HTTPResponse{Code: 503,
				Headers: []*httpgrpc.Header{
					{Key: "Content-Length", Values: []string{"46"}},
					{Key: "Content-Type", Values: []string{"application/json"}},
					{Key: "X-Content-Type-Options", Values: []string{"nosniff"}},
				},
				Body: jsonMarshalStatus(t, codes.Internal, "some random push error"),
			},
			expectedGrpcErrorMessage: "rpc error: code = Code(503) desc = some random push error",
		},
	}

	hc := httpgrpc.NewHTTPClient(conn)
	httpClient := http.Client{}

	for name, tc := range testcases {
		t.Run(fmt.Sprintf("grpc: %s", name), func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "test")
			resp, err := hc.Handle(ctx, tc.request)
			if err != nil {
				require.EqualError(t, err, tc.expectedGrpcErrorMessage)

				errresp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok, "errors reported by OTLP handler should always be convertible to HTTP response")
				resp = errresp
			} else if tc.expectedGrpcErrorMessage != "" {
				require.Failf(t, "expected error message %q, but got no error", tc.expectedGrpcErrorMessage)
			}

			// Before comparing response, we sort headers, to keep comparison stable.
			slices.SortFunc(resp.Headers, func(a, b *httpgrpc.Header) int {
				return strings.Compare(a.Key, b.Key)
			})
			require.Equal(t, tc.expectedResponse, resp)
		})

		t.Run(fmt.Sprintf("http: %s", name), func(t *testing.T) {
			req, err := httpgrpc.ToHTTPRequest(context.Background(), tc.request)
			require.NoError(t, err)

			req.Header.Add("X-Scope-OrgID", "test")
			req.RequestURI = ""
			req.URL.Scheme = "http"
			req.URL.Host = srv.HTTPListenAddr().String()

			resp, err := httpClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			if len(body) == 0 {
				body = nil // to simplify test
			}

			// Verify that body is the same as we expect through gRPC.
			require.Equal(t, tc.expectedResponse.Body, body)

			// Verify that expected headers are in the response.
			for _, h := range tc.expectedResponse.Headers {
				assert.Equal(t, h.Values, resp.Header.Values(h.Key))
			}

			// Verify that header that indicates grpc error for httpgrpc.Server is not in the response.
			assert.Empty(t, resp.Header.Get(server.ErrorMessageHeaderKey))
		})
	}
}

func protoMarshalStatus(t *testing.T, code codes.Code, msg string) []byte {
	t.Helper()
	bytes, err := status.New(code, msg).Proto().Marshal()
	require.NoError(t, err)
	return bytes
}

func jsonMarshalStatus(t *testing.T, code codes.Code, msg string) []byte {
	t.Helper()
	bytes, err := json.Marshal(status.New(code, msg).Proto())
	require.NoError(t, err)
	return bytes
}

type otlpLimitsMock struct{}

func (o otlpLimitsMock) OTelMetricSuffixesEnabled(string) bool {
	return false
}

func (o otlpLimitsMock) OTelCreatedTimestampZeroIngestionEnabled(string) bool {
	return false
}

func (o otlpLimitsMock) PromoteOTelResourceAttributes(string) []string {
	return nil
}

func (o otlpLimitsMock) OTelKeepIdentifyingResourceAttributes(string) bool {
	return false
}

func (o otlpLimitsMock) OTelConvertHistogramsToNHCB(string) bool { return false }

func (o otlpLimitsMock) OTelPromoteScopeMetadata(string) bool {
	return false
}

func (o otlpLimitsMock) OTelNativeDeltaIngestion(string) bool { return false }

func (o otlpLimitsMock) OTelTranslationStrategy(string) otlptranslator.TranslationStrategyOption {
	return otlptranslator.UnderscoreEscapingWithoutSuffixes
}

func (o otlpLimitsMock) NameValidationScheme(string) model.ValidationScheme {
	return model.LegacyValidation
}

func promToMimirHistogram(h *prompb.Histogram) mimirpb.Histogram {
	pSpans := make([]mimirpb.BucketSpan, 0, len(h.PositiveSpans))
	for _, span := range h.PositiveSpans {
		pSpans = append(
			pSpans, mimirpb.BucketSpan{
				Offset: span.Offset,
				Length: span.Length,
			},
		)
	}
	nSpans := make([]mimirpb.BucketSpan, 0, len(h.NegativeSpans))
	for _, span := range h.NegativeSpans {
		nSpans = append(
			nSpans, mimirpb.BucketSpan{
				Offset: span.Offset,
				Length: span.Length,
			},
		)
	}

	return mimirpb.Histogram{
		Count:          &mimirpb.Histogram_CountInt{CountInt: h.GetCountInt()},
		Sum:            h.Sum,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCount:      &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: h.GetZeroCountInt()},
		NegativeSpans:  nSpans,
		NegativeDeltas: h.NegativeDeltas,
		NegativeCounts: h.NegativeCounts,
		PositiveSpans:  pSpans,
		PositiveDeltas: h.PositiveDeltas,
		PositiveCounts: h.PositiveCounts,
		Timestamp:      h.Timestamp,
		ResetHint:      mimirpb.Histogram_ResetHint(h.ResetHint),
	}
}
