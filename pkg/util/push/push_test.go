// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/push/push_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package push

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/middleware"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestHandler_remoteWrite(t *testing.T) {
	req := createRequest(t, createPrometheusRemoteWriteProtobuf(t))
	resp := httptest.NewRecorder()
	handler := Handler(100000, nil, false, verifyWriteRequestHandler(t, mimirpb.API))
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestHandler_otlpWriteNoCompression(t *testing.T) {
	req := createOTLPRequest(t, createOTLPMetricRequest(t), false)
	resp := httptest.NewRecorder()
	handler := OTLPHandler(100000, nil, false, verifyWriteRequestHandler(t, mimirpb.API))
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestHandler_otlpDroppedMetricsPanic(t *testing.T) {
	// https://github.com/grafana/mimir/issues/3037 is triggered by a single metric
	// having two different datapoints that correspond to different Prometheus metrics.

	// For the error to be triggered, md.MetricCount() < len(tsMap), hence we're inserting 3 valid
	// samples from one metric (len = 3), and one invalid metric (metric count = 2).

	md := pmetric.NewMetrics()
	const name = "foo"
	attributes := pcommon.NewMap()
	attributes.InsertString(model.MetricNameLabel, name)

	metric1 := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric1.SetName(name)
	metric1.SetDataType(pmetric.MetricDataTypeGauge)

	datapoint1 := metric1.Gauge().DataPoints().AppendEmpty()
	datapoint1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	datapoint1.SetDoubleVal(0)
	attributes.CopyTo(datapoint1.Attributes())
	datapoint1.Attributes().InsertString("diff_label", "bar")

	datapoint2 := metric1.Gauge().DataPoints().AppendEmpty()
	datapoint2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	datapoint2.SetDoubleVal(0)
	attributes.CopyTo(datapoint2.Attributes())
	datapoint2.Attributes().InsertString("diff_label", "baz")

	datapoint3 := metric1.Gauge().DataPoints().AppendEmpty()
	datapoint3.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	datapoint3.SetDoubleVal(0)
	attributes.CopyTo(datapoint3.Attributes())
	datapoint3.Attributes().InsertString("diff_label", "food")

	metric2 := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric2.SetName(name)
	metric2.SetDataType(pmetric.MetricDataTypeGauge)

	req := createOTLPRequest(t, pmetricotlp.NewRequestFromMetrics(md), false)
	resp := httptest.NewRecorder()
	handler := OTLPHandler(100000, nil, false, func(ctx context.Context, request *mimirpb.WriteRequest, cleanup func()) (response *mimirpb.WriteResponse, err error) {
		assert.Len(t, request.Timeseries, 3)
		assert.False(t, request.SkipLabelNameValidation)
		cleanup()
		return &mimirpb.WriteResponse{}, nil
	})
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestHandler_otlpWriteWithCompression(t *testing.T) {
	req := createOTLPRequest(t, createOTLPMetricRequest(t), true)
	resp := httptest.NewRecorder()
	handler := OTLPHandler(100000, nil, false, verifyWriteRequestHandler(t, mimirpb.API))
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestHandler_otlpWriteRequestTooBigNoCompression(t *testing.T) {
	req := createOTLPRequest(t, createOTLPMetricRequest(t), false)
	resp := httptest.NewRecorder()

	// This one is caught in the r.ContentLength check.
	handler := OTLPHandler(30, nil, false, verifyWriteRequestHandler(t, mimirpb.API))
	handler.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.Code)
	assert.Contains(t, resp.Body.String(), "the incoming push request has been rejected because its message size of 37 bytes is larger than the allowed limit of 30 bytes (err-mimir-distributor-max-write-message-size). To adjust the related limit, configure -distributor.max-recv-msg-size, or contact your service administrator.")
}

func TestHandler_otlpWriteRequestTooBigWithCompression(t *testing.T) {

	// createOTLPRequest will create a request which is BIGGER with compression (37 vs 58 bytes).
	// Hence creating a dummy request.
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	_, err := gz.Write(make([]byte, 100000))
	require.NoError(t, err)
	require.NoError(t, gz.Close())

	req, err := http.NewRequest("POST", "http://localhost/", bytes.NewReader(b.Bytes()))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "gzip")

	resp := httptest.NewRecorder()

	handler := OTLPHandler(140, nil, false, verifyWriteRequestHandler(t, mimirpb.API))
	handler.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.Code)
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(body), "the incoming push request has been rejected because its message size is larger than the allowed limit of 140 bytes (err-mimir-distributor-max-write-message-size). To adjust the related limit, configure -distributor.max-recv-msg-size, or contact your service administrator.")
}

func TestHandler_otlpWriteRequestWithUnSupportedCompression(t *testing.T) {
	req := createOTLPRequest(t, createOTLPMetricRequest(t), true)
	req.Header.Set("Content-Encoding", "snappy")

	resp := httptest.NewRecorder()
	handler := OTLPHandler(100000, nil, false, verifyWriteRequestHandler(t, mimirpb.API))
	handler.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusUnsupportedMediaType, resp.Code)
}

func TestHandler_mimirWriteRequest(t *testing.T) {
	req := createRequest(t, createMimirWriteRequestProtobuf(t, false))
	resp := httptest.NewRecorder()
	sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)")
	handler := Handler(100000, sourceIPs, false, verifyWriteRequestHandler(t, mimirpb.RULE))
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestHandler_contextCanceledRequest(t *testing.T) {
	req := createRequest(t, createMimirWriteRequestProtobuf(t, false))
	resp := httptest.NewRecorder()
	sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)")
	handler := Handler(100000, sourceIPs, false, func(_ context.Context, _ *mimirpb.WriteRequest, cleanup func()) (*mimirpb.WriteResponse, error) {
		defer cleanup()
		return nil, fmt.Errorf("the request failed: %w", context.Canceled)
	})
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 499, resp.Code)
}

func TestHandler_EnsureSkipLabelNameValidationBehaviour(t *testing.T) {
	tests := []struct {
		name                                      string
		allowSkipLabelNameValidation              bool
		req                                       *http.Request
		includeAllowSkiplabelNameValidationHeader bool
		verifyReqHandler                          func(ctx context.Context, request *mimirpb.WriteRequest, cleanup func()) (response *mimirpb.WriteResponse, err error)
		expectedStatusCode                        int
	}{
		{
			name:                         "config flag set to false means SkipLabelNameValidation is false",
			allowSkipLabelNameValidation: false,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, false)),
			verifyReqHandler: func(ctx context.Context, request *mimirpb.WriteRequest, cleanup func()) (response *mimirpb.WriteResponse, err error) {
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelNameValidation)
				cleanup()
				return &mimirpb.WriteResponse{}, nil
			},
			includeAllowSkiplabelNameValidationHeader: true,
			expectedStatusCode:                        http.StatusOK,
		},
		{
			name:                         "config flag set to false means SkipLabelNameValidation is always false even if write requests sets it to true",
			allowSkipLabelNameValidation: false,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, true)),
			verifyReqHandler: func(ctx context.Context, request *mimirpb.WriteRequest, cleanup func()) (response *mimirpb.WriteResponse, err error) {
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelNameValidation)
				cleanup()
				return &mimirpb.WriteResponse{}, nil
			},
			includeAllowSkiplabelNameValidationHeader: true,
			expectedStatusCode:                        http.StatusOK,
		},
		{
			name:                         "config flag set to true but write request set to false means SkipLabelNameValidation is false",
			allowSkipLabelNameValidation: true,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, false)),
			verifyReqHandler: func(ctx context.Context, request *mimirpb.WriteRequest, cleanup func()) (response *mimirpb.WriteResponse, err error) {
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelNameValidation)
				cleanup()
				return &mimirpb.WriteResponse{}, nil
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:                         "config flag set to true and write request set to true means SkipLabelNameValidation is true",
			allowSkipLabelNameValidation: true,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, true)),
			verifyReqHandler: func(ctx context.Context, request *mimirpb.WriteRequest, cleanup func()) (response *mimirpb.WriteResponse, err error) {
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.True(t, request.SkipLabelNameValidation)
				cleanup()
				return &mimirpb.WriteResponse{}, nil
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:                         "config flag set to true and write request set to true but header not sent means SkipLabelNameValidation is false",
			allowSkipLabelNameValidation: true,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, true)),
			verifyReqHandler: func(ctx context.Context, request *mimirpb.WriteRequest, cleanup func()) (response *mimirpb.WriteResponse, err error) {
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelNameValidation)
				cleanup()
				return &mimirpb.WriteResponse{}, nil
			},
			includeAllowSkiplabelNameValidationHeader: true,
			expectedStatusCode:                        http.StatusOK,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := httptest.NewRecorder()
			handler := Handler(100000, nil, tc.allowSkipLabelNameValidation, tc.verifyReqHandler)
			if !tc.includeAllowSkiplabelNameValidationHeader {
				tc.req.Header.Set(SkipLabelNameValidationHeader, "true")
			}
			handler.ServeHTTP(resp, tc.req)
			assert.Equal(t, tc.expectedStatusCode, resp.Code)
		})
	}
}

func verifyWriteRequestHandler(t *testing.T, expectSource mimirpb.WriteRequest_SourceEnum) func(ctx context.Context, request *mimirpb.WriteRequest, cleanup func()) (response *mimirpb.WriteResponse, err error) {
	t.Helper()
	return func(ctx context.Context, request *mimirpb.WriteRequest, cleanup func()) (response *mimirpb.WriteResponse, err error) {
		assert.Len(t, request.Timeseries, 1)
		assert.Equal(t, "__name__", request.Timeseries[0].Labels[0].Name)
		assert.Equal(t, "foo", request.Timeseries[0].Labels[0].Value)
		assert.Equal(t, expectSource, request.Source)
		assert.False(t, request.SkipLabelNameValidation)
		cleanup()
		return &mimirpb.WriteResponse{}, nil
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
	return req
}

func createOTLPRequest(t testing.TB, metricRequest pmetricotlp.Request, compress bool) *http.Request {
	t.Helper()

	rawBytes, err := metricRequest.MarshalProto()
	require.NoError(t, err)

	body := rawBytes

	if compress {
		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		_, err := gz.Write(rawBytes)
		require.NoError(t, err)
		require.NoError(t, gz.Close())

		body = b.Bytes()
	}

	req, err := http.NewRequest("POST", "http://localhost/", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Scope-OrgID", "test")

	// We need this for testing dropped metrics codepath which requires
	// tenantID to be present.
	_, ctx, err := tenant.ExtractTenantIDFromHTTPRequest(req)
	require.NoError(t, err)
	req = req.WithContext(ctx)

	if compress {
		req.Header.Set("Content-Encoding", "gzip")
	}
	return req
}

func createOTLPMetricRequest(t testing.TB) pmetricotlp.Request {
	input := createPrometheusRemoteWriteProtobuf(t)
	prwReq := &prompb.WriteRequest{}
	require.NoError(t, proto.Unmarshal(input, prwReq))

	return TimeseriesToOTLPRequest(prwReq.Timeseries)
}

func createPrometheusRemoteWriteProtobuf(t testing.TB) []byte {
	t.Helper()
	input := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "foo"},
				},
				Samples: []prompb.Sample{
					{Value: 1, Timestamp: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
				},
			},
		},
	}
	inoutBytes, err := input.Marshal()
	require.NoError(t, err)
	return inoutBytes
}

func createMimirWriteRequestProtobuf(t *testing.T, skipLabelNameValidation bool) []byte {
	t.Helper()
	ts := mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{
				{Name: "__name__", Value: "foo"},
			},
			Samples: []mimirpb.Sample{
				{Value: 1, TimestampMs: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
			},
		},
	}
	input := mimirpb.WriteRequest{
		Timeseries:              []mimirpb.PreallocTimeseries{ts},
		Source:                  mimirpb.RULE,
		SkipLabelNameValidation: skipLabelNameValidation,
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
		Timeseries:              []mimirpb.PreallocTimeseries{ts},
		Source:                  mimirpb.RULE,
		SkipLabelNameValidation: skipLabelNameValidation,
	}
	inoutBytes, err := input.Marshal()
	require.NoError(t, err)
	return inoutBytes
}

func BenchmarkPushHandler(b *testing.B) {
	protobuf := createPrometheusRemoteWriteProtobuf(b)
	buf := bytes.NewBuffer(snappy.Encode(nil, protobuf))
	req := createRequest(b, protobuf)
	pushFunc := func(ctx context.Context, request *mimirpb.WriteRequest, cleanup func()) (response *mimirpb.WriteResponse, err error) {
		cleanup()
		return &mimirpb.WriteResponse{}, nil
	}
	handler := Handler(100000, nil, false, pushFunc)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		req.Body = bufCloser{Buffer: buf} // reset Body so it can be read each time round the loop
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		assert.Equal(b, 200, resp.Code)
	}
}

// Implements both io.ReadCloser required by http.NewRequest and BytesBuffer used by push handler.
type bufCloser struct {
	*bytes.Buffer
}

func (bufCloser) Close() error                 { return nil }
func (n bufCloser) BytesBuffer() *bytes.Buffer { return n.Buffer }

func TestNewDistributorMaxWriteMessageSizeErr(t *testing.T) {
	err := distributorMaxWriteMessageSizeErr{actual: 100, limit: 50}
	msg := `the incoming push request has been rejected because its message size of 100 bytes is larger than the allowed limit of 50 bytes (err-mimir-distributor-max-write-message-size). To adjust the related limit, configure -distributor.max-recv-msg-size, or contact your service administrator.`

	assert.Equal(t, msg, err.Error())
}
