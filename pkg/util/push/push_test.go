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

	"github.com/golang/snappy"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestHandler_remoteWrite(t *testing.T) {
	req := createRequest(t, createPrometheusRemoteWriteProtobuf(t))
	resp := httptest.NewRecorder()
	handler := Handler(100000, nil, false, verifyWritePushFunc(t, mimirpb.API))
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestHandlerOTLPPush(t *testing.T) {
	sampleSeries :=
		[]prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "foo"},
				},
				Samples: []prompb.Sample{
					{Value: 1, Timestamp: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
				},
			},
		}
	samplesVerifierFunc := func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
		request, err := pushReq.WriteRequest()
		assert.NoError(t, err)

		series := request.Timeseries
		assert.Len(t, series, 1)

		samples := series[0].Samples
		assert.Equal(t, 1, len(samples))
		assert.Equal(t, float64(1), samples[0].Value)
		assert.Equal(t, "__name__", series[0].Labels[0].Name)
		assert.Equal(t, "foo", series[0].Labels[0].Value)

		pushReq.CleanUp()
		return &mimirpb.WriteResponse{}, nil
	}

	tests := []struct {
		name   string
		series []prompb.TimeSeries

		compression bool
		encoding    string
		maxMsgSize  int

		verifyFunc   Func
		responseCode int
		errMessage   string
	}{
		{
			name:         "Write samples. No compression",
			maxMsgSize:   100000,
			verifyFunc:   samplesVerifierFunc,
			series:       sampleSeries,
			responseCode: http.StatusOK,
		},
		{
			name:         "Write samples. With compression",
			compression:  true,
			maxMsgSize:   100000,
			verifyFunc:   samplesVerifierFunc,
			series:       sampleSeries,
			responseCode: http.StatusOK,
		},
		{
			name:        "Write samples. Request too big",
			compression: false,
			maxMsgSize:  30,
			series:      sampleSeries,
			verifyFunc: func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
				_, err = pushReq.WriteRequest()
				return &mimirpb.WriteResponse{}, err
			},
			responseCode: http.StatusRequestEntityTooLarge,
			errMessage:   "the incoming push request has been rejected because its message size of 37 bytes is larger",
		},
		{
			name:       "Write samples. Unsupported compression",
			encoding:   "snappy",
			maxMsgSize: 100000,
			series:     sampleSeries,
			verifyFunc: func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
				_, err = pushReq.WriteRequest()
				return &mimirpb.WriteResponse{}, err
			},
			responseCode: http.StatusUnsupportedMediaType,
			errMessage:   "Only \"gzip\" or no compression supported",
		},
		{
			name:       "Write histograms",
			maxMsgSize: 100000,
			series: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "foo"},
					},
					Histograms: []prompb.Histogram{
						remote.HistogramToHistogramProto(1337, test.GenerateTestHistogram(1)),
					},
				},
			},
			verifyFunc: func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)

				series := request.Timeseries
				assert.Len(t, series, 1)

				histograms := series[0].Histograms
				assert.Equal(t, 1, len(histograms))
				assert.Equal(t, 1, int(histograms[0].Schema))

				pushReq.CleanUp()
				return &mimirpb.WriteResponse{}, nil
			},
			responseCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exportReq := TimeseriesToOTLPRequest(tt.series)
			req := createOTLPRequest(t, exportReq, tt.compression)
			if tt.encoding != "" {
				req.Header.Set("Content-Encoding", tt.encoding)
			}

			handler := OTLPHandler(tt.maxMsgSize, nil, false, nil, tt.verifyFunc)

			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)

			assert.Equal(t, tt.responseCode, resp.Code)
			if tt.errMessage != "" {
				body, err := io.ReadAll(resp.Body)
				assert.NoError(t, err)
				assert.Contains(t, string(body), tt.errMessage)
			}
		})
	}
}

func TestHandler_otlpDroppedMetricsPanic(t *testing.T) {
	// https://github.com/grafana/mimir/issues/3037 is triggered by a single metric
	// having two different datapoints that correspond to different Prometheus metrics.

	// For the error to be triggered, md.MetricCount() < len(tsMap), hence we're inserting 3 valid
	// samples from one metric (len = 3), and one invalid metric (metric count = 2).

	md := pmetric.NewMetrics()
	const name = "foo"
	attributes := pcommon.NewMap()
	attributes.PutStr(model.MetricNameLabel, name)

	metric1 := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric1.SetName(name)
	metric1.SetEmptyGauge()

	datapoint1 := metric1.Gauge().DataPoints().AppendEmpty()
	datapoint1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	datapoint1.SetDoubleValue(0)
	attributes.CopyTo(datapoint1.Attributes())
	datapoint1.Attributes().PutStr("diff_label", "bar")

	datapoint2 := metric1.Gauge().DataPoints().AppendEmpty()
	datapoint2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	datapoint2.SetDoubleValue(0)
	attributes.CopyTo(datapoint2.Attributes())
	datapoint2.Attributes().PutStr("diff_label", "baz")

	datapoint3 := metric1.Gauge().DataPoints().AppendEmpty()
	datapoint3.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	datapoint3.SetDoubleValue(0)
	attributes.CopyTo(datapoint3.Attributes())
	datapoint3.Attributes().PutStr("diff_label", "food")

	metric2 := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric2.SetName(name)
	metric2.SetEmptyGauge()

	req := createOTLPRequest(t, pmetricotlp.NewExportRequestFromMetrics(md), false)
	resp := httptest.NewRecorder()
	handler := OTLPHandler(100000, nil, false, nil, func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
		request, err := pushReq.WriteRequest()
		assert.NoError(t, err)
		assert.Len(t, request.Timeseries, 3)
		assert.False(t, request.SkipLabelNameValidation)
		pushReq.CleanUp()
		return &mimirpb.WriteResponse{}, nil
	})
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestHandler_otlpDroppedMetricsPanic2(t *testing.T) {
	// After the above test, the panic occurred again.
	// This test is to ensure that the panic is fixed for the new cases as well.

	// First case is to make sure that target_info is counted correctly.
	md := pmetric.NewMetrics()
	const name = "foo"
	attributes := pcommon.NewMap()
	attributes.PutStr(model.MetricNameLabel, name)

	resource1 := md.ResourceMetrics().AppendEmpty()
	resource1.Resource().Attributes().PutStr("region", "us-central1")

	metric1 := resource1.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric1.SetName(name)
	metric1.SetEmptyGauge()
	datapoint1 := metric1.Gauge().DataPoints().AppendEmpty()
	datapoint1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	datapoint1.SetDoubleValue(0)
	attributes.CopyTo(datapoint1.Attributes())
	datapoint1.Attributes().PutStr("diff_label", "bar")

	metric2 := resource1.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric2.SetName(name)
	metric2.SetEmptyGauge()

	req := createOTLPRequest(t, pmetricotlp.NewExportRequestFromMetrics(md), false)
	resp := httptest.NewRecorder()
	handler := OTLPHandler(100000, nil, false, nil, func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
		request, err := pushReq.WriteRequest()
		assert.NoError(t, err)
		assert.Len(t, request.Timeseries, 2)
		assert.False(t, request.SkipLabelNameValidation)
		pushReq.CleanUp()
		return &mimirpb.WriteResponse{}, nil
	})
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)

	// Second case is to make sure that histogram metrics are counted correctly.
	metric3 := resource1.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric3.SetName("http_request_duration_seconds")
	metric3.SetEmptyHistogram()
	metric3.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	datapoint3 := metric3.Histogram().DataPoints().AppendEmpty()
	datapoint3.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	datapoint3.SetCount(50)
	datapoint3.SetSum(100)
	datapoint3.ExplicitBounds().FromRaw([]float64{0.1, 0.2, 0.3, 0.4, 0.5})
	datapoint3.BucketCounts().FromRaw([]uint64{10, 20, 30, 40, 50})
	attributes.CopyTo(datapoint3.Attributes())

	req = createOTLPRequest(t, pmetricotlp.NewExportRequestFromMetrics(md), false)
	resp = httptest.NewRecorder()
	handler = OTLPHandler(100000, nil, false, nil, func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
		request, err := pushReq.WriteRequest()
		assert.NoError(t, err)
		assert.Len(t, request.Timeseries, 10) // 6 buckets (including +Inf) + 2 sum/count + 2 from the first case
		assert.False(t, request.SkipLabelNameValidation)
		pushReq.CleanUp()
		return &mimirpb.WriteResponse{}, nil
	})
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
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

	handler := OTLPHandler(140, nil, false, nil, readBodyPushFunc(t))
	handler.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.Code)
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(body), "the incoming push request has been rejected because its message size is larger than the allowed limit of 140 bytes (err-mimir-distributor-max-write-message-size). To adjust the related limit, configure -distributor.max-recv-msg-size, or contact your service administrator.")
}

func TestHandler_mimirWriteRequest(t *testing.T) {
	req := createRequest(t, createMimirWriteRequestProtobuf(t, false))
	resp := httptest.NewRecorder()
	sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)")
	handler := Handler(100000, sourceIPs, false, verifyWritePushFunc(t, mimirpb.RULE))
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestHandler_contextCanceledRequest(t *testing.T) {
	req := createRequest(t, createMimirWriteRequestProtobuf(t, false))
	resp := httptest.NewRecorder()
	sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)")
	handler := Handler(100000, sourceIPs, false, func(_ context.Context, req *Request) (*mimirpb.WriteResponse, error) {
		defer req.CleanUp()
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
		verifyReqHandler                          Func
		expectedStatusCode                        int
	}{
		{
			name:                         "config flag set to false means SkipLabelNameValidation is false",
			allowSkipLabelNameValidation: false,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, false)),
			verifyReqHandler: func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelNameValidation)
				pushReq.CleanUp()
				return &mimirpb.WriteResponse{}, nil
			},
			includeAllowSkiplabelNameValidationHeader: true,
			expectedStatusCode:                        http.StatusOK,
		},
		{
			name:                         "config flag set to false means SkipLabelNameValidation is always false even if write requests sets it to true",
			allowSkipLabelNameValidation: false,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, true)),
			verifyReqHandler: func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelNameValidation)
				pushReq.CleanUp()
				return &mimirpb.WriteResponse{}, nil
			},
			includeAllowSkiplabelNameValidationHeader: true,
			expectedStatusCode:                        http.StatusOK,
		},
		{
			name:                         "config flag set to true but write request set to false means SkipLabelNameValidation is false",
			allowSkipLabelNameValidation: true,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, false)),
			verifyReqHandler: func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelNameValidation)
				pushReq.CleanUp()
				return &mimirpb.WriteResponse{}, nil
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:                         "config flag set to true and write request set to true means SkipLabelNameValidation is true",
			allowSkipLabelNameValidation: true,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, true)),
			verifyReqHandler: func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.True(t, request.SkipLabelNameValidation)
				pushReq.CleanUp()
				return &mimirpb.WriteResponse{}, nil
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:                         "config flag set to true and write request set to true but header not sent means SkipLabelNameValidation is false",
			allowSkipLabelNameValidation: true,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, true)),
			verifyReqHandler: func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelNameValidation)
				pushReq.CleanUp()
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

func verifyWritePushFunc(t *testing.T, expectSource mimirpb.WriteRequest_SourceEnum) Func {
	t.Helper()
	return func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
		request, err := pushReq.WriteRequest()
		assert.NoError(t, err)
		assert.Len(t, request.Timeseries, 1)
		assert.Equal(t, "__name__", request.Timeseries[0].Labels[0].Name)
		assert.Equal(t, "foo", request.Timeseries[0].Labels[0].Value)
		assert.Equal(t, expectSource, request.Source)
		assert.False(t, request.SkipLabelNameValidation)
		pushReq.CleanUp()
		return &mimirpb.WriteResponse{}, nil
	}
}

func readBodyPushFunc(t *testing.T) Func {
	t.Helper()
	return func(ctx context.Context, req *Request) (*mimirpb.WriteResponse, error) {
		_, err := req.WriteRequest()
		return &mimirpb.WriteResponse{}, err
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

func createOTLPRequest(t testing.TB, metricRequest pmetricotlp.ExportRequest, compress bool) *http.Request {
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
				Histograms: []prompb.Histogram{
					remote.HistogramToHistogramProto(1337, test.GenerateTestHistogram(1))},
			},
		},
	}
	inputBytes, err := input.Marshal()
	require.NoError(t, err)
	return inputBytes
}

func createMimirWriteRequestProtobuf(t *testing.T, skipLabelNameValidation bool) []byte {
	t.Helper()
	h := remote.HistogramToHistogramProto(1337, test.GenerateTestHistogram(1))
	ts := mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{
				{Name: "__name__", Value: "foo"},
			},
			Samples: []mimirpb.Sample{
				{Value: 1, TimestampMs: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
			},
			Histograms: []mimirpb.Histogram{promToMimirHistogram(&h)},
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
	pushFunc := func(ctx context.Context, pushReq *Request) (response *mimirpb.WriteResponse, err error) {
		pushReq.WriteRequest()
		pushReq.CleanUp()
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

func TestHandler_ErrorTranslation(t *testing.T) {
	testCases := []struct {
		name               string
		err                error
		expectedHTTPStatus int
	}{
		{
			name:               "a generic error gets an HTTP 400",
			err:                fmt.Errorf("something's wrong"),
			expectedHTTPStatus: http.StatusBadRequest,
		},
		{
			name:               "an gRPC error with a status gets translated into HTTP error",
			err:                httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "too big"),
			expectedHTTPStatus: http.StatusRequestEntityTooLarge,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parserFunc := func(context.Context, *http.Request, int, []byte, *mimirpb.PreallocWriteRequest) ([]byte, error) {
				return nil, tc.err
			}
			pushFunc := func(ctx context.Context, req *Request) (*mimirpb.WriteResponse, error) {
				_, err := req.WriteRequest() // just read the body so we can trigger the parser
				return nil, err
			}

			h := handler(10, nil, false, pushFunc, parserFunc)

			recorder := httptest.NewRecorder()
			h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/push", bufCloser{&bytes.Buffer{}}))

			assert.Equal(t, tc.expectedHTTPStatus, recorder.Code)
		})
	}
}
