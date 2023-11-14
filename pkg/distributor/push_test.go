// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/push/push_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/httpgrpc/server"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestHandler_remoteWrite(t *testing.T) {
	req := createRequest(t, createPrometheusRemoteWriteProtobuf(t))
	resp := httptest.NewRecorder()
	handler := Handler(100000, nil, false, nil, verifyWritePushFunc(t, mimirpb.API), log.NewNopLogger())
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestOtelMetricsToMetadata(t *testing.T) {
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

	sampleMetadata := []*mimirpb.MetricMetadata{
		{
			Help:             "",
			Unit:             "Count",
			Type:             mimirpb.GAUGE,
			MetricFamilyName: "name",
		},
		{
			Help:             "",
			Unit:             "Count",
			Type:             mimirpb.GAUGE,
			MetricFamilyName: "test",
		},
	}

	res := otelMetricsToMetadata(otelMetrics)
	assert.Equal(t, sampleMetadata, res)
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
	// Sample Metadata needs to contain metadata for every series in the sampleSeries
	sampleMetadata := []mimirpb.MetricMetadata{
		{
			Help: "metric_help",
			Unit: "metric_unit",
		},
	}
	samplesVerifierFunc := func(ctx context.Context, pushReq *Request) error {
		request, err := pushReq.WriteRequest()
		assert.NoError(t, err)

		series := request.Timeseries
		assert.Len(t, series, 1)

		samples := series[0].Samples
		assert.Equal(t, 1, len(samples))
		assert.Equal(t, float64(1), samples[0].Value)
		assert.Equal(t, "__name__", series[0].Labels[0].Name)
		assert.Equal(t, "foo", series[0].Labels[0].Value)

		metadata := request.Metadata
		assert.Equal(t, mimirpb.GAUGE, metadata[0].GetType())
		assert.Equal(t, "foo", metadata[0].GetMetricFamilyName())
		assert.Equal(t, "metric_help", metadata[0].GetHelp())
		assert.Equal(t, "metric_unit", metadata[0].GetUnit())

		pushReq.CleanUp()
		return nil
	}

	samplesVerifierFuncDisabledMetadataIngest := func(ctx context.Context, pushReq *Request) error {
		request, err := pushReq.WriteRequest()
		assert.NoError(t, err)

		series := request.Timeseries
		assert.Len(t, series, 1)

		samples := series[0].Samples
		assert.Equal(t, 1, len(samples))
		assert.Equal(t, float64(1), samples[0].Value)
		assert.Equal(t, "__name__", series[0].Labels[0].Name)
		assert.Equal(t, "foo", series[0].Labels[0].Value)

		metadata := request.Metadata
		assert.Equal(t, []*mimirpb.MetricMetadata(nil), metadata)

		pushReq.CleanUp()
		return nil
	}

	tests := []struct {
		name     string
		series   []prompb.TimeSeries
		metadata []mimirpb.MetricMetadata

		compression bool
		encoding    string
		maxMsgSize  int

		verifyFunc                PushFunc
		responseCode              int
		errMessage                string
		enableOtelMetadataStorage bool
	}{
		{
			name:                      "Write samples. No compression",
			maxMsgSize:                100000,
			verifyFunc:                samplesVerifierFunc,
			series:                    sampleSeries,
			metadata:                  sampleMetadata,
			responseCode:              http.StatusOK,
			enableOtelMetadataStorage: true,
		},
		{
			name:                      "Write samples. Not enabled metadata ingest",
			maxMsgSize:                100000,
			verifyFunc:                samplesVerifierFuncDisabledMetadataIngest,
			series:                    sampleSeries,
			metadata:                  sampleMetadata,
			responseCode:              http.StatusOK,
			enableOtelMetadataStorage: false,
		},
		{
			name:                      "Write samples. With compression",
			compression:               true,
			maxMsgSize:                100000,
			verifyFunc:                samplesVerifierFunc,
			series:                    sampleSeries,
			metadata:                  sampleMetadata,
			responseCode:              http.StatusOK,
			enableOtelMetadataStorage: true,
		},
		{
			name:        "Write samples. Request too big",
			compression: false,
			maxMsgSize:  30,
			series:      sampleSeries,
			metadata:    sampleMetadata,
			verifyFunc: func(ctx context.Context, pushReq *Request) error {
				_, err := pushReq.WriteRequest()
				return err
			},
			responseCode: http.StatusRequestEntityTooLarge,
			errMessage:   "the incoming push request has been rejected because its message size of 63 bytes is larger",
		},
		{
			name:       "Write samples. Unsupported compression",
			encoding:   "snappy",
			maxMsgSize: 100000,
			series:     sampleSeries,
			metadata:   sampleMetadata,
			verifyFunc: func(ctx context.Context, pushReq *Request) error {
				_, err := pushReq.WriteRequest()
				return err
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
			metadata: []mimirpb.MetricMetadata{
				{
					Help: "metric_help",
					Unit: "metric_unit",
				},
			},
			verifyFunc: func(ctx context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)

				series := request.Timeseries
				assert.Len(t, series, 1)

				histograms := series[0].Histograms
				assert.Equal(t, 1, len(histograms))
				assert.Equal(t, 1, int(histograms[0].Schema))

				metadata := request.Metadata
				assert.Equal(t, mimirpb.HISTOGRAM, metadata[0].GetType())
				assert.Equal(t, "foo", metadata[0].GetMetricFamilyName())
				assert.Equal(t, "metric_help", metadata[0].GetHelp())
				assert.Equal(t, "metric_unit", metadata[0].GetUnit())

				pushReq.CleanUp()
				return nil
			},
			responseCode:              http.StatusOK,
			enableOtelMetadataStorage: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exportReq := TimeseriesToOTLPRequest(tt.series, tt.metadata)
			req := createOTLPRequest(t, exportReq, tt.compression)
			if tt.encoding != "" {
				req.Header.Set("Content-Encoding", tt.encoding)
			}

			handler := OTLPHandler(tt.maxMsgSize, nil, false, tt.enableOtelMetadataStorage, nil, nil, tt.verifyFunc, log.NewNopLogger())

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
	handler := OTLPHandler(100000, nil, false, true, nil, nil, func(ctx context.Context, pushReq *Request) error {
		request, err := pushReq.WriteRequest()
		assert.NoError(t, err)
		assert.Len(t, request.Timeseries, 3)
		assert.False(t, request.SkipLabelNameValidation)
		pushReq.CleanUp()
		return nil
	}, log.NewNopLogger())
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
	handler := OTLPHandler(100000, nil, false, true, nil, nil, func(ctx context.Context, pushReq *Request) error {
		request, err := pushReq.WriteRequest()
		assert.NoError(t, err)
		assert.Len(t, request.Timeseries, 2)
		assert.False(t, request.SkipLabelNameValidation)
		pushReq.CleanUp()
		return nil
	}, log.NewNopLogger())
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
	handler = OTLPHandler(100000, nil, false, true, nil, nil, func(ctx context.Context, pushReq *Request) error {
		request, err := pushReq.WriteRequest()
		assert.NoError(t, err)
		assert.Len(t, request.Timeseries, 10) // 6 buckets (including +Inf) + 2 sum/count + 2 from the first case
		assert.False(t, request.SkipLabelNameValidation)
		pushReq.CleanUp()
		return nil
	}, log.NewNopLogger())
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

	handler := OTLPHandler(140, nil, false, true, nil, nil, readBodyPushFunc(t), log.NewNopLogger())
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
	handler := Handler(100000, sourceIPs, false, nil, verifyWritePushFunc(t, mimirpb.RULE), log.NewNopLogger())
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestHandler_contextCanceledRequest(t *testing.T) {
	req := createRequest(t, createMimirWriteRequestProtobuf(t, false))
	resp := httptest.NewRecorder()
	sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)")
	handler := Handler(100000, sourceIPs, false, nil, func(_ context.Context, req *Request) error {
		defer req.CleanUp()
		return fmt.Errorf("the request failed: %w", context.Canceled)
	}, log.NewNopLogger())
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 499, resp.Code)
}

func TestHandler_EnsureSkipLabelNameValidationBehaviour(t *testing.T) {
	tests := []struct {
		name                                      string
		allowSkipLabelNameValidation              bool
		req                                       *http.Request
		includeAllowSkiplabelNameValidationHeader bool
		verifyReqHandler                          PushFunc
		expectedStatusCode                        int
	}{
		{
			name:                         "config flag set to false means SkipLabelNameValidation is false",
			allowSkipLabelNameValidation: false,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, false)),
			verifyReqHandler: func(ctx context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelNameValidation)
				pushReq.CleanUp()
				return nil
			},
			includeAllowSkiplabelNameValidationHeader: true,
			expectedStatusCode:                        http.StatusOK,
		},
		{
			name:                         "config flag set to false means SkipLabelNameValidation is always false even if write requests sets it to true",
			allowSkipLabelNameValidation: false,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, true)),
			verifyReqHandler: func(ctx context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelNameValidation)
				pushReq.CleanUp()
				return nil
			},
			includeAllowSkiplabelNameValidationHeader: true,
			expectedStatusCode:                        http.StatusOK,
		},
		{
			name:                         "config flag set to true but write request set to false means SkipLabelNameValidation is false",
			allowSkipLabelNameValidation: true,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, false)),
			verifyReqHandler: func(ctx context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelNameValidation)
				pushReq.CleanUp()
				return nil
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:                         "config flag set to true and write request set to true means SkipLabelNameValidation is true",
			allowSkipLabelNameValidation: true,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, true)),
			verifyReqHandler: func(ctx context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.True(t, request.SkipLabelNameValidation)
				pushReq.CleanUp()
				return nil
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:                         "config flag set to true and write request set to true but header not sent means SkipLabelNameValidation is false",
			allowSkipLabelNameValidation: true,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, true)),
			verifyReqHandler: func(ctx context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelNameValidation)
				pushReq.CleanUp()
				return nil
			},
			includeAllowSkiplabelNameValidationHeader: true,
			expectedStatusCode:                        http.StatusOK,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := httptest.NewRecorder()
			handler := Handler(100000, nil, tc.allowSkipLabelNameValidation, nil, tc.verifyReqHandler, log.NewNopLogger())
			if !tc.includeAllowSkiplabelNameValidationHeader {
				tc.req.Header.Set(SkipLabelNameValidationHeader, "true")
			}
			handler.ServeHTTP(resp, tc.req)
			assert.Equal(t, tc.expectedStatusCode, resp.Code)
		})
	}
}

func verifyWritePushFunc(t *testing.T, expectSource mimirpb.WriteRequest_SourceEnum) PushFunc {
	t.Helper()
	return func(ctx context.Context, pushReq *Request) error {
		request, err := pushReq.WriteRequest()
		assert.NoError(t, err)
		assert.Len(t, request.Timeseries, 1)
		assert.Equal(t, "__name__", request.Timeseries[0].Labels[0].Name)
		assert.Equal(t, "foo", request.Timeseries[0].Labels[0].Value)
		assert.Equal(t, expectSource, request.Source)
		assert.False(t, request.SkipLabelNameValidation)
		pushReq.CleanUp()
		return nil
	}
}

func readBodyPushFunc(t *testing.T) PushFunc {
	t.Helper()
	return func(ctx context.Context, req *Request) error {
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
	pushFunc := func(ctx context.Context, pushReq *Request) error {
		if _, err := pushReq.WriteRequest(); err != nil {
			return err
		}
		pushReq.CleanUp()
		return nil
	}
	handler := Handler(100000, nil, false, nil, pushFunc, log.NewNopLogger())
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
	errMsg := "this is an error"
	parserTestCases := []struct {
		name                 string
		err                  error
		expectedHTTPStatus   int
		expectedErrorMessage string
	}{
		{
			name:                 "a generic error during request parsing gets an HTTP 400",
			err:                  fmt.Errorf(errMsg),
			expectedHTTPStatus:   http.StatusBadRequest,
			expectedErrorMessage: errMsg,
		},
		{
			name:                 "a gRPC error with a status during request parsing gets translated into HTTP error without DoNotLogError header",
			err:                  httpgrpc.Errorf(http.StatusRequestEntityTooLarge, errMsg),
			expectedHTTPStatus:   http.StatusRequestEntityTooLarge,
			expectedErrorMessage: errMsg,
		},
	}
	for _, tc := range parserTestCases {
		t.Run(tc.name, func(t *testing.T) {
			parserFunc := func(context.Context, *http.Request, int, []byte, *mimirpb.PreallocWriteRequest, log.Logger) ([]byte, error) {
				return nil, tc.err
			}
			pushFunc := func(ctx context.Context, req *Request) error {
				_, err := req.WriteRequest() // just read the body so we can trigger the parser
				return err
			}

			h := handler(10, nil, false, nil, pushFunc, log.NewNopLogger(), parserFunc)

			recorder := httptest.NewRecorder()
			h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/push", bufCloser{&bytes.Buffer{}}))

			assert.Equal(t, tc.expectedHTTPStatus, recorder.Code)
			assert.Equal(t, fmt.Sprintf("%s\n", tc.expectedErrorMessage), recorder.Body.String())
		})
	}

	testCases := []struct {
		name                        string
		err                         error
		expectedHTTPStatus          int
		expectedErrorMessage        string
		expectedDoNotLogErrorHeader bool
	}{
		{
			name:               "no error during push gets translated into a HTTP 200",
			err:                nil,
			expectedHTTPStatus: http.StatusOK,
		},
		{
			name:                 "a generic error during push gets a HTTP 500 without DoNotLogError header",
			err:                  fmt.Errorf(errMsg),
			expectedHTTPStatus:   http.StatusInternalServerError,
			expectedErrorMessage: errMsg,
		},
		{
			name:                        "a DoNotLogError of a generic error during push gets a HTTP 500 with DoNotLogError header",
			err:                         middleware.DoNotLogError{Err: fmt.Errorf(errMsg)},
			expectedHTTPStatus:          http.StatusInternalServerError,
			expectedErrorMessage:        errMsg,
			expectedDoNotLogErrorHeader: true,
		},
		{
			name:                 "a gRPC error with a status during push gets translated into HTTP error without DoNotLogError header",
			err:                  httpgrpc.Errorf(http.StatusRequestEntityTooLarge, errMsg),
			expectedHTTPStatus:   http.StatusRequestEntityTooLarge,
			expectedErrorMessage: errMsg,
		},
		{
			name:                        "a DoNotLogError of a gRPC error with a status during push gets translated into HTTP error without DoNotLogError header",
			err:                         middleware.DoNotLogError{Err: httpgrpc.Errorf(http.StatusRequestEntityTooLarge, errMsg)},
			expectedHTTPStatus:          http.StatusRequestEntityTooLarge,
			expectedErrorMessage:        errMsg,
			expectedDoNotLogErrorHeader: true,
		},
		{
			name:                 "a context.Canceled error during push gets translated into a HTTP 499",
			err:                  context.Canceled,
			expectedHTTPStatus:   statusClientClosedRequest,
			expectedErrorMessage: context.Canceled.Error(),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parserFunc := func(context.Context, *http.Request, int, []byte, *mimirpb.PreallocWriteRequest, log.Logger) ([]byte, error) {
				return nil, nil
			}
			pushFunc := func(ctx context.Context, req *Request) error {
				_, err := req.WriteRequest() // just read the body so we can trigger the parser
				if err != nil {
					return err
				}
				return tc.err
			}

			h := handler(10, nil, false, nil, pushFunc, log.NewNopLogger(), parserFunc)

			recorder := httptest.NewRecorder()
			h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/push", bufCloser{&bytes.Buffer{}}))

			assert.Equal(t, tc.expectedHTTPStatus, recorder.Code)
			if tc.err != nil {
				assert.Equal(t, fmt.Sprintf("%s\n", tc.expectedErrorMessage), recorder.Body.String())
			}
			header := recorder.Header().Get(server.DoNotLogErrorHeaderKey)
			if tc.expectedDoNotLogErrorHeader {
				require.Equal(t, "true", header)
			} else {
				require.Equal(t, "", header)
			}
		})
	}
}

func TestHandler_ToHTTPStatus(t *testing.T) {
	userID := "user"
	originalMsg := "this is an error"
	originalErr := errors.New(originalMsg)
	replicasNotMatchErr := newReplicasDidNotMatchError("a", "b")
	tooManyClustersErr := newTooManyClustersError(10)
	ingestionRateLimitedErr := newIngestionRateLimitedError(10, 10)
	requestRateLimitedErr := newRequestRateLimitedError(10, 10)

	type testStruct struct {
		err                         error
		serviceOverloadErrorEnabled bool
		expectedHTTPStatus          int
		expectedErrorMsg            string
	}
	testCases := map[string]testStruct{
		"a generic error gets translated into a HTTP 500": {
			err:                originalErr,
			expectedHTTPStatus: http.StatusInternalServerError,
			expectedErrorMsg:   originalMsg,
		},
		"a DoNotLog of a generic error gets translated into a HTTP 500": {
			err:                middleware.DoNotLogError{Err: originalErr},
			expectedHTTPStatus: http.StatusInternalServerError,
			expectedErrorMsg:   originalMsg,
		},
		"a context.DeadlineExceeded gets translated into a HTTP 500": {
			err:                context.DeadlineExceeded,
			expectedHTTPStatus: http.StatusInternalServerError,
			expectedErrorMsg:   context.DeadlineExceeded.Error(),
		},
		"a replicasDidNotMatchError gets translated into an HTTP 202": {
			err:                replicasNotMatchErr,
			expectedHTTPStatus: http.StatusAccepted,
			expectedErrorMsg:   replicasNotMatchErr.Error(),
		},
		"a DoNotLogError of a replicasDidNotMatchError gets translated into an HTTP 202": {
			err:                middleware.DoNotLogError{Err: replicasNotMatchErr},
			expectedHTTPStatus: http.StatusAccepted,
			expectedErrorMsg:   replicasNotMatchErr.Error(),
		},
		"a tooManyClustersError gets translated into an HTTP 400": {
			err:                tooManyClustersErr,
			expectedHTTPStatus: http.StatusBadRequest,
			expectedErrorMsg:   tooManyClustersErr.Error(),
		},
		"a DoNotLogError of a tooManyClustersError gets translated into an HTTP 400": {
			err:                middleware.DoNotLogError{Err: tooManyClustersErr},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedErrorMsg:   tooManyClustersErr.Error(),
		},
		"a validationError gets translated into an HTTP 400": {
			err:                newValidationError(originalErr),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedErrorMsg:   originalMsg,
		},
		"a DoNotLogError of a validationError gets translated into an HTTP 400": {
			err:                middleware.DoNotLogError{Err: newValidationError(originalErr)},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedErrorMsg:   originalMsg,
		},
		"an ingestionRateLimitedError gets translated into an HTTP 429": {
			err:                ingestionRateLimitedErr,
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedErrorMsg:   ingestionRateLimitedErr.Error(),
		},
		"a DoNotLogError of an ingestionRateLimitedError gets translated into an HTTP 429": {
			err:                middleware.DoNotLogError{Err: ingestionRateLimitedErr},
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedErrorMsg:   ingestionRateLimitedErr.Error(),
		},
		"a requestRateLimitedError with serviceOverloadErrorEnabled gets translated into an HTTP 529": {
			err:                         requestRateLimitedErr,
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
			expectedErrorMsg:            requestRateLimitedErr.Error(),
		},
		"a DoNotLogError of a requestRateLimitedError with serviceOverloadErrorEnabled gets translated into an HTTP 529": {
			err:                         middleware.DoNotLogError{Err: requestRateLimitedErr},
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
			expectedErrorMsg:            requestRateLimitedErr.Error(),
		},
		"a requestRateLimitedError without serviceOverloadErrorEnabled gets translated into an HTTP 429": {
			err:                         requestRateLimitedErr,
			serviceOverloadErrorEnabled: false,
			expectedHTTPStatus:          http.StatusTooManyRequests,
			expectedErrorMsg:            requestRateLimitedErr.Error(),
		},
		"a DoNotLogError of a requestRateLimitedError without serviceOverloadErrorEnabled gets translated into an HTTP 429": {
			err:                         middleware.DoNotLogError{Err: requestRateLimitedErr},
			serviceOverloadErrorEnabled: false,
			expectedHTTPStatus:          http.StatusTooManyRequests,
			expectedErrorMsg:            requestRateLimitedErr.Error(),
		},
		"an ingesterPushError with BAD_DATA cause gets translated into an HTTP 400": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.BAD_DATA)),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedErrorMsg:   fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
		},
		"a DoNotLogError of an ingesterPushError with BAD_DATA cause gets translated into an HTTP 400": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.FailedPrecondition, originalMsg, mimirpb.BAD_DATA))},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedErrorMsg:   fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
		},
		"an ingesterPushError with TSDB_UNAVAILABLE cause gets translated into an HTTP 503": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.TSDB_UNAVAILABLE)),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedErrorMsg:   fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
		},
		"a DoNotLogError of an ingesterPushError with TSDB_UNAVAILABLE cause gets translated into an HTTP 503": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.TSDB_UNAVAILABLE))},
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedErrorMsg:   fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
		},
		"an ingesterPushError with SERVICE_UNAVAILABLE cause gets translated into an HTTP 500": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.SERVICE_UNAVAILABLE)),
			expectedHTTPStatus: http.StatusInternalServerError,
			expectedErrorMsg:   fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
		},
		"a DoNotLogError of an ingesterPushError with SERVICE_UNAVAILABLE cause gets translated into an HTTP 500": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.SERVICE_UNAVAILABLE))},
			expectedHTTPStatus: http.StatusInternalServerError,
			expectedErrorMsg:   fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
		},
		"an ingesterPushError with INSTANCE_LIMIT cause gets translated into an HTTP 500": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.INSTANCE_LIMIT)),
			expectedHTTPStatus: http.StatusInternalServerError,
			expectedErrorMsg:   fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
		},
		"a DoNotLogError of an ingesterPushError with INSTANCE_LIMIT cause gets translated into an HTTP 500": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.INSTANCE_LIMIT))},
			expectedHTTPStatus: http.StatusInternalServerError,
			expectedErrorMsg:   fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
		},
		"an ingesterPushError with UNKNOWN_CAUSE cause gets translated into an HTTP 500": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.UNKNOWN_CAUSE)),
			expectedHTTPStatus: http.StatusInternalServerError,
			expectedErrorMsg:   fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
		},
		"a DoNotLogError of an ingesterPushError with UNKNOWN_CAUSE cause gets translated into an HTTP 500": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.UNKNOWN_CAUSE))},
			expectedHTTPStatus: http.StatusInternalServerError,
			expectedErrorMsg:   fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
		},
		"an ingesterPushError obtained from a DeadlineExceeded coming from the ingester gets translated into an HTTP 500": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, context.DeadlineExceeded.Error(), mimirpb.UNKNOWN_CAUSE)),
			expectedHTTPStatus: http.StatusInternalServerError,
			expectedErrorMsg:   fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, context.DeadlineExceeded),
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), userID)

			tenantLimits := map[string]*validation.Limits{
				userID: {
					ServiceOverloadStatusCodeOnRateLimitEnabled: tc.serviceOverloadErrorEnabled,
				},
			}
			limits, err := validation.NewOverrides(
				validation.Limits{},
				validation.NewMockTenantLimits(tenantLimits),
			)
			require.NoError(t, err)

			status := toHTTPStatus(ctx, tc.err, limits)
			msg := tc.err.Error()
			assert.Equal(t, tc.expectedHTTPStatus, status)
			assert.Equal(t, tc.expectedErrorMsg, msg)
		})
	}
}
