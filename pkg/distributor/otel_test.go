// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/user"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	prometheustranslator "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"golang.org/x/exp/slices"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestOTelMetricsToTimeSeries(t *testing.T) {
	const tenantID = "testTenant"
	discardedDueToOTelParseError := promauto.With(nil).NewCounterVec(prometheus.CounterOpts{
		Name: "discarded_due_to_otel_parse_error",
		Help: "Number of metrics discarded due to OTLP parse errors.",
	}, []string{tenantID, "group"})
	resourceAttrs := map[string]string{
		"service.name":        "service name",
		"service.instance.id": "service ID",
		"existent-attr":       "resource value",
		// This one is for testing conflict with metric attribute.
		"metric-attr": "resource value",
		// This one is for testing conflict with auto-generated job attribute.
		"job": "resource value",
		// This one is for testing conflict with auto-generated instance attribute.
		"instance": "resource value",
	}
	expTargetInfoLabels := []mimirpb.LabelAdapter{
		{
			Name:  labels.MetricName,
			Value: "target_info",
		},
	}
	for k, v := range resourceAttrs {
		switch k {
		case "service.name":
			k = "job"
		case "service.instance.id":
			k = "instance"
		case "job", "instance":
			// Ignore, as these labels are generated from service.name and service.instance.id
			continue
		default:
			k = prometheustranslator.NormalizeLabel(k)
		}
		expTargetInfoLabels = append(expTargetInfoLabels, mimirpb.LabelAdapter{
			Name:  k,
			Value: v,
		})
	}
	slices.SortStableFunc(expTargetInfoLabels, func(a, b mimirpb.LabelAdapter) int {
		return strings.Compare(a.Name, b.Name)
	})

	md := pmetric.NewMetrics()
	{
		rm := md.ResourceMetrics().AppendEmpty()
		for k, v := range resourceAttrs {
			rm.Resource().Attributes().PutStr(k, v)
		}
		il := rm.ScopeMetrics().AppendEmpty()
		m := il.Metrics().AppendEmpty()
		m.SetName("test_metric")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(123)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.Attributes().PutStr("metric-attr", "metric value")
	}

	testCases := []struct {
		name                      string
		promoteResourceAttributes []string
		expectedLabels            []mimirpb.LabelAdapter
	}{
		{
			name:                      "Successful conversion without resource attribute promotion",
			promoteResourceAttributes: nil,
			expectedLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
			},
		},
		{
			name:                      "Successful conversion with resource attribute promotion",
			promoteResourceAttributes: []string{"non-existent-attr", "existent-attr"},
			expectedLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
			},
		},
		{
			name:                      "Successful conversion with resource attribute promotion, conflicting resource attributes are ignored",
			promoteResourceAttributes: []string{"non-existent-attr", "existent-attr", "metric-attr", "job", "instance"},
			expectedLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mimirTS, err := otelMetricsToTimeseries(
				context.Background(), tenantID, true, false, tc.promoteResourceAttributes, discardedDueToOTelParseError, log.NewNopLogger(), md,
			)
			require.NoError(t, err)
			require.Len(t, mimirTS, 2)
			var ts mimirpb.PreallocTimeseries
			var targetInfo mimirpb.PreallocTimeseries
			for i := range mimirTS {
				for _, lbl := range mimirTS[i].Labels {
					if lbl.Name != labels.MetricName {
						continue
					}

					if lbl.Value == "target_info" {
						targetInfo = mimirTS[i]
					} else {
						ts = mimirTS[i]
					}
				}
			}

			assert.ElementsMatch(t, ts.Labels, tc.expectedLabels)
			assert.ElementsMatch(t, targetInfo.Labels, expTargetInfoLabels)
		})
	}
}

func BenchmarkOTLPHandler(b *testing.B) {
	var samples []prompb.Sample
	for i := 0; i < 1000; i++ {
		ts := time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Second)
		samples = append(samples, prompb.Sample{
			Value:     1,
			Timestamp: ts.UnixNano(),
		})
	}
	sampleSeries := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "foo"},
			},
			Samples: samples,
			Histograms: []prompb.Histogram{
				prompb.FromIntHistogram(1337, test.GenerateTestHistogram(1)),
			},
		},
	}
	// Sample metadata needs to correspond to every series in the sampleSeries
	sampleMetadata := []mimirpb.MetricMetadata{
		{
			Help: "metric_help",
			Unit: "metric_unit",
		},
	}
	exportReq := TimeseriesToOTLPRequest(sampleSeries, sampleMetadata)

	pushFunc := func(_ context.Context, pushReq *Request) error {
		if _, err := pushReq.WriteRequest(); err != nil {
			return err
		}

		pushReq.CleanUp()
		return nil
	}
	limits, err := validation.NewOverrides(
		validation.Limits{},
		validation.NewMockTenantLimits(map[string]*validation.Limits{}),
	)
	require.NoError(b, err)
	handler := OTLPHandler(100000, nil, nil, limits, nil, RetryConfig{}, pushFunc, nil, nil, log.NewNopLogger())

	b.Run("protobuf", func(b *testing.B) {
		req := createOTLPProtoRequest(b, exportReq, "")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(b, http.StatusOK, resp.Code)
			req.Body.(*reusableReader).Reset()
		}
	})

	b.Run("JSON", func(b *testing.B) {
		req := createOTLPJSONRequest(b, exportReq, "")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(b, http.StatusOK, resp.Code)
			req.Body.(*reusableReader).Reset()
		}
	})
}

func createOTLPProtoRequest(tb testing.TB, metricRequest pmetricotlp.ExportRequest, compression string) *http.Request {
	tb.Helper()

	body, err := metricRequest.MarshalProto()
	require.NoError(tb, err)

	return createOTLPRequest(tb, body, compression, "application/x-protobuf")
}

func createOTLPJSONRequest(tb testing.TB, metricRequest pmetricotlp.ExportRequest, compression string) *http.Request {
	tb.Helper()

	body, err := metricRequest.MarshalJSON()
	require.NoError(tb, err)

	return createOTLPRequest(tb, body, compression, "application/json")
}

func createOTLPRequest(tb testing.TB, body []byte, compression, contentType string) *http.Request {
	tb.Helper()

	switch compression {
	case "gzip":
		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		_, err := gz.Write(body)
		require.NoError(tb, err)
		require.NoError(tb, gz.Close())

		body = b.Bytes()
	case "lz4":
		var b bytes.Buffer
		lz4Writer := lz4.NewWriter(&b)
		_, err := lz4Writer.Write(body)
		require.NoError(tb, err)
		require.NoError(tb, lz4Writer.Close())

		body = b.Bytes()
	}

	// reusableReader is suitable for benchmarks
	req, err := http.NewRequest("POST", "http://localhost/", newReusableReader(body))
	require.NoError(tb, err)
	// Since http.NewRequest will deduce content length only from known io.Reader implementations,
	// define it ourselves
	req.ContentLength = int64(len(body))
	req.Header.Set("Content-Type", contentType)
	const tenantID = "test"
	req.Header.Set("X-Scope-OrgID", tenantID)
	ctx := user.InjectOrgID(context.Background(), tenantID)
	req = req.WithContext(ctx)
	if compression != "" {
		req.Header.Set("Content-Encoding", compression)
	}

	return req
}

type reusableReader struct {
	*bytes.Reader
	raw []byte
}

func newReusableReader(raw []byte) *reusableReader {
	return &reusableReader{
		Reader: bytes.NewReader(raw),
		raw:    raw,
	}
}

func (r *reusableReader) Close() error {
	return nil
}

func (r *reusableReader) Reset() {
	r.Reader.Reset(r.raw)
}

var _ io.ReadCloser = &reusableReader{}

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

	type testCase struct {
		name                             string
		series                           []prompb.TimeSeries
		metadata                         []mimirpb.MetricMetadata
		compression                      string
		maxMsgSize                       int
		verifyFunc                       func(*testing.T, *Request, testCase) error
		responseCode                     int
		errMessage                       string
		expectedLogs                     []string
		expectedRetryHeader              bool
		promoteResourceAttributes        []string
		expectedAttributePromotions      map[string]string
		resourceAttributePromotionConfig OTelResourceAttributePromotionConfig
	}

	samplesVerifierFunc := func(t *testing.T, pushReq *Request, tc testCase) error {
		t.Helper()

		request, err := pushReq.WriteRequest()
		require.NoError(t, err)

		series := request.Timeseries
		require.Len(t, series, 1)

		for name, value := range tc.expectedAttributePromotions {
			require.Truef(t, slices.ContainsFunc(series[0].Labels, func(l mimirpb.LabelAdapter) bool {
				return l.Name == name && l.Value == value
			}), "OTel resource attribute should have been promoted to label %s=%s", name, value)
		}

		samples := series[0].Samples
		require.Len(t, samples, 1)
		assert.Equal(t, float64(1), samples[0].Value)
		assert.Equal(t, "__name__", series[0].Labels[0].Name)
		assert.Equal(t, "foo", series[0].Labels[0].Value)

		metadata := request.Metadata
		require.Len(t, metadata, 1)
		assert.Equal(t, mimirpb.GAUGE, metadata[0].GetType())
		assert.Equal(t, "foo", metadata[0].GetMetricFamilyName())
		assert.Equal(t, "metric_help", metadata[0].GetHelp())
		assert.Equal(t, "metric_unit", metadata[0].GetUnit())

		return nil
	}

	tests := []testCase{
		{
			name:         "Write samples. No compression",
			maxMsgSize:   100000,
			verifyFunc:   samplesVerifierFunc,
			series:       sampleSeries,
			metadata:     sampleMetadata,
			responseCode: http.StatusOK,
		},
		{
			name:                        "Write samples. No compression. Resource attribute promotion.",
			maxMsgSize:                  100000,
			verifyFunc:                  samplesVerifierFunc,
			series:                      sampleSeries,
			metadata:                    sampleMetadata,
			responseCode:                http.StatusOK,
			promoteResourceAttributes:   []string{"resource.attr"},
			expectedAttributePromotions: map[string]string{"resource_attr": "value"},
		},
		{
			name:                      "Write samples. No compression. Specialized resource attribute promotion.",
			maxMsgSize:                100000,
			verifyFunc:                samplesVerifierFunc,
			series:                    sampleSeries,
			metadata:                  sampleMetadata,
			responseCode:              http.StatusOK,
			promoteResourceAttributes: nil,
			resourceAttributePromotionConfig: fakeResourceAttributePromotionConfig{
				promote: []string{"resource.attr"},
			},
			expectedAttributePromotions: map[string]string{"resource_attr": "value"},
		},
		{
			name:         "Write samples. With gzip compression",
			compression:  "gzip",
			maxMsgSize:   100000,
			verifyFunc:   samplesVerifierFunc,
			series:       sampleSeries,
			metadata:     sampleMetadata,
			responseCode: http.StatusOK,
		},
		{
			name:         "Write samples. With lz4 compression",
			compression:  "lz4",
			maxMsgSize:   100000,
			verifyFunc:   samplesVerifierFunc,
			series:       sampleSeries,
			metadata:     sampleMetadata,
			responseCode: http.StatusOK,
		},
		{
			name:       "Write samples. No compression, request too big",
			maxMsgSize: 30,
			series:     sampleSeries,
			metadata:   sampleMetadata,
			verifyFunc: func(_ *testing.T, pushReq *Request, _ testCase) error {
				_, err := pushReq.WriteRequest()
				return err
			},
			responseCode: http.StatusRequestEntityTooLarge,
			errMessage:   "the incoming OTLP request has been rejected because its message size of 89 bytes is larger",
			expectedLogs: []string{`level=error user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=413 err="rpc error: code = Code(413) desc = the incoming OTLP request has been rejected because its message size of 89 bytes is larger than the allowed limit of 30 bytes (err-mimir-distributor-max-otlp-request-size). To adjust the related limit, configure -distributor.max-otlp-request-size, or contact your service administrator." insight=true`},
		},
		{
			name:        "Write samples. Unsupported compression",
			compression: "snappy",
			maxMsgSize:  100000,
			series:      sampleSeries,
			metadata:    sampleMetadata,
			verifyFunc: func(_ *testing.T, pushReq *Request, _ testCase) error {
				_, err := pushReq.WriteRequest()
				return err
			},
			responseCode: http.StatusUnsupportedMediaType,
			errMessage:   "Only \"gzip\", \"lz4\", or no compression supported",
			expectedLogs: []string{`level=error user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=415 err="rpc error: code = Code(415) desc = unsupported compression: snappy. Only \"gzip\", \"lz4\", or no compression supported" insight=true`},
		},
		{
			name:        "Write samples. With gzip compression, request too big",
			compression: "gzip",
			maxMsgSize:  30,
			series:      sampleSeries,
			metadata:    sampleMetadata,
			verifyFunc: func(_ *testing.T, pushReq *Request, _ testCase) error {
				_, err := pushReq.WriteRequest()
				return err
			},
			responseCode: http.StatusRequestEntityTooLarge,
			errMessage:   "the incoming OTLP request has been rejected because its message size of 104 bytes is larger",
			expectedLogs: []string{`level=error user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=413 err="rpc error: code = Code(413) desc = the incoming OTLP request has been rejected because its message size of 104 bytes is larger than the allowed limit of 30 bytes (err-mimir-distributor-max-otlp-request-size). To adjust the related limit, configure -distributor.max-otlp-request-size, or contact your service administrator." insight=true`},
		},
		{
			name:        "Write samples. With lz4 compression, request too big",
			compression: "lz4",
			maxMsgSize:  30,
			series:      sampleSeries,
			metadata:    sampleMetadata,
			verifyFunc: func(_ *testing.T, pushReq *Request, _ testCase) error {
				_, err := pushReq.WriteRequest()
				return err
			},
			responseCode: http.StatusRequestEntityTooLarge,
			errMessage:   "the incoming OTLP request has been rejected because its message size of 106 bytes is larger",
			expectedLogs: []string{`level=error user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=413 err="rpc error: code = Code(413) desc = the incoming OTLP request has been rejected because its message size of 106 bytes is larger than the allowed limit of 30 bytes (err-mimir-distributor-max-otlp-request-size). To adjust the related limit, configure -distributor.max-otlp-request-size, or contact your service administrator." insight=true`},
		},
		{
			name:       "Rate limited request",
			maxMsgSize: 100000,
			series:     sampleSeries,
			metadata:   sampleMetadata,
			verifyFunc: func(*testing.T, *Request, testCase) error {
				return httpgrpc.Errorf(http.StatusTooManyRequests, "go slower")
			},
			responseCode:        http.StatusTooManyRequests,
			errMessage:          "go slower",
			expectedLogs:        []string{`level=error user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=429 err="rpc error: code = Code(429) desc = go slower" insight=true`},
			expectedRetryHeader: true,
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
						prompb.FromIntHistogram(1337, test.GenerateTestHistogram(1)),
					},
				},
			},
			metadata: []mimirpb.MetricMetadata{
				{
					Help: "metric_help",
					Unit: "metric_unit",
				},
			},
			verifyFunc: func(t *testing.T, pushReq *Request, _ testCase) error {
				request, err := pushReq.WriteRequest()
				require.NoError(t, err)

				series := request.Timeseries
				require.Len(t, series, 1)

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
			responseCode: http.StatusOK,
		},
		{
			name:        "Write histograms. With lz4 compression",
			compression: "lz4",
			maxMsgSize:  100000,
			series: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "foo"},
					},
					Histograms: []prompb.Histogram{
						prompb.FromIntHistogram(1337, test.GenerateTestHistogram(1)),
					},
				},
			},
			metadata: []mimirpb.MetricMetadata{
				{
					Help: "metric_help",
					Unit: "metric_unit",
				},
			},
			verifyFunc: func(t *testing.T, pushReq *Request, _ testCase) error {
				request, err := pushReq.WriteRequest()
				require.NoError(t, err)

				series := request.Timeseries
				require.Len(t, series, 1)

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
			responseCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exportReq := TimeseriesToOTLPRequest(tt.series, tt.metadata)
			req := createOTLPProtoRequest(t, exportReq, tt.compression)

			testLimits := &validation.Limits{
				PromoteOTelResourceAttributes: tt.promoteResourceAttributes,
			}
			limits, err := validation.NewOverrides(
				validation.Limits{},
				validation.NewMockTenantLimits(map[string]*validation.Limits{
					"test": testLimits,
				}),
			)
			require.NoError(t, err)
			pusher := func(_ context.Context, pushReq *Request) error {
				t.Helper()
				t.Cleanup(pushReq.CleanUp)
				return tt.verifyFunc(t, pushReq, tt)
			}

			logs := &concurrency.SyncBuffer{}
			retryConfig := RetryConfig{Enabled: true, MinBackoff: 5 * time.Second, MaxBackoff: 5 * time.Second}
			handler := OTLPHandler(tt.maxMsgSize, nil, nil, limits, tt.resourceAttributePromotionConfig, retryConfig, pusher, nil, nil, level.NewFilter(log.NewLogfmtLogger(logs), level.AllowInfo()))

			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)

			assert.Equal(t, tt.responseCode, resp.Code)
			if tt.errMessage != "" {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				respStatus := &status.Status{}
				err = proto.Unmarshal(body, respStatus)
				assert.NoError(t, err)
				assert.Contains(t, respStatus.GetMessage(), tt.errMessage)
			}

			var logLines []string
			if logsStr := logs.String(); logsStr != "" {
				logLines = strings.Split(strings.TrimSpace(logsStr), "\n")
			}
			assert.Equal(t, tt.expectedLogs, logLines)

			retryAfter := resp.Header().Get("Retry-After")
			assert.Equal(t, tt.expectedRetryHeader, retryAfter != "")
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

	limits, err := validation.NewOverrides(
		validation.Limits{},
		validation.NewMockTenantLimits(map[string]*validation.Limits{}),
	)
	require.NoError(t, err)

	req := createOTLPProtoRequest(t, pmetricotlp.NewExportRequestFromMetrics(md), "")
	resp := httptest.NewRecorder()
	handler := OTLPHandler(100000, nil, nil, limits, nil, RetryConfig{}, func(_ context.Context, pushReq *Request) error {
		request, err := pushReq.WriteRequest()
		assert.NoError(t, err)
		assert.Len(t, request.Timeseries, 3)
		assert.False(t, request.SkipLabelValidation)
		pushReq.CleanUp()
		return nil
	}, nil, nil, log.NewNopLogger())
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

	limits, err := validation.NewOverrides(
		validation.Limits{},
		validation.NewMockTenantLimits(map[string]*validation.Limits{}),
	)
	require.NoError(t, err)

	req := createOTLPProtoRequest(t, pmetricotlp.NewExportRequestFromMetrics(md), "")
	resp := httptest.NewRecorder()
	handler := OTLPHandler(100000, nil, nil, limits, nil, RetryConfig{}, func(_ context.Context, pushReq *Request) error {
		request, err := pushReq.WriteRequest()
		t.Cleanup(pushReq.CleanUp)
		require.NoError(t, err)
		assert.Len(t, request.Timeseries, 1)
		assert.False(t, request.SkipLabelValidation)
		return nil
	}, nil, nil, log.NewNopLogger())
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

	req = createOTLPProtoRequest(t, pmetricotlp.NewExportRequestFromMetrics(md), "")
	resp = httptest.NewRecorder()
	handler = OTLPHandler(100000, nil, nil, limits, nil, RetryConfig{}, func(_ context.Context, pushReq *Request) error {
		request, err := pushReq.WriteRequest()
		t.Cleanup(pushReq.CleanUp)
		require.NoError(t, err)
		assert.Len(t, request.Timeseries, 9) // 6 buckets (including +Inf) + 2 sum/count + 2 from the first case
		assert.False(t, request.SkipLabelValidation)
		return nil
	}, nil, nil, log.NewNopLogger())
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestHandler_otlpWriteRequestTooBigWithCompression(t *testing.T) {
	// createOTLPProtoRequest will create a request which is BIGGER with compression (37 vs 58 bytes).
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

	handler := OTLPHandler(140, nil, nil, nil, nil, RetryConfig{}, readBodyPushFunc(t), nil, nil, log.NewNopLogger())
	handler.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.Code)
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	respStatus := &status.Status{}
	err = proto.Unmarshal(body, respStatus)
	assert.NoError(t, err)
	assert.Contains(t, respStatus.GetMessage(), "the incoming OTLP request has been rejected because its message size is larger than the allowed limit of 140 bytes (err-mimir-distributor-max-otlp-request-size). To adjust the related limit, configure -distributor.max-otlp-request-size, or contact your service administrator.")
}

func TestHandler_toOtlpGRPCHTTPStatus(t *testing.T) {
	const (
		ingesterID  = "ingester-25"
		originalMsg = "this is an error"
	)
	originalErr := errors.New(originalMsg)
	replicasNotMatchErr := newReplicasDidNotMatchError("a", "b")
	tooManyClustersErr := newTooManyClustersError(10)
	ingestionRateLimitedErr := newIngestionRateLimitedError(10, 10)

	type testStruct struct {
		err                error
		expectedHTTPStatus int
		expectedGRPCStatus codes.Code
	}
	testCases := map[string]testStruct{
		"a generic error gets translated into gRPC code.Internal and HTTP 503 statuses": {
			err:                originalErr,
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
		},
		"a DoNotLog of a generic error gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                middleware.DoNotLogError{Err: originalErr},
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
		},
		"a context.DeadlineExceeded gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                context.DeadlineExceeded,
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
		},
		"a replicasDidNotMatchError gets translated into gRPC codes.AlreadyExists and HTTP 202 statuses": {
			err:                replicasNotMatchErr,
			expectedHTTPStatus: http.StatusAccepted,
			expectedGRPCStatus: codes.AlreadyExists,
		},
		"a DoNotLogError of a replicasDidNotMatchError gets translated into gRPC codes.AlreadyExists and HTTP 202 statuses": {
			err:                middleware.DoNotLogError{Err: replicasNotMatchErr},
			expectedHTTPStatus: http.StatusAccepted,
			expectedGRPCStatus: codes.AlreadyExists,
		},
		"a tooManyClustersError gets translated into gRPC codes.FailedPrecondition and HTTP 400 statuses": {
			err:                tooManyClustersErr,
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.FailedPrecondition,
		},
		"a DoNotLogError of a tooManyClustersError gets translated into gRPC codes.FailedPrecondition and HTTP 400 statuses": {
			err:                middleware.DoNotLogError{Err: tooManyClustersErr},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.FailedPrecondition,
		},
		"a validationError gets translated into gRPC codes.InvalidArgument and HTTP 400 statuses": {
			err:                newValidationError(originalErr),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.InvalidArgument,
		},
		"a DoNotLogError of a validationError gets translated into gRPC codes.InvalidArgument and HTTP 400 statuses": {
			err:                middleware.DoNotLogError{Err: newValidationError(originalErr)},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.InvalidArgument,
		},
		"an ingestionRateLimitedError gets translated into gRPC codes.ResourceExhausted and HTTP 429 statuses": {
			err:                ingestionRateLimitedErr,
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedGRPCStatus: codes.ResourceExhausted,
		},
		"a DoNotLogError of an ingestionRateLimitedError gets translated into gRPC codes.ResourceExhausted and HTTP 429 statuses": {
			err:                middleware.DoNotLogError{Err: ingestionRateLimitedErr},
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedGRPCStatus: codes.ResourceExhausted,
		},
		"an ingesterPushError with BAD_DATA cause gets translated into gRPC codes.InvalidArgument and HTTP 400 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.InvalidArgument, originalMsg, mimirpb.BAD_DATA), ingesterID),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.InvalidArgument,
		},
		"a DoNotLogError of an ingesterPushError with BAD_DATA cause gets translated into gRPC codes.InvalidArgument and HTTP 400 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.InvalidArgument, originalMsg, mimirpb.BAD_DATA), ingesterID)},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.InvalidArgument,
		},
		"an ingesterPushError with TENANT_LIMIT cause gets translated into gRPC codes.FailedPrecondition and HTTP 400 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.FailedPrecondition, originalMsg, mimirpb.TENANT_LIMIT), ingesterID),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.FailedPrecondition,
		},
		"a DoNotLogError of an ingesterPushError with TENANT_LIMIT cause gets translated into gRPC codes.FailedPrecondition and HTTP 400 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.FailedPrecondition, originalMsg, mimirpb.TENANT_LIMIT), ingesterID)},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.FailedPrecondition,
		},
		"an ingesterPushError with METHOD_NOT_ALLOWED cause gets translated into gRPC codes.Unimplemented and HTTP 503 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unimplemented, originalMsg, mimirpb.METHOD_NOT_ALLOWED), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Unimplemented,
		},
		"a DoNotLogError of an ingesterPushError with METHOD_NOT_ALLOWED cause gets translated into gRPC codes.Unimplemented and HTTP 503 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unimplemented, originalMsg, mimirpb.METHOD_NOT_ALLOWED), ingesterID)},
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Unimplemented,
		},
		"an ingesterPushError with TSDB_UNAVAILABLE cause gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.TSDB_UNAVAILABLE), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
		},
		"a DoNotLogError of an ingesterPushError with TSDB_UNAVAILABLE cause gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.TSDB_UNAVAILABLE), ingesterID)},
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
		},
		"an ingesterPushError with SERVICE_UNAVAILABLE cause gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.SERVICE_UNAVAILABLE), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
		},
		"a DoNotLogError of an ingesterPushError with SERVICE_UNAVAILABLE cause gets translated gRPC codes.Internal and HTTP 503 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.SERVICE_UNAVAILABLE), ingesterID)},
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
		},
		"an ingesterPushError with INSTANCE_LIMIT cause gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.INSTANCE_LIMIT), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
		},
		"a DoNotLogError of an ingesterPushError with INSTANCE_LIMIT cause gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.INSTANCE_LIMIT), ingesterID)},
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
		},
		"an ingesterPushError with UNKNOWN_CAUSE cause gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.UNKNOWN_CAUSE), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
		},
		"a DoNotLogError of an ingesterPushError with UNKNOWN_CAUSE cause gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.UNKNOWN_CAUSE), ingesterID)},
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
		},
		"an ingesterPushError obtained from a DeadlineExceeded coming from the ingester gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, context.DeadlineExceeded.Error(), mimirpb.UNKNOWN_CAUSE), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
		},
		"an ingesterPushError with CIRCUIT_BREAKER_OPEN cause gets translated into an Unavailable error with CIRCUIT_BREAKER_OPEN cause": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.CIRCUIT_BREAKER_OPEN), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Unavailable,
		},
		"a wrapped ingesterPushError with CIRCUIT_BREAKER_OPEN cause gets translated into an Unavailable error with CIRCUIT_BREAKER_OPEN cause": {
			err:                errors.Wrap(newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.CIRCUIT_BREAKER_OPEN), ingesterID), "wrapped"),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Unavailable,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			gStatus, status := toOtlpGRPCHTTPStatus(tc.err)
			assert.Equal(t, tc.expectedHTTPStatus, status)
			assert.Equal(t, tc.expectedGRPCStatus, gStatus)
		})
	}
}

func TestHttpRetryableToOTLPRetryable(t *testing.T) {
	testCases := map[string]struct {
		httpStatusCode             int
		expectedOtlpHTTPStatusCode int
	}{
		"HTTP status codes 2xx gets translated into themselves": {
			httpStatusCode:             http.StatusAccepted,
			expectedOtlpHTTPStatusCode: http.StatusAccepted,
		},
		"HTTP status code 400 gets translated into itself": {
			httpStatusCode:             http.StatusBadRequest,
			expectedOtlpHTTPStatusCode: http.StatusBadRequest,
		},
		"HTTP status code 429 gets translated into itself": {
			httpStatusCode:             http.StatusTooManyRequests,
			expectedOtlpHTTPStatusCode: http.StatusTooManyRequests,
		},
		"HTTP status code 500 gets translated into 503": {
			httpStatusCode:             http.StatusInternalServerError,
			expectedOtlpHTTPStatusCode: http.StatusServiceUnavailable,
		},
		"HTTP status code 501 gets translated into 503": {
			httpStatusCode:             http.StatusNotImplemented,
			expectedOtlpHTTPStatusCode: http.StatusServiceUnavailable,
		},
		"HTTP status code 502 gets translated into itself": {
			httpStatusCode:             http.StatusBadGateway,
			expectedOtlpHTTPStatusCode: http.StatusBadGateway,
		},
		"HTTP status code 503 gets translated into itself": {
			httpStatusCode:             http.StatusServiceUnavailable,
			expectedOtlpHTTPStatusCode: http.StatusServiceUnavailable,
		},
		"HTTP status code 504 gets translated into itself": {
			httpStatusCode:             http.StatusGatewayTimeout,
			expectedOtlpHTTPStatusCode: http.StatusGatewayTimeout,
		},
		"HTTP status code 507 gets translated into 503": {
			httpStatusCode:             http.StatusInsufficientStorage,
			expectedOtlpHTTPStatusCode: http.StatusServiceUnavailable,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			otlpHTTPStatusCode := httpRetryableToOTLPRetryable(testCase.httpStatusCode)
			require.Equal(t, testCase.expectedOtlpHTTPStatusCode, otlpHTTPStatusCode)
		})
	}
}

type fakeResourceAttributePromotionConfig struct {
	promote []string
}

func (c fakeResourceAttributePromotionConfig) PromoteOTelResourceAttributes(string) []string {
	return []string{"resource.attr"}
}
