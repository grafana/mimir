// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/push/push_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/httpgrpc/server"
	"github.com/grafana/dskit/middleware"
	dskit_server "github.com/grafana/dskit/server"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestHandler_remoteWrite(t *testing.T) {
	req := createRequest(t, createPrometheusRemoteWriteProtobuf(t))
	resp := httptest.NewRecorder()
	handler := Handler(100000, nil, nil, false, false, validation.MockDefaultOverrides(), RetryConfig{}, verifyWritePushFunc(t, mimirpb.API), nil, log.NewNopLogger())
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

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

			res := otelMetricsToMetadata(tc.enableSuffixes, otelMetrics)
			assert.Equal(t, sampleMetadata, res)
		})
	}
}

func TestHandler_mimirWriteRequest(t *testing.T) {
	req := createRequest(t, createMimirWriteRequestProtobuf(t, false, false))
	resp := httptest.NewRecorder()
	sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)", false)
	handler := Handler(100000, nil, sourceIPs, false, false, validation.MockDefaultOverrides(), RetryConfig{}, verifyWritePushFunc(t, mimirpb.RULE), nil, log.NewNopLogger())
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestHandler_contextCanceledRequest(t *testing.T) {
	req := createRequest(t, createMimirWriteRequestProtobuf(t, false, false))
	resp := httptest.NewRecorder()
	sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)", false)
	handler := Handler(100000, nil, sourceIPs, false, false, validation.MockDefaultOverrides(), RetryConfig{}, func(_ context.Context, req *Request) error {
		defer req.CleanUp()
		return fmt.Errorf("the request failed: %w", context.Canceled)
	}, nil, log.NewNopLogger())
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
			name:                         "config flag set to false means SkipLabelValidation is false",
			allowSkipLabelNameValidation: false,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, false)),
			verifyReqHandler: func(_ context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelValidation)
				pushReq.CleanUp()
				return nil
			},
			includeAllowSkiplabelNameValidationHeader: true,
			expectedStatusCode:                        http.StatusOK,
		},
		{
			name:                         "config flag set to false means SkipLabelValidation is always false even if write requests sets it to true",
			allowSkipLabelNameValidation: false,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, true)),
			verifyReqHandler: func(_ context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				require.NoError(t, err)
				t.Cleanup(pushReq.CleanUp)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelValidation)
				return nil
			},
			includeAllowSkiplabelNameValidationHeader: true,
			expectedStatusCode:                        http.StatusOK,
		},
		{
			name:                         "config flag set to true but write request set to false means SkipLabelValidation is false",
			allowSkipLabelNameValidation: true,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, false)),
			verifyReqHandler: func(_ context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelValidation)
				pushReq.CleanUp()
				return nil
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:                         "config flag set to true and write request set to true means SkipLabelValidation is true",
			allowSkipLabelNameValidation: true,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, true)),
			verifyReqHandler: func(_ context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.True(t, request.SkipLabelValidation)
				pushReq.CleanUp()
				return nil
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:                         "config flag set to true and write request set to true but header not sent means SkipLabelValidation is false",
			allowSkipLabelNameValidation: true,
			req:                          createRequest(t, createMimirWriteRequestProtobufWithNonSupportedLabelNames(t, true)),
			verifyReqHandler: func(_ context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, "a-label", request.Timeseries[0].Labels[0].Name)
				assert.Equal(t, "value", request.Timeseries[0].Labels[0].Value)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelValidation)
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
			handler := Handler(100000, nil, nil, tc.allowSkipLabelNameValidation, false, validation.MockDefaultOverrides(), RetryConfig{}, tc.verifyReqHandler, nil, log.NewNopLogger())
			if !tc.includeAllowSkiplabelNameValidationHeader {
				tc.req.Header.Set(SkipLabelNameValidationHeader, "true")
			}
			handler.ServeHTTP(resp, tc.req)
			assert.Equal(t, tc.expectedStatusCode, resp.Code)
		})
	}
}

func TestHandler_EnsureSkipLabelCountValidationBehaviour(t *testing.T) {
	tests := []struct {
		name                                       string
		allowSkipLabelCountValidation              bool
		includeAllowSkipLabelCountValidationHeader bool
		req                                        *http.Request
		verifyReqHandler                           PushFunc
		expectedStatusCode                         int
	}{
		{
			name:                          "config flag set to false means SkipLabelCountValidation is false",
			allowSkipLabelCountValidation: false,
			includeAllowSkipLabelCountValidationHeader: true,
			req: createRequest(t, createMimirWriteRequestProtobuf(t, false, false)),
			verifyReqHandler: func(_ context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelCountValidation)
				pushReq.CleanUp()
				return nil
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:                          "config flag set to false means SkipLabelCountValidation is always false even if write requests sets it to true",
			allowSkipLabelCountValidation: false,
			includeAllowSkipLabelCountValidationHeader: true,
			req: createRequest(t, createMimirWriteRequestProtobuf(t, false, true)),
			verifyReqHandler: func(_ context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				require.NoError(t, err)
				t.Cleanup(pushReq.CleanUp)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelCountValidation)
				return nil
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:                          "config flag set to true but write request set to false means SkipLabelCountValidation is false",
			allowSkipLabelCountValidation: true,
			req:                           createRequest(t, createMimirWriteRequestProtobuf(t, false, false)),
			verifyReqHandler: func(_ context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelCountValidation)
				pushReq.CleanUp()
				return nil
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:                          "config flag set to true and write request set to true means SkipLabelCountValidation is true",
			allowSkipLabelCountValidation: true,
			req:                           createRequest(t, createMimirWriteRequestProtobuf(t, false, true)),
			verifyReqHandler: func(_ context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.True(t, request.SkipLabelCountValidation)
				pushReq.CleanUp()
				return nil
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:                          "config flag set to true and write request set to true but header not sent means SkipLabelCountValidation is false",
			allowSkipLabelCountValidation: true,
			includeAllowSkipLabelCountValidationHeader: true,
			req: createRequest(t, createMimirWriteRequestProtobuf(t, false, true)),
			verifyReqHandler: func(_ context.Context, pushReq *Request) error {
				request, err := pushReq.WriteRequest()
				assert.NoError(t, err)
				assert.Len(t, request.Timeseries, 1)
				assert.Equal(t, mimirpb.RULE, request.Source)
				assert.False(t, request.SkipLabelCountValidation)
				pushReq.CleanUp()
				return nil
			},
			expectedStatusCode: http.StatusOK,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := httptest.NewRecorder()
			limits := validation.MockDefaultOverrides()
			handler := Handler(100000, nil, nil, false, tc.allowSkipLabelCountValidation, limits, RetryConfig{}, tc.verifyReqHandler, nil, log.NewNopLogger())
			if !tc.includeAllowSkipLabelCountValidationHeader {
				tc.req.Header.Set(SkipLabelCountValidationHeader, "true")
			}
			handler.ServeHTTP(resp, tc.req)
			assert.Equal(t, tc.expectedStatusCode, resp.Code)
		})
	}
}

func TestHandler_SkipExemplarUnmarshalingBasedOnLimits(t *testing.T) {
	timestampMs := time.Now().UnixMilli()

	tests := []struct {
		name                      string
		submitTimeseries          mimirpb.TimeSeries
		expectTimeseries          mimirpb.TimeSeries
		maxGlobalExemplarsPerUser int
	}{
		{
			name: "request with exemplars and exemplars are enabled",
			submitTimeseries: mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{
					{Name: "label1", Value: "value1"},
				},
				Samples: []mimirpb.Sample{
					{Value: 1, TimestampMs: timestampMs},
				},
				Exemplars: []mimirpb.Exemplar{
					{Labels: []mimirpb.LabelAdapter{{Name: "label1", Value: "value1"}}, Value: 1, TimestampMs: timestampMs},
					{Labels: []mimirpb.LabelAdapter{{Name: "label2", Value: "value2"}}, Value: 2, TimestampMs: timestampMs},
					{Labels: []mimirpb.LabelAdapter{{Name: "label3", Value: "value3"}}, Value: 3, TimestampMs: timestampMs},
				},
				Histograms: []mimirpb.Histogram{{Sum: 1, Schema: 2, ZeroThreshold: 3, ResetHint: 4, Timestamp: 5}},
			},
			maxGlobalExemplarsPerUser: 1, // exemplars are not disabled
			expectTimeseries: mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{
					{Name: "label1", Value: "value1"},
				},
				Samples: []mimirpb.Sample{
					{Value: 1, TimestampMs: timestampMs},
				},
				Exemplars: []mimirpb.Exemplar{
					{Labels: []mimirpb.LabelAdapter{{Name: "label1", Value: "value1"}}, Value: 1, TimestampMs: timestampMs},
					{Labels: []mimirpb.LabelAdapter{{Name: "label2", Value: "value2"}}, Value: 2, TimestampMs: timestampMs},
					{Labels: []mimirpb.LabelAdapter{{Name: "label3", Value: "value3"}}, Value: 3, TimestampMs: timestampMs},
				},
				Histograms: []mimirpb.Histogram{{Sum: 1, Schema: 2, ZeroThreshold: 3, ResetHint: 4, Timestamp: 5}},
			},
		}, {
			name: "request with exemplars and exemplars are disabled",
			submitTimeseries: mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{
					{Name: "label1", Value: "value1"},
				},
				Samples: []mimirpb.Sample{
					{Value: 1, TimestampMs: timestampMs},
				},
				Exemplars: []mimirpb.Exemplar{
					{Labels: []mimirpb.LabelAdapter{{Name: "label1", Value: "value1"}}, Value: 1, TimestampMs: timestampMs},
					{Labels: []mimirpb.LabelAdapter{{Name: "label2", Value: "value2"}}, Value: 2, TimestampMs: timestampMs},
					{Labels: []mimirpb.LabelAdapter{{Name: "label3", Value: "value3"}}, Value: 3, TimestampMs: timestampMs},
				},
				Histograms:                []mimirpb.Histogram{{Sum: 1, Schema: 2, ZeroThreshold: 3, ResetHint: 4, Timestamp: 5}},
				SkipUnmarshalingExemplars: true,
			},
			maxGlobalExemplarsPerUser: 0, // 0 disables exemplars
			expectTimeseries: mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{
					{Name: "label1", Value: "value1"},
				},
				Samples: []mimirpb.Sample{
					{Value: 1, TimestampMs: timestampMs},
				},
				Exemplars:                 []mimirpb.Exemplar{},
				Histograms:                []mimirpb.Histogram{{Sum: 1, Schema: 2, ZeroThreshold: 3, ResetHint: 4, Timestamp: 5}},
				SkipUnmarshalingExemplars: true,
			},
		}, {
			name: "request without exemplars and exemplars are enabled",
			submitTimeseries: mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{
					{Name: "label1", Value: "value1"},
				},
				Samples: []mimirpb.Sample{
					{Value: 1, TimestampMs: timestampMs},
				},
				Exemplars:  []mimirpb.Exemplar{},
				Histograms: []mimirpb.Histogram{{Sum: 1, Schema: 2, ZeroThreshold: 3, ResetHint: 4, Timestamp: 5}},
			},
			maxGlobalExemplarsPerUser: 1, // exemplars are not disabled
			expectTimeseries: mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{
					{Name: "label1", Value: "value1"},
				},
				Samples: []mimirpb.Sample{
					{Value: 1, TimestampMs: timestampMs},
				},
				Exemplars:  []mimirpb.Exemplar{},
				Histograms: []mimirpb.Histogram{{Sum: 1, Schema: 2, ZeroThreshold: 3, ResetHint: 4, Timestamp: 5}},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reqDecoded := mimirpb.WriteRequest{
				Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &tc.submitTimeseries}},
				Source:     mimirpb.RULE,
			}
			reqEncoded, err := reqDecoded.Marshal()
			require.NoError(t, err)
			reqHTTP := createRequest(t, reqEncoded)

			defaults := validation.MockDefaultLimits()
			defaults.MaxGlobalExemplarsPerUser = tc.maxGlobalExemplarsPerUser
			limits, err := validation.NewOverrides(*defaults, nil)
			require.NoError(t, err)

			var gotReqEncoded *Request
			handler := Handler(100000, nil, nil, true, false, limits, RetryConfig{}, func(_ context.Context, pushReq *Request) error {
				gotReqEncoded = pushReq
				return nil
			}, nil, log.NewNopLogger())

			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, reqHTTP)
			require.Equal(t, http.StatusOK, resp.Code)

			gotReq, err := gotReqEncoded.WriteRequest()
			require.NoError(t, err)

			assert.Len(t, gotReq.Timeseries, 1)
			gotTimeseries := *(gotReq.Timeseries[0].TimeSeries)

			if gotTimeseries.Exemplars == nil {
				// To fix equality if the empty slice is nil.
				gotTimeseries.Exemplars = []mimirpb.Exemplar{}
			}

			assert.EqualValues(t, tc.expectTimeseries, gotTimeseries)
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
					prompb.FromIntHistogram(1337, test.GenerateTestHistogram(1))},
			},
		},
	}
	inputBytes, err := input.Marshal()
	require.NoError(t, err)
	return inputBytes
}

func createMimirWriteRequestProtobuf(t *testing.T, skipLabelNameValidation, skipLabelCountValidation bool) []byte {
	t.Helper()
	h := prompb.FromIntHistogram(1337, test.GenerateTestHistogram(1))
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
		Timeseries:               []mimirpb.PreallocTimeseries{ts},
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

func BenchmarkPushHandler(b *testing.B) {
	protobuf := createPrometheusRemoteWriteProtobuf(b)
	buf := bytes.NewBuffer(snappy.Encode(nil, protobuf))
	req := createRequest(b, protobuf)
	pushFunc := func(_ context.Context, pushReq *Request) error {
		if _, err := pushReq.WriteRequest(); err != nil {
			return err
		}
		pushReq.CleanUp()
		return nil
	}
	handler := Handler(100000, nil, nil, false, false, validation.MockDefaultOverrides(), RetryConfig{}, pushFunc, nil, log.NewNopLogger())
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
		expectedLogs         []string
	}{
		{
			name:                 "a generic error during request parsing gets an HTTP 400",
			err:                  errors.New(errMsg),
			expectedHTTPStatus:   http.StatusBadRequest,
			expectedErrorMessage: errMsg,
			expectedLogs:         []string{`level=error user=testuser msg="detected an error while ingesting Prometheus remote-write request (the request may have been partially ingested)" httpCode=400 err="rpc error: code = Code(400) desc = this is an error" insight=true`},
		},
		{
			name:                 "a gRPC error with a status during request parsing gets translated into HTTP error without DoNotLogError header",
			err:                  httpgrpc.Error(http.StatusRequestEntityTooLarge, errMsg),
			expectedHTTPStatus:   http.StatusRequestEntityTooLarge,
			expectedErrorMessage: errMsg,
			expectedLogs:         []string{`level=error user=testuser msg="detected an error while ingesting Prometheus remote-write request (the request may have been partially ingested)" httpCode=413 err="rpc error: code = Code(413) desc = this is an error" insight=true`},
		},
	}
	for _, tc := range parserTestCases {
		t.Run(tc.name, func(t *testing.T) {
			parserFunc := func(context.Context, *http.Request, int, *util.RequestBuffers, *mimirpb.PreallocWriteRequest, log.Logger) error {
				return tc.err
			}
			pushFunc := func(_ context.Context, req *Request) error {
				_, err := req.WriteRequest() // just read the body so we can trigger the parser
				return err
			}

			logs := &concurrency.SyncBuffer{}
			h := handler(10, nil, nil, false, false, validation.MockDefaultOverrides(), RetryConfig{}, pushFunc, log.NewLogfmtLogger(logs), parserFunc)

			recorder := httptest.NewRecorder()
			ctxWithUser := user.InjectOrgID(context.Background(), "testuser")
			h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/push", bufCloser{&bytes.Buffer{}}).WithContext(ctxWithUser))

			assert.Equal(t, tc.expectedHTTPStatus, recorder.Code)
			assert.Equal(t, fmt.Sprintf("%s\n", tc.expectedErrorMessage), recorder.Body.String())

			var logLines []string
			if logsStr := logs.String(); logsStr != "" {
				logLines = strings.Split(strings.TrimSpace(logsStr), "\n")
			}
			assert.Equal(t, tc.expectedLogs, logLines)
		})
	}

	testCases := []struct {
		name                        string
		err                         error
		expectedHTTPStatus          int
		expectedErrorMessage        string
		expectedDoNotLogErrorHeader bool
		expectedLogs                []string
	}{
		{
			name:               "no error during push gets translated into a HTTP 200",
			err:                nil,
			expectedHTTPStatus: http.StatusOK,
		},
		{
			name:                 "a generic error during push gets a HTTP 500 without DoNotLogError header",
			err:                  errors.New(errMsg),
			expectedHTTPStatus:   http.StatusInternalServerError,
			expectedErrorMessage: errMsg,
			expectedLogs:         []string{`level=error user=testuser msg="detected an error while ingesting Prometheus remote-write request (the request may have been partially ingested)" httpCode=500 err="this is an error"`},
		},
		{
			name:                        "a DoNotLogError of a generic error during push gets a HTTP 500 with DoNotLogError header",
			err:                         middleware.DoNotLogError{Err: errors.New(errMsg)},
			expectedHTTPStatus:          http.StatusInternalServerError,
			expectedErrorMessage:        errMsg,
			expectedDoNotLogErrorHeader: true,
			expectedLogs:                []string{`level=error user=testuser msg="detected an error while ingesting Prometheus remote-write request (the request may have been partially ingested)" httpCode=500 err="this is an error"`},
		},
		{
			name:                 "a gRPC error with a status during push gets translated into HTTP error without DoNotLogError header",
			err:                  httpgrpc.Error(http.StatusRequestEntityTooLarge, errMsg),
			expectedHTTPStatus:   http.StatusRequestEntityTooLarge,
			expectedErrorMessage: errMsg,
			expectedLogs:         []string{`level=error user=testuser msg="detected an error while ingesting Prometheus remote-write request (the request may have been partially ingested)" httpCode=413 err="rpc error: code = Code(413) desc = this is an error" insight=true`},
		},
		{
			name:                        "a DoNotLogError of a gRPC error with a status during push gets translated into HTTP error without DoNotLogError header",
			err:                         middleware.DoNotLogError{Err: httpgrpc.Error(http.StatusRequestEntityTooLarge, errMsg)},
			expectedHTTPStatus:          http.StatusRequestEntityTooLarge,
			expectedErrorMessage:        errMsg,
			expectedDoNotLogErrorHeader: true,
			expectedLogs:                []string{`level=error user=testuser msg="detected an error while ingesting Prometheus remote-write request (the request may have been partially ingested)" httpCode=413 err="rpc error: code = Code(413) desc = this is an error" insight=true`},
		},
		{
			name:                 "a context.Canceled error during push gets translated into a HTTP 499",
			err:                  context.Canceled,
			expectedHTTPStatus:   statusClientClosedRequest,
			expectedErrorMessage: context.Canceled.Error(),
			expectedLogs:         []string{`level=warn user=testuser msg="push request canceled" err="context canceled"`},
		},
		{
			name:                 "StatusBadRequest is logged with insight=true",
			err:                  httpgrpc.Error(http.StatusBadRequest, "limits reached"),
			expectedHTTPStatus:   http.StatusBadRequest,
			expectedErrorMessage: "limits reached",
			expectedLogs:         []string{`level=error user=testuser msg="detected an error while ingesting Prometheus remote-write request (the request may have been partially ingested)" httpCode=400 err="rpc error: code = Code(400) desc = limits reached" insight=true`},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parserFunc := func(context.Context, *http.Request, int, *util.RequestBuffers, *mimirpb.PreallocWriteRequest, log.Logger) error {
				return nil
			}
			pushFunc := func(_ context.Context, req *Request) error {
				_, err := req.WriteRequest() // just read the body so we can trigger the parser
				if err != nil {
					return err
				}
				return tc.err
			}

			logs := &concurrency.SyncBuffer{}
			h := handler(10, nil, nil, false, false, validation.MockDefaultOverrides(), RetryConfig{}, pushFunc, log.NewLogfmtLogger(logs), parserFunc)
			recorder := httptest.NewRecorder()
			ctxWithUser := user.InjectOrgID(context.Background(), "testuser")
			h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/push", bufCloser{&bytes.Buffer{}}).WithContext(ctxWithUser))

			assert.Equal(t, tc.expectedHTTPStatus, recorder.Code)
			if tc.err != nil {
				require.Equal(t, fmt.Sprintf("%s\n", tc.expectedErrorMessage), recorder.Body.String())
			}
			header := recorder.Header().Get(server.DoNotLogErrorHeaderKey)
			if tc.expectedDoNotLogErrorHeader {
				require.Equal(t, "true", header)
			} else {
				require.Equal(t, "", header)
			}

			var logLines []string
			if logsStr := logs.String(); logsStr != "" {
				logLines = strings.Split(strings.TrimSpace(logsStr), "\n")
			}
			assert.Equal(t, tc.expectedLogs, logLines)
		})
	}
}

func TestHandler_HandleRetryAfterHeader(t *testing.T) {
	testCases := []struct {
		name          string
		responseCode  int
		retryAttempt  string
		retryCfg      RetryConfig
		expectRetry   bool
		minRetryAfter int
		maxRetryAfter int
	}{
		{
			name:         "Request canceled, HTTP 499, no Retry-After",
			responseCode: http.StatusRequestTimeout,
			retryAttempt: "1",
			retryCfg:     RetryConfig{Enabled: true, MinBackoff: 3 * time.Second, MaxBackoff: 8 * time.Second},
			expectRetry:  false,
		},
		{
			name:         "Generic error, HTTP 500, no Retry-After",
			responseCode: http.StatusInternalServerError,
			retryCfg:     RetryConfig{Enabled: false, MinBackoff: 3 * time.Second, MaxBackoff: 8 * time.Second},
			expectRetry:  false,
		},
		{
			name:          "Generic error, HTTP 500, Retry-After with no Retry-Attempt set, default Retry-Attempt to 1",
			responseCode:  http.StatusInternalServerError,
			expectRetry:   true,
			retryCfg:      RetryConfig{Enabled: true, MinBackoff: 5 * time.Second, MaxBackoff: 8 * time.Second},
			minRetryAfter: 5,
			maxRetryAfter: 7,
		},
		{
			name:          "Generic error, HTTP 500, Retry-After with Retry-Attempt is not an integer, default Retry-Attempt to 1",
			responseCode:  http.StatusInternalServerError,
			retryAttempt:  "not-an-integer",
			expectRetry:   true,
			retryCfg:      RetryConfig{Enabled: true, MinBackoff: 3 * time.Second, MaxBackoff: 8 * time.Second},
			minRetryAfter: 3,
			maxRetryAfter: 4,
		},
		{
			name:          "Generic error, HTTP 500, Retry-After with Retry-Attempt is float, default Retry-Attempt to 1",
			responseCode:  http.StatusInternalServerError,
			retryAttempt:  "3.50",
			expectRetry:   true,
			retryCfg:      RetryConfig{Enabled: true, MinBackoff: 2 * time.Second, MaxBackoff: 64 * time.Second},
			minRetryAfter: 2,
			maxRetryAfter: 3,
		},
		{
			name:          "Generic error, HTTP 500, Retry-After with Retry-Attempt a list of integers, default Retry-Attempt to 1",
			responseCode:  http.StatusInternalServerError,
			retryAttempt:  "[1, 2, 3]",
			expectRetry:   true,
			retryCfg:      RetryConfig{Enabled: true, MinBackoff: 1 * time.Second, MaxBackoff: 64 * time.Second},
			minRetryAfter: 1,
			maxRetryAfter: 3,
		},
		{
			name:          "Generic error, HTTP 500, Retry-After with Retry-Attempt is negative, default Retry-Attempt to 1",
			responseCode:  http.StatusInternalServerError,
			retryAttempt:  "-1",
			expectRetry:   true,
			retryCfg:      RetryConfig{Enabled: true, MinBackoff: 4 * time.Second, MaxBackoff: 16 * time.Second},
			minRetryAfter: 4,
			maxRetryAfter: 6,
		},
		{
			name:          "Generic error, HTTP 500, Retry-After with valid Retry-Attempts set to 2",
			responseCode:  http.StatusInternalServerError,
			expectRetry:   true,
			retryAttempt:  "2",
			retryCfg:      RetryConfig{Enabled: true, MinBackoff: 2 * time.Second, MaxBackoff: 64 * time.Second},
			minRetryAfter: 4 - 0.5*4,
			maxRetryAfter: 4 + 0.5*4,
		},
		{
			name:          "Generic error, HTTP 429, Retry-After with valid Retry-Attempts set to 3",
			responseCode:  StatusServiceOverloaded,
			expectRetry:   true,
			retryAttempt:  "3",
			retryCfg:      RetryConfig{Enabled: true, MinBackoff: 2 * time.Second, MaxBackoff: 64 * time.Second},
			minRetryAfter: 8 - 0.5*8,
			maxRetryAfter: 8 + 0.5*8,
		},
		{
			name:          "Generic error, HTTP 500, Retry-After with Retry-Attempts set higher than MaxBackoff",
			responseCode:  http.StatusInternalServerError,
			expectRetry:   true,
			retryAttempt:  "8",
			retryCfg:      RetryConfig{Enabled: true, MinBackoff: 3 * time.Second, MaxBackoff: 8 * time.Second},
			minRetryAfter: 8 * 0.5,
			maxRetryAfter: 8,
		},
		{
			name:          "Generic error, HTTP 500, Retry-After with Retry-Attempts set to a very high value (MaxInt64)",
			responseCode:  http.StatusInternalServerError,
			expectRetry:   true,
			retryAttempt:  "9223372036854775807",
			retryCfg:      RetryConfig{Enabled: true, MinBackoff: 3 * time.Second, MaxBackoff: 8 * time.Second},
			minRetryAfter: 4,
			maxRetryAfter: 8,
		},
		{
			name:          "Generic error, HTTP 500, Retry-After with Retry-Attempts set to a too high value fails to parse the value (MaxInt64+1)",
			responseCode:  http.StatusInternalServerError,
			expectRetry:   true,
			retryAttempt:  "9223372036854775808",
			retryCfg:      RetryConfig{Enabled: true, MinBackoff: 3 * time.Second, MaxBackoff: 8 * time.Second},
			minRetryAfter: 2,
			maxRetryAfter: 4,
		},
		{
			name:          "Generic error, HTTP 500, Retry-After with MinBackoff and MaxBackoff set to <1s",
			responseCode:  http.StatusInternalServerError,
			expectRetry:   true,
			retryAttempt:  "3",
			retryCfg:      RetryConfig{Enabled: true, MinBackoff: 3 * time.Millisecond, MaxBackoff: 8 * time.Millisecond},
			minRetryAfter: 0,
			maxRetryAfter: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/push", bufCloser{&bytes.Buffer{}})

			if tc.retryAttempt != "" {
				req.Header.Add("Retry-Attempt", tc.retryAttempt)
			}

			addHeaders(recorder, nil, req, tc.responseCode, tc.retryCfg)

			retryAfter := recorder.Header().Get("Retry-After")
			if !tc.expectRetry {
				assert.Empty(t, retryAfter)
			} else {
				assert.NotEmpty(t, retryAfter)
				retryAfterInt, err := strconv.Atoi(retryAfter)
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, retryAfterInt, tc.minRetryAfter)
				assert.LessOrEqual(t, retryAfterInt, tc.maxRetryAfter)
			}
		})
	}
}

func TestHandler_toHTTPStatus(t *testing.T) {
	const (
		ingesterID  = "ingester-25"
		userID      = "user"
		originalMsg = "this is an error"
	)
	originalErr := errors.New(originalMsg)
	replicasNotMatchErr := newReplicasDidNotMatchError("a", "b")
	tooManyClustersErr := newTooManyClustersError(10)
	ingestionRateLimitedErr := newIngestionRateLimitedError(10, 10)
	requestRateLimitedErr := newRequestRateLimitedError(10, 10)

	type testStruct struct {
		err                         error
		serviceOverloadErrorEnabled bool
		expectedHTTPStatus          int
	}
	testCases := map[string]testStruct{
		"a generic error gets translated into a HTTP 500": {
			err:                originalErr,
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"a DoNotLog of a generic error gets translated into a HTTP 500": {
			err:                middleware.DoNotLogError{Err: originalErr},
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"a context.DeadlineExceeded gets translated into a HTTP 500": {
			err:                context.DeadlineExceeded,
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"a replicasDidNotMatchError gets translated into an HTTP 202": {
			err:                replicasNotMatchErr,
			expectedHTTPStatus: http.StatusAccepted,
		},
		"a DoNotLogError of a replicasDidNotMatchError gets translated into an HTTP 202": {
			err:                middleware.DoNotLogError{Err: replicasNotMatchErr},
			expectedHTTPStatus: http.StatusAccepted,
		},
		"a tooManyClustersError gets translated into an HTTP 400": {
			err:                tooManyClustersErr,
			expectedHTTPStatus: http.StatusBadRequest,
		},
		"a DoNotLogError of a tooManyClustersError gets translated into an HTTP 400": {
			err:                middleware.DoNotLogError{Err: tooManyClustersErr},
			expectedHTTPStatus: http.StatusBadRequest,
		},
		"a validationError gets translated into an HTTP 400": {
			err:                newValidationError(originalErr),
			expectedHTTPStatus: http.StatusBadRequest,
		},
		"a DoNotLogError of a validationError gets translated into an HTTP 400": {
			err:                middleware.DoNotLogError{Err: newValidationError(originalErr)},
			expectedHTTPStatus: http.StatusBadRequest,
		},
		"an ingestionRateLimitedError gets translated into an HTTP 429": {
			err:                ingestionRateLimitedErr,
			expectedHTTPStatus: http.StatusTooManyRequests,
		},
		"an ingestionRateLimitedError with serviceOverloadErrorEnabled gets translated into an HTTP 529": {
			err:                         ingestionRateLimitedErr,
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
		},
		"a DoNotLogError of an ingestionRateLimitedError gets translated into an HTTP 429": {
			err:                middleware.DoNotLogError{Err: ingestionRateLimitedErr},
			expectedHTTPStatus: http.StatusTooManyRequests,
		},
		"a requestRateLimitedError with serviceOverloadErrorEnabled gets translated into an HTTP 529": {
			err:                         requestRateLimitedErr,
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
		},
		"a DoNotLogError of a requestRateLimitedError with serviceOverloadErrorEnabled gets translated into an HTTP 529": {
			err:                         middleware.DoNotLogError{Err: requestRateLimitedErr},
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
		},
		"a requestRateLimitedError without serviceOverloadErrorEnabled gets translated into an HTTP 429": {
			err:                         requestRateLimitedErr,
			serviceOverloadErrorEnabled: false,
			expectedHTTPStatus:          http.StatusTooManyRequests,
		},
		"a DoNotLogError of a requestRateLimitedError without serviceOverloadErrorEnabled gets translated into an HTTP 429": {
			err:                         middleware.DoNotLogError{Err: requestRateLimitedErr},
			serviceOverloadErrorEnabled: false,
			expectedHTTPStatus:          http.StatusTooManyRequests,
		},
		"an ingesterPushError with BAD_DATA cause gets translated into an HTTP 400": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.BAD_DATA), ingesterID),
			expectedHTTPStatus: http.StatusBadRequest,
		},
		"a DoNotLogError of an ingesterPushError with BAD_DATA cause gets translated into an HTTP 400": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.FailedPrecondition, originalMsg, mimirpb.BAD_DATA), ingesterID)},
			expectedHTTPStatus: http.StatusBadRequest,
		},
		"an ingesterPushError with METHOD_NOT_ALLOWED cause gets translated into an HTTP 501": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unimplemented, originalMsg, mimirpb.METHOD_NOT_ALLOWED), ingesterID),
			expectedHTTPStatus: http.StatusNotImplemented,
		},
		"a DoNotLogError of an ingesterPushError with METHOD_NOT_ALLOWED cause gets translated into an HTTP 501": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unimplemented, originalMsg, mimirpb.METHOD_NOT_ALLOWED), ingesterID)},
			expectedHTTPStatus: http.StatusNotImplemented,
		},
		"an ingesterPushError with TSDB_UNAVAILABLE cause gets translated into an HTTP 503": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.TSDB_UNAVAILABLE), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
		},
		"a DoNotLogError of an ingesterPushError with TSDB_UNAVAILABLE cause gets translated into an HTTP 503": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.TSDB_UNAVAILABLE), ingesterID)},
			expectedHTTPStatus: http.StatusServiceUnavailable,
		},
		"an ingesterPushError with SERVICE_UNAVAILABLE cause gets translated into an HTTP 500": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.SERVICE_UNAVAILABLE), ingesterID),
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"a DoNotLogError of an ingesterPushError with SERVICE_UNAVAILABLE cause gets translated into an HTTP 500": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.SERVICE_UNAVAILABLE), ingesterID)},
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"an ingesterPushError with INSTANCE_LIMIT cause gets translated into an HTTP 500": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.INSTANCE_LIMIT), ingesterID),
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"a DoNotLogError of an ingesterPushError with INSTANCE_LIMIT cause gets translated into an HTTP 500": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.INSTANCE_LIMIT), ingesterID)},
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"an ingesterPushError with UNKNOWN_CAUSE cause gets translated into an HTTP 500": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.UNKNOWN_CAUSE), ingesterID),
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"a DoNotLogError of an ingesterPushError with UNKNOWN_CAUSE cause gets translated into an HTTP 500": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.UNKNOWN_CAUSE), ingesterID)},
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"an ingesterPushError obtained from a DeadlineExceeded coming from the ingester gets translated into an HTTP 500": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, context.DeadlineExceeded.Error(), mimirpb.UNKNOWN_CAUSE), ingesterID),
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"an ingesterPushError with CIRCUIT_BREAKER_OPEN cause gets translated into an Unavailable error with CIRCUIT_BREAKER_OPEN cause": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.CIRCUIT_BREAKER_OPEN), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
		},
		"a wrapped ingesterPushError with CIRCUIT_BREAKER_OPEN cause gets translated into an Unavailable error with CIRCUIT_BREAKER_OPEN cause": {
			err:                errors.Wrap(newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.CIRCUIT_BREAKER_OPEN), ingesterID), "wrapped"),
			expectedHTTPStatus: http.StatusServiceUnavailable,
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
			assert.Equal(t, tc.expectedHTTPStatus, status)
		})
	}
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
	h := OTLPHandler(200, util.NewBufferPool(), nil, otlpLimitsMock{}, RetryConfig{}, push, newPushMetrics(reg), reg, log.NewNopLogger(), true)
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

	testcases := map[string]testCase{
		"missing content type returns 415": {
			request: &httpgrpc.HTTPRequest{
				Method: "POST",
				Url:    "/otlp",
				Body:   []byte("hello"),
			},
			expectedResponse: &httpgrpc.HTTPResponse{Code: 415,
				Headers: []*httpgrpc.Header{
					{Key: "Content-Type", Values: []string{"application/octet-stream"}},
					{Key: "X-Content-Type-Options", Values: []string{"nosniff"}},
				},
				Body: mustMarshalStatus(t, 415, "unsupported content type: , supported: [application/json, application/x-protobuf]"),
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
					{Key: "Content-Type", Values: []string{"application/octet-stream"}},
					{Key: "X-Content-Type-Options", Values: []string{"nosniff"}},
				},
				Body: mustMarshalStatus(t, 400, "ReadObjectCB: expect { or n, but found i, error found in #1 byte of ...|invalid|..., bigger context ...|invalid|..."),
			},
			expectedGrpcErrorMessage: "rpc error: code = Code(400) desc = ReadObjectCB: expect { or n, but found i, error found in #1 byte of ...|invalid|..., bigger context ...|invalid|...",
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
				Headers: nil, // No headers expected for 200.
				Body:    nil, // No body expected for 200 code.
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
				// This is simple OTLP request, with "report_server_error".
				Body: []byte(`{"resourceMetrics": [{"scopeMetrics": [{"metrics": [{"name": "report_server_error", "gauge": {"dataPoints": [{"timeUnixNano": "1679912463340000000", "asDouble": 10.66}]}}]}]}]}`),
			},
			expectedResponse: &httpgrpc.HTTPResponse{Code: 503,
				Headers: []*httpgrpc.Header{
					{Key: "Content-Type", Values: []string{"application/octet-stream"}},
					{Key: "X-Content-Type-Options", Values: []string{"nosniff"}},
				},
				Body: mustMarshalStatus(t, codes.Internal, "some random push error"),
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
			sort.Slice(resp.Headers, func(i, j int) bool {
				return resp.Headers[i].Key < resp.Headers[j].Key
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

func mustMarshalStatus(t *testing.T, code codes.Code, msg string) []byte {
	bytes, err := status.New(code, msg).Proto().Marshal()
	require.NoError(t, err)
	return bytes
}

type otlpLimitsMock struct{}

func (o otlpLimitsMock) OTelMetricSuffixesEnabled(_ string) bool {
	return false
}

func (o otlpLimitsMock) OTelCreatedTimestampZeroIngestionEnabled(_ string) bool {
	return false
}
