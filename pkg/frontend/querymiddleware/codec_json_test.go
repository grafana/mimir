// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/marshaling_test.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/go-kit/log"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
)

func TestPrometheusCodec_JSONResponse(t *testing.T) {
	headers := http.Header{"Content-Type": []string{"application/json"}}
	expectedRespHeaders := []*PrometheusResponseHeader{
		{
			Name:   "Content-Type",
			Values: []string{"application/json"},
		},
	}

	for _, tc := range []struct {
		name            string
		responseHeaders http.Header
		resp            prometheusAPIResponse
		expected        *PrometheusResponse
		expectedErr     error
	}{
		{
			name: "successful string response",
			resp: prometheusAPIResponse{
				Status: statusSuccess,
				Data: prometheusResponseData{
					Type:   model.ValString,
					Result: &model.String{Value: "foo", Timestamp: 1_500},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValString.String(),
					Result: []SampleStream{
						{
							Labels:  []mimirpb.LabelAdapter{{Name: "value", Value: "foo"}},
							Samples: []mimirpb.Sample{{TimestampMs: 1_500}},
						},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful scalar response",
			resp: prometheusAPIResponse{
				Status: statusSuccess,
				Data: prometheusResponseData{
					Type: model.ValScalar,
					Result: &model.Scalar{
						Value:     200,
						Timestamp: 1_000,
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValScalar.String(),
					Result: []SampleStream{
						{Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 200}}},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful instant response",
			resp: prometheusAPIResponse{
				Status: statusSuccess,
				Data: prometheusResponseData{
					Type: model.ValVector,
					Result: model.Vector{
						{Metric: model.Metric{"foo": "bar"}, Timestamp: 1_000, Value: 200},
						{Metric: model.Metric{"bar": "baz"}, Timestamp: 1_000, Value: 201},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValVector.String(),
					Result: []SampleStream{
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 200}}},
						{Labels: []mimirpb.LabelAdapter{{Name: "bar", Value: "baz"}}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 201}}},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful range response",
			resp: prometheusAPIResponse{
				Status: statusSuccess,
				Data: prometheusResponseData{
					Type: model.ValMatrix,
					Result: model.Matrix{
						{Metric: model.Metric{"foo": "bar"}, Values: []model.SamplePair{{Timestamp: 1_000, Value: 100}, {Timestamp: 2_000, Value: 200}}},
						{Metric: model.Metric{"bar": "baz"}, Values: []model.SamplePair{{Timestamp: 1_000, Value: 101}, {Timestamp: 2_000, Value: 201}}},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: []SampleStream{
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 100}, {TimestampMs: 2_000, Value: 200}}},
						{Labels: []mimirpb.LabelAdapter{{Name: "bar", Value: "baz"}}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 101}, {TimestampMs: 2_000, Value: 201}}},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful empty matrix response",
			resp: prometheusAPIResponse{
				Status: statusSuccess,
				Data: prometheusResponseData{
					Type:   model.ValMatrix,
					Result: model.Matrix{},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result:     []SampleStream{},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "error response",
			resp: prometheusAPIResponse{
				Status:    statusError,
				ErrorType: "expected",
				Error:     "failed",
			},
			expectedErr: apierror.New(apierror.Type("expected"), "failed"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			codec := NewPrometheusCodec(reg, formatJSON)

			body, err := json.Marshal(tc.resp)
			require.NoError(t, err)
			httpResponse := &http.Response{
				StatusCode:    200,
				Header:        headers,
				Body:          io.NopCloser(bytes.NewBuffer(body)),
				ContentLength: int64(len(body)),
			}
			decoded, err := codec.DecodeResponse(context.Background(), httpResponse, nil, log.NewNopLogger())
			if err != nil || tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, decoded)

			metrics, err := dskit_metrics.NewMetricFamilyMapFromGatherer(reg)
			require.NoError(t, err)
			durationHistogram, err := findHistogramMatchingLabels(metrics, "cortex_frontend_query_response_codec_duration_seconds", "format", "json", "operation", "decode")
			require.NoError(t, err)
			require.Equal(t, uint64(1), *durationHistogram.SampleCount)
			require.Less(t, *durationHistogram.SampleSum, 0.1)
			payloadSizeHistogram, err := findHistogramMatchingLabels(metrics, "cortex_frontend_query_response_codec_payload_bytes", "format", "json", "operation", "decode")
			require.NoError(t, err)
			require.Equal(t, uint64(1), *payloadSizeHistogram.SampleCount)
			require.Equal(t, float64(len(body)), *payloadSizeHistogram.SampleSum)

			// Reset response, as the above call will have consumed the body reader.
			httpResponse = &http.Response{
				StatusCode:    200,
				Header:        headers,
				Body:          io.NopCloser(bytes.NewBuffer(body)),
				ContentLength: int64(len(body)),
			}
			encoded, err := codec.EncodeResponse(context.Background(), decoded)
			require.NoError(t, err)

			metrics, err = dskit_metrics.NewMetricFamilyMapFromGatherer(reg)
			require.NoError(t, err)
			durationHistogram, err = findHistogramMatchingLabels(metrics, "cortex_frontend_query_response_codec_duration_seconds", "format", "json", "operation", "encode")
			require.NoError(t, err)
			require.Equal(t, uint64(1), *durationHistogram.SampleCount)
			require.Less(t, *durationHistogram.SampleSum, 0.1)
			payloadSizeHistogram, err = findHistogramMatchingLabels(metrics, "cortex_frontend_query_response_codec_payload_bytes", "format", "json", "operation", "encode")
			require.NoError(t, err)
			require.Equal(t, uint64(1), *payloadSizeHistogram.SampleCount)
			require.Equal(t, float64(len(body)), *payloadSizeHistogram.SampleSum)

			expectedJSON, err := bodyBuffer(httpResponse)
			require.NoError(t, err)
			encodedJSON, err := bodyBuffer(encoded)
			require.NoError(t, err)

			require.JSONEq(t, string(expectedJSON), string(encodedJSON))
			require.Equal(t, httpResponse, encoded)
		})
	}
}

func findHistogramMatchingLabels(metrics dskit_metrics.MetricFamilyMap, name string, labelValuePairs ...string) (*dto.Histogram, error) {
	metricFamily, ok := metrics[name]
	if !ok {
		return nil, fmt.Errorf("no metric with name %v found", name)
	}

	l := labels.FromStrings(labelValuePairs...)
	var matchingMetrics []*dto.Metric

	for _, metric := range metricFamily.Metric {
		if util.MatchesSelectors(metric, l) {
			matchingMetrics = append(matchingMetrics, metric)
		}
	}

	if len(matchingMetrics) != 1 {
		return nil, fmt.Errorf("wanted exactly one matching metric, but found %v", len(matchingMetrics))
	}

	metric := matchingMetrics[0]

	if metric.Histogram == nil {
		return nil, errors.New("found a single matching metric, but it is not a histogram")
	}

	return metric.Histogram, nil
}
