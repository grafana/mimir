// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	gogostatus "github.com/gogo/status"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/user"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/otlptranslator"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/mimirpb/testutil"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestOTelMetricsToTimeSeries(t *testing.T) {
	resourceAttrs := map[string]string{
		"service.name":        "service name",
		"service.namespace":   "service namespace",
		"service.instance.id": "service ID",
		"existent-attr":       "resource value",
		// This one is for testing conflict with metric attribute.
		"metric-attr": "resource value",
		// This one is for testing conflict with auto-generated job attribute.
		"job": "resource value",
		// This one is for testing conflict with auto-generated instance attribute.
		"instance": "resource value",
	}

	testCases := []struct {
		name                              string
		promoteResourceAttributes         []string
		keepIdentifyingResourceAttributes bool
		appendCustomMetric                func(pmetric.MetricSlice)
		expectedLabels                    []mimirpb.LabelAdapter
		expectedInfoLabels                []mimirpb.LabelAdapter
		underscoreSanitization            bool
		preserveMultipleUnderscores       bool
	}{
		{
			name:                        "Successful conversion without resource attribute promotion",
			promoteResourceAttributes:   nil,
			underscoreSanitization:      true,
			preserveMultipleUnderscores: true,
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
					Value: "service namespace/service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
			},
			expectedInfoLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "target_info",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "resource value",
				},
				{
					Name:  "job",
					Value: "service namespace/service name",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
			},
		},
		{
			name:                              "Successful conversion without resource attribute promotion, and keep identifying resource attributes",
			promoteResourceAttributes:         nil,
			keepIdentifyingResourceAttributes: true,
			underscoreSanitization:            true,
			preserveMultipleUnderscores:       true,
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
					Value: "service namespace/service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
			},
			expectedInfoLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "target_info",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "resource value",
				},
				{
					Name:  "job",
					Value: "service namespace/service name",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "service_instance_id",
					Value: "service ID",
				},
				{
					Name:  "service_name",
					Value: "service name",
				},
				{
					Name:  "service_namespace",
					Value: "service namespace",
				},
			},
		},
		{
			name:                        "Successful conversion with resource attribute promotion",
			promoteResourceAttributes:   []string{"non-existent-attr", "existent-attr"},
			underscoreSanitization:      true,
			preserveMultipleUnderscores: true,
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
					Value: "service namespace/service name",
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
			expectedInfoLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "target_info",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "resource value",
				},
				{
					Name:  "job",
					Value: "service namespace/service name",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
			},
		},
		{
			name:                        "Successful conversion with resource attribute promotion, conflicting resource attributes are ignored",
			promoteResourceAttributes:   []string{"non-existent-attr", "existent-attr", "metric-attr", "job", "instance"},
			underscoreSanitization:      true,
			preserveMultipleUnderscores: true,
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
					Value: "service namespace/service name",
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
			expectedInfoLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "target_info",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service namespace/service name",
				},
				{
					Name:  "metric_attr",
					Value: "resource value",
				},
			},
		},
		{
			name:                        "Successful conversion of cumulative non-monotonic sum",
			promoteResourceAttributes:   nil,
			underscoreSanitization:      true,
			preserveMultipleUnderscores: true,
			appendCustomMetric: func(metricSlice pmetric.MetricSlice) {
				m := metricSlice.AppendEmpty()
				m.SetName("test_metric")
				sum := m.SetEmptySum()
				sum.SetIsMonotonic(false)
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetIntValue(123)
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp.Attributes().PutStr("metric-attr", "metric value")
			},
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
					Value: "service namespace/service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
			},
			expectedInfoLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "target_info",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "resource value",
				},
				{
					Name:  "job",
					Value: "service namespace/service name",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
			},
		},
		{
			name:                        "Successful conversion of cumulative monotonic sum",
			promoteResourceAttributes:   nil,
			underscoreSanitization:      true,
			preserveMultipleUnderscores: true,
			appendCustomMetric: func(metricSlice pmetric.MetricSlice) {
				m := metricSlice.AppendEmpty()
				m.SetName("test_metric")
				sum := m.SetEmptySum()
				sum.SetIsMonotonic(true)
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetIntValue(123)
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp.Attributes().PutStr("metric-attr", "metric value")
			},
			expectedLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "test_metric_total",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service namespace/service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
			},
			expectedInfoLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "target_info",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "resource value",
				},
				{
					Name:  "job",
					Value: "service namespace/service name",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
			},
		},
		{
			name:                        "Underscore sanitization",
			promoteResourceAttributes:   nil,
			underscoreSanitization:      true,
			preserveMultipleUnderscores: true,
			appendCustomMetric: func(metricSlice pmetric.MetricSlice) {
				m := metricSlice.AppendEmpty()
				m.SetName("test_metric")
				sum := m.SetEmptySum()
				sum.SetIsMonotonic(false)
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetIntValue(123)
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp.Attributes().PutStr("_foo", "bar")
				dp.Attributes().PutStr("_1", "bar")
				dp.Attributes().PutStr("some__thing", "bar")
			},
			expectedLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "key_foo",
					Value: "bar",
				},
				{
					Name:  "key_1",
					Value: "bar",
				},
				{
					Name:  "some__thing",
					Value: "bar",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service namespace/service name",
				},
			},
			expectedInfoLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "target_info",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "resource value",
				},
				{
					Name:  "job",
					Value: "service namespace/service name",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
			},
		},
		{
			name:                        "Disable underscore sanitization",
			promoteResourceAttributes:   nil,
			underscoreSanitization:      false,
			preserveMultipleUnderscores: true,
			appendCustomMetric: func(metricSlice pmetric.MetricSlice) {
				m := metricSlice.AppendEmpty()
				m.SetName("test_metric")
				sum := m.SetEmptySum()
				sum.SetIsMonotonic(false)
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetIntValue(123)
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp.Attributes().PutStr("_foo", "bar")
				dp.Attributes().PutStr("_1", "bar")
				dp.Attributes().PutStr("some__thing", "bar")
			},
			expectedLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "_foo",
					Value: "bar",
				},
				{
					Name:  "_1",
					Value: "bar",
				},
				{
					Name:  "some__thing",
					Value: "bar",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service namespace/service name",
				},
			},
			expectedInfoLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "target_info",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "resource value",
				},
				{
					Name:  "job",
					Value: "service namespace/service name",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
			},
		},
		{
			name:                        "Disable multiple underscore preservation",
			promoteResourceAttributes:   nil,
			underscoreSanitization:      false,
			preserveMultipleUnderscores: false,
			appendCustomMetric: func(metricSlice pmetric.MetricSlice) {
				m := metricSlice.AppendEmpty()
				m.SetName("test_metric")
				sum := m.SetEmptySum()
				sum.SetIsMonotonic(false)
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetIntValue(123)
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp.Attributes().PutStr("_foo", "bar")
				dp.Attributes().PutStr("_1", "bar")
				dp.Attributes().PutStr("some__thing", "bar")
			},
			expectedLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "_foo",
					Value: "bar",
				},
				{
					Name:  "_1",
					Value: "bar",
				},
				{
					Name:  "some_thing",
					Value: "bar",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service namespace/service name",
				},
			},
			expectedInfoLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "target_info",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "resource value",
				},
				{
					Name:  "job",
					Value: "service namespace/service name",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			md := pmetric.NewMetrics()
			{
				rm := md.ResourceMetrics().AppendEmpty()
				for k, v := range resourceAttrs {
					rm.Resource().Attributes().PutStr(k, v)
				}
				scopeMetrics := rm.ScopeMetrics().AppendEmpty()
				metrics := scopeMetrics.Metrics()
				if tc.appendCustomMetric == nil {
					m := metrics.AppendEmpty()
					m.SetName("test_metric")
					dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
					dp.SetIntValue(123)
					dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
					dp.Attributes().PutStr("metric-attr", "metric value")
				} else {
					tc.appendCustomMetric(metrics)
				}
			}
			converter := newOTLPMimirConverter(nil)
			mimirTS, _, dropped, err := otelMetricsToSeriesAndMetadata(
				context.Background(),
				converter,
				md,
				conversionOptions{
					addSuffixes:                       true,
					keepIdentifyingResourceAttributes: tc.keepIdentifyingResourceAttributes,
					promoteResourceAttributes:         tc.promoteResourceAttributes,
					underscoreSanitization:            tc.underscoreSanitization,
					preserveMultipleUnderscores:       tc.preserveMultipleUnderscores,
				},
				log.NewNopLogger(),
			)
			require.NoError(t, err)
			require.Len(t, mimirTS, 2)
			require.Equal(t, 0, dropped)
			var ts mimirpb.PreallocTimeseries
			var targetInfo mimirpb.PreallocTimeseries
			for i := range mimirTS {
				for _, lbl := range mimirTS[i].Labels {
					if lbl.Name != model.MetricNameLabel {
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
			assert.ElementsMatch(t, targetInfo.Labels, tc.expectedInfoLabels)
		})
	}
}

func TestConvertOTelHistograms(t *testing.T) {
	resourceAttrs := map[string]string{
		"service.name":        "service name",
		"service.namespace":   "service namespace",
		"service.instance.id": "service ID",
		"existent-attr":       "resource value",
		// This one is for testing conflict with metric attribute.
		"metric-attr": "resource value",
		// This one is for testing conflict with auto-generated job attribute.
		"job": "resource value",
		// This one is for testing conflict with auto-generated instance attribute.
		"instance": "resource value",
	}

	md := pmetric.NewMetrics()
	{
		rm := md.ResourceMetrics().AppendEmpty()
		for k, v := range resourceAttrs {
			rm.Resource().Attributes().PutStr(k, v)
		}
		il := rm.ScopeMetrics().AppendEmpty()
		m := il.Metrics().AppendEmpty()
		m.SetName("test_histogram_metric")
		m.SetEmptyHistogram()
		m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp := m.Histogram().DataPoints().AppendEmpty()
		dp.SetCount(3)
		dp.SetSum(17.8)

		dp.BucketCounts().FromRaw([]uint64{2, 0, 1})
		dp.ExplicitBounds().FromRaw([]float64{5, 10})

		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.Attributes().PutStr("metric-attr", "metric value")
	}

	for _, convertHistogramsToNHCB := range []bool{false, true} {
		converter := newOTLPMimirConverter(nil)
		mimirTS, _, dropped, err := otelMetricsToSeriesAndMetadata(
			context.Background(),
			converter,
			md,
			conversionOptions{
				addSuffixes:             true,
				convertHistogramsToNHCB: convertHistogramsToNHCB,
			},
			log.NewNopLogger(),
		)
		require.NoError(t, err)
		require.Equal(t, 0, dropped)
		if convertHistogramsToNHCB {
			ts := make([]mimirpb.PreallocTimeseries, 0, len(mimirTS))
			// Filter out target_info series
			for i := range mimirTS {
				var metricName string
				for _, lbl := range mimirTS[i].Labels {
					if lbl.Name == model.MetricNameLabel {
						metricName = lbl.Value
						break
					}
				}
				if metricName == "target_info" {
					continue
				}
				ts = append(ts, mimirTS[i])
			}
			require.Len(t, ts, 1)
			require.Len(t, ts[0].Histograms, 1)
		} else {
			require.Len(t, mimirTS, 6)
			for i := range mimirTS {
				require.Len(t, mimirTS[i].Histograms, 0)
			}
		}
	}
}

func TestOTelDeltaIngestion(t *testing.T) {
	ts := time.Unix(100, 0)

	testCases := []struct {
		name        string
		allowDelta  bool
		input       pmetric.Metrics
		expected    mimirpb.TimeSeries
		expectedErr string
	}{
		{
			name:       "delta counter not allowed",
			allowDelta: false,
			input: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				il := rm.ScopeMetrics().AppendEmpty()
				m := il.Metrics().AppendEmpty()
				m.SetName("test_metric")
				sum := m.SetEmptySum()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp.Attributes().PutStr("metric-attr", "metric value")
				return md
			}(),
			expectedErr: `otlp parse error: invalid temporality and type combination for metric "test_metric"`,
		},
		{
			name:       "delta counter allowed",
			allowDelta: true,
			input: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				il := rm.ScopeMetrics().AppendEmpty()
				m := il.Metrics().AppendEmpty()
				m.SetName("test_metric")
				sum := m.SetEmptySum()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
				dp.SetIntValue(5)
				dp.Attributes().PutStr("metric-attr", "metric value")
				return md
			}(),
			expected: mimirpb.TimeSeries{
				Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: "test_metric"}, {Name: "metric_attr", Value: "metric value"}},
				Samples: []mimirpb.Sample{{TimestampMs: ts.UnixMilli(), Value: 5}},
			},
		},
		{
			name:       "delta exponential histogram not allowed",
			allowDelta: false,
			input: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				il := rm.ScopeMetrics().AppendEmpty()
				m := il.Metrics().AppendEmpty()
				m.SetName("test_metric")
				sum := m.SetEmptyExponentialHistogram()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetCount(1)
				dp.SetSum(5)
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp.Attributes().PutStr("metric-attr", "metric value")
				return md
			}(),
			expectedErr: `otlp parse error: invalid temporality and type combination for metric "test_metric"`,
		},
		{
			name:       "delta exponential histogram allowed",
			allowDelta: true,
			input: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				il := rm.ScopeMetrics().AppendEmpty()
				m := il.Metrics().AppendEmpty()
				m.SetName("test_metric")
				sum := m.SetEmptyExponentialHistogram()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetCount(1)
				dp.SetSum(5)
				dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
				dp.Attributes().PutStr("metric-attr", "metric value")
				return md
			}(),
			expected: mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "test_metric"}, {Name: "metric_attr", Value: "metric value"}},
				Histograms: []mimirpb.Histogram{
					{
						Count:         &mimirpb.Histogram_CountInt{CountInt: 1},
						Sum:           5,
						Schema:        0,
						ZeroThreshold: 1e-128,
						ZeroCount:     &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 0},
						Timestamp:     ts.UnixMilli(),
						ResetHint:     mimirpb.Histogram_GAUGE,
					},
				},
			},
		},
		{
			name:       "delta histogram as nhcb not allowed",
			allowDelta: false,
			input: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				il := rm.ScopeMetrics().AppendEmpty()
				m := il.Metrics().AppendEmpty()
				m.SetName("test_metric")
				sum := m.SetEmptyHistogram()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetCount(20)
				dp.SetSum(30)
				dp.BucketCounts().FromRaw([]uint64{10, 10, 0})
				dp.ExplicitBounds().FromRaw([]float64{1, 2})
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp.Attributes().PutStr("metric-attr", "metric value")
				return md
			}(),
			expectedErr: `otlp parse error: invalid temporality and type combination for metric "test_metric"`,
		},
		{
			name:       "delta histogram as nhcb allowed",
			allowDelta: true,
			input: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				il := rm.ScopeMetrics().AppendEmpty()
				m := il.Metrics().AppendEmpty()
				m.SetName("test_metric")
				sum := m.SetEmptyHistogram()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetCount(20)
				dp.SetSum(30)
				dp.BucketCounts().FromRaw([]uint64{10, 10, 0})
				dp.ExplicitBounds().FromRaw([]float64{1, 2})
				dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
				dp.Attributes().PutStr("metric-attr", "metric value")
				return md
			}(),
			expected: mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "test_metric"}, {Name: "metric_attr", Value: "metric value"}},
				Histograms: []mimirpb.Histogram{
					{
						Count:         &mimirpb.Histogram_CountInt{CountInt: 20},
						Sum:           30,
						Schema:        -53,
						ZeroThreshold: 0,
						ZeroCount:     &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 0},
						PositiveSpans: []mimirpb.BucketSpan{
							{
								Length: 3,
							},
						},
						PositiveDeltas: []int64{10, 0, -10},
						CustomValues:   []float64{1, 2},
						Timestamp:      ts.UnixMilli(),
						ResetHint:      mimirpb.Histogram_GAUGE,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			converter := newOTLPMimirConverter(nil)
			mimirTS, _, dropped, err := otelMetricsToSeriesAndMetadata(
				context.Background(),
				converter,
				tc.input,
				conversionOptions{
					convertHistogramsToNHCB: true,
					allowDeltaTemporality:   tc.allowDelta,
				},
				log.NewNopLogger(),
			)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				require.Len(t, mimirTS, 0)
				require.Equal(t, 1, dropped)
			} else {
				require.NoError(t, err)
				mimirTS = testutil.RemoveEmptyObjectFromSeries(mimirTS)
				require.Len(t, mimirTS, 1)
				require.Equal(t, 0, dropped)
				require.Equal(t, tc.expected, *mimirTS[0].TimeSeries)
			}
		})
	}
}

// TestOTelCTZeroIngestion checks that when conversionOptions.enableCTZeroIngestion is true,
// the otel start time is turned into the created timestamp of the resulting time series.
// Also check what happens if the option is off.
func TestOTelCTZeroIngestion(t *testing.T) {
	ts := time.Unix(100, 0)

	testCases := []struct {
		name         string
		enableCTZero bool
		input        pmetric.Metrics
		expected     mimirpb.TimeSeries
	}{
		{
			name:         "enable CT zero ingestion",
			enableCTZero: true,
			input: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				il := rm.ScopeMetrics().AppendEmpty()
				m := il.Metrics().AppendEmpty()
				m.SetName("test_metric")
				sum := m.SetEmptySum()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
				dp.SetStartTimestamp(pcommon.NewTimestampFromTime(ts.Add(-time.Minute)))
				dp.SetIntValue(5)
				dp.Attributes().PutStr("metric-attr", "metric value")
				return md
			}(),
			expected: mimirpb.TimeSeries{
				Labels:           []mimirpb.LabelAdapter{{Name: "__name__", Value: "test_metric"}, {Name: "metric_attr", Value: "metric value"}},
				Samples:          []mimirpb.Sample{{TimestampMs: ts.UnixMilli(), Value: 5}},
				CreatedTimestamp: ts.Add(-time.Minute).UnixMilli(),
			},
		},
		{
			name:         "disable CT zero ingestion",
			enableCTZero: false,
			input: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				il := rm.ScopeMetrics().AppendEmpty()
				m := il.Metrics().AppendEmpty()
				m.SetName("test_metric")
				sum := m.SetEmptySum()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
				dp.SetStartTimestamp(pcommon.NewTimestampFromTime(ts.Add(-time.Minute)))
				dp.SetIntValue(5)
				dp.Attributes().PutStr("metric-attr", "metric value")
				return md
			}(),
			expected: mimirpb.TimeSeries{
				Labels:           []mimirpb.LabelAdapter{{Name: "__name__", Value: "test_metric"}, {Name: "metric_attr", Value: "metric value"}},
				Samples:          []mimirpb.Sample{{TimestampMs: ts.UnixMilli(), Value: 5}},
				CreatedTimestamp: 0,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			converter := newOTLPMimirConverter(nil)
			mimirTS, _, dropped, err := otelMetricsToSeriesAndMetadata(
				context.Background(),
				converter,
				tc.input,
				conversionOptions{
					addSuffixes:             true,
					enableCTZeroIngestion:   tc.enableCTZero,
					convertHistogramsToNHCB: true,
				},
				log.NewNopLogger(),
			)
			require.NoError(t, err)
			mimirTS = testutil.RemoveEmptyObjectFromSeries(mimirTS)
			require.Len(t, mimirTS, 1)
			require.Equal(t, 0, dropped)
			require.Equal(t, tc.expected, *mimirTS[0].TimeSeries)
		})
	}
}

// Extra labels to make a more realistic workload - taken from Kubernetes' embedded cAdvisor metrics.
var extraLabels = []labels.Label{
	{Name: "kubernetes_io_arch", Value: "amd64"},
	{Name: "kubernetes_io_instance_type", Value: "c3.somesize"},
	{Name: "kubernetes_io_os", Value: "linux"},
	{Name: "container_name", Value: "some-name"},
	{Name: "failure_domain_kubernetes_io_region", Value: "somewhere-1"},
	{Name: "failure_domain_kubernetes_io_zone", Value: "somewhere-1b"},
	{Name: "id", Value: "/kubepods/burstable/pod6e91c467-e4c5-11e7-ace3-0a97ed59c75e/a3c8498918bd6866349fed5a6f8c643b77c91836427fb6327913276ebc6bde28"},
	{Name: "image", Value: "registry/organisation/name@sha256:dca3d877a80008b45d71d7edc4fd2e44c0c8c8e7102ba5cbabec63a374d1d506"},
	{Name: "instance", Value: "ip-111-11-1-11.ec2.internal"},
	{Name: "job", Value: "kubernetes-cadvisor"},
	{Name: "kubernetes_io_hostname", Value: "ip-111-11-1-11"},
	{Name: "monitor", Value: "prod"},
	{Name: "name", Value: "k8s_some-name_some-other-name-5j8s8_kube-system_6e91c467-e4c5-11e7-ace3-0a97ed59c75e_0"},
	{Name: "namespace", Value: "kube-system"},
	{Name: "pod_name", Value: "some-other-name-5j8s8"},
}

func BenchmarkOTLPHandler(b *testing.B) {
	const numSeries = 2000
	const numSamplesPerSeries = 1
	var samples []prompb.Sample
	var histograms []prompb.Histogram
	var exemplars []prompb.Exemplar
	var histogramExemplars []prompb.Exemplar
	for i := 0; i < numSamplesPerSeries; i++ {
		ts := time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Second)
		samples = append(samples, prompb.Sample{
			Value:     1,
			Timestamp: ts.UnixNano(),
		})
		histograms = append(histograms, prompb.FromIntHistogram(1337, test.GenerateTestHistogram(1)))
	}
	{
		ts := time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC)
		ex := prompb.Exemplar{
			Value:     1,
			Timestamp: ts.UnixNano(),
			Labels:    make([]prompb.Label, 0, len(extraLabels)),
		}
		for _, lbl := range extraLabels {
			ex.Labels = append(ex.Labels, prompb.Label{Name: lbl.Name, Value: lbl.Value})
		}
		exemplars = append(exemplars, ex)
		for i := 0; i < 10; i++ {
			histogramExemplars = append(histogramExemplars, prompb.Exemplar{
				Value:     float64(i),
				Timestamp: ts.UnixNano(),
				Labels:    make([]prompb.Label, 0, len(extraLabels)),
			})
			for _, lbl := range extraLabels {
				lastExemplar := &histogramExemplars[len(histogramExemplars)-1]
				lastExemplar.Labels = append(lastExemplar.Labels, prompb.Label{Name: lbl.Name, Value: lbl.Value})
			}
		}
	}

	sampleSeries := make([]prompb.TimeSeries, 0, numSeries)
	sampleMetadata := make([]mimirpb.MetricMetadata, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		// Create a series with a unique name and some extra labels.
		lbls := make([]prompb.Label, 0, 1+len(extraLabels))
		lbls = append(lbls, prompb.Label{Name: "__name__", Value: "foo" + strconv.Itoa(i)})
		for _, lbl := range extraLabels {
			lbls = append(lbls, prompb.Label{Name: lbl.Name, Value: lbl.Value})
		}
		if i%10 == 0 {
			// Add 10% exponential histograms.
			sampleSeries = append(sampleSeries, prompb.TimeSeries{
				Labels:     lbls,
				Histograms: histograms,
				Exemplars:  histogramExemplars,
			})
		} else {
			sampleSeries = append(sampleSeries, prompb.TimeSeries{
				Labels:    lbls,
				Samples:   samples,
				Exemplars: exemplars,
			})
		}
		sampleMetadata = append(sampleMetadata, mimirpb.MetricMetadata{
			MetricFamilyName: "foo" + strconv.Itoa(i),
			Help:             "metric_help_" + strconv.Itoa(i),
			Unit:             "metric_unit_" + strconv.Itoa(i),
		})
	}

	exportReq := TimeseriesToOTLPRequest(sampleSeries, sampleMetadata)

	pushFunc := func(_ context.Context, pushReq *Request) error {
		if _, err := pushReq.WriteRequest(); err != nil {
			return err
		}

		pushReq.CleanUp()
		return nil
	}
	limits := validation.MockDefaultOverrides()
	handler := OTLPHandler(
		10000000, nil, nil, limits, nil, nil,
		RetryConfig{}, nil, pushFunc, nil, nil, log.NewNopLogger(),
	)

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

func BenchmarkOTLPHandlerWithLargeMessage(b *testing.B) {
	const numberOfMetrics = 150
	const numberOfResourceMetrics = 300
	const numberOfDatapoints = 4
	const stepDuration = 10 * time.Second

	startTime := time.Date(2020, time.October, 30, 23, 0, 0, 0, time.UTC)

	createMetrics := func(mts pmetric.MetricSlice) {
		mts.EnsureCapacity(numberOfMetrics)
		for idx := range numberOfMetrics {
			mt := mts.AppendEmpty()
			mt.SetName(fmt.Sprintf("metric-%d", idx))
			datapoints := mt.SetEmptyGauge().DataPoints()
			datapoints.EnsureCapacity(numberOfDatapoints)

			sampleTime := startTime
			for j := range numberOfDatapoints {
				datapoint := datapoints.AppendEmpty()
				datapoint.SetTimestamp(pcommon.NewTimestampFromTime(sampleTime))
				datapoint.SetIntValue(int64(j))
				attrs := datapoint.Attributes()
				attrs.PutStr("route", "/hello")
				attrs.PutStr("status", "200")
				sampleTime = sampleTime.Add(stepDuration)
			}
		}
	}

	createScopedMetrics := func(rm pmetric.ResourceMetrics) {
		sms := rm.ScopeMetrics()

		sm := sms.AppendEmpty()
		scope := sm.Scope()
		scope.SetName("scope")
		metrics := sm.Metrics()
		createMetrics(metrics)
	}

	createResourceMetrics := func(md pmetric.Metrics) {
		rms := md.ResourceMetrics()
		rms.EnsureCapacity(numberOfResourceMetrics)
		for idx := range numberOfResourceMetrics {
			rm := rms.AppendEmpty()
			attrs := rm.Resource().Attributes()
			attrs.PutStr("env", "dev")
			attrs.PutStr("region", "us-east-1")
			attrs.PutStr("pod", fmt.Sprintf("pod-%d", idx))
			createScopedMetrics(rm)
		}
	}

	pushFunc := func(_ context.Context, pushReq *Request) error {
		if _, err := pushReq.WriteRequest(); err != nil {
			return err
		}

		pushReq.CleanUp()
		return nil
	}
	limits := validation.MockDefaultOverrides()
	handler := OTLPHandler(
		200000000, nil, nil, limits, nil, nil,
		RetryConfig{}, nil, pushFunc, nil, nil, log.NewNopLogger(),
	)

	b.Run("protobuf", func(b *testing.B) {
		md := pmetric.NewMetrics()
		createResourceMetrics(md)
		exportReq := pmetricotlp.NewExportRequestFromMetrics(md)
		req := createOTLPProtoRequest(b, exportReq, "")

		b.ResetTimer()
		for range b.N {
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

	var b bytes.Buffer
	var compressor io.WriteCloser
	switch compression {
	case "gzip":
		compressor = gzip.NewWriter(&b)
	case "lz4":
		compressor = lz4.NewWriter(&b)
	case "zstd":
		var err error
		compressor, err = zstd.NewWriter(&b)
		require.NoError(tb, err)
	}
	if compressor != nil {
		_, err := compressor.Write(body)
		require.NoError(tb, err)
		require.NoError(tb, compressor.Close())
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
			MetricFamilyName: "foo",
			Help:             "metric_help",
			Unit:             "metric_unit",
		},
	}

	const (
		jsonContentType = "application/json"
		pbContentType   = "application/x-protobuf"
	)

	type testCase struct {
		name                                        string
		series                                      []prompb.TimeSeries
		metadata                                    []mimirpb.MetricMetadata
		compression                                 string
		maxMsgSize                                  int
		verifyFunc                                  func(*testing.T, context.Context, *Request, testCase) error
		requestContentType                          string
		responseCode                                int
		responseContentType                         string
		responseContentLength                       int
		errMessage                                  string
		expectedLogs                                []string
		expectedRetryHeader                         bool
		expectedPartialSuccess                      *colmetricpb.ExportMetricsPartialSuccess
		promoteResourceAttributes                   []string
		expectedAttributePromotions                 map[string]string
		resourceAttributePromotionConfig            OTelResourceAttributePromotionConfig
		keepIdentifyingOTelResourceAttributesConfig KeepIdentifyingOTelResourceAttributesConfig
	}

	samplesVerifierFunc := func(t *testing.T, _ context.Context, pushReq *Request, tc testCase) error {
		t.Helper()

		request, err := pushReq.WriteRequest()
		require.NoError(t, err)
		require.Equal(t, mimirpb.OTLP, request.Source)

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
			name:                "Write samples. No compression",
			maxMsgSize:          100000,
			verifyFunc:          samplesVerifierFunc,
			series:              sampleSeries,
			metadata:            sampleMetadata,
			responseCode:        http.StatusOK,
			responseContentType: pbContentType,
		},
		{
			name:                  "Write samples. No compression. JSON format.",
			maxMsgSize:            100000,
			verifyFunc:            samplesVerifierFunc,
			series:                sampleSeries,
			metadata:              sampleMetadata,
			requestContentType:    jsonContentType,
			responseCode:          http.StatusOK,
			responseContentType:   jsonContentType,
			responseContentLength: 2,
		},
		{
			name:                        "Write samples. No compression. Resource attribute promotion.",
			maxMsgSize:                  100000,
			verifyFunc:                  samplesVerifierFunc,
			series:                      sampleSeries,
			metadata:                    sampleMetadata,
			responseCode:                http.StatusOK,
			responseContentType:         pbContentType,
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
			responseContentType:       pbContentType,
			promoteResourceAttributes: nil,
			resourceAttributePromotionConfig: fakeResourceAttributePromotionConfig{
				promote: []string{"resource.attr"},
			},
			expectedAttributePromotions: map[string]string{"resource_attr": "value"},
		},
		{
			name:                "Write samples. With gzip compression",
			compression:         "gzip",
			maxMsgSize:          100000,
			verifyFunc:          samplesVerifierFunc,
			series:              sampleSeries,
			metadata:            sampleMetadata,
			responseCode:        http.StatusOK,
			responseContentType: pbContentType,
		},
		{
			name:                "Write samples. With lz4 compression",
			compression:         "lz4",
			maxMsgSize:          100000,
			verifyFunc:          samplesVerifierFunc,
			series:              sampleSeries,
			metadata:            sampleMetadata,
			responseCode:        http.StatusOK,
			responseContentType: pbContentType,
		},
		{
			name:                "Write samples. With zstd compression",
			compression:         "zstd",
			maxMsgSize:          100000,
			verifyFunc:          samplesVerifierFunc,
			series:              sampleSeries,
			metadata:            sampleMetadata,
			responseCode:        http.StatusOK,
			responseContentType: pbContentType,
		},
		{
			name:       "Write samples. No compression, request too big",
			maxMsgSize: 30,
			series:     sampleSeries,
			metadata:   sampleMetadata,
			verifyFunc: func(_ *testing.T, _ context.Context, pushReq *Request, _ testCase) error {
				_, err := pushReq.WriteRequest()
				return err
			},
			responseCode:          http.StatusRequestEntityTooLarge,
			responseContentType:   pbContentType,
			responseContentLength: 307,
			errMessage:            "the incoming OTLP request has been rejected because its message size of 89 bytes (uncompressed) is larger",
			expectedLogs:          []string{`level=warn user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=413 err="rpc error: code = Code(413) desc = the incoming OTLP request has been rejected because its message size of 89 bytes (uncompressed) is larger than the allowed limit of 30 bytes (err-mimir-distributor-max-otlp-request-size). To adjust the related limit, configure -distributor.max-otlp-request-size, or contact your service administrator." insight=true`},
		},
		{
			name:        "Write samples. Unsupported compression",
			compression: "snappy",
			maxMsgSize:  100000,
			series:      sampleSeries,
			metadata:    sampleMetadata,
			verifyFunc: func(_ *testing.T, _ context.Context, pushReq *Request, _ testCase) error {
				_, err := pushReq.WriteRequest()
				return err
			},
			responseCode:          http.StatusUnsupportedMediaType,
			responseContentLength: 93,
			responseContentType:   pbContentType,
			errMessage:            "Only \"gzip\", \"lz4\", \"zstd\", or no compression supported",
			expectedLogs:          []string{`level=warn user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=415 err="rpc error: code = Code(415) desc = unsupported compression: snappy. Only \"gzip\", \"lz4\", \"zstd\", or no compression supported" insight=true`},
		},
		{
			name:        "Write samples. Unsupported compression. JSON format",
			compression: "snappy",
			maxMsgSize:  100000,
			series:      sampleSeries,
			metadata:    sampleMetadata,
			verifyFunc: func(_ *testing.T, _ context.Context, pushReq *Request, _ testCase) error {
				_, err := pushReq.WriteRequest()
				return err
			},
			requestContentType:    jsonContentType,
			responseCode:          http.StatusUnsupportedMediaType,
			responseContentLength: 119,
			responseContentType:   jsonContentType,
			errMessage:            "Only \"gzip\", \"lz4\", \"zstd\", or no compression supported",
			expectedLogs:          []string{`level=warn user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=415 err="rpc error: code = Code(415) desc = unsupported compression: snappy. Only \"gzip\", \"lz4\", \"zstd\", or no compression supported" insight=true`},
		},
		{
			name:        "Write samples. With gzip compression, request too big",
			compression: "gzip",
			maxMsgSize:  30,
			series:      sampleSeries,
			metadata:    sampleMetadata,
			verifyFunc: func(_ *testing.T, _ context.Context, pushReq *Request, _ testCase) error {
				_, err := pushReq.WriteRequest()
				return err
			},
			responseCode:          http.StatusRequestEntityTooLarge,
			responseContentType:   pbContentType,
			responseContentLength: 308,
			errMessage:            "the incoming OTLP request has been rejected because its message size of 104 bytes (uncompressed) is larger",
			expectedLogs:          []string{`level=warn user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=413 err="rpc error: code = Code(413) desc = the incoming OTLP request has been rejected because its message size of 104 bytes (uncompressed) is larger than the allowed limit of 30 bytes (err-mimir-distributor-max-otlp-request-size). To adjust the related limit, configure -distributor.max-otlp-request-size, or contact your service administrator." insight=true`},
		},
		{
			name:        "Write samples. With lz4 compression, request too big",
			compression: "lz4",
			maxMsgSize:  30,
			series:      sampleSeries,
			metadata:    sampleMetadata,
			verifyFunc: func(_ *testing.T, _ context.Context, pushReq *Request, _ testCase) error {
				_, err := pushReq.WriteRequest()
				return err
			},
			responseCode:          http.StatusRequestEntityTooLarge,
			responseContentType:   pbContentType,
			responseContentLength: 308,
			errMessage:            "the incoming OTLP request has been rejected because its message size of 106 bytes (uncompressed) is larger",
			expectedLogs:          []string{`level=warn user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=413 err="rpc error: code = Code(413) desc = the incoming OTLP request has been rejected because its message size of 106 bytes (uncompressed) is larger than the allowed limit of 30 bytes (err-mimir-distributor-max-otlp-request-size). To adjust the related limit, configure -distributor.max-otlp-request-size, or contact your service administrator." insight=true`},
		},
		{
			name:       "Rate limited request",
			maxMsgSize: 100000,
			series:     sampleSeries,
			metadata:   sampleMetadata,
			verifyFunc: func(*testing.T, context.Context, *Request, testCase) error {
				return httpgrpc.Errorf(http.StatusTooManyRequests, "go slower")
			},
			responseCode:          http.StatusTooManyRequests,
			responseContentType:   pbContentType,
			responseContentLength: 14,
			errMessage:            "go slower",
			expectedLogs:          []string{`level=warn user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=429 err="rpc error: code = Code(429) desc = go slower" insight=true`},
			expectedRetryHeader:   true,
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
					MetricFamilyName: "foo",
					Help:             "metric_help",
					Unit:             "metric_unit",
				},
			},
			verifyFunc: func(t *testing.T, _ context.Context, pushReq *Request, _ testCase) error {
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
			responseCode:        http.StatusOK,
			responseContentType: pbContentType,
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
					MetricFamilyName: "foo",
					Help:             "metric_help",
					Unit:             "metric_unit",
				},
			},
			verifyFunc: func(t *testing.T, _ context.Context, pushReq *Request, _ testCase) error {
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
			responseCode:        http.StatusOK,
			responseContentType: pbContentType,
		},
		{
			name:       "Attribute value too long",
			maxMsgSize: 100000,
			series: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "foo"},
						{Name: "too_long", Value: "huge value"},
					},
					Samples: []prompb.Sample{
						{Value: 1, Timestamp: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
					},
				},
			},
			metadata: sampleMetadata,
			verifyFunc: func(_ *testing.T, ctx context.Context, pushReq *Request, _ testCase) error {
				var limitsCfg validation.Limits
				flagext.DefaultValues(&limitsCfg)
				limitsCfg.MaxLabelValueLength = len("huge value") - 1
				distributors, _, _, _ := prepare(t, prepConfig{numDistributors: 1, limits: &limitsCfg})
				distributor := distributors[0]
				return distributor.prePushValidationMiddleware(func(context.Context, *Request) error { return nil })(ctx, pushReq)
			},
			responseCode:          http.StatusBadRequest,
			responseContentType:   pbContentType,
			responseContentLength: 286,
			errMessage:            "received a metric whose attribute value length of 10 exceeds the limit of 9, attribute: 'too_long', value: 'huge value' (truncated) metric: 'foo{too_long=\"huge value\"}'. See: https://grafana.com/docs/grafana-cloud/send-data/otlp/otlp-format-considerations/#metrics-ingestion-limits",
			expectedLogs:          []string{`level=warn user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=400 err="received a metric whose attribute value length of 10 exceeds the limit of 9, attribute: 'too_long', value: 'huge value' (truncated) metric: 'foo{too_long=\"huge value\"}'. See: https://grafana.com/docs/grafana-cloud/send-data/otlp/otlp-format-considerations/#metrics-ingestion-limits" insight=true`},
			expectedRetryHeader:   false,
		},
		{
			name:       "Unexpected gRPC status error",
			maxMsgSize: 100000,
			series:     sampleSeries,
			metadata:   sampleMetadata,
			verifyFunc: func(*testing.T, context.Context, *Request, testCase) error {
				return grpcstatus.New(codes.Unknown, "unexpected error calling some dependency").Err()
			},
			responseCode:          http.StatusServiceUnavailable,
			responseContentType:   pbContentType,
			responseContentLength: 44,
			errMessage:            "unexpected error calling some dependency",
			expectedLogs:          []string{`level=error user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=503 err="rpc error: code = Unknown desc = unexpected error calling some dependency"`},
			expectedRetryHeader:   true,
		},
		{
			name:       "Unexpected ingesterPushError",
			maxMsgSize: 100000,
			series:     sampleSeries,
			metadata:   sampleMetadata,
			verifyFunc: func(*testing.T, context.Context, *Request, testCase) error {
				return ingesterPushError{message: "unexpected ingester error", cause: mimirpb.ERROR_CAUSE_BAD_DATA, soft: false}
			},
			responseCode:          http.StatusBadRequest,
			responseContentType:   pbContentType,
			responseContentLength: 29,
			errMessage:            "unexpected ingester error",
			expectedLogs:          []string{`level=warn user=test msg="detected an error while ingesting OTLP metrics request (the request may have been partially ingested)" httpCode=400 err="unexpected ingester error" insight=true`},
			expectedRetryHeader:   false,
		},
		{
			name:       "Unexpected soft ingesterPushError",
			maxMsgSize: 100000,
			series:     sampleSeries,
			metadata:   sampleMetadata,
			verifyFunc: func(*testing.T, context.Context, *Request, testCase) error {
				return ingesterPushError{message: "unexpected ingester error", cause: mimirpb.ERROR_CAUSE_BAD_DATA, soft: true}
			},
			responseCode:          http.StatusOK,
			responseContentType:   pbContentType,
			responseContentLength: 29,
			expectedRetryHeader:   false,
			expectedPartialSuccess: &colmetricpb.ExportMetricsPartialSuccess{
				RejectedDataPoints: 0,
				ErrorMessage:       "unexpected ingester error",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exportReq := TimeseriesToOTLPRequest(tt.series, tt.metadata)
			var req *http.Request
			if tt.requestContentType == jsonContentType {
				req = createOTLPJSONRequest(t, exportReq, tt.compression)
			} else {
				req = createOTLPProtoRequest(t, exportReq, tt.compression)
			}

			testLimits := &validation.Limits{
				PromoteOTelResourceAttributes: tt.promoteResourceAttributes,
				NameValidationScheme:          model.LegacyValidation,
				OTelMetricSuffixesEnabled:     false,
			}
			limits := validation.NewOverrides(
				validation.Limits{},
				validation.NewMockTenantLimits(map[string]*validation.Limits{
					"test": testLimits,
				}),
			)

			pusher := func(ctx context.Context, pushReq *Request) error {
				t.Helper()
				t.Cleanup(pushReq.CleanUp)
				return tt.verifyFunc(t, ctx, pushReq, tt)
			}

			logs := &concurrency.SyncBuffer{}
			retryConfig := RetryConfig{Enabled: true, MinBackoff: 5 * time.Second, MaxBackoff: 5 * time.Second}
			handler := OTLPHandler(
				tt.maxMsgSize, nil, nil, limits,
				tt.resourceAttributePromotionConfig, tt.keepIdentifyingOTelResourceAttributesConfig,
				retryConfig, nil, pusher, nil, nil,
				util_log.MakeLeveledLogger(logs, "info"),
			)

			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)

			assert.Equal(t, tt.responseCode, resp.Code)
			assert.Equal(t, tt.responseContentType, resp.Header().Get("Content-Type"))
			assert.Equal(t, strconv.Itoa(tt.responseContentLength), resp.Header().Get("Content-Length"))
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			if tt.responseCode/100 == 2 {
				var exportResp colmetricpb.ExportMetricsServiceResponse
				if tt.responseContentType == jsonContentType {
					err = json.Unmarshal(body, &exportResp)
				} else {
					err = proto.Unmarshal(body, &exportResp)
				}
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPartialSuccess, exportResp.PartialSuccess)
			} else {
				respStatus := &status.Status{}
				if tt.responseContentType == jsonContentType {
					err = json.Unmarshal(body, respStatus)
				} else {
					err = proto.Unmarshal(body, respStatus)
				}
				require.NoError(t, err)
				require.Contains(t, respStatus.GetMessage(), tt.errMessage)
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

	limits := validation.NewOverrides(
		validation.Limits{
			NameValidationScheme:    model.LegacyValidation,
			OTelTranslationStrategy: validation.OTelTranslationStrategyValue(otlptranslator.UnderscoreEscapingWithoutSuffixes),
		},
		validation.NewMockTenantLimits(map[string]*validation.Limits{}),
	)

	req := createOTLPProtoRequest(t, pmetricotlp.NewExportRequestFromMetrics(md), "")
	resp := httptest.NewRecorder()
	handler := OTLPHandler(
		100000, nil, nil, limits, nil, nil,
		RetryConfig{}, nil, func(_ context.Context, pushReq *Request) error {
			request, err := pushReq.WriteRequest()
			assert.NoError(t, err)
			assert.Len(t, request.Timeseries, 3)
			assert.False(t, request.SkipLabelValidation)
			pushReq.CleanUp()
			return nil
		}, nil, nil, log.NewNopLogger(),
	)
	handler.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusBadRequest, resp.Code)
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

	limits := validation.MockDefaultOverrides()

	req := createOTLPProtoRequest(t, pmetricotlp.NewExportRequestFromMetrics(md), "")
	resp := httptest.NewRecorder()
	handler := OTLPHandler(
		100000, nil, nil, limits, nil, nil,
		RetryConfig{}, nil, func(_ context.Context, pushReq *Request) error {
			request, err := pushReq.WriteRequest()
			t.Cleanup(pushReq.CleanUp)
			require.NoError(t, err)
			assert.Len(t, request.Timeseries, 1)
			assert.False(t, request.SkipLabelValidation)
			return nil
		}, nil, nil, log.NewNopLogger(),
	)
	handler.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusBadRequest, resp.Code)

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
	handler = OTLPHandler(
		100000, nil, nil, limits, nil, nil,
		RetryConfig{}, nil, func(_ context.Context, pushReq *Request) error {
			request, err := pushReq.WriteRequest()
			t.Cleanup(pushReq.CleanUp)
			require.NoError(t, err)
			assert.Len(t, request.Timeseries, 9) // 6 buckets (including +Inf) + 2 sum/count + 2 from the first case
			assert.False(t, request.SkipLabelValidation)
			return nil
		}, nil, nil, log.NewNopLogger(),
	)
	handler.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusBadRequest, resp.Code)
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

	handler := OTLPHandler(
		140, nil, nil, nil, nil, nil,
		RetryConfig{}, nil, readBodyPushFunc(t), nil, nil, log.NewNopLogger(),
	)
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
		expectedSoft       bool
	}
	testCases := map[string]testStruct{
		"a generic error gets translated into gRPC code.Internal and HTTP 503 statuses": {
			err:                originalErr,
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
			expectedSoft:       false,
		},
		"a DoNotLog of a generic error gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                middleware.DoNotLogError{Err: originalErr},
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
			expectedSoft:       false,
		},
		"a context.DeadlineExceeded gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                context.DeadlineExceeded,
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
			expectedSoft:       false,
		},
		"a replicasDidNotMatchError gets translated into gRPC codes.AlreadyExists and HTTP 202 statuses": {
			err:                replicasNotMatchErr,
			expectedHTTPStatus: http.StatusAccepted,
			expectedGRPCStatus: codes.AlreadyExists,
			expectedSoft:       false,
		},
		"a DoNotLogError of a replicasDidNotMatchError gets translated into gRPC codes.AlreadyExists and HTTP 202 statuses": {
			err:                middleware.DoNotLogError{Err: replicasNotMatchErr},
			expectedHTTPStatus: http.StatusAccepted,
			expectedGRPCStatus: codes.AlreadyExists,
			expectedSoft:       false,
		},
		"a tooManyClustersError gets translated into gRPC codes.FailedPrecondition and HTTP 400 statuses": {
			err:                tooManyClustersErr,
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.FailedPrecondition,
			expectedSoft:       false,
		},
		"a DoNotLogError of a tooManyClustersError gets translated into gRPC codes.FailedPrecondition and HTTP 400 statuses": {
			err:                middleware.DoNotLogError{Err: tooManyClustersErr},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.FailedPrecondition,
			expectedSoft:       false,
		},
		"a validationError gets translated into gRPC codes.InvalidArgument and HTTP 400 statuses": {
			err:                newValidationError(originalErr),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.InvalidArgument,
			expectedSoft:       false,
		},
		"a DoNotLogError of a validationError gets translated into gRPC codes.InvalidArgument and HTTP 400 statuses": {
			err:                middleware.DoNotLogError{Err: newValidationError(originalErr)},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.InvalidArgument,
			expectedSoft:       false,
		},
		"an ingestionRateLimitedError gets translated into gRPC codes.ResourceExhausted and HTTP 429 statuses": {
			err:                ingestionRateLimitedErr,
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedGRPCStatus: codes.ResourceExhausted,
			expectedSoft:       false,
		},
		"a DoNotLogError of an ingestionRateLimitedError gets translated into gRPC codes.ResourceExhausted and HTTP 429 statuses": {
			err:                middleware.DoNotLogError{Err: ingestionRateLimitedErr},
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedGRPCStatus: codes.ResourceExhausted,
			expectedSoft:       false,
		},
		"an ingesterPushError with BAD_DATA cause gets translated into gRPC codes.InvalidArgument and HTTP 400 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.InvalidArgument, originalMsg, mimirpb.ERROR_CAUSE_BAD_DATA), ingesterID),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.InvalidArgument,
			expectedSoft:       false,
		},
		"a DoNotLogError of an ingesterPushError with BAD_DATA cause gets translated into gRPC codes.InvalidArgument and HTTP 400 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.InvalidArgument, originalMsg, mimirpb.ERROR_CAUSE_BAD_DATA), ingesterID)},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.InvalidArgument,
			expectedSoft:       false,
		},
		"an ingesterPushError with TENANT_LIMIT cause gets translated into gRPC codes.FailedPrecondition and HTTP 400 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.FailedPrecondition, originalMsg, mimirpb.ERROR_CAUSE_TENANT_LIMIT), ingesterID),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.FailedPrecondition,
			expectedSoft:       false,
		},
		"a DoNotLogError of an ingesterPushError with TENANT_LIMIT cause gets translated into gRPC codes.FailedPrecondition and HTTP 400 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.FailedPrecondition, originalMsg, mimirpb.ERROR_CAUSE_TENANT_LIMIT), ingesterID)},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.FailedPrecondition,
			expectedSoft:       false,
		},
		"an ingesterPushError with METHOD_NOT_ALLOWED cause gets translated into gRPC codes.Unimplemented and HTTP 503 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unimplemented, originalMsg, mimirpb.ERROR_CAUSE_METHOD_NOT_ALLOWED), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Unimplemented,
			expectedSoft:       false,
		},
		"a DoNotLogError of an ingesterPushError with METHOD_NOT_ALLOWED cause gets translated into gRPC codes.Unimplemented and HTTP 503 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unimplemented, originalMsg, mimirpb.ERROR_CAUSE_METHOD_NOT_ALLOWED), ingesterID)},
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Unimplemented,
			expectedSoft:       false,
		},
		"an ingesterPushError with TSDB_UNAVAILABLE cause gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.ERROR_CAUSE_TSDB_UNAVAILABLE), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
			expectedSoft:       false,
		},
		"a DoNotLogError of an ingesterPushError with TSDB_UNAVAILABLE cause gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.ERROR_CAUSE_TSDB_UNAVAILABLE), ingesterID)},
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
			expectedSoft:       false,
		},
		"an ingesterPushError with SERVICE_UNAVAILABLE cause gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.ERROR_CAUSE_SERVICE_UNAVAILABLE), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
			expectedSoft:       false,
		},
		"a DoNotLogError of an ingesterPushError with SERVICE_UNAVAILABLE cause gets translated gRPC codes.Internal and HTTP 503 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.ERROR_CAUSE_SERVICE_UNAVAILABLE), ingesterID)},
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
			expectedSoft:       false,
		},
		"an ingesterPushError with INSTANCE_LIMIT cause gets translated into gRPC codes.Unavailable and HTTP 503 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.ERROR_CAUSE_INSTANCE_LIMIT), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Unavailable,
			expectedSoft:       false,
		},
		"a DoNotLogError of an ingesterPushError with INSTANCE_LIMIT cause gets translated into gRPC codes.Unavailable and HTTP 503 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.ERROR_CAUSE_INSTANCE_LIMIT), ingesterID)},
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Unavailable,
			expectedSoft:       false,
		},
		"an ingesterPushError with UNKNOWN_CAUSE cause gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.ERROR_CAUSE_UNKNOWN), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
			expectedSoft:       false,
		},
		"a DoNotLogError of an ingesterPushError with UNKNOWN_CAUSE cause gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.ERROR_CAUSE_UNKNOWN), ingesterID)},
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
			expectedSoft:       false,
		},
		"an ingesterPushError obtained from a DeadlineExceeded coming from the ingester gets translated into gRPC codes.Internal and HTTP 503 statuses": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Internal, context.DeadlineExceeded.Error(), mimirpb.ERROR_CAUSE_UNKNOWN), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Internal,
			expectedSoft:       false,
		},
		"an ingesterPushError with CIRCUIT_BREAKER_OPEN cause gets translated into an Unavailable error with CIRCUIT_BREAKER_OPEN cause": {
			err:                newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.ERROR_CAUSE_CIRCUIT_BREAKER_OPEN), ingesterID),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Unavailable,
			expectedSoft:       false,
		},
		"a wrapped ingesterPushError with CIRCUIT_BREAKER_OPEN cause gets translated into an Unavailable error with CIRCUIT_BREAKER_OPEN cause": {
			err:                errors.Wrap(newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.ERROR_CAUSE_CIRCUIT_BREAKER_OPEN), ingesterID), "wrapped"),
			expectedHTTPStatus: http.StatusServiceUnavailable,
			expectedGRPCStatus: codes.Unavailable,
			expectedSoft:       false,
		},
		"a soft ingesterPushError with BAD_DATA cause gets translated into gRPC codes.InvalidArgument and HTTP 200 statuses": {
			err:                newIngesterPushError(createSoftStatusWithDetails(t, codes.InvalidArgument, originalMsg, mimirpb.ERROR_CAUSE_BAD_DATA), ingesterID),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.InvalidArgument,
			expectedSoft:       true,
		},
		"a wrapped soft ingesterPushError with BAD_DATA cause gets translated into gRPC codes.InvalidArgument and HTTP 200 statuses": {
			err:                errors.Wrap(newIngesterPushError(createSoftStatusWithDetails(t, codes.InvalidArgument, originalMsg, mimirpb.ERROR_CAUSE_BAD_DATA), ingesterID), "wrapped"),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedGRPCStatus: codes.InvalidArgument,
			expectedSoft:       true,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			gStatus, status, soft := toOtlpGRPCHTTPStatus(tc.err)
			assert.Equal(t, tc.expectedHTTPStatus, status)
			assert.Equal(t, tc.expectedGRPCStatus, gStatus)
			assert.Equal(t, tc.expectedSoft, soft)
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

func TestOTLPResponseContentType(t *testing.T) {

	exportReq := TimeseriesToOTLPRequest([]prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "__name__", Value: "value"}},
		Samples: []prompb.Sample{{Value: 1, Timestamp: time.Now().UnixMilli()}},
	}}, nil)

	tests := map[string]struct {
		req        *http.Request
		expectCode int
		expectType string
	}{
		"protobuf": {
			req:        createOTLPProtoRequest(t, exportReq, ""),
			expectCode: http.StatusOK,
			expectType: "application/x-protobuf",
		},
		"json": {
			req:        createOTLPJSONRequest(t, exportReq, ""),
			expectCode: http.StatusOK,
			expectType: "application/json",
		},
		"valid_json_wrong_content_type": {
			req: func() *http.Request {
				body, _ := exportReq.MarshalJSON()
				return createOTLPRequest(t, body, "", "text/plain")
			}(),
			expectCode: http.StatusUnsupportedMediaType,
			expectType: "application/x-protobuf",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			limits := validation.NewOverrides(
				validation.Limits{},
				validation.NewMockTenantLimits(map[string]*validation.Limits{
					"test": {NameValidationScheme: model.LegacyValidation, OTelMetricSuffixesEnabled: false},
				}),
			)
			handler := OTLPHandler(100000, nil, nil, limits, nil, nil, RetryConfig{}, nil, func(_ context.Context, req *Request) error {
				_, err := req.WriteRequest()
				return err
			}, nil, nil, log.NewNopLogger())
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, tc.req)

			require.Equal(t, tc.expectCode, resp.Code)
			require.Equal(t, tc.expectType, resp.Header().Get("Content-Type"))
		})
	}
}

func TestOTLPJSONEnumEncoding(t *testing.T) {
	// Verify the marshaled JSON contains integer code, not string
	st := gogostatus.New(codes.Internal, "msg").Proto()
	data, err := json.Marshal(st)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"code":13`, "code field must be encoded as integer (13), not string")
}

type fakeResourceAttributePromotionConfig struct {
	promote []string
}

func (c fakeResourceAttributePromotionConfig) PromoteOTelResourceAttributes(string) []string {
	return c.promote
}
