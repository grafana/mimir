// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// TestOTLPWriterE2E tests the end-to-end conversion of Prometheus samples to OTLP bytes
// and back to Mimir types. This validates that the conversion logic in otlp_writer.go
// (Prometheus -> OTLP bytes) is compatible with the conversion logic in otel.go
// (OTLP bytes -> Mimir types).
func TestOTLPWriterE2E(t *testing.T) {
	now := time.Now()
	ts1 := now.Add(-2 * time.Minute).UnixMilli()
	ts2 := now.Add(-1 * time.Minute).UnixMilli()
	ts3 := now.UnixMilli()

	// Create input Prometheus timeseries with samples at different timestamps.
	inputTimeseries := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "test_metric_one"},
				{Name: "label_a", Value: "value_a"},
			},
			Samples: []prompb.Sample{
				{Timestamp: ts1, Value: 1.0},
				{Timestamp: ts2, Value: 2.0},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "test_metric_two"},
				{Name: "label_b", Value: "value_b"},
				{Name: "label_c", Value: "value_c"},
			},
			Samples: []prompb.Sample{
				{Timestamp: ts2, Value: 100.5},
				{Timestamp: ts3, Value: 200.5},
			},
		},
	}

	// Step 1: Convert Prometheus timeseries to OTLP and marshal to bytes.
	// This mirrors the logic in otlp_writer.go's sendWriteRequest method.
	otlpRequest := distributor.TimeseriesToOTLPRequest(inputTimeseries, nil)
	rawBytes, err := otlpRequest.MarshalProto()
	require.NoError(t, err)
	require.NotEmpty(t, rawBytes)

	// Step 2: Unmarshal bytes back to OTLP request.
	// This mirrors what happens in otel.go when receiving an OTLP request.
	unmarshaledRequest := pmetricotlp.NewExportRequest()
	err = unmarshaledRequest.UnmarshalProto(rawBytes)
	require.NoError(t, err)

	// Step 3: Convert OTLP to Mimir types.
	// This mirrors the logic in otel.go's newOTLPParser function.
	mimirTimeseries, err := distributor.OTLPToMimir(context.Background(), unmarshaledRequest.Metrics(), log.NewNopLogger())
	require.NoError(t, err)

	// We expect 2 timeseries (one for each input metric).
	// Note: The OTLP conversion may also generate target_info series from resource attributes,
	// so we filter to just the metrics we care about.
	var resultTimeseries []mimirpb.PreallocTimeseries
	for _, ts := range mimirTimeseries {
		for _, label := range ts.Labels {
			if label.Name == "__name__" && (label.Value == "test_metric_one" || label.Value == "test_metric_two") {
				resultTimeseries = append(resultTimeseries, ts)
				break
			}
		}
	}
	require.Len(t, resultTimeseries, 2)

	// Verify metric one.
	metricOne := findTimeseriesByName(t, resultTimeseries, "test_metric_one")
	require.NotNil(t, metricOne)
	require.Len(t, metricOne.Samples, 2)
	require.Equal(t, ts1, metricOne.Samples[0].TimestampMs)
	require.Equal(t, 1.0, metricOne.Samples[0].Value)
	require.Equal(t, ts2, metricOne.Samples[1].TimestampMs)
	require.Equal(t, 2.0, metricOne.Samples[1].Value)
	requireLabelValue(t, metricOne.Labels, "label_a", "value_a")

	// Verify metric two.
	metricTwo := findTimeseriesByName(t, resultTimeseries, "test_metric_two")
	require.NotNil(t, metricTwo)
	require.Len(t, metricTwo.Samples, 2)
	require.Equal(t, ts2, metricTwo.Samples[0].TimestampMs)
	require.Equal(t, 100.5, metricTwo.Samples[0].Value)
	require.Equal(t, ts3, metricTwo.Samples[1].TimestampMs)
	require.Equal(t, 200.5, metricTwo.Samples[1].Value)
	requireLabelValue(t, metricTwo.Labels, "label_b", "value_b")
	requireLabelValue(t, metricTwo.Labels, "label_c", "value_c")
}

// findTimeseriesByName returns the timeseries with the given metric name, or nil if not found.
func findTimeseriesByName(t *testing.T, timeseries []mimirpb.PreallocTimeseries, name string) *mimirpb.TimeSeries {
	t.Helper()
	for _, ts := range timeseries {
		for _, label := range ts.Labels {
			if label.Name == "__name__" && label.Value == name {
				return ts.TimeSeries
			}
		}
	}
	return nil
}

// requireLabelValue asserts that the given labels contain the expected name/value pair.
func requireLabelValue(t *testing.T, labels []mimirpb.LabelAdapter, name, expectedValue string) {
	t.Helper()
	for _, label := range labels {
		if label.Name == name {
			require.Equal(t, expectedValue, label.Value, "label %s has unexpected value", name)
			return
		}
	}
	t.Fatalf("label %s not found", name)
}
