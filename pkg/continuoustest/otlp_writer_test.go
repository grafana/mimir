// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
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
	const (
		metricName = "mimir_continuous_test_sine_wave_v2"
		numSeries  = 3
	)

	// Generate timestamp aligned to writeInterval, matching how the continuous test generates them.
	ts := alignTimestampToInterval(time.Now(), writeInterval)

	// Generate input timeseries using generateSineWaveSeries.
	inputTimeseries := generateSineWaveSeries(metricName, ts, numSeries)

	metadata := make([]mimirpb.MetricMetadata, 0, len(floatMetricMetadata))
	for _, m := range floatMetricMetadata {
		metadata = append(metadata, mimirpb.MetricMetadata{
			Type:             mimirpb.MetricMetadata_MetricType(m.Type),
			MetricFamilyName: m.MetricFamilyName,
			Help:             m.Help,
			Unit:             m.Unit,
		})
	}
	otlpRequest := distributor.TimeseriesToOTLPRequest(inputTimeseries, metadata)
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
	opts := distributor.ConversionOptions{
		AddSuffixes:                       true,
		EnableCTZeroIngestion:             true,
		KeepIdentifyingResourceAttributes: true,
		ConvertHistogramsToNHCB:           true,
		PromoteScopeMetadata:              true,
		PromoteResourceAttributes:         []string{"job", "instance"},
		AllowDeltaTemporality:             true,
		AllowUTF8:                         true,
		UnderscoreSanitization:            true,
		PreserveMultipleUnderscores:       true,
	}
	mimirTimeseries, err := distributor.OTLPToMimir(context.Background(), unmarshaledRequest.Metrics(), opts, log.NewNopLogger())
	require.NoError(t, err)

	// Filter to just the test metrics (excludes target_info generated from resource attributes).
	var resultTimeseries []mimirpb.PreallocTimeseries
	for _, ts := range mimirTimeseries {
		for _, label := range ts.Labels {
			if label.Name == "__name__" && label.Value == metricName {
				resultTimeseries = append(resultTimeseries, ts)
				break
			}
			if label.Name == "__name__" && label.Value != metricName {
				t.Fatalf("%s", label.Value)
			}
		}
	}

	// We generated numSeries series, each with 1 sample.
	require.Len(t, resultTimeseries, numSeries)

	// Verify each series has one sample at the expected timestamp.
	for _, series := range resultTimeseries {
		require.Len(t, series.Samples, 1, "each series should have 1 sample")
		require.Equal(t, ts.UnixMilli(), series.Samples[0].TimestampMs)

		// Verify expected labels are present.
		requireLabelExists(t, series.Labels, "__name__")
		requireLabelExists(t, series.Labels, "series_id")
		requireLabelExists(t, series.Labels, "hash_extra")
	}
}

// requireLabelExists asserts that the given labels contain a label with the specified name.
func requireLabelExists(t *testing.T, labels []mimirpb.LabelAdapter, name string) {
	t.Helper()
	for _, label := range labels {
		if label.Name == name {
			return
		}
	}
	t.Fatalf("label %s not found", name)
}
