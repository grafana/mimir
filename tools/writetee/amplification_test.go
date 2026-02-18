// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// TestSampleWriteRequest_RW2 tests RW2 request sampling scenarios.
func TestSampleWriteRequest_RW2(t *testing.T) {
	t.Run("rejects amplification factor > 1.0", func(t *testing.T) {
		req := makeRW2RequestWithLabels(1)
		compressed := compressRequest(t, &req)

		_, err := SampleWriteRequest(compressed, 2.0, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "only handles sampling")
	})

	t.Run("sampling is deterministic", func(t *testing.T) {
		req := makeRW2Request(4) // 4 series
		compressed := compressRequest(t, &req)

		// Run sampling multiple times - should get identical results each time
		var firstSeriesCount int
		for i := 0; i < 3; i++ {
			result, err := SampleWriteRequest(compressed, 0.5, nil)
			require.NoError(t, err)

			decompressed := decompressAndUnmarshalRW2(t, result.Body)

			if i == 0 {
				firstSeriesCount = len(decompressed.Timeseries)
				assert.Greater(t, firstSeriesCount, 0, "should keep some series")
				assert.Less(t, firstSeriesCount, 4, "should not keep all series")
			} else {
				assert.Equal(t, firstSeriesCount, len(decompressed.Timeseries), "should be deterministic")
			}
		}
	})

	t.Run("sampling does not modify symbols table", func(t *testing.T) {
		req := makeRW2RequestWithLabels(4)
		originalSymbols := req.SymbolsRW2
		compressed := compressRequest(t, &req)

		result, err := SampleWriteRequest(compressed, 0.5, nil)
		require.NoError(t, err)

		decompressed := decompressAndUnmarshalRW2(t, result.Body)

		// Symbols table should be unchanged during sampling
		assert.Equal(t, originalSymbols, decompressed.Symbols, "symbols table should not be modified during sampling")
	})
}

// TestAmplifyRequestBody_RW2 tests amplifying RW2 request bodies.
func TestAmplifyRequestBody_RW2(t *testing.T) {
	t.Run("rejects startSuffix < 1", func(t *testing.T) {
		req := makeRW2RequestWithLabels(1)
		compressed := compressRequest(t, &req)

		_, err := AmplifyRequestBody(compressed, 1, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "startSuffix must be >= 1")
	})

	t.Run("replica 2 suffixes all label values", func(t *testing.T) {
		req := makeRW2RequestWithLabels(1)
		compressed := compressRequest(t, &req)

		suffixed, err := AmplifyRequestBody(compressed, 1, 2)
		require.NoError(t, err)
		require.Len(t, suffixed, 1)

		decompressed := decompressAndUnmarshalRW2(t, suffixed[0])

		// Verify symbols table includes suffixed values
		assert.Contains(t, decompressed.Symbols, "prometheus_amp2")
		assert.Contains(t, decompressed.Symbols, "localhost:9090_amp2")

		// Verify the series has suffixed values
		labels := resolveLabels(decompressed.Symbols, decompressed.Timeseries[0].LabelsRefs)
		assert.Equal(t, "metric_0", labels["__name__"], "__name__ should not be suffixed")
		assert.Equal(t, "prometheus_amp2", labels["job"], "job should be suffixed with _amp2")
		assert.Equal(t, "localhost:9090_amp2", labels["instance"], "instance should be suffixed with _amp2")
	})

	t.Run("replica 3 suffixes with _amp3", func(t *testing.T) {
		req := makeRW2RequestWithLabels(1)
		compressed := compressRequest(t, &req)

		suffixed, err := AmplifyRequestBody(compressed, 1, 3)
		require.NoError(t, err)
		require.Len(t, suffixed, 1)

		decompressed := decompressAndUnmarshalRW2(t, suffixed[0])

		// Verify the series has suffixed values
		labels := resolveLabels(decompressed.Symbols, decompressed.Timeseries[0].LabelsRefs)
		assert.Equal(t, "metric_0", labels["__name__"], "__name__ should not be suffixed")
		assert.Equal(t, "prometheus_amp3", labels["job"], "job should be suffixed with _amp3")
		assert.Equal(t, "localhost:9090_amp3", labels["instance"], "instance should be suffixed with _amp3")
	})

	t.Run("series without __name__ suffixes all values", func(t *testing.T) {
		req := makeRW2RequestWithoutName(1)
		compressed := compressRequest(t, &req)

		suffixed, err := AmplifyRequestBody(compressed, 1, 2)
		require.NoError(t, err)
		require.Len(t, suffixed, 1)

		decompressed := decompressAndUnmarshalRW2(t, suffixed[0])

		// All values should be suffixed (no __name__ to exclude)
		labels := resolveLabels(decompressed.Symbols, decompressed.Timeseries[0].LabelsRefs)
		assert.Equal(t, "prometheus_amp2", labels["job"], "job should be suffixed")
		assert.Equal(t, "localhost:9090_amp2", labels["instance"], "instance should be suffixed")
	})
}

// TestSampleWriteRequest_RW1 tests RW1 request sampling scenarios.
func TestSampleWriteRequest_RW1(t *testing.T) {
	t.Run("rejects amplification factor > 1.0", func(t *testing.T) {
		req := makeRW1RequestWithLabels(1)
		compressed := compressRequest(t, &req)

		_, err := SampleWriteRequest(compressed, 2.0, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "only handles sampling")
	})

	t.Run("sampling is deterministic", func(t *testing.T) {
		req := makeRW1Request(4) // 4 series: metric_0 to metric_3
		compressed := compressRequest(t, &req)

		// Run sampling multiple times - should get identical results each time
		var firstSeriesCount int
		for i := 0; i < 3; i++ {
			result, err := SampleWriteRequest(compressed, 0.5, nil)
			require.NoError(t, err)

			decompressed := decompressAndUnmarshalRW1(t, result.Body)

			if i == 0 {
				firstSeriesCount = len(decompressed.Timeseries)
				assert.Greater(t, firstSeriesCount, 0, "should keep some series")
				assert.Less(t, firstSeriesCount, 4, "should not keep all series")
			} else {
				assert.Equal(t, firstSeriesCount, len(decompressed.Timeseries), "should be deterministic")
			}
		}
	})

}

// TestAmplifyRequestBody_RW1 tests amplifying RW1 request bodies.
func TestAmplifyRequestBody_RW1(t *testing.T) {
	t.Run("rejects startSuffix < 1", func(t *testing.T) {
		req := makeRW1RequestWithLabels(1)
		compressed := compressRequest(t, &req)

		_, err := AmplifyRequestBody(compressed, 1, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "startSuffix must be >= 1")
	})

	t.Run("replica 2 suffixes all label values", func(t *testing.T) {
		req := makeRW1RequestWithLabels(1)
		compressed := compressRequest(t, &req)

		suffixed, err := AmplifyRequestBody(compressed, 1, 2)
		require.NoError(t, err)
		require.Len(t, suffixed, 1)

		decompressed := decompressAndUnmarshalRW1(t, suffixed[0])

		// Verify the series has suffixed values
		labels := labelsToMap(decompressed.Timeseries[0].Labels)
		assert.Equal(t, "metric_0", labels["__name__"], "__name__ should not be suffixed")
		assert.Equal(t, "prometheus_amp2", labels["job"], "job should be suffixed with _amp2")
		assert.Equal(t, "localhost:9090_amp2", labels["instance"], "instance should be suffixed with _amp2")
	})

	t.Run("replica 3 suffixes with _amp3", func(t *testing.T) {
		req := makeRW1RequestWithLabels(1)
		compressed := compressRequest(t, &req)

		suffixed, err := AmplifyRequestBody(compressed, 1, 3)
		require.NoError(t, err)
		require.Len(t, suffixed, 1)

		decompressed := decompressAndUnmarshalRW1(t, suffixed[0])

		// Verify the series has suffixed values
		labels := labelsToMap(decompressed.Timeseries[0].Labels)
		assert.Equal(t, "metric_0", labels["__name__"], "__name__ should not be suffixed")
		assert.Equal(t, "prometheus_amp3", labels["job"], "job should be suffixed with _amp3")
		assert.Equal(t, "localhost:9090_amp3", labels["instance"], "instance should be suffixed with _amp3")
	})

	t.Run("series without __name__ suffixes all values", func(t *testing.T) {
		req := makeRW1RequestWithoutName(1)
		compressed := compressRequest(t, &req)

		suffixed, err := AmplifyRequestBody(compressed, 1, 2)
		require.NoError(t, err)
		require.Len(t, suffixed, 1)

		decompressed := decompressAndUnmarshalRW1(t, suffixed[0])

		// All values should be suffixed (no __name__ to exclude)
		labels := labelsToMap(decompressed.Timeseries[0].Labels)
		assert.Equal(t, "prometheus_amp2", labels["job"], "job should be suffixed")
		assert.Equal(t, "localhost:9090_amp2", labels["instance"], "instance should be suffixed")
	})
}

// Test helpers

func makeRW1Request(numSeries int) mimirpb.WriteRequest {
	var timeseries []mimirpb.PreallocTimeseries
	for i := 0; i < numSeries; i++ {
		timeseries = append(timeseries, mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: fmt.Sprintf("metric_%d", i)}},
				Samples: []mimirpb.Sample{{Value: float64(i), TimestampMs: 1000}},
			},
		})
	}
	return mimirpb.WriteRequest{Timeseries: timeseries}
}

// makeRW1RequestWithLabels creates RW1 requests with multiple labels for testing value suffixing.
func makeRW1RequestWithLabels(numSeries int) mimirpb.WriteRequest {
	var timeseries []mimirpb.PreallocTimeseries
	for i := 0; i < numSeries; i++ {
		timeseries = append(timeseries, mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{
					{Name: "__name__", Value: fmt.Sprintf("metric_%d", i)},
					{Name: "instance", Value: "localhost:9090"},
					{Name: "job", Value: "prometheus"},
				},
				Samples: []mimirpb.Sample{{Value: float64(i), TimestampMs: 1000}},
			},
		})
	}
	return mimirpb.WriteRequest{Timeseries: timeseries}
}

func makeRW2Request(numSeries int) mimirpb.WriteRequest {
	symbols := []string{"", "__name__"}
	var timeseries []mimirpb.TimeSeriesRW2
	for i := 0; i < numSeries; i++ {
		symbols = append(symbols, fmt.Sprintf("metric_%d", i))
		timeseries = append(timeseries, mimirpb.TimeSeriesRW2{
			LabelsRefs: []uint32{1, uint32(i + 2)},
			Samples:    []mimirpb.Sample{{Value: float64(i), TimestampMs: 1000}},
		})
	}
	return mimirpb.WriteRequest{SymbolsRW2: symbols, TimeseriesRW2: timeseries}
}

// makeRW2RequestWithLabels creates RW2 requests with multiple labels for testing value suffixing.
func makeRW2RequestWithLabels(numSeries int) mimirpb.WriteRequest {
	// Symbol table: [0]="", [1]="__name__", [2]="instance", [3]="job", [4]="localhost:9090", [5]="prometheus", [6+]="metric_N"
	symbols := []string{"", "__name__", "instance", "job", "localhost:9090", "prometheus"}
	var timeseries []mimirpb.TimeSeriesRW2
	for i := 0; i < numSeries; i++ {
		metricNameRef := uint32(len(symbols))
		symbols = append(symbols, fmt.Sprintf("metric_%d", i))
		timeseries = append(timeseries, mimirpb.TimeSeriesRW2{
			// __name__=metric_N, instance=localhost:9090, job=prometheus
			LabelsRefs: []uint32{1, metricNameRef, 2, 4, 3, 5},
			Samples:    []mimirpb.Sample{{Value: float64(i), TimestampMs: 1000}},
		})
	}
	return mimirpb.WriteRequest{SymbolsRW2: symbols, TimeseriesRW2: timeseries}
}

// makeRW1RequestWithoutName creates RW1 requests without __name__ label.
func makeRW1RequestWithoutName(numSeries int) mimirpb.WriteRequest {
	var timeseries []mimirpb.PreallocTimeseries
	for i := 0; i < numSeries; i++ {
		timeseries = append(timeseries, mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{
					{Name: "instance", Value: "localhost:9090"},
					{Name: "job", Value: "prometheus"},
				},
				Samples: []mimirpb.Sample{{Value: float64(i), TimestampMs: 1000}},
			},
		})
	}
	return mimirpb.WriteRequest{Timeseries: timeseries}
}

// makeRW2RequestWithoutName creates RW2 requests without __name__ label.
func makeRW2RequestWithoutName(numSeries int) mimirpb.WriteRequest {
	// Symbol table: [0]="", [1]="instance", [2]="job", [3]="localhost:9090", [4]="prometheus"
	symbols := []string{"", "instance", "job", "localhost:9090", "prometheus"}
	var timeseries []mimirpb.TimeSeriesRW2
	for i := 0; i < numSeries; i++ {
		timeseries = append(timeseries, mimirpb.TimeSeriesRW2{
			// instance=localhost:9090, job=prometheus (no __name__)
			LabelsRefs: []uint32{1, 3, 2, 4},
			Samples:    []mimirpb.Sample{{Value: float64(i), TimestampMs: 1000}},
		})
	}
	return mimirpb.WriteRequest{SymbolsRW2: symbols, TimeseriesRW2: timeseries}
}

// labelsToMap converts a slice of LabelAdapter to a map for easier testing.
func labelsToMap(labels []mimirpb.LabelAdapter) map[string]string {
	result := make(map[string]string)
	for _, label := range labels {
		result[label.Name] = label.Value
	}
	return result
}

// resolveLabels resolves label refs to a map of label name -> value.
func resolveLabels(symbols []string, labelRefs []uint32) map[string]string {
	result := make(map[string]string)
	for i := 0; i < len(labelRefs); i += 2 {
		nameRef := labelRefs[i]
		valueRef := labelRefs[i+1]
		if int(nameRef) < len(symbols) && int(valueRef) < len(symbols) {
			result[symbols[nameRef]] = symbols[valueRef]
		}
	}
	return result
}

func compressRequest(t *testing.T, req *mimirpb.WriteRequest) []byte {
	marshaled, err := proto.Marshal(req)
	require.NoError(t, err)
	return snappy.Encode(nil, marshaled)
}

func decompressAndUnmarshalRW1(t *testing.T, body []byte) mimirpb.WriteRequest {
	decompressed, err := snappy.Decode(nil, body)
	require.NoError(t, err)
	var result mimirpb.WriteRequest
	require.NoError(t, proto.Unmarshal(decompressed, &result))
	return result
}

func decompressAndUnmarshalRW2(t *testing.T, body []byte) *mimirpb.WriteRequestRW2 {
	decompressed, err := snappy.Decode(nil, body)
	require.NoError(t, err)
	result, err := mimirpb.UnmarshalWriteRequestRW2Native(decompressed)
	require.NoError(t, err)
	return result
}
