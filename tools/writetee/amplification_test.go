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

// TestAmplifyWriteRequest_RW2 tests RW2 request amplification scenarios.
func TestAmplifyWriteRequest_RW2(t *testing.T) {
	t.Run("2x amplification", func(t *testing.T) {
		req := makeRW2RequestWithLabels(1)
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 2.0, 0, nil)
		require.NoError(t, err)

		require.Len(t, result.Bodies, 1, "should have single body without splitting")
		decompressed := decompressAndUnmarshalRW2(t, result.Bodies[0])

		// Verify results
		assert.Equal(t, 2, len(decompressed.Timeseries), "should have 2 time series (original + 1 amplified copy)")
		assert.True(t, result.IsRW2, "should be detected as RW 2.0")
		assert.True(t, result.WasAmplified, "should be amplified")
		assert.Equal(t, 1, result.OriginalSeriesCount)
		assert.Equal(t, 1, result.AmplifiedSeriesCount)

		// Verify symbols table includes suffixed values for replica 2
		assert.Contains(t, decompressed.Symbols, "prometheus_amp2")
		assert.Contains(t, decompressed.Symbols, "localhost:9090_amp2")

		// First series should be original (same number of label refs)
		assert.Equal(t, 6, len(decompressed.Timeseries[0].LabelsRefs)) // __name__, job, instance = 3 pairs = 6 refs

		// Second series should have same number of label refs (values are suffixed, not added)
		assert.Equal(t, 6, len(decompressed.Timeseries[1].LabelsRefs))

		// Verify the original series keeps original values
		originalLabels := resolveLabels(decompressed.Symbols, decompressed.Timeseries[0].LabelsRefs)
		assert.Equal(t, "metric_0", originalLabels["__name__"])
		assert.Equal(t, "prometheus", originalLabels["job"])
		assert.Equal(t, "localhost:9090", originalLabels["instance"])

		// Verify the replica 2 series has suffixed values
		replicaLabels := resolveLabels(decompressed.Symbols, decompressed.Timeseries[1].LabelsRefs)
		assert.Equal(t, "metric_0", replicaLabels["__name__"], "__name__ should not be suffixed")
		assert.Equal(t, "prometheus_amp2", replicaLabels["job"], "job should be suffixed with _amp2")
		assert.Equal(t, "localhost:9090_amp2", replicaLabels["instance"], "instance should be suffixed with _amp2")
	})

	t.Run("3x amplification", func(t *testing.T) {
		req := makeRW2RequestWithLabels(1)
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 3.0, 0, nil)
		require.NoError(t, err)

		require.Len(t, result.Bodies, 1, "should have single body without splitting")
		decompressed := decompressAndUnmarshalRW2(t, result.Bodies[0])

		// Verify results: 1 original + 2 copies = 3 total
		assert.Equal(t, 3, len(decompressed.Timeseries))

		// Verify symbols table includes suffixed values for replicas 2 and 3
		assert.Contains(t, decompressed.Symbols, "prometheus_amp2")
		assert.Contains(t, decompressed.Symbols, "prometheus_amp3")
		assert.Contains(t, decompressed.Symbols, "localhost:9090_amp2")
		assert.Contains(t, decompressed.Symbols, "localhost:9090_amp3")

		// Verify each series has correct suffixes
		for i, ts := range decompressed.Timeseries {
			labels := resolveLabels(decompressed.Symbols, ts.LabelsRefs)
			if i == 0 {
				// Original (replica 1) - no suffix
				assert.Equal(t, "prometheus", labels["job"])
				assert.Equal(t, "localhost:9090", labels["instance"])
			} else {
				// Replicas 2, 3, ... should have _amp{i+1} suffix
				expectedSuffix := fmt.Sprintf("_amp%d", i+1)
				assert.Equal(t, "prometheus"+expectedSuffix, labels["job"], "replica %d job", i+1)
				assert.Equal(t, "localhost:9090"+expectedSuffix, labels["instance"], "replica %d instance", i+1)
			}
			// __name__ should never be suffixed
			assert.Equal(t, "metric_0", labels["__name__"])
		}
	})

	t.Run("sampling is deterministic", func(t *testing.T) {
		req := makeRW2Request(4) // 4 series
		compressed := compressRequest(t, &req)

		// Run sampling multiple times - should get identical results each time
		var firstSeriesCount int
		for i := 0; i < 3; i++ {
			result, err := AmplifyWriteRequest(compressed, 0.5, 0, nil)
			require.NoError(t, err)
			assert.True(t, result.IsRW2)

			require.Len(t, result.Bodies, 1, "should have single body without splitting")
		decompressed := decompressAndUnmarshalRW2(t, result.Bodies[0])

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

		result, err := AmplifyWriteRequest(compressed, 0.5, 0, nil)
		require.NoError(t, err)

		require.Len(t, result.Bodies, 1, "should have single body without splitting")
		decompressed := decompressAndUnmarshalRW2(t, result.Bodies[0])

		// Symbols table should be unchanged during sampling
		assert.Equal(t, originalSymbols, decompressed.Symbols, "symbols table should not be modified during sampling")
	})

	t.Run("fractional amplification is deterministic", func(t *testing.T) {
		// Use series with multiple labels so they can be amplified
		req := makeRW2RequestWithLabels(10)
		compressed := compressRequest(t, &req)

		// With 1.5x factor: 10 originals + ~5 fractional copies = ~15 total
		var firstSeriesCount int
		for i := 0; i < 3; i++ {
			result, err := AmplifyWriteRequest(compressed, 1.5, 0, nil)
			require.NoError(t, err)

			require.Len(t, result.Bodies, 1, "should have single body without splitting")
		decompressed := decompressAndUnmarshalRW2(t, result.Bodies[0])

			if i == 0 {
				firstSeriesCount = len(decompressed.Timeseries)
				assert.Greater(t, firstSeriesCount, 10, "should have more than originals")
				assert.Less(t, firstSeriesCount, 20, "should have less than 2x")
			} else {
				assert.Equal(t, firstSeriesCount, len(decompressed.Timeseries), "should be deterministic")
			}
		}
	})

	t.Run("series without __name__ label suffixes all values", func(t *testing.T) {
		// Create a request with a series that has no __name__ label
		// All label values should be suffixed
		req := makeRW2RequestWithoutName(1)
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 2.0, 0, nil)
		require.NoError(t, err)

		require.Len(t, result.Bodies, 1, "should have single body without splitting")
		decompressed := decompressAndUnmarshalRW2(t, result.Bodies[0])

		// Verify results: 1 original + 1 copy = 2 total
		assert.Equal(t, 2, len(decompressed.Timeseries))

		// Original should have original values
		originalLabels := resolveLabels(decompressed.Symbols, decompressed.Timeseries[0].LabelsRefs)
		assert.Equal(t, "prometheus", originalLabels["job"])
		assert.Equal(t, "localhost:9090", originalLabels["instance"])

		// Replica 2 should have ALL values suffixed (since no __name__ to exclude)
		replicaLabels := resolveLabels(decompressed.Symbols, decompressed.Timeseries[1].LabelsRefs)
		assert.Equal(t, "prometheus_amp2", replicaLabels["job"], "job should be suffixed")
		assert.Equal(t, "localhost:9090_amp2", replicaLabels["instance"], "instance should be suffixed")
	})
}

// TestAmplifyWriteRequest_RW1 tests RW1 request amplification scenarios.
func TestAmplifyWriteRequest_RW1(t *testing.T) {
	t.Run("2x amplification", func(t *testing.T) {
		req := makeRW1RequestWithLabels(1)
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 2.0, 0, nil)
		require.NoError(t, err)

		require.Len(t, result.Bodies, 1, "should have single body without splitting")
		decompressed := decompressAndUnmarshalRW1(t, result.Bodies[0])

		// Verify results
		assert.Equal(t, 2, len(decompressed.Timeseries), "should have 2 time series (original + 1 amplified copy)")
		assert.False(t, result.IsRW2)
		assert.True(t, result.WasAmplified)

		// First series should be original (3 labels: __name__, job, instance)
		assert.Equal(t, 3, len(decompressed.Timeseries[0].Labels))
		originalLabels := labelsToMap(decompressed.Timeseries[0].Labels)
		assert.Equal(t, "metric_0", originalLabels["__name__"])
		assert.Equal(t, "prometheus", originalLabels["job"])
		assert.Equal(t, "localhost:9090", originalLabels["instance"])

		// Second series should have same number of labels (values are suffixed, not added)
		assert.Equal(t, 3, len(decompressed.Timeseries[1].Labels))

		// Verify the replica 2 series has suffixed values
		replicaLabels := labelsToMap(decompressed.Timeseries[1].Labels)
		assert.Equal(t, "metric_0", replicaLabels["__name__"], "__name__ should not be suffixed")
		assert.Equal(t, "prometheus_amp2", replicaLabels["job"], "job should be suffixed with _amp2")
		assert.Equal(t, "localhost:9090_amp2", replicaLabels["instance"], "instance should be suffixed with _amp2")
	})

	t.Run("3x amplification", func(t *testing.T) {
		req := makeRW1RequestWithLabels(1)
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 3.0, 0, nil)
		require.NoError(t, err)

		require.Len(t, result.Bodies, 1, "should have single body without splitting")
		decompressed := decompressAndUnmarshalRW1(t, result.Bodies[0])

		// Verify results: 1 original + 2 copies = 3 total
		assert.Equal(t, 3, len(decompressed.Timeseries))

		// Verify each series has correct suffixes
		for i, ts := range decompressed.Timeseries {
			labels := labelsToMap(ts.Labels)
			if i == 0 {
				// Original (replica 1) - no suffix
				assert.Equal(t, "prometheus", labels["job"])
				assert.Equal(t, "localhost:9090", labels["instance"])
			} else {
				// Replicas 2, 3, ... should have _amp{i+1} suffix
				expectedSuffix := fmt.Sprintf("_amp%d", i+1)
				assert.Equal(t, "prometheus"+expectedSuffix, labels["job"], "replica %d job", i+1)
				assert.Equal(t, "localhost:9090"+expectedSuffix, labels["instance"], "replica %d instance", i+1)
			}
			// __name__ should never be suffixed
			assert.Equal(t, "metric_0", labels["__name__"])
		}
	})

	t.Run("sampling is deterministic", func(t *testing.T) {
		req := makeRW1Request(4) // 4 series: metric_0 to metric_3
		compressed := compressRequest(t, &req)

		// Run sampling multiple times - should get identical results each time
		var firstSeriesCount int
		for i := 0; i < 3; i++ {
			result, err := AmplifyWriteRequest(compressed, 0.5, 0, nil)
			require.NoError(t, err)

			require.Len(t, result.Bodies, 1, "should have single body without splitting")
		decompressed := decompressAndUnmarshalRW1(t, result.Bodies[0])

			if i == 0 {
				firstSeriesCount = len(decompressed.Timeseries)
				assert.Greater(t, firstSeriesCount, 0, "should keep some series")
				assert.Less(t, firstSeriesCount, 4, "should not keep all series")
			} else {
				assert.Equal(t, firstSeriesCount, len(decompressed.Timeseries), "should be deterministic")
			}
		}
	})

	t.Run("fractional amplification is deterministic", func(t *testing.T) {
		// Use series with multiple labels so they can be amplified
		req := makeRW1RequestWithLabels(10)
		compressed := compressRequest(t, &req)

		// With 1.5x factor: 10 originals + ~5 fractional copies = ~15 total
		var firstSeriesCount int
		for i := 0; i < 3; i++ {
			result, err := AmplifyWriteRequest(compressed, 1.5, 0, nil)
			require.NoError(t, err)

			require.Len(t, result.Bodies, 1, "should have single body without splitting")
		decompressed := decompressAndUnmarshalRW1(t, result.Bodies[0])

			if i == 0 {
				firstSeriesCount = len(decompressed.Timeseries)
				assert.Greater(t, firstSeriesCount, 10, "should have more than originals")
				assert.Less(t, firstSeriesCount, 20, "should have less than 2x")
			} else {
				assert.Equal(t, firstSeriesCount, len(decompressed.Timeseries), "should be deterministic")
			}
		}
	})

	t.Run("series without __name__ label suffixes all values", func(t *testing.T) {
		// Create a request with a series that has no __name__ label
		// All label values should be suffixed
		req := makeRW1RequestWithoutName(1)
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 2.0, 0, nil)
		require.NoError(t, err)

		require.Len(t, result.Bodies, 1, "should have single body without splitting")
		decompressed := decompressAndUnmarshalRW1(t, result.Bodies[0])

		// Verify results: 1 original + 1 copy = 2 total
		assert.Equal(t, 2, len(decompressed.Timeseries))

		// Original should have original values
		originalLabels := labelsToMap(decompressed.Timeseries[0].Labels)
		assert.Equal(t, "prometheus", originalLabels["job"])
		assert.Equal(t, "localhost:9090", originalLabels["instance"])

		// Replica 2 should have ALL values suffixed (since no __name__ to exclude)
		replicaLabels := labelsToMap(decompressed.Timeseries[1].Labels)
		assert.Equal(t, "prometheus_amp2", replicaLabels["job"], "job should be suffixed")
		assert.Equal(t, "localhost:9090_amp2", replicaLabels["instance"], "instance should be suffixed")
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

// TestAmplifyWriteRequest_Splitting tests request splitting at replica boundaries.
func TestAmplifyWriteRequest_Splitting(t *testing.T) {
	t.Run("no splitting when maxSeriesPerRequest is 0", func(t *testing.T) {
		// 100 series with 5x amplification = 500 total series
		req := makeRW1RequestWithLabels(100)
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 5.0, 0, nil)
		require.NoError(t, err)

		// Should return single body when splitting is disabled
		require.Len(t, result.Bodies, 1, "should have single body when maxSeriesPerRequest=0")
		assert.True(t, result.WasAmplified)

		decompressed := decompressAndUnmarshalRW1(t, result.Bodies[0])
		assert.Equal(t, 500, len(decompressed.Timeseries))
	})

	t.Run("no splitting when under limit", func(t *testing.T) {
		// 10 series with 3x amplification = 30 total series (under 100 limit)
		req := makeRW1RequestWithLabels(10)
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 3.0, 100, nil)
		require.NoError(t, err)

		// Should return single body when under limit
		require.Len(t, result.Bodies, 1, "should have single body when under limit")

		decompressed := decompressAndUnmarshalRW1(t, result.Bodies[0])
		assert.Equal(t, 30, len(decompressed.Timeseries))
	})

	t.Run("RW1 splitting at replica boundaries", func(t *testing.T) {
		// 100 series with 5x amplification = 500 total series
		// With limit of 250, expect 2 batches:
		// Batch 1: original (100) + replica 2 (100) = 200
		// Batch 2: replica 3 (100) + replica 4 (100) = 200
		// Batch 3: replica 5 (100) = 100
		// Total: 500 series across 3 batches
		req := makeRW1RequestWithLabels(100)
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 5.0, 250, nil)
		require.NoError(t, err)

		assert.True(t, result.WasAmplified)
		assert.Equal(t, 100, result.OriginalSeriesCount)
		assert.Equal(t, 400, result.AmplifiedSeriesCount) // 500 total - 100 original

		// Expect 3 batches
		require.Len(t, result.Bodies, 3, "should split into 3 batches")

		// Verify batch 1: originals + replica 2
		batch1 := decompressAndUnmarshalRW1(t, result.Bodies[0])
		assert.Equal(t, 200, len(batch1.Timeseries), "batch 1 should have originals + replica 2")

		// Verify batch 2: replica 3 + replica 4
		batch2 := decompressAndUnmarshalRW1(t, result.Bodies[1])
		assert.Equal(t, 200, len(batch2.Timeseries), "batch 2 should have replica 3 + replica 4")

		// Verify batch 3: replica 5
		batch3 := decompressAndUnmarshalRW1(t, result.Bodies[2])
		assert.Equal(t, 100, len(batch3.Timeseries), "batch 3 should have replica 5")

		// Verify total series across all batches
		totalSeries := len(batch1.Timeseries) + len(batch2.Timeseries) + len(batch3.Timeseries)
		assert.Equal(t, 500, totalSeries)

		// Verify batch 1 has originals (no suffix) and replica 2 (_amp2)
		hasOriginal := false
		hasAmp2 := false
		for _, ts := range batch1.Timeseries {
			labels := labelsToMap(ts.Labels)
			if labels["job"] == "prometheus" {
				hasOriginal = true
			}
			if labels["job"] == "prometheus_amp2" {
				hasAmp2 = true
			}
		}
		assert.True(t, hasOriginal, "batch 1 should have original series")
		assert.True(t, hasAmp2, "batch 1 should have replica 2 series")

		// Verify batch 2 has replica 3 and 4
		hasAmp3 := false
		hasAmp4 := false
		for _, ts := range batch2.Timeseries {
			labels := labelsToMap(ts.Labels)
			if labels["job"] == "prometheus_amp3" {
				hasAmp3 = true
			}
			if labels["job"] == "prometheus_amp4" {
				hasAmp4 = true
			}
		}
		assert.True(t, hasAmp3, "batch 2 should have replica 3 series")
		assert.True(t, hasAmp4, "batch 2 should have replica 4 series")

		// Verify batch 3 has replica 5
		hasAmp5 := false
		for _, ts := range batch3.Timeseries {
			labels := labelsToMap(ts.Labels)
			if labels["job"] == "prometheus_amp5" {
				hasAmp5 = true
			}
		}
		assert.True(t, hasAmp5, "batch 3 should have replica 5 series")
	})

	t.Run("RW2 splitting with minimal symbol tables", func(t *testing.T) {
		// 100 series with 5x amplification, limit 250
		req := makeRW2RequestWithLabels(100)
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 5.0, 250, nil)
		require.NoError(t, err)

		assert.True(t, result.IsRW2)
		assert.True(t, result.WasAmplified)

		// Expect 3 batches
		require.Len(t, result.Bodies, 3, "should split into 3 batches")

		// Verify batch 1 has minimal symbol table (base + _amp2 suffixes)
		batch1 := decompressAndUnmarshalRW2(t, result.Bodies[0])
		assert.Contains(t, batch1.Symbols, "prometheus_amp2", "batch 1 should have _amp2 suffix")
		assert.NotContains(t, batch1.Symbols, "prometheus_amp3", "batch 1 should NOT have _amp3 suffix")

		// Verify batch 2 has minimal symbol table (base + _amp3 + _amp4 suffixes)
		batch2 := decompressAndUnmarshalRW2(t, result.Bodies[1])
		assert.Contains(t, batch2.Symbols, "prometheus_amp3", "batch 2 should have _amp3 suffix")
		assert.Contains(t, batch2.Symbols, "prometheus_amp4", "batch 2 should have _amp4 suffix")
		assert.NotContains(t, batch2.Symbols, "prometheus_amp2", "batch 2 should NOT have _amp2 suffix")
		assert.NotContains(t, batch2.Symbols, "prometheus_amp5", "batch 2 should NOT have _amp5 suffix")

		// Verify batch 3 has minimal symbol table (base + _amp5 suffixes)
		batch3 := decompressAndUnmarshalRW2(t, result.Bodies[2])
		assert.Contains(t, batch3.Symbols, "prometheus_amp5", "batch 3 should have _amp5 suffix")
		assert.NotContains(t, batch3.Symbols, "prometheus_amp4", "batch 3 should NOT have _amp4 suffix")

		// Verify total series count
		totalSeries := len(batch1.Timeseries) + len(batch2.Timeseries) + len(batch3.Timeseries)
		assert.Equal(t, 500, totalSeries)
	})

	t.Run("splitting with fractional amplification", func(t *testing.T) {
		// 100 series with 3.5x amplification
		// Original (100) + replica 2 (100) + replica 3 (100) + ~50 fractional = ~350 total
		req := makeRW1RequestWithLabels(100)
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 3.5, 200, nil)
		require.NoError(t, err)

		// Should split into multiple batches
		require.Greater(t, len(result.Bodies), 1, "should split into multiple batches")

		// Verify total series count
		totalSeries := 0
		for _, body := range result.Bodies {
			decompressed := decompressAndUnmarshalRW1(t, body)
			totalSeries += len(decompressed.Timeseries)
		}

		// Total should be original + amplified
		expectedTotal := result.OriginalSeriesCount + result.AmplifiedSeriesCount
		assert.Equal(t, expectedTotal, totalSeries)
	})

	t.Run("exact limit boundary", func(t *testing.T) {
		// 50 series with 2x amplification = 100 total, exactly at limit
		req := makeRW1RequestWithLabels(50)
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 2.0, 100, nil)
		require.NoError(t, err)

		// Should NOT split when exactly at limit
		require.Len(t, result.Bodies, 1, "should not split when exactly at limit")

		decompressed := decompressAndUnmarshalRW1(t, result.Bodies[0])
		assert.Equal(t, 100, len(decompressed.Timeseries))
	})

	t.Run("empty request", func(t *testing.T) {
		req := makeRW1RequestWithLabels(0)
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 5.0, 100, nil)
		require.NoError(t, err)

		// Empty request should return single body
		require.Len(t, result.Bodies, 1)
	})

	t.Run("series with only __name__ are not amplified but included", func(t *testing.T) {
		// Create series with only __name__ (cannot be amplified)
		req := makeRW1Request(50) // These have only __name__ label
		compressed := compressRequest(t, &req)

		result, err := AmplifyWriteRequest(compressed, 5.0, 100, nil)
		require.NoError(t, err)

		// Should have single body since no amplification happens
		require.Len(t, result.Bodies, 1)

		decompressed := decompressAndUnmarshalRW1(t, result.Bodies[0])
		// Original series should be present, but no copies
		assert.Equal(t, 50, len(decompressed.Timeseries))
	})
}

// TestComputeReplicaBatches tests the batch computation logic.
func TestComputeReplicaBatches(t *testing.T) {
	t.Run("all replicas fit in one batch", func(t *testing.T) {
		batches := computeReplicaBatches(10, 10, 3, 0, 100)
		require.Len(t, batches, 1)
		assert.Equal(t, []int{1, 2, 3}, batches[0])
	})

	t.Run("each replica in separate batch", func(t *testing.T) {
		batches := computeReplicaBatches(100, 100, 3, 0, 100)
		require.Len(t, batches, 3)
		assert.Equal(t, []int{1}, batches[0])
		assert.Equal(t, []int{2}, batches[1])
		assert.Equal(t, []int{3}, batches[2])
	})

	t.Run("two replicas per batch", func(t *testing.T) {
		batches := computeReplicaBatches(100, 100, 5, 0, 250)
		require.Len(t, batches, 3)
		assert.Equal(t, []int{1, 2}, batches[0])
		assert.Equal(t, []int{3, 4}, batches[1])
		assert.Equal(t, []int{5}, batches[2])
	})

	t.Run("with fractional copies", func(t *testing.T) {
		// 100 originals, 100 amplifiable, 3 replicas, 50 fractional copies
		batches := computeReplicaBatches(100, 100, 3, 50, 200)
		require.Len(t, batches, 2)
		assert.Equal(t, []int{1, 2}, batches[0])       // 100 + 100 = 200
		assert.Equal(t, []int{3}, batches[1])         // 50 fractional copies
	})
}
