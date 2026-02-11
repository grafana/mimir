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

// TestAmplifyWriteRequest_RW2 asserts that RW2 requests are amplified in native format
// and that the __amplified__ label is correctly added via symbol table references.
func TestAmplifyWriteRequest_RW2(t *testing.T) {
	req := makeRW2Request(1)
	compressed := compressRequest(t, &req)

	result, err := AmplifyWriteRequest(compressed, 2.0, nil)
	require.NoError(t, err)

	decompressed := decompressAndUnmarshalRW2(t, result.Body)

	// Verify results
	assert.Equal(t, 2, len(decompressed.Timeseries), "should have 2 time series (original + 1 amplified copy)")
	assert.True(t, result.IsRW2, "should be detected as RW 2.0")
	assert.True(t, result.WasAmplified, "should be amplified")
	assert.Equal(t, 1, result.OriginalSeriesCount)
	assert.Equal(t, 1, result.AmplifiedSeriesCount)

	// Verify symbols table includes __amplified__ and replica number
	assert.Contains(t, decompressed.Symbols, "__amplified__")
	assert.Contains(t, decompressed.Symbols, "1")

	// First series should be original (2 label refs: __name__, metric_0)
	assert.Equal(t, 2, len(decompressed.Timeseries[0].LabelsRefs))

	// Second series should have amplified label added (4 label refs: original 2 + __amplified__ + "1")
	assert.Equal(t, 4, len(decompressed.Timeseries[1].LabelsRefs))
}

// TestAmplifyWriteRequest_RW1 asserts that RW1 requests are amplified correctly
// and that the __amplified__ label is added to duplicated series.
func TestAmplifyWriteRequest_RW1(t *testing.T) {
	req := makeRW1Request(1)
	compressed := compressRequest(t, &req)

	result, err := AmplifyWriteRequest(compressed, 2.0, nil)
	require.NoError(t, err)

	decompressed := decompressAndUnmarshalRW1(t, result.Body)

	// Verify results
	assert.Equal(t, 2, len(decompressed.Timeseries), "should have 2 time series (original + 1 amplified copy)")
	assert.False(t, result.IsRW2)
	assert.True(t, result.WasAmplified)

	// First series should be original (1 label: __name__=metric_0)
	assert.Equal(t, 1, len(decompressed.Timeseries[0].Labels))
	assert.Equal(t, "__name__", decompressed.Timeseries[0].Labels[0].Name)
	assert.Equal(t, "metric_0", decompressed.Timeseries[0].Labels[0].Value)

	// Second series should have __amplified__ label added (2 labels)
	assert.Equal(t, 2, len(decompressed.Timeseries[1].Labels))

	// Find the amplified label
	var hasAmplifiedLabel bool
	for _, label := range decompressed.Timeseries[1].Labels {
		if label.Name == "__amplified__" {
			hasAmplifiedLabel = true
			assert.Equal(t, "1", label.Value)
		}
	}
	assert.True(t, hasAmplifiedLabel, "second time series should have __amplified__ label")
}

// TestAmplifyWriteRequest_Sampling_RW1_Deterministic asserts that sampling (factor < 1.0)
// produces deterministic results for RW1 requests - the same series are always kept.
func TestAmplifyWriteRequest_Sampling_RW1_Deterministic(t *testing.T) {
	req := makeRW1Request(4) // 4 series: metric_0 to metric_3
	compressed := compressRequest(t, &req)

	// Run sampling multiple times - should get identical results each time
	var firstSeriesCount int
	for i := 0; i < 3; i++ {
		result, err := AmplifyWriteRequest(compressed, 0.5, nil)
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
}

// TestAmplifyWriteRequest_Sampling_RW2_Deterministic asserts that sampling (factor < 1.0)
// produces deterministic results for RW2 requests - the same series are always kept.
func TestAmplifyWriteRequest_Sampling_RW2_Deterministic(t *testing.T) {
	req := makeRW2Request(4) // 4 series
	compressed := compressRequest(t, &req)

	// Run sampling multiple times - should get identical results each time
	var firstSeriesCount int
	for i := 0; i < 3; i++ {
		result, err := AmplifyWriteRequest(compressed, 0.5, nil)
		require.NoError(t, err)
		assert.True(t, result.IsRW2)

		decompressed := decompressAndUnmarshalRW2(t, result.Body)

		if i == 0 {
			firstSeriesCount = len(decompressed.Timeseries)
			assert.Greater(t, firstSeriesCount, 0, "should keep some series")
			assert.Less(t, firstSeriesCount, 4, "should not keep all series")
		} else {
			assert.Equal(t, firstSeriesCount, len(decompressed.Timeseries), "should be deterministic")
		}
	}
}

// TestAmplifyWriteRequest_FractionalAmplification_Deterministic asserts that fractional
// amplification (e.g., 1.5x) produces deterministic results for RW1 - the same series always get extra copies.
func TestAmplifyWriteRequest_FractionalAmplification_Deterministic(t *testing.T) {
	req := makeRW1Request(10)
	compressed := compressRequest(t, &req)

	// With 1.5x factor: 10 originals + ~5 fractional copies = ~15 total
	var firstSeriesCount int
	for i := 0; i < 3; i++ {
		result, err := AmplifyWriteRequest(compressed, 1.5, nil)
		require.NoError(t, err)

		decompressed := decompressAndUnmarshalRW1(t, result.Body)

		if i == 0 {
			firstSeriesCount = len(decompressed.Timeseries)
			assert.Greater(t, firstSeriesCount, 10, "should have more than originals")
			assert.Less(t, firstSeriesCount, 20, "should have less than 2x")
		} else {
			assert.Equal(t, firstSeriesCount, len(decompressed.Timeseries), "should be deterministic")
		}
	}
}

// TestAmplifyWriteRequest_FractionalAmplification_RW2_Deterministic asserts that fractional
// amplification (e.g., 1.5x) produces deterministic results for RW2 - the same series always get extra copies.
func TestAmplifyWriteRequest_FractionalAmplification_RW2_Deterministic(t *testing.T) {
	req := makeRW2Request(10)
	compressed := compressRequest(t, &req)

	// With 1.5x factor: 10 originals + ~5 fractional copies = ~15 total
	var firstSeriesCount int
	for i := 0; i < 3; i++ {
		result, err := AmplifyWriteRequest(compressed, 1.5, nil)
		require.NoError(t, err)

		decompressed := decompressAndUnmarshalRW2(t, result.Body)

		if i == 0 {
			firstSeriesCount = len(decompressed.Timeseries)
			assert.Greater(t, firstSeriesCount, 10, "should have more than originals")
			assert.Less(t, firstSeriesCount, 20, "should have less than 2x")
		} else {
			assert.Equal(t, firstSeriesCount, len(decompressed.Timeseries), "should be deterministic")
		}
	}
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
