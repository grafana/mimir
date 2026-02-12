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

// TestAmplifyWriteRequest_RW2_Deterministic asserts that sampling (factor < 1.0)
// produces deterministic results for RW2 requests - the same series are always kept.
func TestAmplifyWriteRequest_RW2_Deterministic(t *testing.T) {
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

// TestAmplifyWriteRequest_RW1_Deterministic asserts that sampling (factor < 1.0)
// produces deterministic results for RW1 requests - the same series are always kept.
func TestAmplifyWriteRequest_RW1_Deterministic(t *testing.T) {
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

// TestAmplifyWriteRequest_RejectAmplification asserts that the function rejects factor > 1.0
func TestAmplifyWriteRequest_RejectAmplification(t *testing.T) {
	req := makeRW1Request(1)
	compressed := compressRequest(t, &req)

	_, err := AmplifyWriteRequest(compressed, 2.0, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only handles sampling")
}

// TestAddAmplificationLabel_RW1 tests adding __amplified__ label to RW1 requests
func TestAddAmplificationLabel_RW1(t *testing.T) {
	req := makeRW1Request(1)
	compressed := compressRequest(t, &req)

	labeled, err := AddAmplificationLabel(compressed, 5)
	require.NoError(t, err)

	decompressed := decompressAndUnmarshalRW1(t, labeled)

	// Should have 1 series with __amplified__ label
	require.Len(t, decompressed.Timeseries, 1)

	// Find the amplified label
	var hasAmplifiedLabel bool
	for _, label := range decompressed.Timeseries[0].Labels {
		if label.Name == "__amplified__" {
			hasAmplifiedLabel = true
			assert.Equal(t, "5", label.Value)
		}
	}
	assert.True(t, hasAmplifiedLabel, "should have __amplified__ label")
}

// TestAddAmplificationLabel_RW2 tests adding __amplified__ label to RW2 requests
func TestAddAmplificationLabel_RW2(t *testing.T) {
	req := makeRW2Request(1)
	compressed := compressRequest(t, &req)

	labeled, err := AddAmplificationLabel(compressed, 5)
	require.NoError(t, err)

	decompressed := decompressAndUnmarshalRW2(t, labeled)

	// Should have 1 series with __amplified__ label added
	require.Len(t, decompressed.Timeseries, 1)

	// Verify symbols table includes __amplified__ and "5"
	assert.Contains(t, decompressed.Symbols, "__amplified__")
	assert.Contains(t, decompressed.Symbols, "5")

	// Original series had 2 label refs (__name__, metric_0)
	// Should now have 4 label refs (original 2 + __amplified__ + "5")
	assert.Equal(t, 4, len(decompressed.Timeseries[0].LabelsRefs))
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
