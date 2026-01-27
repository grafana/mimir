// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAmplifyWriteRequest_RW2(t *testing.T) {
	// Create a Remote Write 2.0 request using WriteRequest with RW2 fields
	req := mimirpb.WriteRequest{
		SymbolsRW2: []string{
			"",           // index 0: empty string (required by RW 2.0 spec)
			"__name__",   // index 1
			"http_requests_total", // index 2
			"method",     // index 3
			"GET",        // index 4
		},
		TimeseriesRW2: []mimirpb.TimeSeriesRW2{
			{
				// Labels: __name__="http_requests_total", method="GET"
				// Encoded as alternating name/value indices: [1, 2, 3, 4]
				LabelsRefs: []uint32{1, 2, 3, 4},
				Samples: []mimirpb.Sample{
					{Value: 100, TimestampMs: 1000},
				},
			},
		},
	}

	// Marshal and compress
	marshaled, err := proto.Marshal(&req)
	require.NoError(t, err)
	compressed := snappy.Encode(nil, marshaled)

	// Amplify with factor 2.0 (no tracker for test)
	// RW 2.0 requests are now kept in native RW 2.0 format (symbol table + uint32 refs)
	ampResult, err := AmplifyWriteRequest(compressed, 2.0, nil)
	require.NoError(t, err)

	// Decompress and unmarshal the result using native RW2 unmarshal
	decompressed, err := snappy.Decode(nil, ampResult.Body)
	require.NoError(t, err)

	result, err := mimirpb.UnmarshalWriteRequestRW2Native(decompressed)
	require.NoError(t, err)

	// Verify results
	assert.Equal(t, 2, len(result.Timeseries), "should have 2 time series (original + 1 amplified copy)")
	assert.True(t, ampResult.IsRW2, "should be detected as RW 2.0")
	assert.True(t, ampResult.WasAmplified, "should be amplified")
	assert.Equal(t, 1, ampResult.OriginalSeriesCount)
	assert.Equal(t, 1, ampResult.AmplifiedSeriesCount)

	// Verify symbols table includes original symbols plus __amplified__ and "1"
	assert.Contains(t, result.Symbols, "__amplified__", "should have __amplified__ symbol")
	assert.Contains(t, result.Symbols, "1", "should have replica number symbol")

	// Verify first time series is the original (4 label refs: name, value, method, GET)
	assert.Equal(t, 4, len(result.Timeseries[0].LabelsRefs), "first time series should have 4 label refs")

	// Verify second time series has amplified label (6 label refs: original 4 + __amplified__ + replica)
	assert.Equal(t, 6, len(result.Timeseries[1].LabelsRefs), "second time series should have 6 label refs (original 4 + amplified 2)")

	// Verify the amplified label is in the symbol table and referenced
	amplifiedSymbolIdx := -1
	replicaSymbolIdx := -1
	for i, sym := range result.Symbols {
		if sym == "__amplified__" {
			amplifiedSymbolIdx = i
		}
		if sym == "1" {
			replicaSymbolIdx = i
		}
	}
	assert.NotEqual(t, -1, amplifiedSymbolIdx, "__amplified__ should be in symbol table")
	assert.NotEqual(t, -1, replicaSymbolIdx, "replica number should be in symbol table")

	// Verify the last two refs in the amplified series point to __amplified__ and replica number
	assert.Equal(t, uint32(amplifiedSymbolIdx), result.Timeseries[1].LabelsRefs[4], "should reference __amplified__ symbol")
	assert.Equal(t, uint32(replicaSymbolIdx), result.Timeseries[1].LabelsRefs[5], "should reference replica number symbol")

	// Verify samples are preserved
	assert.Equal(t, 1, len(result.Timeseries[1].Samples))
	assert.Equal(t, float64(100), result.Timeseries[1].Samples[0].Value)
	assert.Equal(t, int64(1000), result.Timeseries[1].Samples[0].TimestampMs)
}

func TestAmplifyWriteRequest_RW1(t *testing.T) {
	// Create a Remote Write 1.0 request
	req := mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__name__", Value: "http_requests_total"},
						{Name: "method", Value: "GET"},
					},
					Samples: []mimirpb.Sample{
						{Value: 100, TimestampMs: 1000},
					},
				},
			},
		},
	}

	// Marshal and compress
	marshaled, err := proto.Marshal(&req)
	require.NoError(t, err)
	compressed := snappy.Encode(nil, marshaled)

	// Amplify with factor 2.0 (no tracker for test)
	ampResult, err := AmplifyWriteRequest(compressed, 2.0, nil)
	require.NoError(t, err)

	// Decompress and unmarshal the result
	decompressed, err := snappy.Decode(nil, ampResult.Body)
	require.NoError(t, err)

	var result mimirpb.WriteRequest
	err = proto.Unmarshal(decompressed, &result)
	require.NoError(t, err)

	// Verify results
	assert.Equal(t, 2, len(result.Timeseries), "should have 2 time series (original + 1 amplified copy)")

	// Verify first time series is the original
	assert.Equal(t, 2, len(result.Timeseries[0].Labels), "first time series should have 2 labels")
	assert.Equal(t, "__name__", result.Timeseries[0].Labels[0].Name)
	assert.Equal(t, "http_requests_total", result.Timeseries[0].Labels[0].Value)

	// Verify second time series has the amplified label
	assert.Equal(t, 3, len(result.Timeseries[1].Labels), "second time series should have 3 labels (original + amplified)")

	// Find the amplified label
	var hasAmplifiedLabel bool
	for _, label := range result.Timeseries[1].Labels {
		if label.Name == "__amplified__" {
			hasAmplifiedLabel = true
			assert.Equal(t, "1", label.Value)
		}
	}
	assert.True(t, hasAmplifiedLabel, "second time series should have __amplified__ label")

	// Verify samples are preserved
	assert.Equal(t, 1, len(result.Timeseries[1].Samples))
	assert.Equal(t, float64(100), result.Timeseries[1].Samples[0].Value)
	assert.Equal(t, int64(1000), result.Timeseries[1].Samples[0].TimestampMs)
}
