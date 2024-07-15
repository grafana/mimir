// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// Regression test for a bug where we were copying the loop variable address.
func TestProtobufDecoderDecodeVectorHistogramMemoryHandling(t *testing.T) {
	vd := &mimirpb.VectorData{
		Histograms: []mimirpb.VectorHistogram{
			{
				Metric: []string{"__name__", "test"},
				Histogram: mimirpb.FloatHistogram{
					CounterResetHint: 3,
					Schema:           3,
					ZeroThreshold:    0.1,
					ZeroCount:        0,
					Count:            3,
					Sum:              3,
				},
				TimestampMs: 123,
			},
			{
				Metric: []string{"__name__", "test"},
				Histogram: mimirpb.FloatHistogram{
					CounterResetHint: 3,
					Schema:           3,
					ZeroThreshold:    0.1,
					ZeroCount:        0,
					Count:            1337,
					Sum:              3,
				},
				TimestampMs: 123,
			},
		},
	}
	decoder := protobufDecoder{}
	v, err := decoder.decodeVector(vd)
	require.NoError(t, err)
	require.Equal(t, 2, len(v))
	// If the encoding takes the address of the loop variable, the value will be 1337.
	require.Equal(t, float64(3), v[0].H.Count)

	// Also check the second histogram.
	require.Equal(t, float64(1337), v[1].H.Count)

	// Ensure that the decoding is zero copy for performance reasons.
	vd.Histograms[0].Histogram.Count = 42
	require.Equal(t, float64(42), v[0].H.Count)
}
