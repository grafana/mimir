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
	require.Equal(t, float64(3), v[0].H.Count)

	// Check that the decoding is zero copy
	ph := &(vd.Histograms[0].Histogram)
	ph.Count = 42
	require.Equal(t, float64(42), v[0].H.Count)
}
