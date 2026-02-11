// SPDX-License-Identifier: AGPL-3.0-only

// Most split function behavior should be tested by adding to the promql test cases in
// pkg/streamingpromql/testdata/ours-only/range_vector_splitting_2h.test.
// Tests here should be for specific edge cases that benefit from direct unit testing.
package functions

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestRateCombineFloat_WithEmptySplits(t *testing.T) {
	splits := []RateIntermediate{
		// Split 0: has samples
		{
			FirstSample: &mimirpb.Sample{
				TimestampMs: 1000,
				Value:       100.0,
			},
			LastSample: &mimirpb.Sample{
				TimestampMs: 2000,
				Value:       110.0,
			},
			Delta:       10.0,
			SampleCount: 5,
			IsHistogram: false,
		},
		// Split 1: empty (no samples)
		{
			FirstSample: nil,
			LastSample:  nil,
			Delta:       0,
			SampleCount: 0,
			IsHistogram: false,
		},
		// Split 2: has samples
		{
			FirstSample: &mimirpb.Sample{
				TimestampMs: 6000,
				Value:       120.0,
			},
			LastSample: &mimirpb.Sample{
				TimestampMs: 7000,
				Value:       130.0,
			},
			Delta:       10.0,
			SampleCount: 5,
			IsHistogram: false,
		},
	}

	result, hasResult, err := rateCombineFloat(splits, 0, 9000, true)
	require.NoError(t, err)
	require.True(t, hasResult)
	require.NotZero(t, result)
}
