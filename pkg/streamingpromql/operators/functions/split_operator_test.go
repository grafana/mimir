// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestResultGetter_OutOfOrderAccess(t *testing.T) {
	const totalSeries = 3
	callCount := 0

	nextSeriesFunc := func(ctx context.Context) ([]string, error) {
		callCount++
		if callCount > totalSeries {
			// Simulate the "no more metadata to pop" panic
			return nil, types.EOS
		}
		return []string{string(rune('A' + callCount - 1))}, nil
	}

	getter := NewResultGetter(nextSeriesFunc)

	result, err := getter.GetResultsAtIdx(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, []string{"C"}, result)

	require.Equal(t, 3, callCount)

	result, err = getter.GetResultsAtIdx(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, []string{"A"}, result)

	result, err = getter.GetResultsAtIdx(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []string{"B"}, result)
}

func TestResultGetter_SequentialAccess(t *testing.T) {
	callCount := 0

	nextSeriesFunc := func(ctx context.Context) ([]string, error) {
		callCount++
		if callCount > 3 {
			return nil, types.EOS
		}
		return []string{string(rune('A' + callCount - 1))}, nil
	}

	getter := NewResultGetter(nextSeriesFunc)

	result0, err := getter.GetResultsAtIdx(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, []string{"A"}, result0)
	require.Equal(t, 1, callCount)

	result1, err := getter.GetResultsAtIdx(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []string{"B"}, result1)
	require.Equal(t, 2, callCount)

	result2, err := getter.GetResultsAtIdx(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, []string{"C"}, result2)
	require.Equal(t, 3, callCount)
}

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
