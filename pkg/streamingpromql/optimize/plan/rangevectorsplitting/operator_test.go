// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestResultGetter_OutOfOrderAccess(t *testing.T) {
	const totalSeries = 3
	callCount := 0

	nextSeriesFunc := func(ctx context.Context) ([]string, error) {
		callCount++
		if callCount > totalSeries {
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
