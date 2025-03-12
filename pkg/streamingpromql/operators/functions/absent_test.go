// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestAbsent_NextSeries_ExhaustedCondition(t *testing.T) {
	memTracker := limiting.NewMemoryConsumptionTracker(0, nil)

	a := &Absent{
		TimeRange: types.QueryTimeRange{
			StepCount: 1,
		},
		MemoryConsumptionTracker: memTracker,
		presence:                 []bool{false}, // Single false value to generate one point
	}

	t.Run("first call should return data", func(t *testing.T) {
		result1, err := a.NextSeries(context.Background())
		require.NoError(t, err)
		require.NotNil(t, result1.Floats)
		require.Len(t, result1.Floats, 1)

		if result1.Floats != nil {
			types.FPointSlicePool.Put(result1.Floats, memTracker)
		}
	})

	t.Run("second call should return EOS", func(t *testing.T) {
		result2, err := a.NextSeries(context.Background())
		require.Equal(t, types.EOS, err)
		require.Empty(t, result2.Floats)
		require.True(t, a.exhausted)
	})
}
