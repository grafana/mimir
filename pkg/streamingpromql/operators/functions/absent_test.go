// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestAbsent_NextSeries_ExhaustedCondition(t *testing.T) {
	ctx := context.Background()
	memTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	a := &Absent{
		TimeRange: types.QueryTimeRange{
			StepCount: 1,
		},
		MemoryConsumptionTracker: memTracker,
		presence:                 []bool{false}, // Single false value to generate one point
	}

	// First call should return data.
	result1, err := a.NextSeries(ctx)
	require.NoError(t, err)
	require.NotNil(t, result1.Floats)
	require.Len(t, result1.Floats, 1)
	types.FPointSlicePool.Put(&result1.Floats, memTracker)

	// Second call should return EOS.
	result2, err := a.NextSeries(ctx)
	require.Equal(t, types.EOS, err)
	require.Empty(t, result2.Floats)
	require.True(t, a.exhausted)
}
