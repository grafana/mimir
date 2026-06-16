// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestMaxOf(t *testing.T) {
	// Note that we're only testing "max_of" because it is identical to "min_of" but
	// with a different comparison function. Both are tested for correctness via PromQL
	// test data files.
	tests := []struct {
		name           string
		a, b, expected []promql.FPoint
	}{
		{
			name:     "positive literals",
			a:        []promql.FPoint{{T: 0, F: 3}, {T: 10, F: 3}},
			b:        []promql.FPoint{{T: 0, F: 1}, {T: 10, F: 9}},
			expected: []promql.FPoint{{T: 0, F: 3}, {T: 10, F: 9}},
		},
		{
			name:     "negative literals",
			a:        []promql.FPoint{{T: 0, F: -10}, {T: 10, F: -2}},
			b:        []promql.FPoint{{T: 0, F: -5}, {T: 10, F: -5}},
			expected: []promql.FPoint{{T: 0, F: -5}, {T: 10, F: -2}},
		},
		{
			name:     "equal literals",
			a:        []promql.FPoint{{T: 0, F: 3}, {T: 10, F: 3}},
			b:        []promql.FPoint{{T: 0, F: 3}, {T: 10, F: 3}},
			expected: []promql.FPoint{{T: 0, F: 3}, {T: 10, F: 3}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			memoryTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			timeRange := types.NewRangeQueryTimeRange(time.UnixMilli(0), time.UnixMilli(10), 10*time.Millisecond)

			opA := newTestScalarOperator(t, tc.a, memoryTracker)
			opB := newTestScalarOperator(t, tc.b, memoryTracker)
			op := NewMaxOf(opA, opB, timeRange, memoryTracker, posrange.PositionRange{})

			require.NoError(t, op.Prepare(ctx, nil))
			require.NoError(t, op.AfterPrepare(ctx))

			vals, err := op.GetValues(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.expected, vals.Samples)
			types.FPointSlicePool.Put(&vals.Samples, memoryTracker)

			require.NoError(t, op.FinishedReading(ctx))

			stats, _, err := op.Finalize(ctx)
			require.NoError(t, err)
			if stats != nil {
				stats.Close()
			}
			op.Close()

			require.Equal(t, uint64(0), memoryTracker.CurrentEstimatedMemoryConsumptionBytes())
		})
	}
}

func newTestScalarOperator(t *testing.T, points []promql.FPoint, mem *limiter.MemoryConsumptionTracker) types.ScalarOperator {
	s, err := types.FPointSlicePool.Get(len(points), mem)
	require.NoError(t, err)

	s = s[0:len(points)]
	copy(s, points)

	return &testScalarOperator{
		value: types.ScalarData{Samples: s},
	}
}
