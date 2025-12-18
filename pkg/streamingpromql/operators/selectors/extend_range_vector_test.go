package selectors

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestFPointRingBufferUndoModifications(t *testing.T) {
	original := []promql.FPoint{
		{T: 10, F: 20},
		{T: 20, F: 30},
		{T: 30, F: 40},
		{T: 40, F: 60},
	}

	testCases := map[string]struct {
		mods     AnchoredExtensionMetadata
		expected []promql.FPoint
	}{
		"no modifications": {
			mods:     AnchoredExtensionMetadata{},
			expected: original,
		},
		"no modifications - no undo": {
			mods:     AnchoredExtensionMetadata{undoTailModifications: none},
			expected: original,
		},
		"remove tail": {
			mods: AnchoredExtensionMetadata{
				undoTailModifications: remove,
			},
			expected: []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 30}, {T: 30, F: 40}},
		},
		"replace tail": {
			mods: AnchoredExtensionMetadata{
				undoTailModifications: replace,
				last:                  promql.FPoint{T: 50, F: 70},
			},
			expected: []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 30}, {T: 30, F: 40}, {T: 50, F: 70}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			buff := types.NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
			require.NoError(t, buff.Use(original))
			require.NoError(t, tc.mods.UndoSyntheticPoints(buff))
			view := buff.ViewAll(nil)
			require.Equal(t, len(tc.expected), view.Count())
			for i, point := range tc.expected {
				require.Equal(t, point, view.PointAt(i))
			}
		})
	}
}
