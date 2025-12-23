// SPDX-License-Identifier: AGPL-3.0-only

package selectors

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestExtendRangeVectorSliceSearch(t *testing.T) {
	tests := map[string]struct {
		pts      []promql.FPoint
		minIdx   int
		t        int64
		expected int
	}{
		"nil slice": {
			pts:      nil,
			minIdx:   0,
			t:        0,
			expected: 1,
		},
		"nil slice - higher minIdx": {
			pts:      nil,
			minIdx:   10,
			t:        0,
			expected: 1,
		},
		"empty slice": {
			pts:      []promql.FPoint{},
			minIdx:   0,
			t:        0,
			expected: 1,
		},
		"single point - less then t": {
			pts:      []promql.FPoint{{T: 10}},
			minIdx:   0,
			t:        20,
			expected: 1,
		},
		"single point - index out of bounds": {
			pts:      []promql.FPoint{{T: 10}},
			minIdx:   1,
			t:        20,
			expected: 1,
		},
		"single point - equals t": {
			pts:      []promql.FPoint{{T: 10}},
			minIdx:   0,
			t:        10,
			expected: 1,
		},
		"single point - greater than t": {
			pts:      []promql.FPoint{{T: 10}},
			minIdx:   0,
			t:        5,
			expected: 0,
		},
		"multiple point - all less then t": {
			pts:      []promql.FPoint{{T: 10}, {T: 20}, {T: 30}},
			minIdx:   0,
			t:        31,
			expected: 3,
		},
		"multiple point - all less then t with minIdx": {
			pts:      []promql.FPoint{{T: 10}, {T: 20}, {T: 30}},
			minIdx:   1,
			t:        31,
			expected: 3,
		},
		"multiple point - equal t": {
			pts:      []promql.FPoint{{T: 10}, {T: 20}, {T: 30}},
			minIdx:   0,
			t:        30,
			expected: 3,
		},
		"multiple point - all greater then t": {
			pts:      []promql.FPoint{{T: 10}, {T: 20}, {T: 30}},
			minIdx:   0,
			t:        0,
			expected: 0,
		},
		"multiple point - all greater then t with minIdx": {
			pts:      []promql.FPoint{{T: 10}, {T: 20}, {T: 30}},
			minIdx:   1,
			t:        0,
			expected: 1,
		},
		"multiple point - first on on boundary": {
			pts:      []promql.FPoint{{T: 10}, {T: 20}, {T: 30}},
			minIdx:   0,
			t:        10,
			expected: 1,
		},
		"multiple point - middle on boundary": {
			pts:      []promql.FPoint{{T: 10}, {T: 20}, {T: 30}, {T: 40}, {T: 50}, {T: 60}},
			minIdx:   0,
			t:        30,
			expected: 3,
		},
		"multiple point - middle on boundary with minIdx": {
			pts:      []promql.FPoint{{T: 10}, {T: 20}, {T: 30}, {T: 40}, {T: 50}, {T: 60}},
			minIdx:   1,
			t:        30,
			expected: 3,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, search(tc.pts, tc.t, tc.minIdx), name)
		})
	}

}

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
			actual, err := view.CopyPoints()
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}
