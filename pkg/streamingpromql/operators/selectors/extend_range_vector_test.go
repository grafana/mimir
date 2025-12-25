// SPDX-License-Identifier: AGPL-3.0-only

package selectors

import (
	"context"
	"slices"
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
		"remove head": {
			mods: AnchoredExtensionMetadata{
				undoHeadModifications: remove,
			},
			expected: []promql.FPoint{{T: 20, F: 30}, {T: 30, F: 40}, {T: 40, F: 60}},
		},
		"replace head": {
			mods: AnchoredExtensionMetadata{
				undoHeadModifications: replace,
				first:                 promql.FPoint{T: 5, F: 70},
			},
			expected: []promql.FPoint{{T: 5, F: 70}, {T: 20, F: 30}, {T: 30, F: 40}, {T: 40, F: 60}},
		},
		"restore excluded head": {
			mods: AnchoredExtensionMetadata{
				restoreExcludedFirst: true,
				excludedFirst:        promql.FPoint{T: 5, F: 70},
			},
			expected: []promql.FPoint{{T: 5, F: 70}, {T: 10, F: 20}, {T: 20, F: 30}, {T: 30, F: 40}, {T: 40, F: 60}},
		},
		"restore excluded tail": {
			mods: AnchoredExtensionMetadata{
				restoreExcludedLast: true,
				excludedLast:        promql.FPoint{T: 100, F: 7000},
			},
			expected: []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 30}, {T: 30, F: 40}, {T: 40, F: 60}, {T: 100, F: 7000}},
		},
		"combined": {
			mods: AnchoredExtensionMetadata{
				restoreExcludedFirst:  true,
				excludedFirst:         promql.FPoint{T: 5, F: 70},
				restoreExcludedLast:   true,
				excludedLast:          promql.FPoint{T: 100, F: 7000},
				undoHeadModifications: replace,
				first:                 promql.FPoint{T: 8, F: 80},
				undoTailModifications: replace,
				last:                  promql.FPoint{T: 50, F: 70},
			},
			expected: []promql.FPoint{{T: 5, F: 70}, {T: 8, F: 80}, {T: 20, F: 30}, {T: 30, F: 40}, {T: 50, F: 70}, {T: 100, F: 7000}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			buff := types.NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
			require.NoError(t, buff.Use(slices.Clone(original)))
			require.NoError(t, tc.mods.UndoChanges(buff))
			view := buff.ViewAll(nil)
			actual, err := view.CopyPoints()
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}
