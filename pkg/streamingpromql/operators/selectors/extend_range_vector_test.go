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

func TestRevertibleExtendedPointsUtilityUndoModifications(t *testing.T) {
	original := []promql.FPoint{
		{T: 10, F: 20},
		{T: 20, F: 30},
		{T: 30, F: 40},
		{T: 40, F: 60},
	}

	testCases := map[string]struct {
		mods     RevertibleExtendedPointsState
		expected []promql.FPoint
	}{
		"no modifications": {
			mods:     RevertibleExtendedPointsState{},
			expected: original,
		},
		"no modifications - no undo": {
			mods:     RevertibleExtendedPointsState{undoTailModifications: none},
			expected: original,
		},
		"removed tail": {
			mods: RevertibleExtendedPointsState{
				undoTailModifications: removed,
			},
			expected: []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 30}, {T: 30, F: 40}},
		},
		"replaced tail": {
			mods: RevertibleExtendedPointsState{
				undoTailModifications: replaced,
				last:                  promql.FPoint{T: 50, F: 70},
			},
			expected: []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 30}, {T: 30, F: 40}, {T: 50, F: 70}},
		},
		"removed head": {
			mods: RevertibleExtendedPointsState{
				undoHeadModifications: removed,
			},
			expected: []promql.FPoint{{T: 20, F: 30}, {T: 30, F: 40}, {T: 40, F: 60}},
		},
		"replaced head": {
			mods: RevertibleExtendedPointsState{
				undoHeadModifications: replaced,
				first:                 promql.FPoint{T: 5, F: 70},
			},
			expected: []promql.FPoint{{T: 5, F: 70}, {T: 20, F: 30}, {T: 30, F: 40}, {T: 40, F: 60}},
		},
		"restore excluded head": {
			mods: RevertibleExtendedPointsState{
				restoreExcludedFirst: true,
				excludedFirst:        promql.FPoint{T: 5, F: 70},
			},
			expected: []promql.FPoint{{T: 5, F: 70}, {T: 10, F: 20}, {T: 20, F: 30}, {T: 30, F: 40}, {T: 40, F: 60}},
		},
		"restore excluded tail": {
			mods: RevertibleExtendedPointsState{
				restoreExcludedLast: true,
				excludedLast:        promql.FPoint{T: 100, F: 7000},
			},
			expected: []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 30}, {T: 30, F: 40}, {T: 40, F: 60}, {T: 100, F: 7000}},
		},
		"combined": {
			mods: RevertibleExtendedPointsState{
				restoreExcludedFirst:  true,
				excludedFirst:         promql.FPoint{T: 5, F: 70},
				restoreExcludedLast:   true,
				excludedLast:          promql.FPoint{T: 100, F: 7000},
				undoHeadModifications: replaced,
				first:                 promql.FPoint{T: 8, F: 80},
				undoTailModifications: replaced,
				last:                  promql.FPoint{T: 50, F: 70},
			},
			expected: []promql.FPoint{{T: 5, F: 70}, {T: 8, F: 80}, {T: 20, F: 30}, {T: 30, F: 40}, {T: 50, F: 70}, {T: 100, F: 7000}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			buff := types.NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
			require.NoError(t, buff.Use(slices.Clone(original)))
			tc.mods.buff = buff
			require.NoError(t, tc.mods.UndoChanges())
			view := buff.ViewAll(nil)
			actual, err := view.CopyPoints()
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)

			// idempotent test
			require.NoError(t, tc.mods.UndoChanges())
			view = buff.ViewAll(nil)
			actual, err = view.CopyPoints()
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestRevertibleExtendedPointsUtilityReset(t *testing.T) {
	original := []promql.FPoint{
		{T: 10, F: 20},
		{T: 20, F: 30},
		{T: 30, F: 40},
		{T: 40, F: 60},
	}

	testCases := map[string]struct {
		mods *RevertibleExtendedPointsState
	}{
		"no modifications": {
			mods: &RevertibleExtendedPointsState{},
		},
		"removed tail": {
			mods: &RevertibleExtendedPointsState{
				undoTailModifications: removed,
			},
		},
		"replaced tail": {
			mods: &RevertibleExtendedPointsState{
				undoTailModifications: replaced,
				last:                  promql.FPoint{T: 50, F: 70},
			},
		},
		"removed head": {
			mods: &RevertibleExtendedPointsState{
				undoHeadModifications: removed,
			},
		},
		"replaced head": {
			mods: &RevertibleExtendedPointsState{
				undoHeadModifications: replaced,
				first:                 promql.FPoint{T: 5, F: 70},
			},
		},
		"restore excluded head": {
			mods: &RevertibleExtendedPointsState{
				restoreExcludedFirst: true,
				excludedFirst:        promql.FPoint{T: 5, F: 70},
			},
		},
		"restore excluded tail": {
			mods: &RevertibleExtendedPointsState{
				restoreExcludedLast: true,
				excludedLast:        promql.FPoint{T: 100, F: 7000},
			},
		},
		"combined": {
			mods: &RevertibleExtendedPointsState{
				restoreExcludedFirst:  true,
				excludedFirst:         promql.FPoint{T: 5, F: 70},
				restoreExcludedLast:   true,
				excludedLast:          promql.FPoint{T: 100, F: 7000},
				undoHeadModifications: replaced,
				first:                 promql.FPoint{T: 8, F: 80},
				undoTailModifications: replaced,
				last:                  promql.FPoint{T: 50, F: 70},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			buff := types.NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
			require.NoError(t, buff.Use(slices.Clone(original)))
			tc.mods.buff = buff

			// clear the changelog
			tc.mods.Reset()

			// undo changes should now be a no-op
			require.NoError(t, tc.mods.UndoChanges())

			// the buffer should have no changes
			view := buff.ViewAll(nil)
			actual, err := view.CopyPoints()
			require.NoError(t, err)
			require.Equal(t, original, actual)
		})
	}
}
