// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveSliceIndexes(t *testing.T) {
	type testCase struct {
		name                    string
		input                   []int
		removeIndexes           []int
		expectedOutuput         []int
		expectedRemovedElements int
		expectedRemovedRanges   int
	}

	testCases := []testCase{
		{
			name:                    "no change",
			input:                   []int{0, 1, 2, 3, 4},
			removeIndexes:           []int{},
			expectedOutuput:         []int{0, 1, 2, 3, 4},
			expectedRemovedElements: 0,
			expectedRemovedRanges:   0,
		}, {
			name:                    "remove first",
			input:                   []int{0, 1, 2, 3, 4},
			removeIndexes:           []int{0},
			expectedOutuput:         []int{1, 2, 3, 4},
			expectedRemovedElements: 1,
			expectedRemovedRanges:   1,
		}, {
			name:                    "remove middle",
			input:                   []int{0, 1, 2, 3, 4},
			removeIndexes:           []int{2},
			expectedOutuput:         []int{0, 1, 3, 4},
			expectedRemovedElements: 1,
			expectedRemovedRanges:   1,
		}, {
			name:                    "remove last",
			input:                   []int{0, 1, 2, 3, 4},
			removeIndexes:           []int{4},
			expectedOutuput:         []int{0, 1, 2, 3},
			expectedRemovedElements: 1,
			expectedRemovedRanges:   1,
		}, {
			name:                    "remove all",
			input:                   []int{0, 1, 2, 3, 4},
			removeIndexes:           []int{0, 1, 2, 3, 4},
			expectedOutuput:         []int{},
			expectedRemovedElements: 5,
			expectedRemovedRanges:   1,
		}, {
			name:                    "remove two ranges",
			input:                   []int{0, 1, 2, 3, 4},
			removeIndexes:           []int{0, 1, 3, 4},
			expectedOutuput:         []int{2},
			expectedRemovedElements: 4,
			expectedRemovedRanges:   2,
		}, {
			name:                    "index overrun",
			input:                   []int{0, 1, 2, 3, 4},
			removeIndexes:           []int{4, 5},
			expectedOutuput:         []int{0, 1, 2, 3},
			expectedRemovedElements: 1,
			expectedRemovedRanges:   1,
		}, {
			name:                    "duplicate indexes",
			input:                   []int{0, 1, 2, 3, 4},
			removeIndexes:           []int{4, 4, 4, 4},
			expectedOutuput:         []int{0, 1, 2, 3},
			expectedRemovedElements: 1,
			expectedRemovedRanges:   1,
		}, {
			name:                    "out of order indexes",
			input:                   []int{0, 1, 2, 3, 4},
			removeIndexes:           []int{4, 3, 1, 0},
			expectedOutuput:         []int{2},
			expectedRemovedElements: 4,
			expectedRemovedRanges:   2,
		}, {
			name:                    "negative indexes",
			input:                   []int{0, 1, 2, 3, 4},
			removeIndexes:           []int{-1, 0, 1},
			expectedOutuput:         []int{2, 3, 4},
			expectedRemovedElements: 2,
			expectedRemovedRanges:   1,
		}, {
			name:                    "all cases combined",
			input:                   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			removeIndexes:           []int{9, 8, 9, 10, 11, -1, -1, 0, 3, 5, 6},
			expectedOutuput:         []int{1, 2, 4, 7},
			expectedRemovedElements: 6,
			expectedRemovedRanges:   4,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output, removedElements, removedRanges := RemoveSliceIndexes(tc.input, tc.removeIndexes)
			assert.Equal(t, tc.expectedOutuput, output)
			assert.Equal(t, tc.expectedRemovedElements, removedElements)
			assert.Equal(t, tc.expectedRemovedRanges, removedRanges)
		})
	}
}
