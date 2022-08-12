// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type removeSliceIndexesTestCase struct {
	name                    string
	input                   []int
	removeIndexes           []int
	expectedOutput          []int
	expectedRemovedElements int
	expectedRemovedRanges   int
}

var removeSliceIndexesTestCases = []removeSliceIndexesTestCase{
	{
		name:                    "no change",
		input:                   []int{0, 1, 2, 3, 4},
		removeIndexes:           []int{},
		expectedOutput:          []int{0, 1, 2, 3, 4},
		expectedRemovedElements: 0,
		expectedRemovedRanges:   0,
	}, {
		name:                    "remove first",
		input:                   []int{0, 1, 2, 3, 4},
		removeIndexes:           []int{0},
		expectedOutput:          []int{1, 2, 3, 4},
		expectedRemovedElements: 1,
		expectedRemovedRanges:   1,
	}, {
		name:                    "remove middle",
		input:                   []int{0, 1, 2, 3, 4},
		removeIndexes:           []int{2},
		expectedOutput:          []int{0, 1, 3, 4},
		expectedRemovedElements: 1,
		expectedRemovedRanges:   1,
	}, {
		name:                    "remove last",
		input:                   []int{0, 1, 2, 3, 4},
		removeIndexes:           []int{4},
		expectedOutput:          []int{0, 1, 2, 3},
		expectedRemovedElements: 1,
		expectedRemovedRanges:   1,
	}, {
		name:                    "remove all",
		input:                   []int{0, 1, 2, 3, 4},
		removeIndexes:           []int{0, 1, 2, 3, 4},
		expectedOutput:          []int{},
		expectedRemovedElements: 5,
		expectedRemovedRanges:   1,
	}, {
		name:                    "remove two ranges",
		input:                   []int{0, 1, 2, 3, 4},
		removeIndexes:           []int{0, 1, 3, 4},
		expectedOutput:          []int{2},
		expectedRemovedElements: 4,
		expectedRemovedRanges:   2,
	}, {
		name:                    "index overrun",
		input:                   []int{0, 1, 2, 3, 4},
		removeIndexes:           []int{4, 5},
		expectedOutput:          []int{0, 1, 2, 3},
		expectedRemovedElements: 1,
		expectedRemovedRanges:   1,
	}, {
		name:                    "duplicate indexes",
		input:                   []int{0, 1, 2, 3, 4},
		removeIndexes:           []int{4, 4, 4, 4},
		expectedOutput:          []int{0, 1, 2, 3},
		expectedRemovedElements: 1,
		expectedRemovedRanges:   1,
	}, {
		name:                    "negative indexes",
		input:                   []int{0, 1, 2, 3, 4},
		removeIndexes:           []int{-1, 0, 1},
		expectedOutput:          []int{2, 3, 4},
		expectedRemovedElements: 2,
		expectedRemovedRanges:   1,
	}, {
		name:                    "all cases combined",
		input:                   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		removeIndexes:           []int{-1, -1, 0, 3, 5, 6, 8, 9, 9, 10, 11},
		expectedOutput:          []int{1, 2, 4, 7},
		expectedRemovedElements: 6,
		expectedRemovedRanges:   4,
	},
}

func runTestCase(tb testing.TB, input, removeIndexes, expectedOutput []int, expectedRemovedElements, expectedRemovedRanges int) {
	output, removedElements, removedRanges := removeSliceIndexes(input, removeIndexes)
	assert.Equal(tb, expectedOutput, output)
	assert.Equal(tb, expectedRemovedElements, removedElements)
	assert.Equal(tb, expectedRemovedRanges, removedRanges)
}

func TestRemoveSliceIndexes(t *testing.T) {
	for _, tc := range removeSliceIndexesTestCases {
		t.Run(tc.name, func(t *testing.T) {
			runTestCase(t, tc.input, tc.removeIndexes, tc.expectedOutput, tc.expectedRemovedElements, tc.expectedRemovedRanges)
		})
	}
}

func BenchmarkRemoveSliceIndexes(b *testing.B) {
	for _, tc := range removeSliceIndexesTestCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			// Allocate "input" with enough space for the input of the test case.
			input := make([]int, 0, len(tc.input))

			for i := 0; i < b.N; i++ {
				// Copy values of "tc.input" to "input" because "RemoveSliceIndexes" is going to modify the input.
				input = input[:len(tc.input)]
				copy(input, tc.input)

				runTestCase(b, input, tc.removeIndexes, tc.expectedOutput, tc.expectedRemovedElements, tc.expectedRemovedRanges)
			}
		})
	}
}
