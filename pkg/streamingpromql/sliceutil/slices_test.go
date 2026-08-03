// SPDX-License-Identifier: AGPL-3.0-only

package sliceutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackwardsIndexFunc(t *testing.T) {
	testCases := map[string]struct {
		input    []int
		expected int
	}{
		"empty slice": {
			input:    []int{},
			expected: -1,
		},
		"nil slice": {
			input:    nil,
			expected: -1,
		},
		"never returns true": {
			input:    []int{1, 2, 3},
			expected: -1,
		},
		"single element slice that returns true": {
			input:    []int{5},
			expected: 0,
		},
		"single element slice that returns false": {
			input:    []int{6},
			expected: -1,
		},
		"multiple elements return true, should return index closest to end": {
			input:    []int{1, 2, 5, 5, 4},
			expected: 3,
		},
		"multiple elements at end return true, should return index closest to end": {
			input:    []int{1, 2, 5, 5, 5},
			expected: 4,
		},
		"first element returns true": {
			input:    []int{5, 1, 2, 3},
			expected: 0,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := BackwardsIndexFunc(tc.input, func(i int) bool { return i == 5 })
			require.Equal(t, tc.expected, actual)
		})
	}
}
