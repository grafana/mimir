// SPDX-License-Identifier: AGPL-3.0-only

package testutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCombinations(t *testing.T) {
	tests := map[string]struct {
		name     string
		input    []string
		length   int
		expected [][]string
		panics   bool
	}{
		"combinations of 2 from 3 items": {

			input:    []string{"a", "b", "c"},
			length:   2,
			expected: [][]string{{"a", "b"}, {"a", "c"}, {"b", "c"}},
		},
		"combinations of 3 from 4 items": {

			input:    []string{"a", "b", "c", "d"},
			length:   3,
			expected: [][]string{{"a", "b", "c"}, {"a", "b", "d"}, {"a", "c", "d"}, {"b", "c", "d"}},
		},
		"combinations of 1 from 3 items": {
			input:    []string{"a", "b", "c"},
			length:   1,
			expected: [][]string{{"a"}, {"b"}, {"c"}},
		},
		"combinations of 3 from 3 items": {
			input:    []string{"a", "b", "c"},
			length:   3,
			expected: [][]string{{"a", "b", "c"}},
		},
		"combinations of 0 length": {
			input:    []string{"a", "b", "c"},
			length:   0,
			expected: [][]string{{}},
		},
		"length greater than array size": {
			input:  []string{"a", "b"},
			length: 3,
			panics: true,
		},
		"empty array": {
			input:    []string{},
			length:   0,
			expected: [][]string{{}},
		},
	}

	for tName, test := range tests {
		t.Run(tName, func(t *testing.T) {
			if test.panics {
				f := func() {
					Combinations(test.input, test.length)
				}
				require.Panics(t, f)
				return
			}
			output := Combinations(test.input, test.length)
			require.Equal(t, test.expected, output)
		})
	}
}
