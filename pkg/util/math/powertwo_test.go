// SPDX-License-Identifier: AGPL-3.0-only

package math

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsPowerTwo(t *testing.T) {
	require.False(t, IsPowerOfTwo(0))
	require.True(t, IsPowerOfTwo(1))
	require.True(t, IsPowerOfTwo(2))
	require.True(t, IsPowerOfTwo(4))
	require.True(t, IsPowerOfTwo(8))
	require.True(t, IsPowerOfTwo(16))
	require.True(t, IsPowerOfTwo(32))

	require.False(t, IsPowerOfTwo(3))
	require.False(t, IsPowerOfTwo(5))
	require.False(t, IsPowerOfTwo(6))
	require.False(t, IsPowerOfTwo(7))
	require.False(t, IsPowerOfTwo(9))
}

func TestPowerTwo(t *testing.T) {
	testCases := map[string]struct {
		value    int
		expected int
	}{
		"zero": {
			value:    0,
			expected: 2,
		},
		"one": {
			value:    1,
			expected: 2,
		},
		"two": {
			value:    2,
			expected: 2,
		},
		"three": {
			value:    3,
			expected: 4,
		},
		"four": {
			value:    4,
			expected: 4,
		},
		"five": {
			value:    5,
			expected: 8,
		},
	}
	for testName, testCase := range testCases {
		require.Equal(t, testCase.expected, NextPowerTwo(testCase.value), testName)
		require.True(t, IsPowerOfTwo(testCase.expected), testName)
	}
}
