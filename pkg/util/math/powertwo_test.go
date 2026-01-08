// SPDX-License-Identifier: AGPL-3.0-only

package math

import (
	"testing"

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
	testCases := map[int]int{
		0: 2,
		1: 2,
		2: 2,
		3: 4,
		4: 4,
		5: 8,
	}
	for value, expected := range testCases {
		require.Equal(t, expected, NextPowerTwo(value))
		require.True(t, IsPowerOfTwo(expected))
	}
}
