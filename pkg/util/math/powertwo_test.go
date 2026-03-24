// SPDX-License-Identifier: AGPL-3.0-only

package math

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
	}
}
