// SPDX-License-Identifier: AGPL-3.0-only

package math

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLog10Func(t *testing.T) {
	tests := []struct {
		name     string
		factor   int
		input    int
		expected int
	}{
		{
			name:     "Small number using pre-computed value",
			factor:   1,
			input:    5,
			expected: 1,
		},
		{
			name:     "Large number using post-computed value",
			factor:   1,
			input:    1000,
			expected: 3,
		},
		{
			name:     "Using factor of 2",
			factor:   2,
			input:    100,
			expected: 4,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logFunc := Log10Func(tc.factor)
			assert.Equal(t, tc.expected, logFunc(tc.input))
		})
	}
}
