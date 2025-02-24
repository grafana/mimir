// SPDX-License-Identifier: AGPL-3.0-only

package math

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEWMA_AddAndValue(t *testing.T) {
	tests := []struct {
		name          string
		windowSize    uint
		warmupSamples uint8
		values        []float64
		expected      float64
	}{
		{
			name:          "average during warmup",
			windowSize:    5,
			warmupSamples: 3,
			values:        []float64{10, 20, 30},
			expected:      20.0,
		},
		{
			name:          "average after warmup",
			windowSize:    3,
			warmupSamples: 2,
			values:        []float64{10, 20, 30, 40},
			expected:      31,
		},
		{
			name:          "average without warmup",
			windowSize:    5,
			warmupSamples: 0,
			values:        []float64{100, 50},
			expected:      39,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ma := NewEwma(tc.windowSize, tc.warmupSamples)
			for _, v := range tc.values {
				ma.Add(v)
			}
			assert.Equal(t, tc.expected, math.Round(ma.Value()))
		})
	}
}

func TestEWMA_SetAndValue(t *testing.T) {
	ma := NewEwma(5, 3)
	ma.Set(50.0)

	assert.Equal(t, 50.0, math.Round(ma.Value()))
}

func TestEWMA_AddAfterSet(t *testing.T) {
	ma := NewEwma(5, 1)
	ma.Set(50.0)
	result := ma.Add(100.0)

	assert.Equal(t, float64(67), math.Round(result))
}
