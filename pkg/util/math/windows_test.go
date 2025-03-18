// SPDX-License-Identifier: AGPL-3.0-only

package math

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRollingSum(t *testing.T) {
	tests := []struct {
		name              string
		values            []float64
		capacity          uint
		expectedVariation float64
		expectedSize      int
		shouldNaN         bool
	}{
		{
			name:         "less than two positive values returns NaN",
			values:       []float64{1.0, -1.0},
			capacity:     3,
			expectedSize: 2,
			shouldNaN:    true,
		},
		{
			name:              "zeros are ignored",
			values:            []float64{5.0, 0.0, 10.0},
			capacity:          3,
			expectedVariation: 0.3333,
			expectedSize:      2,
		},
		{
			name:              "window fills up to max capacity",
			values:            []float64{5.0, 10.0, 15.0, 20.0},
			capacity:          3,
			expectedVariation: 0.2722,
			expectedSize:      3,
		},
		{
			name:              "identical values give zero variation",
			values:            []float64{10.0, 10.0, 10.0},
			capacity:          3,
			expectedVariation: 0.0,
			expectedSize:      3,
		},
		{
			name:              "leading zeros are ignored",
			values:            []float64{0.0, 0.0, 5.0, 10.0},
			capacity:          3,
			expectedVariation: 0.3333,
			expectedSize:      2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := NewRollingSum(tc.capacity)
			var cv float64
			for _, v := range tc.values {
				w.Add(v)
				cv, _, _ = w.CalculateCV()
			}

			if tc.shouldNaN {
				assert.True(t, math.IsNaN(cv))
			} else {
				assert.InDelta(t, tc.expectedVariation, cv, 0.0001)
			}
			assert.Equal(t, tc.expectedSize, w.size)
		})
	}
}

func TestRollingSumSliding(t *testing.T) {
	w := NewRollingSum(3)

	w.Add(10.0)
	w.Add(20.0)
	w.Add(30.0)
	cv, _, _ := w.CalculateCV()
	assert.InDelta(t, 0.4082, cv, 0.0001)
	w.Add(40.0)
	cv, _, _ = w.CalculateCV()
	assert.InDelta(t, 0.2722, cv, 0.0001)
	w.Add(0.0)
	cv, _, _ = w.CalculateCV()
	assert.InDelta(t, 0.2722, cv, 0.0001)
	assert.Equal(t, 3, w.size)
}

func TestCorrelationWindow(t *testing.T) {
	tests := []struct {
		name                string
		capacity            uint
		xValues             []float64
		yValues             []float64
		expectedCorrelation float64
		expectedSize        int
	}{
		{
			name:                "single pair returns zero",
			capacity:            3,
			xValues:             []float64{1.0},
			yValues:             []float64{2.0},
			expectedCorrelation: 0.0,
			expectedSize:        1,
		},
		{
			name:                "perfect positive correlation",
			capacity:            3,
			xValues:             []float64{1.0, 2.0, 3.0},
			yValues:             []float64{10.0, 20.0, 30.0},
			expectedCorrelation: 1.0,
			expectedSize:        3,
		},
		{
			name:                "perfect negative correlation",
			capacity:            3,
			xValues:             []float64{1.0, 2.0, 3.0},
			yValues:             []float64{30.0, 20.0, 10.0},
			expectedCorrelation: -1.0,
			expectedSize:        3,
		},
		{
			name:                "low variation returns zero",
			capacity:            3,
			xValues:             []float64{100.0, 100.1, 100.05},
			yValues:             []float64{200.0, 200.1, 200.05},
			expectedCorrelation: 0.0,
			expectedSize:        3,
		},
		{
			name:                "rolling window",
			capacity:            3,
			xValues:             []float64{1.0, 2.0, 3.0, 4.0, 5.0},
			yValues:             []float64{10.0, 20.0, 30.0, 40.0, 50.0},
			expectedCorrelation: 1.0,
			expectedSize:        3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := NewCorrelationWindow(tc.capacity, 0)
			var correlation float64
			for i := range tc.xValues {
				correlation, _, _ = w.Add(tc.xValues[i], tc.yValues[i])
			}

			assert.InDelta(t, tc.expectedCorrelation, correlation, 0.0001)
			assert.Equal(t, tc.expectedSize, w.xSamples.size)
		})
	}
}

func TestCorrelationWindowSliding(t *testing.T) {
	w := NewCorrelationWindow(3, 0)

	corr, _, _ := w.Add(1.0, 10.0)
	assert.InDelta(t, 0.0, corr, 0.0001)
	corr, _, _ = w.Add(2.0, 20.0)
	assert.InDelta(t, 1.0, corr, 0.0001)
	corr, _, _ = w.Add(3.0, 30.0)
	assert.InDelta(t, 1.0, corr, 0.0001)
	corr, _, _ = w.Add(4.0, 40.0)
	assert.InDelta(t, 1.0, corr, 0.0001)
	assert.Equal(t, 3, w.xSamples.size)
}
