// SPDX-License-Identifier: AGPL-3.0-only

package math

import (
	"math"
)

func NewRollingSum(capacity uint) *RollingSum {
	return &RollingSum{samples: make([]float64, capacity)}
}

// RollingSum can be used to compute the coefficient of variation over rolling windowed values, along with slope calulation.
//
// This type is not concurrency safe.
type RollingSum struct {
	// For variation and covariance
	samples []float64
	size    int
	index   int

	// Rolling sum fields
	sumY       float64 // Y values are the samples
	sumSquares float64
}

// Add adds the value to the window if it's non-zero, updates the sums, and returns the old value along with whether the
// window is full.
func (r *RollingSum) Add(value float64) (oldValue float64, full bool) {
	if value != 0 {
		if r.size == len(r.samples) {
			full = true

			// Remove oldest value
			oldValue = r.samples[r.index]
			r.sumY -= oldValue
			r.sumSquares -= oldValue * oldValue
		} else {
			r.size++
		}

		// Add new value
		r.samples[r.index] = value

		// Update rolling computations
		r.sumY += value
		r.sumSquares += value * value

		// Move index forward
		r.index = (r.index + 1) % len(r.samples)
	}

	return oldValue, full
}

// CalculateCV calculates the coefficient of variation (relative variance), mean, and variance for the sum. Returns NaN
// values if there are < 2 samples, the variance is < 0, or the mean is 0.
func (r *RollingSum) CalculateCV() (cv, mean, variance float64) {
	if r.size < 2 {
		return math.NaN(), math.NaN(), math.NaN()
	}

	mean = r.sumY / float64(r.size)
	variance = (r.sumSquares / float64(r.size)) - (mean * mean)
	if variance < 0 || mean == 0 {
		return math.NaN(), math.NaN(), math.NaN()
	}

	cv = math.Sqrt(variance) / mean
	return cv, mean, variance
}

// CorrelationWindow can be used to compute correlation for sets of values over a rolling window.
//
// This type is not concurrency safe.
type CorrelationWindow struct {
	warmupSamples uint8
	xSamples      *RollingSum
	ySamples      *RollingSum
	corrSumXY     float64
}

func NewCorrelationWindow(capacity uint, warmupSamples uint8) *CorrelationWindow {
	return &CorrelationWindow{
		warmupSamples: warmupSamples,
		xSamples:      NewRollingSum(capacity),
		ySamples:      NewRollingSum(capacity),
	}
}

// Add adds the values to the window and returns the current correlation coefficient.
// Returns a value between 0 and 1 when a correlation between increasing x and y values is present.
// Returns a value between -1 and 0 when a correlation between increasing x and decreasing y values is present.
// Returns 0 values if < warmup or low CV (< .01)
func (w *CorrelationWindow) Add(x, y float64) (correlation, cvX, cvY float64) {
	if math.IsInf(x, 0) || math.IsInf(y, 0) {
		return 0, 0, 0
	}

	oldX, full := w.xSamples.Add(x)
	oldY, _ := w.ySamples.Add(y)
	cvX, meanX, varX := w.xSamples.CalculateCV()
	cvY, meanY, varY := w.ySamples.CalculateCV()

	if full {
		// Remove old value
		w.corrSumXY -= oldX * oldY
	}

	// Add new value
	w.corrSumXY += x * y

	if math.IsNaN(cvX) || math.IsNaN(cvY) {
		return 0, 0, 0
	}

	// Ignore warmup
	if w.xSamples.size < int(w.warmupSamples) {
		return 0, 0, 0
	}

	// Ignore measurements that vary by less than 1%
	minCV := 0.01
	if cvX < minCV || cvY < minCV {
		return 0, cvX, cvY
	}

	covariance := (w.corrSumXY / float64(w.xSamples.size)) - (meanX * meanY)
	correlation = covariance / (math.Sqrt(varX) * math.Sqrt(varY))

	return correlation, cvX, cvY
}
