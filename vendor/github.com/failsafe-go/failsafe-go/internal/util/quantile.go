package util

import "math"

// MovingQuantile estimates a streaming quantile using the Windowless Moving Percentile algorithm (Martin Jambon).
// This provides O(1) time and space quantile estimation that adapts to distribution changes.
//
// This type is not concurrency safe.
type MovingQuantile struct {
	quantile float64
	r        float64
	alpha    float64

	// Mutable state
	count    int
	value    float64
	mean     float64
	variance float64
}

// NewMovingQuantile creates a new MovingQuantile for the given quantile (0-1), step ratio r, and age. The age controls
// how far back in time the estimate effectively "remembers" - smaller ages adapt faster to recent changes, while larger
// ages provide more stability by retaining influence from older samples.
func NewMovingQuantile(quantile float64, r float64, age uint) MovingQuantile {
	return MovingQuantile{
		quantile: quantile,
		r:        r,
		alpha:    2 / (float64(age) + 1),
	}
}

// Add adds a sample and returns the updated quantile estimate.
func (q *MovingQuantile) Add(sample float64) float64 {
	q.count++
	if q.count == 1 {
		q.value = sample
		q.mean = sample
		return q.value
	}

	// Update EMA mean and variance
	oldMean := q.mean
	q.mean = Smooth(q.mean, sample, q.alpha)
	q.variance = Smooth(q.variance, (sample-oldMean)*(sample-q.mean), q.alpha)

	// Compute step size
	delta := math.Sqrt(q.variance) * q.r
	if delta == 0 {
		return q.value
	}

	// Adjust estimate
	if sample < q.value {
		q.value -= delta / q.quantile
	} else if sample > q.value {
		q.value += delta / (1 - q.quantile)
	}
	return q.value
}

// Value returns the current quantile estimate.
func (q *MovingQuantile) Value() float64 {
	return q.value
}

// Count returns the number of samples added.
func (q *MovingQuantile) Count() int {
	return q.count
}

// Reset resets the quantile estimate.
func (q *MovingQuantile) Reset() {
	q.value = 0
	q.mean = 0
	q.variance = 0
	q.count = 0
}
