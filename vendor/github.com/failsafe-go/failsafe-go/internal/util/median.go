package util

import (
	"slices"
)

// MovingMedian provides the median value over a moving window.
//
// This type is not concurrency safe.
type MovingMedian struct {
	values []float64
	sorted []float64
	index  int
	size   int
}

func NewMovingMedian(size int) MovingMedian {
	return MovingMedian{
		values: make([]float64, size),
		sorted: make([]float64, size),
	}
}

// Add adds a value to the window, sorts the values, and returns the current median.
func (m *MovingMedian) Add(value float64) float64 {
	m.values[m.index] = value
	m.index = (m.index + 1) % len(m.values)

	if m.size < len(m.values)-1 {
		m.size++
		return value
	}

	copy(m.sorted, m.values)
	slices.Sort(m.sorted)
	return m.Median()
}

// Median returns the current median, else 0 if the window isn't full yet.
func (m *MovingMedian) Median() float64 {
	if m.size < len(m.values)-1 {
		return 0
	}
	return m.sorted[len(m.sorted)/2]
}

// Reset resets the window to its initial value.
func (m *MovingMedian) Reset() {
	for i := range m.values {
		m.values[i] = 0
		m.sorted[i] = 0
	}
	m.index = 0
	m.size = 0
}
