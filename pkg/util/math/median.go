// SPDX-License-Identifier: AGPL-3.0-only

package math

import (
	"slices"
)

// MedianFilter provides the median value over a rolling window.
//
// This type is not concurrency safe.
type MedianFilter struct {
	values []float64
	sorted []float64
	index  int
	size   int
}

func NewMedianFilter(size int) *MedianFilter {
	return &MedianFilter{
		values: make([]float64, size),
		sorted: make([]float64, size),
	}
}

// Add adds a value to the filter, sorts the values, and returns the current median.
func (f *MedianFilter) Add(value float64) float64 {
	f.values[f.index] = value
	f.index = (f.index + 1) % len(f.values)

	if f.size < len(f.values)-1 {
		f.size++
		return value
	}

	copy(f.sorted, f.values)
	slices.Sort(f.sorted)
	return f.Median()
}

// Median returns the current median, else 0 if the filter isn't full yet.
func (f *MedianFilter) Median() float64 {
	if f.size < len(f.values)-1 {
		return 0
	}
	return f.sorted[len(f.sorted)/2]
}
