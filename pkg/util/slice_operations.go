// SPDX-License-Identifier: AGPL-3.0-only

package util

import "sort"

type idxRange struct {
	from int
	to   int
}

// RemoveSliceIndexes takes a slice of elements of any type and a slice of indexes referring to elements in the former slice.
// It removes the elements at the given indexes from the given slice, while minimizing the number of copy operations
// that are required to do so by grouping consecutive indexes into ranges and removing them in single operations.
// The returned values are:
// - The resulting slice of elements after the elements at the given indexes have been removed.
// - The number of elements that have been removed.
// - The number of ranges of consecutive elements that have been removed.
func RemoveSliceIndexes[T any](data []T, indexes []int) ([]T, int, int) {
	sort.Ints(indexes)
	ranges := make([]idxRange, 0, len(data))
	none := idxRange{-1, -1}
	currentRange := none

	// Keep track of the last index to filter duplicates.
	lastIndex := -1
	for _, index := range indexes {
		if index < 0 || index >= len(data) {
			// Invalid indexes given, ignore them.
			continue
		}

		// Skip duplicates.
		if index == lastIndex {
			continue
		}
		lastIndex = index

		// Initialize the current range variable.
		if currentRange == none {
			currentRange.from = index
			currentRange.to = index
			continue
		}

		// Extend the current range by one, because this index connected to it.
		if currentRange.to+1 == index {
			currentRange.to = index
			continue
		}

		// The current range is finished, add it to the ranges list.
		ranges = append(ranges, currentRange)

		// Initialize the next range.
		currentRange.from = index
		currentRange.to = index
	}

	if currentRange != none {
		// Add the last range.
		ranges = append(ranges, currentRange)
	}

	removed := 0
	for _, r := range ranges {
		// Remove range while offsetting by the number of already removed elements.
		data = append(data[:r.from-removed], data[r.to+1-removed:]...)

		// Update the number of removed elements.
		removed += r.to - r.from + 1
	}

	return data, removed, len(ranges)
}
