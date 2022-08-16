// SPDX-License-Identifier: AGPL-3.0-only

package util

// RemoveSliceIndexes takes a slice of elements of any type and a sorted slice of indexes of elements in the former slice.
// It removes the elements at the given indexes from the given slice, while minimizing the number of copy operations
// that are required to do so by grouping consecutive indexes into ranges and removing the ranges in single operations.
// The given number of indexes must be sorted in ascending order.
// Indexes which are duplicate or out of the range of the given slice are ignored.
// It returns the updated slice.
func RemoveSliceIndexes[T any](data []T, indexes []int) []T {
	data, _, _ = removeSliceIndexes(data, indexes)
	return data
}

func removeSliceIndexes[T any](data []T, indexes []int) ([]T, int, int) {
	rangeStart := -1
	rangeEnd := -1
	rangeCount := 0
	removed := 0

	// Keep track of the last index to filter duplicates.
	for _, index := range indexes {
		if index < 0 || index >= len(data)+removed {
			// Invalid indexes given, ignore them.
			continue
		}

		// Skip duplicates or unsorted indexes.
		if rangeEnd >= 0 && index <= rangeEnd {
			continue
		}

		// Initialize the current range.
		if rangeStart < 0 {
			rangeStart = index
			rangeEnd = index
			continue
		}

		// Extend the current range by one, because this index connected to it.
		if rangeEnd+1 == index {
			rangeEnd = index
			continue
		}

		// Otherwise we can remove the previous range
		rangeCount++
		data = append(data[:rangeStart-removed], data[rangeEnd+1-removed:]...)

		// Update the number of removed elements.
		removed += rangeEnd - rangeStart + 1

		// Initialize the next range.
		rangeStart = index
		rangeEnd = index
	}

	// Remove last range.
	if rangeStart >= 0 {
		rangeCount++
		data = append(data[:rangeStart-removed], data[rangeEnd+1-removed:]...)
		removed += rangeEnd - rangeStart + 1
	}

	return data, removed, rangeCount
}
