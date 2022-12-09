// SPDX-License-Identifier: AGPL-3.0-only

package test

import "github.com/stretchr/testify/assert"

// EqualSlices is a comparison function for slices that treats nil slices as equivalent
// to empty slices (which the assert.Equals assertion does not).
func EqualSlices[T any](expected, actual []T) assert.Comparison {
	if len(expected) == 0 {
		return func() bool {
			return len(expected) == len(actual)
		}
	}

	return func() bool {
		return assert.ObjectsAreEqual(expected, actual)
	}
}
