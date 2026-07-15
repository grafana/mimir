// SPDX-License-Identifier: AGPL-3.0-only

package sliceutil

// BackwardsIndexFunc returns the index of the first element in s for which f returns true,
// searching backwards from the end of s. It returns -1 if no such element is found.
func BackwardsIndexFunc[S ~[]E, E any](s S, f func(E) bool) int {
	for i := len(s) - 1; i >= 0; i-- {
		if f(s[i]) {
			return i
		}
	}

	return -1
}
