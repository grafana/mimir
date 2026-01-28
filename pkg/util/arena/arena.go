// SPDX-License-Identifier: AGPL-3.0-only

package arena

// Append is like the append built-in, except it ensures that, if a new
// underlying array needs to be allocated, it is allocated in the provided arena.
func Append[T any](a *Arena, slice []T, elems ...T) []T {
	if cap(slice)-len(slice) <= len(elems) || a == nil {
		return append(slice, elems...)
	}
	return append(append(MakeSlice[T](a, 0, len(slice)+len(elems)), slice...), elems...)
}
