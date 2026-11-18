// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

// BoundByLimit caps what a request asked for (max_groups) at the tenant's limit.
// Asking for none (0) gets the limit; a limit of 0 or less leaves it as asked.
func BoundByLimit(asked, limit int64) int64 {
	if limit <= 0 {
		return asked
	}
	if asked <= 0 || asked > limit {
		return limit
	}
	return asked
}

// HashBytes is the memory one held label-set hash costs, as a map entry.
const HashBytes = 24

// GroupBytes is about the memory one group of hashes costs before its first hash: its own set and its
// entry in the groups map. A multiple of HashBytes.
const GroupBytes = 8 * HashBytes
