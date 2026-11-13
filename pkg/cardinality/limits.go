// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

// BoundMaxIndexBytes caps the max_index_bytes a request asked for at the tenant's limit.
// Asking for none (0) gets the limit; a limit of 0 or less leaves it as asked.
func BoundMaxIndexBytes(asked, limit int64) int64 {
	if limit <= 0 {
		return asked
	}
	if asked <= 0 || asked > limit {
		return limit
	}
	return asked
}

// BoundMaxGroups is BoundMaxIndexBytes for max_groups.
func BoundMaxGroups(asked, limit int64) int64 {
	return BoundMaxIndexBytes(asked, limit)
}

// HashBytes is the memory one held label-set hash costs, as a map entry.
const HashBytes = 24

// HashBudget is how many hashes an estimate may hold in memoryBytes, 0 for no limit.
func HashBudget(memoryBytes uint64) int64 {
	if memoryBytes == 0 {
		return 0
	}
	return max(1, int64(memoryBytes/HashBytes))
}

// BoundMaxHashes is BoundMaxIndexBytes for max_hashes, with a HashBudget.
func BoundMaxHashes(asked, budget int64) int64 {
	return BoundMaxIndexBytes(asked, budget)
}
