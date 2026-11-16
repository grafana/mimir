// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

// BoundByLimit caps what a request asked for (max_index_bytes, max_groups or max_hashes) at the tenant's limit.
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

// HashBudgetBytes is the memory the budget allows for each hash: HashBytes once merged, and the 8 bytes
// it takes in the source's answer, which is held until merged.
const HashBudgetBytes = HashBytes + 8

// HashBudget is how many hashes an estimate may hold in memoryBytes, 0 for no limit.
func HashBudget(memoryBytes uint64) int64 {
	if memoryBytes == 0 {
		return 0
	}
	return max(1, int64(memoryBytes/HashBudgetBytes))
}
