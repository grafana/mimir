// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

// BoundMaxIndexBytes applies a tenant's limit on the index bytes one
// cardinality estimate may read to the max_index_bytes a request asked for:
// a request that asks for none (0) gets the limit, and one that asks for
// more is cut to it. A limit of 0 or less leaves the request as asked.
func BoundMaxIndexBytes(asked, limit int64) int64 {
	if limit <= 0 {
		return asked
	}
	if asked <= 0 || asked > limit {
		return limit
	}
	return asked
}

// BoundMaxGroups applies a tenant's limit on the groups one cardinality
// estimate may answer to the max_groups a request asked for, as
// BoundMaxIndexBytes does for index bytes.
func BoundMaxGroups(asked, limit int64) int64 {
	return BoundMaxIndexBytes(asked, limit)
}

// HashBytes is the memory one label-set hash held in the querier costs,
// as a map entry.
const HashBytes = 24

// HashBudget is how many label-set hashes a deduplicating estimate may hold
// in memoryBytes, the memory the tenant allows one estimate: at least one
// when the limit is set, or 0 for no limit when it is not.
func HashBudget(memoryBytes uint64) int64 {
	if memoryBytes == 0 {
		return 0
	}
	return max(1, int64(memoryBytes/HashBytes))
}

// BoundMaxHashes applies a tenant's hash budget (HashBudget) to the
// max_hashes a request asked for, as BoundMaxIndexBytes does for index
// bytes: a request that asks for none (0) gets the budget, and one that asks
// for more is cut to it. A budget of 0 leaves the request as asked.
func BoundMaxHashes(asked, budget int64) int64 {
	return BoundMaxIndexBytes(asked, budget)
}
