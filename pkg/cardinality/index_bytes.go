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
