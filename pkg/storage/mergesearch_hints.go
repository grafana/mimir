// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"github.com/prometheus/prometheus/storage"
)

// PairwiseMergeSearchSetsWithHints is a convenience wrapper around
// PairwiseMergeSearchSets that extracts the ordering and limit from a
// *storage.SearchHints. A nil hints defaults to OrderByValueAsc and no
// limit, matching Prometheus's mergeSearchSets behaviour when called
// without hints.
//
// This is a Mimir-side wrapper — the underlying merge logic in
// mergesearch.go is the verbatim Prometheus port.
func PairwiseMergeSearchSetsWithHints(sets []storage.SearchResultSet, hints *storage.SearchHints) storage.SearchResultSet {
	order := storage.OrderByValueAsc
	limit := 0
	if hints != nil {
		order = hints.OrderBy
		limit = hints.Limit
	}
	return PairwiseMergeSearchSets(sets, order, limit)
}
