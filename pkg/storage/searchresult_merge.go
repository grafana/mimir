// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"cmp"
	"slices"

	"github.com/prometheus/prometheus/storage"
)

// MergeSearchResults merges per-source []storage.SearchResult slices, dedups
// by Value taking the max Score across sources, sorts the result per
// hints.OrderBy, and truncates to hints.Limit. It is the slice-based
// counterpart to Prometheus's (unexported) pairwiseMergeSearchSets, intended
// for cross-replica / cross-store-gateway fan-out where each leaf has already
// applied the search filter and emitted scored results.
//
// Unlike storage.ApplySearchHints (which operates on []string and re-runs the
// filter to score), this preserves the scores computed by the leaf Searchers.
// Spec invariant 3 requires Score to be deterministic per (Value, Filter), so
// duplicates from different sources tie in normal operation; the max defends
// against drift between leaves that should never happen but is cheap to
// guard.
//
// hints may be nil; in that case ordering defaults to OrderByValueAsc and no
// limit is applied.
func MergeSearchResults(perSource [][]storage.SearchResult, hints *storage.SearchHints) []storage.SearchResult {
	if len(perSource) == 0 {
		return nil
	}

	// Hash dedup: value → best (max) score seen across sources. The map
	// also serves as our cross-source dedup, since each Value collapses to
	// a single entry. We can lift this to a streaming k-way merge if
	// memory pressure ever bites — for now, the slice-based path matches
	// the BucketStore cross-block merge from PR #1.
	best := make(map[string]float64, len(perSource[0]))
	for _, src := range perSource {
		for _, r := range src {
			if cur, seen := best[r.Value]; !seen || r.Score > cur {
				best[r.Value] = r.Score
			}
		}
	}

	out := make([]storage.SearchResult, 0, len(best))
	for v, s := range best {
		out = append(out, storage.SearchResult{Value: v, Score: s})
	}

	order := storage.OrderByValueAsc
	limit := 0
	if hints != nil {
		order = hints.OrderBy
		limit = hints.Limit
	}
	sortSearchResults(out, order)

	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// sortSearchResults orders rs in place per the Prometheus contract:
//   - OrderByValueAsc / OrderByValueDesc: lexicographic on Value.
//   - OrderByScoreDesc: descending Score, with ascending Value as the
//     deterministic tiebreak (matches storage.compareSearchResults).
func sortSearchResults(rs []storage.SearchResult, order storage.Ordering) {
	switch order {
	case storage.OrderByValueDesc:
		slices.SortFunc(rs, func(a, b storage.SearchResult) int {
			return cmp.Compare(b.Value, a.Value)
		})
	case storage.OrderByScoreDesc:
		slices.SortFunc(rs, func(a, b storage.SearchResult) int {
			if a.Score != b.Score {
				return cmp.Compare(b.Score, a.Score)
			}
			return cmp.Compare(a.Value, b.Value)
		})
	default:
		slices.SortFunc(rs, func(a, b storage.SearchResult) int {
			return cmp.Compare(a.Value, b.Value)
		})
	}
}
