// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"cmp"
	"slices"

	"github.com/prometheus/prometheus/storage"
)

// MergeSearchResults merges per-source []storage.SearchResult slices, dedups
// by Value, sorts the result per hints.OrderBy, and truncates to hints.Limit.
// It is the slice-based counterpart to Prometheus's (unexported)
// pairwiseMergeSearchSets, intended for cross-replica / cross-store-gateway
// fan-out where each leaf has already applied the search filter and emitted
// scored results.
//
// Unlike storage.ApplySearchHints (which operates on []string and re-runs the
// filter to score), this preserves the scores computed by the leaf Searchers.
// Per the Searcher contract (Spec invariant 3), Score is deterministic for a
// given (Value, Filter), so duplicates from different sources carry identical
// scores by construction — the merger takes the first occurrence and skips
// subsequent duplicates without comparing. If two leaves ever disagree on the
// score for a value, that's a bug elsewhere (filter-library drift, wire
// translation, or determinism violation), not something this layer should
// silently paper over.
//
// hints may be nil; in that case ordering defaults to OrderByValueAsc and no
// limit is applied.
func MergeSearchResults(perSource [][]storage.SearchResult, hints *storage.SearchHints) []storage.SearchResult {
	if len(perSource) == 0 {
		return nil
	}

	// Hash dedup: value → score. Each Value collapses to a single entry;
	// duplicates from later sources are skipped on the assumption that
	// their score matches (Spec invariant 3). We can lift this to a
	// streaming k-way merge if memory pressure ever bites.
	seen := make(map[string]float64, len(perSource[0]))
	for _, src := range perSource {
		for _, r := range src {
			if _, exists := seen[r.Value]; !exists {
				seen[r.Value] = r.Score
			}
		}
	}

	out := make([]storage.SearchResult, 0, len(seen))
	for v, s := range seen {
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
