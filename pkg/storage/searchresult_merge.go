// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"cmp"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// NewMergingSearchResultSet returns a SearchResultSet that performs a
// streaming k-way merge across the given sources. Each source MUST emit
// results in the order requested by hints.OrderBy; the merger picks the
// best head across sources per call to Next, advances that source, and
// dedups by Value across sources (advancing any other source whose head
// equals the just-emitted Value).
//
// The merger is the slice-of-iterators counterpart to Prometheus's
// (unexported) pairwiseMergeSearchSets. It does not re-apply any filter —
// the leaf Searchers already scored their values. Per the Searcher contract
// (Spec invariant 3) Score is deterministic for a given (Value, Filter),
// so duplicates from different sources carry identical scores by
// construction; if a leaf reports a different score for the same value,
// that's a bug elsewhere, not something this layer should silently mask.
//
// Limit truncation stops iteration after hints.Limit results without
// draining the remaining queue from each source — the sources are
// cancelled via Close. hints.Limit <= 0 means no limit.
//
// Memory: the merger holds at most one head per source plus whatever each
// source has buffered internally. Pair each gRPC-stream source with
// concurrentSearchResultSet to pre-fetch concurrently and keep the merger
// from serialising network round-trips across sources.
//
// hints may be nil; in that case ordering defaults to OrderByValueAsc and
// no limit is applied. Warnings from each source surface via the merger's
// Warnings() after iteration completes; sources cancelled via Close before
// they exhaust may not contribute their warnings.
func NewMergingSearchResultSet(sources []storage.SearchResultSet, hints *storage.SearchHints) storage.SearchResultSet {
	if len(sources) == 0 {
		return storage.EmptySearchResultSet()
	}
	order := storage.OrderByValueAsc
	limit := 0
	if hints != nil {
		order = hints.OrderBy
		limit = hints.Limit
	}
	return &mergingSearchResultSet{
		sources:   sources,
		order:     order,
		limit:     limit,
		heads:     make([]storage.SearchResult, len(sources)),
		hasHead:   make([]bool, len(sources)),
		exhausted: make([]bool, len(sources)),
	}
}

type mergingSearchResultSet struct {
	sources []storage.SearchResultSet
	order   storage.Ordering
	limit   int

	heads     []storage.SearchResult
	hasHead   []bool
	exhausted []bool

	cur     storage.SearchResult
	emitted int
	done    bool
	err     error
}

// refreshHead ensures sources[i] has a valid head ready to peek at heads[i].
// Returns false if the source is exhausted or has errored. On error sets
// m.err.
func (m *mergingSearchResultSet) refreshHead(i int) bool {
	if m.exhausted[i] || m.err != nil {
		return false
	}
	if m.hasHead[i] {
		return true
	}
	if !m.sources[i].Next() {
		m.exhausted[i] = true
		if err := m.sources[i].Err(); err != nil {
			m.err = err
		}
		return false
	}
	m.heads[i] = m.sources[i].At()
	m.hasHead[i] = true
	return true
}

func (m *mergingSearchResultSet) Next() bool {
	if m.done || m.err != nil {
		return false
	}
	if m.limit > 0 && m.emitted >= m.limit {
		m.done = true
		return false
	}
	// Find the best head across all sources. Linear scan is fine for the
	// small N we expect (replicas typically <= 16, store-gateways <= ~50);
	// switch to a heap if profiling shows hot.
	bestIdx := -1
	for i := range m.sources {
		if !m.refreshHead(i) {
			if m.err != nil {
				return false
			}
			continue
		}
		if bestIdx < 0 || compareSearchResults(m.heads[i], m.heads[bestIdx], m.order) < 0 {
			bestIdx = i
		}
	}
	if bestIdx < 0 {
		m.done = true
		return false
	}
	m.cur = m.heads[bestIdx]
	m.hasHead[bestIdx] = false
	// Cross-source dedup: advance any other source whose head equals the
	// emitted Value. Correct because each source is pre-sorted in the same
	// OrderBy, so equal-Value duplicates are at the heads under value
	// ordering; under OrderByScoreDesc, Spec invariant 3 guarantees
	// identical Score for identical Value, so duplicates tie there too.
	for i := range m.sources {
		if i == bestIdx || !m.hasHead[i] {
			continue
		}
		if m.heads[i].Value == m.cur.Value {
			m.hasHead[i] = false
		}
	}
	m.emitted++
	return true
}

func (m *mergingSearchResultSet) At() storage.SearchResult { return m.cur }

func (m *mergingSearchResultSet) Warnings() annotations.Annotations {
	var out annotations.Annotations
	for _, s := range m.sources {
		out.Merge(s.Warnings())
	}
	return out
}

func (m *mergingSearchResultSet) Err() error { return m.err }

func (m *mergingSearchResultSet) Close() error {
	var firstErr error
	for _, s := range m.sources {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.done = true
	return firstErr
}

// compareSearchResults returns negative if a sorts before b under the given
// ordering. Mirrors Prometheus's (unexported) compareSearchResults:
//   - OrderByValueAsc:  ascending Value
//   - OrderByValueDesc: descending Value
//   - OrderByScoreDesc: descending Score with ascending Value as the
//     deterministic tiebreak.
func compareSearchResults(a, b storage.SearchResult, order storage.Ordering) int {
	switch order {
	case storage.OrderByValueDesc:
		return cmp.Compare(b.Value, a.Value)
	case storage.OrderByScoreDesc:
		if a.Score != b.Score {
			return cmp.Compare(b.Score, a.Score)
		}
		return cmp.Compare(a.Value, b.Value)
	default:
		return cmp.Compare(a.Value, b.Value)
	}
}
