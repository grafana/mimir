// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"cmp"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// NewMergingSearchResultSet returns a SearchResultSet that performs a
// streaming k-way merge across the given sources. Each source MUST emit
// results in the order requested by hints.OrderBy. The merger does not
// re-apply any filter — duplicates from different sources are expected to
// carry identical scores (Prometheus's Searcher contract). Cross-source
// duplicates collapse to a single emit.
//
// Limit truncation stops iteration without draining remaining queues;
// hints.Limit <= 0 means no limit. nil hints defaults to OrderByValueAsc
// with no limit.
//
// Pair gRPC-stream sources with concurrentSearchResultSet so the merger
// doesn't serialise on a single source's network. Warnings surface via
// the merger's Warnings() after iteration completes; sources cancelled
// via Close before exhaustion may not contribute their warnings.
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
		compare:   makeSearchResultComparator(order),
		limit:     limit,
		heads:     make([]storage.SearchResult, len(sources)),
		hasHead:   make([]bool, len(sources)),
		exhausted: make([]bool, len(sources)),
	}
}

// searchResultComparator returns negative if a sorts before b.
type searchResultComparator func(a, b storage.SearchResult) int

type mergingSearchResultSet struct {
	sources []storage.SearchResultSet
	compare searchResultComparator
	limit   int

	heads     []storage.SearchResult
	hasHead   []bool
	exhausted []bool

	cur     storage.SearchResult
	emitted int
	done    bool
	err     error
}

// refreshHead pulls a new head from sources[i] if needed. Returns false if
// exhausted or errored; on error sets m.err.
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
	// Linear scan; switch to a heap if profiling shows hot. Source count
	// is bounded by RF × number of replication sets (distributor) or
	// store-gateway shard size (blocks-store path).
	bestIdx := -1
	for i := range m.sources {
		if !m.refreshHead(i) {
			if m.err != nil {
				return false
			}
			continue
		}
		if bestIdx < 0 || m.compare(m.heads[i], m.heads[bestIdx]) < 0 {
			bestIdx = i
		}
	}
	if bestIdx < 0 {
		m.done = true
		return false
	}
	m.cur = m.heads[bestIdx]
	m.hasHead[bestIdx] = false
	// Cross-source dedup: advance any other source whose head matches the
	// just-emitted Value. Equal-Value duplicates are guaranteed at the
	// heads because each source is pre-sorted in the same OrderBy
	// (Searcher contract).
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

// makeSearchResultComparator binds the ordering once at construction.
// OrderByScoreDesc breaks ties on ascending Value, matching Prometheus's
// compareSearchResults.
func makeSearchResultComparator(order storage.Ordering) searchResultComparator {
	switch order {
	case storage.OrderByValueDesc:
		return func(a, b storage.SearchResult) int {
			return cmp.Compare(b.Value, a.Value)
		}
	case storage.OrderByScoreDesc:
		return func(a, b storage.SearchResult) int {
			if a.Score != b.Score {
				return cmp.Compare(b.Score, a.Score)
			}
			return cmp.Compare(a.Value, b.Value)
		}
	default:
		return func(a, b storage.SearchResult) int {
			return cmp.Compare(a.Value, b.Value)
		}
	}
}
