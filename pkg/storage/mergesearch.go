// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"cmp"
	"errors"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// PairwiseMergeSearchSets merges a slice of pre-ordered storage.SearchResultSets
// into a single streaming SearchResultSet, deduplicating by Value across sources
// and respecting the requested ordering. Each input must emit results in the
// order requested.
//
// This mirrors the behaviour of Prometheus's internal pairwiseMergeSearchSets
// (vendor/github.com/prometheus/prometheus/storage/generic.go), which is
// package-private upstream. The implementation here is a verbatim port — the
// algorithm is well-defined and copying it lets us share the same semantic
// contract across Mimir components that fan out to multiple Searcher sources
// (the store-gateway's per-block fan-out today; cross-replica fan-out in
// follow-up work).
//
// When limit > 0, the merge stops after emitting limit results across the
// whole tree, enabling early termination that avoids consuming the full input
// from child nodes.
func PairwiseMergeSearchSets(sets []storage.SearchResultSet, order storage.Ordering, limit int) storage.SearchResultSet {
	switch len(sets) {
	case 0:
		return storage.EmptySearchResultSet()
	case 1:
		if limit > 0 {
			return &limitSearchResultSet{rs: sets[0], limit: limit}
		}
		return sets[0]
	default:
		mid := len(sets) / 2
		left := PairwiseMergeSearchSets(sets[:mid], order, limit)
		right := PairwiseMergeSearchSets(sets[mid:], order, limit)
		return newMergingSearchResultSet(left, right, order, limit)
	}
}

// compareSearchResults returns the total-order comparison function for the
// given Ordering. For OrderByValueAsc and OrderByValueDesc the order is on
// Value alone. For OrderByScoreDesc the order is (Score desc, Value asc),
// which is a total order and defines the position at which a duplicate Value
// is first emitted by the streaming merge.
func compareSearchResults(o storage.Ordering) func(a, b storage.SearchResult) int {
	switch o {
	case storage.OrderByValueDesc:
		return func(a, b storage.SearchResult) int { return cmp.Compare(b.Value, a.Value) }
	case storage.OrderByScoreDesc:
		return func(a, b storage.SearchResult) int {
			if c := cmp.Compare(b.Score, a.Score); c != 0 {
				return c
			}
			return cmp.Compare(a.Value, b.Value)
		}
	default:
		return func(a, b storage.SearchResult) int { return cmp.Compare(a.Value, b.Value) }
	}
}

// limitSearchResultSet wraps a SearchResultSet and stops after limit results.
type limitSearchResultSet struct {
	rs      storage.SearchResultSet
	limit   int
	emitted int
}

func (s *limitSearchResultSet) Next() bool {
	if s.limit > 0 && s.emitted >= s.limit {
		return false
	}
	if s.rs.Next() {
		s.emitted++
		return true
	}
	return false
}

func (s *limitSearchResultSet) At() storage.SearchResult           { return s.rs.At() }
func (s *limitSearchResultSet) Warnings() annotations.Annotations  { return s.rs.Warnings() }
func (s *limitSearchResultSet) Err() error                         { return s.rs.Err() }
func (s *limitSearchResultSet) Close() error                       { return s.rs.Close() }

// mergingSearchResultSet lazily merges two pre-sorted SearchResultSets using
// the comparison function defined by order. Both inputs must yield results in
// that order. Equal entries (same Value under value orderings, same
// (Score, Value) under OrderByScoreDesc) collapse in place; under value
// orderings the higher score wins.
type mergingSearchResultSet struct {
	a, b         storage.SearchResultSet
	cmpFn        func(a, b storage.SearchResult) int
	valueOrder   bool // true when order collapses adjacent duplicates by Value.
	limit        int
	emitted      int
	curr         storage.SearchResult
	aVal, bVal   storage.SearchResult
	aOk, bOk     bool // Whether aVal/bVal hold a buffered value.
	aInit, bInit bool // Whether a/b have been advanced at least once.
	done         bool
}

func newMergingSearchResultSet(a, b storage.SearchResultSet, order storage.Ordering, limit int) *mergingSearchResultSet {
	return &mergingSearchResultSet{
		a:          a,
		b:          b,
		cmpFn:      compareSearchResults(order),
		valueOrder: order == storage.OrderByValueAsc || order == storage.OrderByValueDesc,
		limit:      limit,
	}
}

func (s *mergingSearchResultSet) Next() bool {
	if s.done {
		return false
	}
	if s.limit > 0 && s.emitted >= s.limit {
		s.done = true
		return false
	}

	// Prime both sides on first call.
	if !s.aInit {
		s.aOk = s.a.Next()
		if s.aOk {
			s.aVal = s.a.At()
		}
		s.aInit = true
	}
	if !s.bInit {
		s.bOk = s.b.Next()
		if s.bOk {
			s.bVal = s.b.At()
		}
		s.bInit = true
	}

	// Check for errors from either side after priming or after the previous
	// advance. An error means we should stop iteration.
	if s.a.Err() != nil || s.b.Err() != nil {
		s.done = true
		return false
	}

	switch {
	case !s.aOk && !s.bOk:
		s.done = true
		return false
	case !s.aOk:
		s.curr = s.bVal
		s.bOk = s.b.Next()
		if s.bOk {
			s.bVal = s.b.At()
		}
	case !s.bOk:
		s.curr = s.aVal
		s.aOk = s.a.Next()
		if s.aOk {
			s.aVal = s.a.At()
		}
	default:
		// Under value-based orderings, equal-Value entries collapse in
		// place and keep the higher score. Under OrderByScoreDesc the
		// comparator tie-breaks on Value, so equal cmp means equal
		// (Score, Value) — collapsing is safe there too.
		c := s.cmpFn(s.aVal, s.bVal)
		switch {
		case c < 0:
			s.curr = s.aVal
			s.aOk = s.a.Next()
			if s.aOk {
				s.aVal = s.a.At()
			}
		case c > 0:
			s.curr = s.bVal
			s.bOk = s.b.Next()
			if s.bOk {
				s.bVal = s.b.At()
			}
		default:
			if s.valueOrder && s.bVal.Score > s.aVal.Score {
				s.curr = s.bVal
			} else {
				s.curr = s.aVal
			}
			s.aOk = s.a.Next()
			if s.aOk {
				s.aVal = s.a.At()
			}
			s.bOk = s.b.Next()
			if s.bOk {
				s.bVal = s.b.At()
			}
		}
	}

	s.emitted++
	return true
}

func (s *mergingSearchResultSet) At() storage.SearchResult { return s.curr }

func (s *mergingSearchResultSet) Warnings() annotations.Annotations {
	var ws annotations.Annotations
	ws.Merge(s.a.Warnings())
	ws.Merge(s.b.Warnings())
	return ws
}

func (s *mergingSearchResultSet) Err() error {
	return errors.Join(s.a.Err(), s.b.Err())
}

func (s *mergingSearchResultSet) Close() error {
	return errors.Join(s.a.Close(), s.b.Close())
}
