// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"fmt"
	"slices"
	"strings"

	"github.com/prometheus/prometheus/util/annotations"
)

const (
	// SearchValueSetChannelSize is the buffer size of the internal channel used by SearchValueSet.
	// Kept small to avoid unbounded memory growth in the producer-consumer pipeline.
	SearchValueSetChannelSize = 100
)

// SearchValueSet is a streaming iterator backed by a producer goroutine.
//
// The producer goroutine writes SearchResult values to ch, then closes ch after
// setting warnings and err. Channel close establishes happens-before, so those
// fields are safe to read after ch is fully drained.
//
// sortBy/sortOrder match the MimirSearchHints encoding: 0=none, 1=alpha, 2=score;
// 0=asc, 1=desc. When sortBy != 0 all values are buffered before sorting (sorted
// path); otherwise values stream out as they arrive (unsorted path).
type SearchValueSet struct {
	ch <-chan SearchResult
	// The record at the current point of the iterator.
	current SearchResult
	// A map to track deduplication of records.
	seen map[string]struct{}
	// Warnings from the producer.
	warnings annotations.Annotations
	// Error from the producer.
	err error
	// Error from within the SearchValueSet itself (e.g. byte limit exceeded during sort).
	vsetErr error

	sortBy    int // 0=none, 1=alpha, 2=score
	sortOrder int // 0=asc, 1=desc
	// Limit on the number of records returned.
	limit int
	// Limit on the max buffered bytes.
	maxBytesLimit int

	// totalBytes tracks the cumulative bytes of de-duplicated values seen on the unsorted path.
	totalBytes int

	// limitHit is set on the unsorted path when the count limit is reached.
	// Warning emission is deferred to Warnings() to avoid racing with the producer.
	limitHit             bool
	limitExceededWarning error

	// done is set on the unsorted path when the count limit is reached.
	done bool

	// sorted path
	sorted     []SearchResult
	sortedPos  int
	sortedInit bool

	useSortedPath bool
}

// NewSearchValueSet starts a producer goroutine and returns a SearchValueSet.
// produce writes SearchResult values to ch; ch is closed after produce returns (warnings and err set first).
// sortBy/sortOrder follow MimirSearchHints encoding: 0=none/1=alpha/2=score and 0=asc/1=desc.
// limitExceededWarning, if non-nil, is emitted as a warning when the count limit is reached.
func NewSearchValueSet(
	produce func(ch chan<- SearchResult) (annotations.Annotations, error),
	sortBy, sortOrder, limit, maxBytesLimit int,
	limitExceededWarning error,
) *SearchValueSet {
	ch := make(chan SearchResult, SearchValueSetChannelSize)
	vset := &SearchValueSet{
		ch:                   ch,
		limit:                limit,
		maxBytesLimit:        maxBytesLimit,
		seen:                 make(map[string]struct{}),
		sortBy:               sortBy,
		sortOrder:            sortOrder,
		limitExceededWarning: limitExceededWarning,
		useSortedPath:        sortBy != 0,
	}
	go func() {
		defer close(ch)
		vset.warnings, vset.err = produce(ch)
	}()
	return vset
}

// Next advances the iterator. Returns false when exhausted.
func (s *SearchValueSet) Next() bool {
	if s.useSortedPath {
		return s.nextSorted()
	}
	return s.nextUnsorted()
}

// nextUnsorted reads the next de-duplicated record from the producer channel.
// Returns false if the count limit has been reached or the channel is exhausted.
func (s *SearchValueSet) nextUnsorted() bool {
	if s.done {
		return false
	}
	for r := range s.ch {
		if _, dup := s.seen[r.Value]; dup {
			continue
		}
		s.totalBytes += len(r.Value)
		if s.maxBytesLimit > 0 && s.totalBytes > s.maxBytesLimit {
			s.vsetErr = fmt.Errorf("buffer exceeded max size of %d bytes", s.maxBytesLimit)
			s.drain()
			return false
		}
		s.seen[r.Value] = struct{}{}
		s.current = SearchResult{Value: strings.Clone(r.Value), Score: r.Score}
		if s.limit > 0 && len(s.seen) >= s.limit {
			s.limitHit = true
			s.done = true
		}
		return true
	}
	return false
}

// nextSorted buffers all de-duplicated records from the producer, then sorts and limits the results.
// Once sorted and limited, the iterator advances through the sorted slice.
func (s *SearchValueSet) nextSorted() bool {
	if !s.sortedInit {
		s.drainAndSort()
		s.sortedInit = true
	}
	if s.vsetErr != nil || s.err != nil || s.sortedPos >= len(s.sorted) {
		return false
	}
	s.current = s.sorted[s.sortedPos]
	s.sortedPos++
	return true
}

// drainAndSort drains ch, deduplicates, checks the byte limit cumulatively,
// sorts, and limits into s.sorted. If the byte limit is exceeded s.vsetErr is
// set and s.sorted is left empty.
func (s *SearchValueSet) drainAndSort() {
	all := make([]SearchResult, 0)
	totalBytes := 0
	for r := range s.ch {
		if _, dup := s.seen[r.Value]; dup {
			continue
		}
		totalBytes += len(r.Value)
		if s.maxBytesLimit > 0 && totalBytes > s.maxBytesLimit {
			s.vsetErr = fmt.Errorf("buffer exceeded max size of %d bytes", s.maxBytesLimit)
			s.drain()
			return
		}
		s.seen[r.Value] = struct{}{}
		all = append(all, SearchResult{Value: strings.Clone(r.Value), Score: r.Score})
	}

	sortFilteredResultsInline(all, s.sortBy, s.sortOrder)
	if s.limit > 0 && len(all) > s.limit {
		all = all[:s.limit]
	}
	s.sorted = all
}

// At returns the current item. Valid only after a successful call to Next.
func (s *SearchValueSet) At() SearchResult {
	return s.current
}

// Warnings returns producer warnings. It drains ch first so the producer goroutine has
// finished and its write to s.warnings is complete before we read or modify it.
// Any count-limit warning is merged here rather than inside nextUnsorted to avoid
// racing with the producer goroutine's concurrent write to s.warnings.
func (s *SearchValueSet) Warnings() annotations.Annotations {
	s.drain()
	if s.limitHit && s.limitExceededWarning != nil {
		(&s.warnings).Add(s.limitExceededWarning)
		s.limitHit = false
	}
	return s.warnings
}

// Err returns any error from the value set itself or from the producer.
// It drains ch first to ensure the producer has finished.
func (s *SearchValueSet) Err() error {
	s.drain()
	if s.vsetErr != nil {
		return s.vsetErr
	}
	return s.err
}

// Close drains ch so the producer goroutine can finish.
func (s *SearchValueSet) Close() error {
	s.drain()
	return nil
}

func (s *SearchValueSet) drain() {
	for range s.ch { //nolint:revive
	}
}

// LimitReached reports whether the count limit was reached during unsorted iteration.
func (s *SearchValueSet) LimitReached() bool {
	return s.limitHit
}

func sortFilteredResultsInline(values []SearchResult, sortBy, sortOrder int) {
	if sortBy == 0 {
		return
	}
	slices.SortFunc(values, SearchCompareFuncByInts(sortBy, sortOrder))
}
