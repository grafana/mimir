// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// ingesterSearcherValueSet implements a streaming iterator backed by a producer goroutine.
//
// The producer goroutine writes string values to ch, then closes ch after setting
// warnings and err. Channel close establishes happens-before, so those fields are
// safe to read after ch is fully drained.
type ingesterSearcherValueSet struct {
	ch <-chan string
	// The record at the current point of the iterator
	current client.SearchLabelValuesResult
	// A map to track deduplication of records
	seen map[string]struct{}
	// Warnings from the producer
	warnings annotations.Annotations
	// Error from the producer
	err error
	// error from within the ingesterSearcherValueSet itself (e.g. byte limit exceeded during sort)
	vsetErr error

	sortBy    client.SortBy
	sortOrder client.SortOrder
	// Limit on the number of records returned
	limit int
	// Limit on the max buffered bytes
	maxBytesLimit int

	// done is set on the unsorted path when the count limit is reached.
	done bool

	// sorted path
	sorted     []client.SearchLabelValuesResult
	sortedPos  int
	sortedInit bool

	useSortedPath bool
}

// newingesterSearcherValueSet starts a producer goroutine and returns an ingesterSearcherValueSet.
// produce writes values to ch; ch is closed after produce returns (warnings and err set first).
// limitExceededWarning, if non-nil, is emitted as a warning when the count limit is reached.
func newingesterSearcherValueSet(
	produce func(ch chan<- string) (annotations.Annotations, error),
	sf *client.SearchLabelValuesFilter,
	limit, maxBytesLimit int,
) *ingesterSearcherValueSet {
	ch := make(chan string, internalChannelSize)
	vset := &ingesterSearcherValueSet{
		ch:            ch,
		limit:         limit,
		maxBytesLimit: maxBytesLimit,
		seen:          make(map[string]struct{}),
	}
	if sf != nil {
		vset.sortBy = sf.SortBy
		vset.sortOrder = sf.SortOrder
		vset.useSortedPath = true
	} else {
		vset.sortBy = client.SORT_BY_NONE
	}
	go func() {
		defer close(ch)
		vset.warnings, vset.err = produce(ch)
	}()
	return vset
}

// Next advances the iterator. Returns false when exhausted.
func (s *ingesterSearcherValueSet) Next() bool {
	if s.useSortedPath {
		return s.nextSorted()
	}
	return s.nextUnsorted()
}

// nextUnsorted reads the next de-duplicated record from the producer channel.
// Returns false if the count limit has been reached or the channel is exhausted.
func (s *ingesterSearcherValueSet) nextUnsorted() bool {
	if s.done {
		return false
	}
	for v := range s.ch {
		if _, dup := s.seen[v]; dup {
			continue
		}
		s.seen[v] = struct{}{}
		s.current = client.SearchLabelValuesResult{Value: strings.Clone(v)}
		if s.limit > 0 && len(s.seen) >= s.limit {
			s.done = true
		}
		return true
	}
	return false
}

// nextSorted buffers all de-duplicated records from the producer, then sorts and limits the results.
// Once sorted and limited, the iterator advances through the sorted slice.
func (s *ingesterSearcherValueSet) nextSorted() bool {
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
func (s *ingesterSearcherValueSet) drainAndSort() {
	all := make([]client.SearchLabelValuesResult, 0)
	totalBytes := 0
	score := 0.0
	for v := range s.ch {
		if _, dup := s.seen[v]; dup {
			continue
		}
		totalBytes += len(v)
		if s.maxBytesLimit > 0 && totalBytes > s.maxBytesLimit {
			s.vsetErr = fmt.Errorf("buffer exceeded max size of %d bytes", s.maxBytesLimit)
			s.drain()
			return
		}
		s.seen[v] = struct{}{}

		score = 0.0
		if s.sortBy == client.SORT_BY_SCORE {
			// TODO: compute real score from searchFilter once it is plumbed into the valueset
			score = -1.0
		}

		all = append(all, client.SearchLabelValuesResult{Value: strings.Clone(v), Score: score})
	}

	sortResultsInline(all, s.sortBy, s.sortOrder)
	if s.limit > 0 && len(all) > s.limit {
		all = all[:s.limit]
	}
	s.sorted = all
}

// At returns the current item. Valid only after a successful call to Next.
func (s *ingesterSearcherValueSet) At() client.SearchLabelValuesResult {
	return s.current
}

// Warnings returns producer warnings. It drains ch first so the producer goroutine has
// finished and its write to s.warnings is complete before we read or modify it.
// Any count-limit warning is merged here rather than inside nextUnsorted to avoid
// racing with the producer goroutine's concurrent write to s.warnings.
func (s *ingesterSearcherValueSet) Warnings() annotations.Annotations {
	s.drain()
	return s.warnings
}

// Err returns any error from the value set itself or from the producer.
// It drains ch first to ensure the producer has finished.
func (s *ingesterSearcherValueSet) Err() error {
	s.drain()
	if s.vsetErr != nil {
		return s.vsetErr
	}
	return s.err
}

// Close drains ch so the producer goroutine can finish.
func (s *ingesterSearcherValueSet) Close() {
	s.drain()
}

func (s *ingesterSearcherValueSet) drain() {
	for range s.ch { //nolint:revive
	}
}

func sortResultsInline(values []client.SearchLabelValuesResult, sortBy client.SortBy, sortOrder client.SortOrder) {
	if len(values) == 0 {
		return
	}

	var sorter sort.Interface
	switch sortBy {
	case client.SORT_BY_NONE:
		return
	case client.SORT_BY_ALPHA:
		sorter = alphaSort{values: values, ascending: sortOrder == client.SORT_ORDER_ASC}
	case client.SORT_BY_SCORE:
		sorter = scoreSort{values: values, ascending: sortOrder == client.SORT_ORDER_ASC}
	}

	sort.Sort(sorter)
}

// scoreSort implements sort.Interface to sort by numeric score for client.SearchLabelValuesResult.
type scoreSort struct {
	values    []client.SearchLabelValuesResult
	ascending bool
}

func (s scoreSort) Len() int { return len(s.values) }
func (s scoreSort) Less(i, j int) bool {
	if s.ascending {
		return s.values[i].Score < s.values[j].Score
	}
	return s.values[i].Score > s.values[j].Score
}
func (s scoreSort) Swap(i, j int) {
	s.values[i], s.values[j] = s.values[j], s.values[i]
}

// alphaSort implements sort.Interface to sort alphabetically for client.SearchLabelValuesResult.
type alphaSort struct {
	values    []client.SearchLabelValuesResult
	ascending bool
}

func (s alphaSort) Len() int { return len(s.values) }
func (s alphaSort) Less(i, j int) bool {
	if s.ascending {
		return strings.Compare(s.values[i].Value, s.values[j].Value) < 0
	}
	return strings.Compare(s.values[j].Value, s.values[i].Value) < 0
}
func (s alphaSort) Swap(i, j int) {
	s.values[i], s.values[j] = s.values[j], s.values[i]
}
