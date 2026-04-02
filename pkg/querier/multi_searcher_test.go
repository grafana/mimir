// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
)

var _ mimirstorage.MimirSearcher = (*fanOutSearcher)(nil)

// fixedValueSet is a SearchResultSet backed by a fixed slice of strings.
type fixedValueSet struct {
	items []string
	pos   int
}

func (s *fixedValueSet) Next() bool {
	if s.pos < len(s.items) {
		s.pos++
		return true
	}
	return false
}

func (s *fixedValueSet) At() mimirstorage.SearchResult {
	// Score is always -1 (no score computed). fixedValueSet is unsuitable for
	// score-sort tests; use a custom SearchResultSet that sets meaningful scores.
	return mimirstorage.SearchResult{Value: s.items[s.pos-1], Score: -1}
}

func (s *fixedValueSet) Warnings() annotations.Annotations { return nil }
func (s *fixedValueSet) Err() error                        { return nil }
func (s *fixedValueSet) Close() error                      { return nil }

// fanOutSearcher is a simple MimirSearcher that returns a fixed list of values.
type fanOutSearcher struct {
	names         []string
	values        []string
	err           error
	capturedHints []*mimirstorage.MimirSearchHints
}

func (m *fanOutSearcher) SearchLabelNames(_ context.Context, hints *mimirstorage.MimirSearchHints, _ ...*labels.Matcher) (mimirstorage.SearchResultSet, annotations.Annotations) {
	m.capturedHints = append(m.capturedHints, hints)
	if m.err != nil {
		return mimirstorage.ErrorSearchResultSet(m.err), nil
	}
	return &fixedValueSet{items: m.names}, nil
}

func (m *fanOutSearcher) SearchLabelValues(_ context.Context, _ string, hints *mimirstorage.MimirSearchHints, _ ...*labels.Matcher) (mimirstorage.SearchResultSet, annotations.Annotations) {
	m.capturedHints = append(m.capturedHints, hints)
	if m.err != nil {
		return mimirstorage.ErrorSearchResultSet(m.err), nil
	}
	return &fixedValueSet{items: m.values}, nil
}

func TestFanOutSearch_Dedup(t *testing.T) {
	s1 := &fanOutSearcher{names: []string{"a", "b", "c"}}
	s2 := &fanOutSearcher{names: []string{"b", "c", "d"}}

	vs, _ := fanOutSearch(context.Background(), nil, []mimirstorage.MimirSearcher{s1, s2},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearchResultSet, annotations.Annotations) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	defer vs.Close()

	got, err := drainValueSet(vs)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b", "c", "d"}, got)
}

func TestFanOutSearch_FilterPassedToSubSearchers(t *testing.T) {
	hints := &mimirstorage.MimirSearchHints{
		Search:    []string{"job"},
		SortBy:    1, // alpha
		SortOrder: 1, // desc
		Limit:     5,
	}

	s1 := &fanOutSearcher{names: []string{"job", "namespace"}}

	vs, _ := fanOutSearch(context.Background(), hints, []mimirstorage.MimirSearcher{s1},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearchResultSet, annotations.Annotations) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	vs.Close()

	require.Len(t, s1.capturedHints, 1)
	subHints := s1.capturedHints[0]
	require.NotNil(t, subHints)
	// Full hints are pushed to sub-Searchers so they can return pre-sorted results for k-way merge.
	assert.NotEmpty(t, subHints.Search)
	assert.Equal(t, mimirstorage.Alpha, subHints.SortBy)
	assert.Equal(t, 5, subHints.Limit)
}

func TestFanOutSearch_ComparatorSortsAcrossSearchers(t *testing.T) {
	// Sub-searchers return values pre-sorted in alpha-desc order, mirroring the
	// contract that fanOutSearch enforces: full hints (including SortBy/SortOrder)
	// are forwarded to every sub-Searcher so their streams arrive pre-sorted for
	// KWayMergeValueSets. The test data is hand-crafted to already satisfy this
	// invariant (s1: c→a, s2: d→b, both descending alpha).
	s1 := &fanOutSearcher{names: []string{"c", "a"}}
	s2 := &fanOutSearcher{names: []string{"d", "b"}}

	hints := &mimirstorage.MimirSearchHints{SortBy: 1, SortOrder: 1} // alpha desc

	vs, _ := fanOutSearch(context.Background(), hints, []mimirstorage.MimirSearcher{s1, s2},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearchResultSet, annotations.Annotations) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	defer vs.Close()

	got, err := drainValueSet(vs)
	require.NoError(t, err)
	// alpha-desc sorts z→a, so expected: d, c, b, a
	assert.Equal(t, []string{"d", "c", "b", "a"}, got)
}

func TestFanOutSearch_LimitAfterSort(t *testing.T) {
	// Sub-searchers return values pre-sorted in alpha-desc order (as they would after receiving hints).
	s1 := &fanOutSearcher{names: []string{"c", "a"}}
	s2 := &fanOutSearcher{names: []string{"d", "b"}}

	hints := &mimirstorage.MimirSearchHints{SortBy: 1, SortOrder: 1, Limit: 2} // alpha desc, limit 2

	vs, _ := fanOutSearch(context.Background(), hints, []mimirstorage.MimirSearcher{s1, s2},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearchResultSet, annotations.Annotations) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	defer vs.Close()

	got, err := drainValueSet(vs)
	require.NoError(t, err)
	// After global sort (d,c,b,a) and limit 2: [d, c]
	assert.Equal(t, []string{"d", "c"}, got)
}

func TestFanOutSearch_LimitWithoutSort(t *testing.T) {
	s1 := &fanOutSearcher{names: []string{"a", "b", "c"}}
	s2 := &fanOutSearcher{names: []string{"d", "e", "f"}}

	hints := &mimirstorage.MimirSearchHints{Limit: 3}

	vs, _ := fanOutSearch(context.Background(), hints, []mimirstorage.MimirSearcher{s1, s2},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearchResultSet, annotations.Annotations) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	defer vs.Close()

	got, err := drainValueSet(vs)
	require.NoError(t, err)
	// Exactly 3 values returned (which 3 depends on goroutine scheduling, but count is enforced).
	assert.Len(t, got, 3)

	type limitReacher interface{ LimitReached() bool }
	lr, ok := vs.(limitReacher)
	require.True(t, ok, "expected SearchResultSet to implement LimitReached()")
	assert.True(t, lr.LimitReached(), "expected LimitReached() to be true when limit was hit")
}

func TestFanOutSearch_ErrorPropagates(t *testing.T) {
	s1 := &fanOutSearcher{names: []string{"a", "b"}}
	s2 := &fanOutSearcher{err: errors.New("searcher failed")}

	vs, _ := fanOutSearch(context.Background(), nil, []mimirstorage.MimirSearcher{s1, s2},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearchResultSet, annotations.Annotations) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	defer vs.Close()

	_, err := drainValueSet(vs)
	assert.ErrorContains(t, err, "searcher failed")
}

func TestFanOutSearch_EmptySearchers(t *testing.T) {
	vs, _ := fanOutSearch(context.Background(), nil, nil,
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearchResultSet, annotations.Annotations) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	defer vs.Close()

	got, err := drainValueSet(vs)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestFanOutSearch_DedupSorted(t *testing.T) {
	// Both sub-searchers return results pre-sorted alpha-desc (as required by KWayMerge).
	// "b" appears in both; the merge must deduplicate it to a single occurrence.
	s1 := &fanOutSearcher{names: []string{"c", "b"}}
	s2 := &fanOutSearcher{names: []string{"d", "b"}}

	hints := &mimirstorage.MimirSearchHints{SortBy: mimirstorage.Alpha, SortOrder: mimirstorage.Desc}

	vs, _ := fanOutSearch(context.Background(), hints, []mimirstorage.MimirSearcher{s1, s2},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearchResultSet, annotations.Annotations) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	defer vs.Close()

	got, err := drainValueSet(vs)
	require.NoError(t, err)
	// alpha-desc: d→c→b (deduped)
	assert.Equal(t, []string{"d", "c", "b"}, got)
}

func TestFanOutSearch_ErrorPropagatesSorted(t *testing.T) {
	// Error from a sub-searcher must propagate through the sorted merge path.
	s1 := &fanOutSearcher{names: []string{"a", "b"}}
	s2 := &fanOutSearcher{err: errors.New("searcher failed")}

	hints := &mimirstorage.MimirSearchHints{SortBy: mimirstorage.Alpha, SortOrder: mimirstorage.Asc}

	vs, _ := fanOutSearch(context.Background(), hints, []mimirstorage.MimirSearcher{s1, s2},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearchResultSet, annotations.Annotations) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	defer vs.Close()

	_, err := drainValueSet(vs)
	assert.ErrorContains(t, err, "searcher failed")
}

// channelValueSet is a SearchResultSet backed by a channel. Items can be pushed
// via send() and the set can be closed via close(). Used to test that Close()
// on the outer stream unblocks a merge goroutine that is waiting for items.
type channelValueSet struct {
	ch  chan mimirstorage.SearchResult
	cur mimirstorage.SearchResult
	ctx context.Context
}

func newChannelValueSet(ctx context.Context) *channelValueSet {
	return &channelValueSet{ch: make(chan mimirstorage.SearchResult), ctx: ctx}
}

func (s *channelValueSet) send(v string) { s.ch <- mimirstorage.SearchResult{Value: v} }
func (s *channelValueSet) close()        { close(s.ch) }

func (s *channelValueSet) Next() bool {
	select {
	case v, ok := <-s.ch:
		if !ok {
			return false
		}
		s.cur = v
		return true
	case <-s.ctx.Done():
		return false
	}
}
func (s *channelValueSet) At() mimirstorage.SearchResult     { return s.cur }
func (s *channelValueSet) Warnings() annotations.Annotations { return nil }
func (s *channelValueSet) Err() error                        { return s.ctx.Err() }
func (s *channelValueSet) Close() error                      { return nil }

type channelBackedSearcher struct {
	vs *channelValueSet
}

func (s *channelBackedSearcher) SearchLabelNames(ctx context.Context, _ *mimirstorage.MimirSearchHints, _ ...*labels.Matcher) (mimirstorage.SearchResultSet, annotations.Annotations) {
	s.vs.ctx = ctx // capture the derived context so Next() unblocks when the outer stream is closed
	return s.vs, nil
}
func (s *channelBackedSearcher) SearchLabelValues(ctx context.Context, _ string, _ *mimirstorage.MimirSearchHints, _ ...*labels.Matcher) (mimirstorage.SearchResultSet, annotations.Annotations) {
	s.vs.ctx = ctx
	return s.vs, nil
}

func TestFanOutSearch_CloseUnblocksSortedMerge(t *testing.T) {
	// Verify that calling Close() on the outer stream unblocks and terminates
	// a merge goroutine that is blocked waiting for the next item from a sub-Searcher.
	ctx := context.Background()
	inner := newChannelValueSet(ctx)
	searcher := &channelBackedSearcher{vs: inner}

	hints := &mimirstorage.MimirSearchHints{SortBy: mimirstorage.Alpha, SortOrder: mimirstorage.Asc}
	vs, _ := fanOutSearch(ctx, hints, []mimirstorage.MimirSearcher{searcher},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearchResultSet, annotations.Annotations) {
			return s.SearchLabelNames(ctx, h)
		},
	)

	// Send one item and read it so the merge goroutine is running.
	inner.send("a")

	// Close the outer stream; this should cancel the merge goroutine's context.
	done := make(chan struct{})
	go func() {
		defer close(done)
		vs.Close()
	}()

	select {
	case <-done:
		// Close returned — goroutine unblocked as expected.
	case <-time.After(3 * time.Second):
		t.Fatal("Close() did not return within 3s; merge goroutine appears to be stuck")
	}
}
