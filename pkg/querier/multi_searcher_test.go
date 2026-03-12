// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
)

var _ mimirstorage.MimirSearcher = (*fanOutSearcher)(nil)

// fixedValueSet is a SearcherValueSet backed by a fixed slice of strings.
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

func (s *fixedValueSet) At() mimirstorage.FilteredResult {
	return mimirstorage.FilteredResult{Value: s.items[s.pos-1], Score: -1}
}

func (s *fixedValueSet) Warnings() annotations.Annotations { return nil }
func (s *fixedValueSet) Err() error                        { return nil }
func (s *fixedValueSet) Close()                            {}

// fanOutSearcher is a simple MimirSearcher that returns a fixed list of values.
type fanOutSearcher struct {
	names         []string
	values        []string
	err           error
	capturedHints []*mimirstorage.MimirSearchHints
}

func (m *fanOutSearcher) SearchLabelNames(_ context.Context, hints *mimirstorage.MimirSearchHints, _ ...*labels.Matcher) (mimirstorage.SearcherValueSet, annotations.Annotations, error) {
	m.capturedHints = append(m.capturedHints, hints)
	if m.err != nil {
		return nil, nil, m.err
	}
	return &fixedValueSet{items: m.names}, nil, nil
}

func (m *fanOutSearcher) SearchLabelValues(_ context.Context, _ string, hints *mimirstorage.MimirSearchHints, _ ...*labels.Matcher) (mimirstorage.SearcherValueSet, annotations.Annotations, error) {
	m.capturedHints = append(m.capturedHints, hints)
	if m.err != nil {
		return nil, nil, m.err
	}
	return &fixedValueSet{items: m.values}, nil, nil
}

func TestFanOutSearch_Dedup(t *testing.T) {
	s1 := &fanOutSearcher{names: []string{"a", "b", "c"}}
	s2 := &fanOutSearcher{names: []string{"b", "c", "d"}}

	vs, _, err := fanOutSearch(context.Background(), nil, []mimirstorage.MimirSearcher{s1, s2},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearcherValueSet, annotations.Annotations, error) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	require.NoError(t, err)
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

	vs, _, err := fanOutSearch(context.Background(), hints, []mimirstorage.MimirSearcher{s1},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearcherValueSet, annotations.Annotations, error) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	require.NoError(t, err)
	vs.Close()

	require.Len(t, s1.capturedHints, 1)
	subHints := s1.capturedHints[0]
	require.NotNil(t, subHints)
	// Full hints are pushed to sub-Searchers so they can return pre-sorted results for k-way merge.
	assert.NotEmpty(t, subHints.Search)
	assert.Equal(t, 1, subHints.SortBy)
	assert.Equal(t, 5, subHints.Limit)
}

func TestFanOutSearch_ComparatorSortsAcrossSearchers(t *testing.T) {
	// Sub-searchers return values pre-sorted in alpha-desc order (as they would after receiving hints).
	s1 := &fanOutSearcher{names: []string{"c", "a"}}
	s2 := &fanOutSearcher{names: []string{"d", "b"}}

	hints := &mimirstorage.MimirSearchHints{SortBy: 1, SortOrder: 1} // alpha desc

	vs, _, err := fanOutSearch(context.Background(), hints, []mimirstorage.MimirSearcher{s1, s2},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearcherValueSet, annotations.Annotations, error) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	require.NoError(t, err)
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

	vs, _, err := fanOutSearch(context.Background(), hints, []mimirstorage.MimirSearcher{s1, s2},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearcherValueSet, annotations.Annotations, error) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	require.NoError(t, err)
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

	vs, _, err := fanOutSearch(context.Background(), hints, []mimirstorage.MimirSearcher{s1, s2},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearcherValueSet, annotations.Annotations, error) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	require.NoError(t, err)
	defer vs.Close()

	got, err := drainValueSet(vs)
	require.NoError(t, err)
	// Exactly 3 values returned (which 3 depends on goroutine scheduling, but count is enforced).
	assert.Len(t, got, 3)
}

func TestFanOutSearch_ErrorPropagates(t *testing.T) {
	s1 := &fanOutSearcher{names: []string{"a", "b"}}
	s2 := &fanOutSearcher{err: errors.New("searcher failed")}

	vs, _, err := fanOutSearch(context.Background(), nil, []mimirstorage.MimirSearcher{s1, s2},
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearcherValueSet, annotations.Annotations, error) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	require.NoError(t, err)
	defer vs.Close()

	_, err = drainValueSet(vs)
	assert.ErrorContains(t, err, "searcher failed")
}

func TestFanOutSearch_EmptySearchers(t *testing.T) {
	vs, _, err := fanOutSearch(context.Background(), nil, nil,
		func(ctx context.Context, s mimirstorage.MimirSearcher, h *mimirstorage.MimirSearchHints) (mimirstorage.SearcherValueSet, annotations.Annotations, error) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	require.NoError(t, err)
	defer vs.Close()

	got, err := drainValueSet(vs)
	require.NoError(t, err)
	assert.Empty(t, got)
}
