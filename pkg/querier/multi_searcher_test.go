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
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

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

// fanOutSearcher is a simple Searcher that returns a fixed list of values.
type fanOutSearcher struct {
	names          []string
	values         []string
	err            error
	capturedHints  []*mimirstorage.SearchHints
}

func (m *fanOutSearcher) SearchLabelNames(_ context.Context, hints *mimirstorage.SearchHints, _ ...*labels.Matcher) (mimirstorage.SearcherValueSet, error) {
	m.capturedHints = append(m.capturedHints, hints)
	if m.err != nil {
		return nil, m.err
	}
	return &fixedValueSet{items: m.names}, nil
}

func (m *fanOutSearcher) SearchLabelValues(_ context.Context, _ string, hints *mimirstorage.SearchHints, _ ...*labels.Matcher) (mimirstorage.SearcherValueSet, error) {
	m.capturedHints = append(m.capturedHints, hints)
	if m.err != nil {
		return nil, m.err
	}
	return &fixedValueSet{items: m.values}, nil
}

func TestFanOutSearch_Dedup(t *testing.T) {
	s1 := &fanOutSearcher{names: []string{"a", "b", "c"}}
	s2 := &fanOutSearcher{names: []string{"b", "c", "d"}}

	vs, err := fanOutSearch(context.Background(), nil, []mimirstorage.Searcher{s1, s2},
		func(ctx context.Context, s mimirstorage.Searcher, h *mimirstorage.SearchHints) (mimirstorage.SearcherValueSet, error) {
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
	chain := streaminglabelvalues.NewFilterChains(false)
	c := streaminglabelvalues.NewFilterChain(streaminglabelvalues.Or, 1)
	c.AddFilter(streaminglabelvalues.NewFilterContains("job"))
	chain.AddFilterChain(c)

	hints := &mimirstorage.SearchHints{
		Filter:  chain,
		Compare: reverseAlphaComparator{},
		Limit:   5,
	}

	s1 := &fanOutSearcher{names: []string{"job", "namespace"}}

	vs, err := fanOutSearch(context.Background(), hints, []mimirstorage.Searcher{s1},
		func(ctx context.Context, s mimirstorage.Searcher, h *mimirstorage.SearchHints) (mimirstorage.SearcherValueSet, error) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	require.NoError(t, err)
	vs.Close()

	require.Len(t, s1.capturedHints, 1)
	subHints := s1.capturedHints[0]
	require.NotNil(t, subHints)
	// Filter is passed to sub-Searchers; Compare and Limit are stripped.
	assert.NotNil(t, subHints.Filter)
	assert.Nil(t, subHints.Compare)
	assert.Equal(t, 0, subHints.Limit)
}

func TestFanOutSearch_ComparatorSortsAcrossSearchers(t *testing.T) {
	s1 := &fanOutSearcher{names: []string{"a", "c"}}
	s2 := &fanOutSearcher{names: []string{"b", "d"}}

	hints := &mimirstorage.SearchHints{Compare: reverseAlphaComparator{}}

	vs, err := fanOutSearch(context.Background(), hints, []mimirstorage.Searcher{s1, s2},
		func(ctx context.Context, s mimirstorage.Searcher, h *mimirstorage.SearchHints) (mimirstorage.SearcherValueSet, error) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	require.NoError(t, err)
	defer vs.Close()

	got, err := drainValueSet(vs)
	require.NoError(t, err)
	// reverseAlphaComparator sorts z→a, so expected: d, c, b, a
	assert.Equal(t, []string{"d", "c", "b", "a"}, got)
}

func TestFanOutSearch_LimitAfterSort(t *testing.T) {
	s1 := &fanOutSearcher{names: []string{"a", "c"}}
	s2 := &fanOutSearcher{names: []string{"b", "d"}}

	hints := &mimirstorage.SearchHints{Compare: reverseAlphaComparator{}, Limit: 2}

	vs, err := fanOutSearch(context.Background(), hints, []mimirstorage.Searcher{s1, s2},
		func(ctx context.Context, s mimirstorage.Searcher, h *mimirstorage.SearchHints) (mimirstorage.SearcherValueSet, error) {
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

	hints := &mimirstorage.SearchHints{Limit: 3}

	vs, err := fanOutSearch(context.Background(), hints, []mimirstorage.Searcher{s1, s2},
		func(ctx context.Context, s mimirstorage.Searcher, h *mimirstorage.SearchHints) (mimirstorage.SearcherValueSet, error) {
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

	vs, err := fanOutSearch(context.Background(), nil, []mimirstorage.Searcher{s1, s2},
		func(ctx context.Context, s mimirstorage.Searcher, h *mimirstorage.SearchHints) (mimirstorage.SearcherValueSet, error) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	require.NoError(t, err)
	defer vs.Close()

	_, err = drainValueSet(vs)
	assert.ErrorContains(t, err, "searcher failed")
}

func TestFanOutSearch_EmptySearchers(t *testing.T) {
	vs, err := fanOutSearch(context.Background(), nil, nil,
		func(ctx context.Context, s mimirstorage.Searcher, h *mimirstorage.SearchHints) (mimirstorage.SearcherValueSet, error) {
			return s.SearchLabelNames(ctx, h)
		},
	)
	require.NoError(t, err)
	defer vs.Close()

	got, err := drainValueSet(vs)
	require.NoError(t, err)
	assert.Empty(t, got)
}
