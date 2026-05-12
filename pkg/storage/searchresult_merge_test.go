// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"errors"
	"testing"

	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMergingSearchResultSet_Empty(t *testing.T) {
	rs := NewMergingSearchResultSet(nil, nil)
	assert.False(t, rs.Next())
	require.NoError(t, rs.Err())
	require.NoError(t, rs.Close())
}

func TestNewMergingSearchResultSet_SingleSource_PreservesScores(t *testing.T) {
	src := &staticResultSet{results: []storage.SearchResult{
		{Value: "bar", Score: 1.0}, // already sorted asc by Value
		{Value: "foo", Score: 0.7},
	}}
	rs := NewMergingSearchResultSet([]storage.SearchResultSet{src}, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	defer rs.Close()
	got := drainTestResults(rs)
	require.NoError(t, rs.Err())
	assert.Equal(t, []storage.SearchResult{
		{Value: "bar", Score: 1.0},
		{Value: "foo", Score: 0.7},
	}, got)
}

func TestNewMergingSearchResultSet_DedupByValue(t *testing.T) {
	// Per Prometheus's Searcher contract duplicates from different sources
	// carry identical scores; the merger collapses to a single entry.
	a := &staticResultSet{results: []storage.SearchResult{{Value: "alpha", Score: 0.9}, {Value: "beta", Score: 1.0}}}
	b := &staticResultSet{results: []storage.SearchResult{{Value: "alpha", Score: 0.9}, {Value: "beta", Score: 1.0}}}
	rs := NewMergingSearchResultSet([]storage.SearchResultSet{a, b}, nil)
	defer rs.Close()
	got := drainTestResults(rs)
	require.NoError(t, rs.Err())
	assert.Equal(t, []storage.SearchResult{
		{Value: "alpha", Score: 0.9},
		{Value: "beta", Score: 1.0},
	}, got)
}

func TestNewMergingSearchResultSet_OrderByValueAsc(t *testing.T) {
	// Each source individually sorted ascending; merger preserves the order.
	a := &staticResultSet{results: []storage.SearchResult{{Value: "alpha", Score: 0.9}, {Value: "gamma", Score: 0.5}}}
	b := &staticResultSet{results: []storage.SearchResult{{Value: "beta", Score: 0.8}}}
	rs := NewMergingSearchResultSet([]storage.SearchResultSet{a, b}, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	defer rs.Close()
	got := drainTestResults(rs)
	require.NoError(t, rs.Err())
	assert.Equal(t, []storage.SearchResult{
		{Value: "alpha", Score: 0.9},
		{Value: "beta", Score: 0.8},
		{Value: "gamma", Score: 0.5},
	}, got)
}

func TestNewMergingSearchResultSet_OrderByValueDesc(t *testing.T) {
	// Each source individually sorted descending.
	a := &staticResultSet{results: []storage.SearchResult{{Value: "gamma", Score: 0.5}, {Value: "alpha", Score: 0.9}}}
	b := &staticResultSet{results: []storage.SearchResult{{Value: "beta", Score: 0.8}}}
	rs := NewMergingSearchResultSet([]storage.SearchResultSet{a, b}, &storage.SearchHints{OrderBy: storage.OrderByValueDesc})
	defer rs.Close()
	got := drainTestResults(rs)
	require.NoError(t, rs.Err())
	assert.Equal(t, []storage.SearchResult{
		{Value: "gamma", Score: 0.5},
		{Value: "beta", Score: 0.8},
		{Value: "alpha", Score: 0.9},
	}, got)
}

func TestNewMergingSearchResultSet_OrderByScoreDesc_AlphaTiebreak(t *testing.T) {
	// Each source pre-sorted by (score desc, value asc) per the Prometheus
	// contract.
	a := &staticResultSet{results: []storage.SearchResult{
		{Value: "beta", Score: 1.0}, // tie with delta
		{Value: "alpha", Score: 0.9},
	}}
	b := &staticResultSet{results: []storage.SearchResult{
		{Value: "delta", Score: 1.0}, // tie with beta
		{Value: "charlie", Score: 0.5},
	}}
	rs := NewMergingSearchResultSet([]storage.SearchResultSet{a, b}, &storage.SearchHints{OrderBy: storage.OrderByScoreDesc})
	defer rs.Close()
	got := drainTestResults(rs)
	require.NoError(t, rs.Err())
	assert.Equal(t, []storage.SearchResult{
		{Value: "beta", Score: 1.0}, // alpha tiebreak: "beta" before "delta"
		{Value: "delta", Score: 1.0},
		{Value: "alpha", Score: 0.9},
		{Value: "charlie", Score: 0.5},
	}, got)
}

func TestNewMergingSearchResultSet_Limit(t *testing.T) {
	a := &staticResultSet{results: []storage.SearchResult{
		{Value: "a", Score: 1.0},
		{Value: "c", Score: 1.0},
	}}
	b := &staticResultSet{results: []storage.SearchResult{
		{Value: "b", Score: 1.0},
		{Value: "d", Score: 1.0},
	}}
	rs := NewMergingSearchResultSet([]storage.SearchResultSet{a, b}, &storage.SearchHints{OrderBy: storage.OrderByValueAsc, Limit: 2})
	defer rs.Close()
	got := drainTestResults(rs)
	require.NoError(t, rs.Err())
	assert.Equal(t, []storage.SearchResult{
		{Value: "a", Score: 1.0},
		{Value: "b", Score: 1.0},
	}, got)
}

func TestNewMergingSearchResultSet_Limit_StopsEarly_DoesNotDrainSources(t *testing.T) {
	// Verify that limit truncation stops pulling from sources; each
	// source's idx tells us how many results were consumed.
	a := &staticResultSet{results: []storage.SearchResult{
		{Value: "a", Score: 1.0},
		{Value: "c", Score: 1.0},
		{Value: "e", Score: 1.0},
	}}
	b := &staticResultSet{results: []storage.SearchResult{
		{Value: "b", Score: 1.0},
		{Value: "d", Score: 1.0},
		{Value: "f", Score: 1.0},
	}}
	rs := NewMergingSearchResultSet([]storage.SearchResultSet{a, b}, &storage.SearchHints{OrderBy: storage.OrderByValueAsc, Limit: 2})
	got := drainTestResults(rs)
	require.NoError(t, rs.Err())
	require.NoError(t, rs.Close())
	require.Len(t, got, 2)
	// To emit "a" and "b" the merger had to peek the first element of
	// each source. It must NOT have advanced past them. So idx <= 2 for
	// each source (one peek, possibly one more peek to satisfy the limit
	// check on the next iteration).
	assert.LessOrEqual(t, a.idx, 2, "source A drained beyond what was needed")
	assert.LessOrEqual(t, b.idx, 2, "source B drained beyond what was needed")
}

func TestNewMergingSearchResultSet_LimitZeroMeansUnlimited(t *testing.T) {
	a := &staticResultSet{results: []storage.SearchResult{{Value: "a"}, {Value: "b"}, {Value: "c"}}}
	rs := NewMergingSearchResultSet([]storage.SearchResultSet{a}, &storage.SearchHints{Limit: 0})
	defer rs.Close()
	got := drainTestResults(rs)
	require.NoError(t, rs.Err())
	assert.Len(t, got, 3)
}

func TestNewMergingSearchResultSet_NilHints_DefaultsToValueAscNoLimit(t *testing.T) {
	a := &staticResultSet{results: []storage.SearchResult{
		{Value: "alpha", Score: 0.9},
		{Value: "gamma", Score: 0.5},
	}}
	rs := NewMergingSearchResultSet([]storage.SearchResultSet{a}, nil)
	defer rs.Close()
	got := drainTestResults(rs)
	require.NoError(t, rs.Err())
	assert.Equal(t, []storage.SearchResult{
		{Value: "alpha", Score: 0.9},
		{Value: "gamma", Score: 0.5},
	}, got)
}

func TestNewMergingSearchResultSet_PropagatesSourceError(t *testing.T) {
	want := errors.New("source-broke")
	a := &staticResultSet{
		results: []storage.SearchResult{{Value: "a", Score: 1.0}},
		err:     want,
	}
	b := &staticResultSet{results: []storage.SearchResult{{Value: "b", Score: 1.0}}}
	rs := NewMergingSearchResultSet([]storage.SearchResultSet{a, b}, nil)
	defer rs.Close()
	for rs.Next() {
		_ = rs.At()
	}
	assert.ErrorIs(t, rs.Err(), want)
}

func TestNewMergingSearchResultSet_AggregatesWarnings(t *testing.T) {
	a := &staticResultSet{
		results: []storage.SearchResult{{Value: "a", Score: 1.0}},
		warns:   addAnnotation(nil, "warn-a"),
	}
	b := &staticResultSet{
		results: []storage.SearchResult{{Value: "b", Score: 1.0}},
		warns:   addAnnotation(nil, "warn-b"),
	}
	rs := NewMergingSearchResultSet([]storage.SearchResultSet{a, b}, nil)
	defer rs.Close()
	for rs.Next() {
		_ = rs.At()
	}
	require.NoError(t, rs.Err())
	msgs := make([]string, 0, 2)
	for _, w := range rs.Warnings() {
		msgs = append(msgs, w.Error())
	}
	assert.ElementsMatch(t, []string{"warn-a", "warn-b"}, msgs)
}

func TestNewMergingSearchResultSet_CloseCascadesToSources(t *testing.T) {
	a := &staticResultSet{results: []storage.SearchResult{{Value: "a", Score: 1.0}}}
	b := &staticResultSet{results: []storage.SearchResult{{Value: "b", Score: 1.0}}}
	rs := NewMergingSearchResultSet([]storage.SearchResultSet{a, b}, nil)
	require.NoError(t, rs.Close())
	assert.True(t, a.closed, "source a must be closed by the merger")
	assert.True(t, b.closed, "source b must be closed by the merger")
}

func TestNewMergingSearchResultSet_CrossSourceDedupAndOrdering(t *testing.T) {
	// Three replicas with overlapping data — each sorted asc by value.
	r0 := &staticResultSet{results: []storage.SearchResult{
		{Value: "foo", Score: 1.0},
		{Value: "footer", Score: 1.0},
	}}
	r1 := &staticResultSet{results: []storage.SearchResult{
		{Value: "foo", Score: 1.0},
		{Value: "foobar", Score: 1.0},
	}}
	r2 := &staticResultSet{results: []storage.SearchResult{
		{Value: "foobar", Score: 1.0},
	}}
	rs := NewMergingSearchResultSet([]storage.SearchResultSet{r0, r1, r2}, &storage.SearchHints{OrderBy: storage.OrderByValueAsc, Limit: 100})
	defer rs.Close()
	got := drainTestResults(rs)
	require.NoError(t, rs.Err())
	assert.Equal(t, []storage.SearchResult{
		{Value: "foo", Score: 1.0},
		{Value: "foobar", Score: 1.0},
		{Value: "footer", Score: 1.0},
	}, got)
}

// drainTestResults reads to EOF and returns the collected results.
func drainTestResults(rs storage.SearchResultSet) []storage.SearchResult {
	var out []storage.SearchResult
	for rs.Next() {
		out = append(out, rs.At())
	}
	return out
}
