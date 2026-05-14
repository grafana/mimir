// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"errors"
	"testing"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSearchResultSet is a minimal in-memory SearchResultSet for testing
// the merge primitive without depending on a real Searcher.
type fakeSearchResultSet struct {
	results  []storage.SearchResult
	idx      int
	warns    annotations.Annotations
	err      error
	closed   bool
	closeErr error
}

func (f *fakeSearchResultSet) Next() bool {
	if f.err != nil {
		return false
	}
	if f.idx >= len(f.results) {
		return false
	}
	f.idx++
	return true
}

func (f *fakeSearchResultSet) At() storage.SearchResult           { return f.results[f.idx-1] }
func (f *fakeSearchResultSet) Warnings() annotations.Annotations  { return f.warns }
func (f *fakeSearchResultSet) Err() error                         { return f.err }
func (f *fakeSearchResultSet) Close() error                       { f.closed = true; return f.closeErr }

func newFake(results ...storage.SearchResult) *fakeSearchResultSet {
	return &fakeSearchResultSet{results: results}
}

func sr(v string, score float64) storage.SearchResult {
	return storage.SearchResult{Value: v, Score: score}
}

func drain(t *testing.T, rs storage.SearchResultSet) []storage.SearchResult {
	t.Helper()
	var got []storage.SearchResult
	for rs.Next() {
		got = append(got, rs.At())
	}
	require.NoError(t, rs.Err())
	return got
}

func TestPairwiseMergeSearchSets_EmptyInput(t *testing.T) {
	rs := PairwiseMergeSearchSets(nil, storage.OrderByValueAsc, 0)
	assert.False(t, rs.Next())
	assert.NoError(t, rs.Err())
}

func TestPairwiseMergeSearchSets_SingleSet(t *testing.T) {
	src := newFake(sr("a", 1.0), sr("b", 0.9))
	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{src}, storage.OrderByValueAsc, 0)
	got := drain(t, rs)
	assert.Equal(t, []storage.SearchResult{sr("a", 1.0), sr("b", 0.9)}, got)
}

func TestPairwiseMergeSearchSets_SingleSetWithLimit(t *testing.T) {
	src := newFake(sr("a", 1.0), sr("b", 0.9), sr("c", 0.8))
	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{src}, storage.OrderByValueAsc, 2)
	got := drain(t, rs)
	assert.Equal(t, []storage.SearchResult{sr("a", 1.0), sr("b", 0.9)}, got)
}

func TestPairwiseMergeSearchSets_TwoSetsValueAsc(t *testing.T) {
	a := newFake(sr("a", 1.0), sr("c", 1.0), sr("e", 1.0))
	b := newFake(sr("b", 1.0), sr("d", 1.0), sr("f", 1.0))
	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{a, b}, storage.OrderByValueAsc, 0)
	got := drain(t, rs)
	assert.Equal(t, []storage.SearchResult{
		sr("a", 1.0), sr("b", 1.0), sr("c", 1.0),
		sr("d", 1.0), sr("e", 1.0), sr("f", 1.0),
	}, got)
}

func TestPairwiseMergeSearchSets_ThreeSetsValueAsc(t *testing.T) {
	// Triggers the recursive split: mid=1, left=[a], right=[b,c].
	a := newFake(sr("a", 1.0), sr("d", 1.0))
	b := newFake(sr("b", 1.0))
	c := newFake(sr("c", 1.0), sr("e", 1.0))
	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{a, b, c}, storage.OrderByValueAsc, 0)
	got := drain(t, rs)
	assert.Equal(t, []storage.SearchResult{
		sr("a", 1.0), sr("b", 1.0), sr("c", 1.0), sr("d", 1.0), sr("e", 1.0),
	}, got)
}

func TestPairwiseMergeSearchSets_DedupValueAsc_KeepsHighestScore(t *testing.T) {
	// Same Value in both sets; under value ordering the higher score wins.
	// Each set must be pre-sorted in the requested order — the merge relies
	// on it.
	a := newFake(sr("baz", 1.0), sr("foo", 0.7))
	b := newFake(sr("bar", 1.0), sr("foo", 0.9))
	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{a, b}, storage.OrderByValueAsc, 0)
	got := drain(t, rs)
	assert.Equal(t, []storage.SearchResult{
		sr("bar", 1.0), sr("baz", 1.0), sr("foo", 0.9),
	}, got)
}

func TestPairwiseMergeSearchSets_ValueDesc(t *testing.T) {
	a := newFake(sr("c", 1.0), sr("a", 1.0))
	b := newFake(sr("d", 1.0), sr("b", 1.0))
	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{a, b}, storage.OrderByValueDesc, 0)
	got := drain(t, rs)
	assert.Equal(t, []storage.SearchResult{
		sr("d", 1.0), sr("c", 1.0), sr("b", 1.0), sr("a", 1.0),
	}, got)
}

func TestPairwiseMergeSearchSets_ScoreDesc(t *testing.T) {
	// Both sides pre-sorted by (Score desc, Value asc).
	a := newFake(sr("alpha", 0.95), sr("delta", 0.5))
	b := newFake(sr("beta", 0.9), sr("gamma", 0.6))
	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{a, b}, storage.OrderByScoreDesc, 0)
	got := drain(t, rs)
	assert.Equal(t, []storage.SearchResult{
		sr("alpha", 0.95), sr("beta", 0.9), sr("gamma", 0.6), sr("delta", 0.5),
	}, got)
}

func TestPairwiseMergeSearchSets_ScoreDescDedup(t *testing.T) {
	// Same Value in both sets with identical scores (Searcher contract).
	// Comparator ties on (Score, Value); the duplicate collapses.
	a := newFake(sr("foo", 0.9), sr("bar", 0.5))
	b := newFake(sr("foo", 0.9), sr("baz", 0.4))
	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{a, b}, storage.OrderByScoreDesc, 0)
	got := drain(t, rs)
	assert.Equal(t, []storage.SearchResult{
		sr("foo", 0.9), sr("bar", 0.5), sr("baz", 0.4),
	}, got)
}

func TestPairwiseMergeSearchSets_LimitStopsEarly(t *testing.T) {
	a := newFake(sr("a", 1.0), sr("c", 1.0), sr("e", 1.0))
	b := newFake(sr("b", 1.0), sr("d", 1.0), sr("f", 1.0))
	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{a, b}, storage.OrderByValueAsc, 3)
	got := drain(t, rs)
	assert.Equal(t, []storage.SearchResult{sr("a", 1.0), sr("b", 1.0), sr("c", 1.0)}, got)
}

func TestPairwiseMergeSearchSets_OneSideEmpty(t *testing.T) {
	a := newFake()
	b := newFake(sr("a", 1.0), sr("b", 1.0))
	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{a, b}, storage.OrderByValueAsc, 0)
	got := drain(t, rs)
	assert.Equal(t, []storage.SearchResult{sr("a", 1.0), sr("b", 1.0)}, got)
}

func TestPairwiseMergeSearchSets_BothEmpty(t *testing.T) {
	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{newFake(), newFake()}, storage.OrderByValueAsc, 0)
	assert.False(t, rs.Next())
	assert.NoError(t, rs.Err())
}

func TestPairwiseMergeSearchSets_WarningsMerge(t *testing.T) {
	var aw annotations.Annotations
	aw.Add(errors.New("warn-a"))
	var bw annotations.Annotations
	bw.Add(errors.New("warn-b"))
	a := &fakeSearchResultSet{results: []storage.SearchResult{sr("x", 1.0)}, warns: aw}
	b := &fakeSearchResultSet{results: []storage.SearchResult{sr("y", 1.0)}, warns: bw}

	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{a, b}, storage.OrderByValueAsc, 0)
	_ = drain(t, rs)
	got := rs.Warnings()
	require.Len(t, got, 2)
	msgs := make(map[string]bool, 2)
	for _, w := range got {
		msgs[w.Error()] = true
	}
	assert.True(t, msgs["warn-a"])
	assert.True(t, msgs["warn-b"])
}

func TestPairwiseMergeSearchSets_ErrorStopsIteration(t *testing.T) {
	wantErr := errors.New("boom")
	a := &fakeSearchResultSet{results: []storage.SearchResult{sr("x", 1.0)}}
	b := &fakeSearchResultSet{results: nil, err: wantErr}

	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{a, b}, storage.OrderByValueAsc, 0)
	for rs.Next() {
	}
	require.Error(t, rs.Err())
	assert.ErrorIs(t, rs.Err(), wantErr)
}

func TestPairwiseMergeSearchSets_CloseClosesAllChildren(t *testing.T) {
	a := newFake(sr("a", 1.0))
	b := newFake(sr("b", 1.0))
	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{a, b}, storage.OrderByValueAsc, 0)
	require.NoError(t, rs.Close())
	assert.True(t, a.closed)
	assert.True(t, b.closed)
}

func TestPairwiseMergeSearchSets_CloseAggregatesErrors(t *testing.T) {
	aErr := errors.New("close-a")
	bErr := errors.New("close-b")
	a := &fakeSearchResultSet{results: []storage.SearchResult{sr("a", 1.0)}, closeErr: aErr}
	b := &fakeSearchResultSet{results: []storage.SearchResult{sr("b", 1.0)}, closeErr: bErr}
	rs := PairwiseMergeSearchSets([]storage.SearchResultSet{a, b}, storage.OrderByValueAsc, 0)
	err := rs.Close()
	require.Error(t, err)
	assert.ErrorIs(t, err, aErr)
	assert.ErrorIs(t, err, bErr)
}

func TestCompareSearchResults_ValueAsc(t *testing.T) {
	c := compareSearchResults(storage.OrderByValueAsc)
	assert.Negative(t, c(sr("a", 0.5), sr("b", 0.9)))
	assert.Zero(t, c(sr("x", 0.1), sr("x", 0.9)))
	assert.Positive(t, c(sr("z", 0.5), sr("a", 0.9)))
}

func TestCompareSearchResults_ValueDesc(t *testing.T) {
	c := compareSearchResults(storage.OrderByValueDesc)
	assert.Positive(t, c(sr("a", 0.5), sr("b", 0.9)))
	assert.Zero(t, c(sr("x", 0.1), sr("x", 0.9)))
	assert.Negative(t, c(sr("z", 0.5), sr("a", 0.9)))
}

func TestCompareSearchResults_ScoreDescTieBreaksOnValue(t *testing.T) {
	c := compareSearchResults(storage.OrderByScoreDesc)
	assert.Negative(t, c(sr("a", 0.9), sr("b", 0.5)))    // higher score first
	assert.Positive(t, c(sr("a", 0.5), sr("b", 0.9)))    // higher score first
	assert.Negative(t, c(sr("a", 0.9), sr("b", 0.9)))    // tie → Value asc
	assert.Positive(t, c(sr("b", 0.9), sr("a", 0.9)))    // tie → Value asc
	assert.Zero(t, c(sr("a", 0.9), sr("a", 0.9)))
}

func TestLimitSearchResultSet_StopsAfterLimit(t *testing.T) {
	src := newFake(sr("a", 1.0), sr("b", 1.0), sr("c", 1.0))
	rs := &limitSearchResultSet{rs: src, limit: 2}
	got := drain(t, rs)
	assert.Equal(t, []storage.SearchResult{sr("a", 1.0), sr("b", 1.0)}, got)
}

func TestLimitSearchResultSet_ZeroLimitMeansUnlimited(t *testing.T) {
	src := newFake(sr("a", 1.0), sr("b", 1.0))
	rs := &limitSearchResultSet{rs: src, limit: 0}
	got := drain(t, rs)
	assert.Equal(t, []storage.SearchResult{sr("a", 1.0), sr("b", 1.0)}, got)
}
