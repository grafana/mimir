// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"testing"

	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
)

func TestMergeSearchResults_Empty(t *testing.T) {
	assert.Nil(t, MergeSearchResults(nil, nil))
	assert.Nil(t, MergeSearchResults([][]storage.SearchResult{}, nil))
}

func TestMergeSearchResults_SingleSource_PreservesScores(t *testing.T) {
	src := []storage.SearchResult{{Value: "foo", Score: 0.7}, {Value: "bar", Score: 1.0}}
	got := MergeSearchResults([][]storage.SearchResult{src}, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	assert.Equal(t, []storage.SearchResult{
		{Value: "bar", Score: 1.0},
		{Value: "foo", Score: 0.7},
	}, got)
}

func TestMergeSearchResults_DedupByValue_TakesMaxScore(t *testing.T) {
	a := []storage.SearchResult{{Value: "alpha", Score: 0.7}, {Value: "beta", Score: 1.0}}
	b := []storage.SearchResult{{Value: "alpha", Score: 0.9}, {Value: "beta", Score: 1.0}}
	got := MergeSearchResults([][]storage.SearchResult{a, b}, nil)
	assert.Equal(t, []storage.SearchResult{
		{Value: "alpha", Score: 0.9}, // max(0.7, 0.9)
		{Value: "beta", Score: 1.0},  // identical: still 1.0
	}, got)
}

func TestMergeSearchResults_OrderByValueAsc(t *testing.T) {
	src := []storage.SearchResult{
		{Value: "gamma", Score: 0.5},
		{Value: "alpha", Score: 0.9},
		{Value: "beta", Score: 0.8},
	}
	got := MergeSearchResults([][]storage.SearchResult{src}, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	assert.Equal(t, []storage.SearchResult{
		{Value: "alpha", Score: 0.9},
		{Value: "beta", Score: 0.8},
		{Value: "gamma", Score: 0.5},
	}, got)
}

func TestMergeSearchResults_OrderByValueDesc(t *testing.T) {
	src := []storage.SearchResult{
		{Value: "alpha", Score: 0.9},
		{Value: "gamma", Score: 0.5},
		{Value: "beta", Score: 0.8},
	}
	got := MergeSearchResults([][]storage.SearchResult{src}, &storage.SearchHints{OrderBy: storage.OrderByValueDesc})
	assert.Equal(t, []storage.SearchResult{
		{Value: "gamma", Score: 0.5},
		{Value: "beta", Score: 0.8},
		{Value: "alpha", Score: 0.9},
	}, got)
}

func TestMergeSearchResults_OrderByScoreDesc_AlphaTiebreak(t *testing.T) {
	// Mirrors Prometheus's storage.compareSearchResults invariant: ties on
	// Score break on ascending Value so two implementations agree.
	src := []storage.SearchResult{
		{Value: "alpha", Score: 0.9},
		{Value: "delta", Score: 1.0},
		{Value: "beta", Score: 1.0},
		{Value: "charlie", Score: 0.5},
	}
	got := MergeSearchResults([][]storage.SearchResult{src}, &storage.SearchHints{OrderBy: storage.OrderByScoreDesc})
	assert.Equal(t, []storage.SearchResult{
		{Value: "beta", Score: 1.0},  // 1.0 ties — alphabetical
		{Value: "delta", Score: 1.0}, // 1.0 ties — alphabetical
		{Value: "alpha", Score: 0.9},
		{Value: "charlie", Score: 0.5},
	}, got)
}

func TestMergeSearchResults_Limit(t *testing.T) {
	src := []storage.SearchResult{
		{Value: "a", Score: 1.0},
		{Value: "b", Score: 1.0},
		{Value: "c", Score: 1.0},
		{Value: "d", Score: 1.0},
	}
	got := MergeSearchResults([][]storage.SearchResult{src}, &storage.SearchHints{OrderBy: storage.OrderByValueAsc, Limit: 2})
	assert.Equal(t, []storage.SearchResult{
		{Value: "a", Score: 1.0},
		{Value: "b", Score: 1.0},
	}, got)
}

func TestMergeSearchResults_LimitZeroMeansUnlimited(t *testing.T) {
	src := []storage.SearchResult{{Value: "a"}, {Value: "b"}, {Value: "c"}}
	got := MergeSearchResults([][]storage.SearchResult{src}, &storage.SearchHints{Limit: 0})
	assert.Len(t, got, 3)
}

func TestMergeSearchResults_NilHints_DefaultsToValueAscNoLimit(t *testing.T) {
	src := []storage.SearchResult{
		{Value: "gamma", Score: 0.5},
		{Value: "alpha", Score: 0.9},
	}
	got := MergeSearchResults([][]storage.SearchResult{src}, nil)
	assert.Equal(t, []storage.SearchResult{
		{Value: "alpha", Score: 0.9},
		{Value: "gamma", Score: 0.5},
	}, got)
}

func TestMergeSearchResults_CrossSourceDedupAndOrdering(t *testing.T) {
	// Multi-source: three replicas with overlapping data. Each replica is
	// already filtered + ordered by its leaf Searcher (here pre-sorted by
	// score-desc per replica), but cross-replica dedup is on us.
	r0 := []storage.SearchResult{
		{Value: "foo", Score: 1.0},
		{Value: "footer", Score: 1.0},
	}
	r1 := []storage.SearchResult{
		{Value: "foo", Score: 1.0},
		{Value: "foobar", Score: 1.0},
	}
	r2 := []storage.SearchResult{
		{Value: "foobar", Score: 1.0},
	}
	got := MergeSearchResults([][]storage.SearchResult{r0, r1, r2}, &storage.SearchHints{OrderBy: storage.OrderByValueAsc, Limit: 100})
	assert.Equal(t, []storage.SearchResult{
		{Value: "foo", Score: 1.0},
		{Value: "foobar", Score: 1.0},
		{Value: "footer", Score: 1.0},
	}, got)
}
