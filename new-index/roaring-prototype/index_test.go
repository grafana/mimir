package main

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- SymbolTable ---

func TestSymbolTable_EncodeNewStrings(t *testing.T) {
	st := NewSymbolTable()
	id0 := st.Encode("foo")
	id1 := st.Encode("bar")
	id2 := st.Encode("baz")

	assert.Equal(t, uint32(0), id0)
	assert.Equal(t, uint32(1), id1)
	assert.Equal(t, uint32(2), id2)
}

func TestSymbolTable_EncodeDuplicates(t *testing.T) {
	st := NewSymbolTable()
	first := st.Encode("hello")
	second := st.Encode("hello")
	assert.Equal(t, first, second)
	assert.Equal(t, 1, len(st.idToStr), "duplicate should not grow the table")
}

func TestSymbolTable_Decode(t *testing.T) {
	st := NewSymbolTable()
	st.Encode("alpha")
	st.Encode("beta")

	assert.Equal(t, "alpha", st.Decode(0))
	assert.Equal(t, "beta", st.Decode(1))
	assert.Equal(t, "", st.Decode(999), "out-of-range ID returns empty string")
}

func TestSymbolTable_Lookup(t *testing.T) {
	st := NewSymbolTable()
	st.Encode("exists")

	id, ok := st.Lookup("exists")
	assert.True(t, ok)
	assert.Equal(t, uint32(0), id)

	_, ok = st.Lookup("missing")
	assert.False(t, ok)
}

// --- Segment ---

func TestSegment_AddAndForward(t *testing.T) {
	st := NewSymbolTable()
	seg := newSegment()

	ls := []Label{{"job", "api"}, {"instance", "host1"}}
	seg.add(st, 1, ls)

	assert.True(t, seg.allSeries.Contains(1))
	assert.Equal(t, ls, seg.forward[1])
}

func TestSegment_FrozenPanics(t *testing.T) {
	st := NewSymbolTable()
	seg := newSegment()
	seg.frozen = true
	assert.Panics(t, func() {
		seg.add(st, 1, []Label{{"k", "v"}})
	})
}

func TestSegment_InvertedIndex(t *testing.T) {
	st := NewSymbolTable()
	seg := newSegment()

	seg.add(st, 1, []Label{{"job", "api"}})
	seg.add(st, 2, []Label{{"job", "api"}})
	seg.add(st, 3, []Label{{"job", "web"}})

	jobID := st.Encode("job")
	apiID := st.Encode("api")
	webID := st.Encode("web")

	apiBM := seg.inverted[invertedKey{jobID, apiID}]
	require.NotNil(t, apiBM)
	assert.ElementsMatch(t, []uint32{1, 2}, apiBM.ToArray())

	webBM := seg.inverted[invertedKey{jobID, webID}]
	require.NotNil(t, webBM)
	assert.ElementsMatch(t, []uint32{3}, webBM.ToArray())
}

// --- Index lifecycle ---

func TestIndex_AddAndSeries(t *testing.T) {
	idx := NewIndex()
	ls := []Label{{"__name__", "up"}, {"job", "test"}}
	idx.Add(1, ls)

	got, ok := idx.Series(1)
	require.True(t, ok)
	assert.Equal(t, ls, got)

	_, ok = idx.Series(999)
	assert.False(t, ok)
}

func TestIndex_FlushMovesDataToSegment(t *testing.T) {
	idx := NewIndex()
	idx.Add(1, []Label{{"a", "b"}})
	idx.Flush()

	assert.Len(t, idx.segments, 1)
	assert.True(t, idx.segments[0].frozen)
	assert.True(t, idx.segments[0].allSeries.Contains(1))

	// Buffer should be empty after flush.
	assert.Equal(t, uint64(0), idx.buf.allSeries.GetCardinality())
}

func TestIndex_SeriesFoundAcrossFlush(t *testing.T) {
	idx := NewIndex()
	idx.Add(1, []Label{{"wave", "first"}})
	idx.Flush()
	idx.Add(2, []Label{{"wave", "second"}})

	// Both refs should be findable.
	_, ok1 := idx.Series(1)
	_, ok2 := idx.Series(2)
	assert.True(t, ok1, "flushed series should still be found")
	assert.True(t, ok2, "buffer series should be found")
}

func TestIndex_DropOldestSegment(t *testing.T) {
	idx := NewIndex()
	idx.Add(1, []Label{{"wave", "first"}})
	idx.Flush()
	idx.Add(2, []Label{{"wave", "second"}})
	idx.Flush()
	idx.Add(3, []Label{{"wave", "third"}})

	idx.DropOldestSegment()
	assert.Len(t, idx.segments, 1)

	_, ok1 := idx.Series(1)
	assert.False(t, ok1, "dropped segment data should be gone")
	_, ok2 := idx.Series(2)
	assert.True(t, ok2)
	_, ok3 := idx.Series(3)
	assert.True(t, ok3)
}

func TestIndex_DropAllSegments(t *testing.T) {
	idx := NewIndex()
	idx.Add(1, []Label{{"k", "v"}})
	idx.Flush()

	idx.DropOldestSegment()
	idx.DropOldestSegment() // no-op on empty

	assert.Len(t, idx.segments, 0)
}

func TestIndex_MultipleFlushes(t *testing.T) {
	idx := NewIndex()
	for i := uint32(1); i <= 5; i++ {
		idx.Add(i, []Label{{"n", fmt.Sprintf("v%d", i)}})
		idx.Flush()
	}
	assert.Len(t, idx.segments, 5)
	for i := uint32(1); i <= 5; i++ {
		_, ok := idx.Series(i)
		assert.True(t, ok, "series %d should be found", i)
	}
}

// --- Postings (exact match) ---

func TestPostings_SingleSegment(t *testing.T) {
	idx := NewIndex()
	idx.Add(1, []Label{{"job", "api"}})
	idx.Add(2, []Label{{"job", "api"}})
	idx.Add(3, []Label{{"job", "web"}})

	bm := idx.Postings("job", "api")
	assert.ElementsMatch(t, []uint32{1, 2}, bm.ToArray())
}

func TestPostings_AcrossSegments(t *testing.T) {
	idx := NewIndex()
	idx.Add(1, []Label{{"job", "api"}})
	idx.Flush()
	idx.Add(2, []Label{{"job", "api"}})

	bm := idx.Postings("job", "api")
	assert.ElementsMatch(t, []uint32{1, 2}, bm.ToArray())
}

func TestPostings_NoMatch(t *testing.T) {
	idx := NewIndex()
	idx.Add(1, []Label{{"job", "api"}})

	bm := idx.Postings("job", "nonexistent")
	assert.Equal(t, uint64(0), bm.GetCardinality())
}

func TestPostings_UnknownLabel(t *testing.T) {
	idx := NewIndex()
	idx.Add(1, []Label{{"job", "api"}})

	bm := idx.Postings("unknown_label", "api")
	assert.Equal(t, uint64(0), bm.GetCardinality())
}

func TestPostings_EmptyIndex(t *testing.T) {
	idx := NewIndex()
	bm := idx.Postings("job", "api")
	assert.Equal(t, uint64(0), bm.GetCardinality())
}

func TestPostings_AfterDrop(t *testing.T) {
	idx := NewIndex()
	idx.Add(1, []Label{{"job", "api"}})
	idx.Flush()
	idx.Add(2, []Label{{"job", "api"}})

	idx.DropOldestSegment()

	bm := idx.Postings("job", "api")
	assert.ElementsMatch(t, []uint32{2}, bm.ToArray())
}

// --- PostingsForMatchers ---

func TestPostingsForMatchers_SinglePositive(t *testing.T) {
	idx := buildStandardIndex()
	bm := idx.PostingsForMatchers(Equal("job", "api"))
	assert.ElementsMatch(t, []uint32{1, 2, 5, 8, 12}, bm.ToArray())
}

func TestPostingsForMatchers_TwoPositive_AND(t *testing.T) {
	idx := buildStandardIndex()
	bm := idx.PostingsForMatchers(
		Equal("__name__", "http_requests_total"),
		Equal("job", "api"),
	)
	assert.ElementsMatch(t, []uint32{1, 2, 8}, bm.ToArray())
}

func TestPostingsForMatchers_PositiveAndNegative_ANDNOT(t *testing.T) {
	idx := buildStandardIndex()
	bm := idx.PostingsForMatchers(
		Equal("__name__", "http_requests_total"),
		NotEqual("job", "api"),
	)
	assert.ElementsMatch(t, []uint32{3, 4, 9}, bm.ToArray())
}

func TestPostingsForMatchers_OnlyNegative(t *testing.T) {
	idx := buildStandardIndex()
	// job!="api" should return all series except those with job=api.
	bm := idx.PostingsForMatchers(NotEqual("job", "api"))
	got := bm.ToArray()
	for _, excluded := range []uint32{1, 2, 5, 8, 12} {
		assert.NotContains(t, got, excluded)
	}
	for _, included := range []uint32{3, 4, 6, 7, 9, 10, 11} {
		assert.Contains(t, got, included)
	}
}

func TestPostingsForMatchers_MultipleNegatives(t *testing.T) {
	idx := buildStandardIndex()
	// job!="api", job!="web" -> everything except job=api and job=web.
	bm := idx.PostingsForMatchers(
		NotEqual("job", "api"),
		NotEqual("job", "web"),
	)
	got := bm.ToArray()
	// Should only have job=db, job=batch series.
	for _, ref := range got {
		ls, ok := idx.Series(ref)
		require.True(t, ok)
		for _, l := range ls {
			if l.Name == "job" {
				assert.NotEqual(t, "api", l.Value)
				assert.NotEqual(t, "web", l.Value)
			}
		}
	}
	assert.ElementsMatch(t, []uint32{7, 9, 10, 11}, got)
}

func TestPostingsForMatchers_NoMatchers(t *testing.T) {
	idx := buildStandardIndex()
	bm := idx.PostingsForMatchers()
	assert.Equal(t, uint64(0), bm.GetCardinality())
}

func TestPostingsForMatchers_NothingMatchesPositive(t *testing.T) {
	idx := buildStandardIndex()
	bm := idx.PostingsForMatchers(Equal("job", "nonexistent"))
	assert.Equal(t, uint64(0), bm.GetCardinality())
}

func TestPostingsForMatchers_NegateNonexistentValue(t *testing.T) {
	idx := buildStandardIndex()
	// job!="nonexistent" -> should return everything (nothing to subtract).
	bm := idx.PostingsForMatchers(NotEqual("job", "nonexistent"))
	assert.Equal(t, uint64(12), bm.GetCardinality())
}

func TestPostingsForMatchers_ThreePositiveMatchers(t *testing.T) {
	idx := buildStandardIndex()
	bm := idx.PostingsForMatchers(
		Equal("__name__", "http_requests_total"),
		Equal("job", "api"),
		Equal("instance", "host1"),
	)
	assert.ElementsMatch(t, []uint32{1}, bm.ToArray())
}

func TestPostingsForMatchers_PositiveAndNegative_DisjointLabel(t *testing.T) {
	idx := buildStandardIndex()
	// __name__="cpu_usage", instance!="host1"
	bm := idx.PostingsForMatchers(
		Equal("__name__", "cpu_usage"),
		NotEqual("instance", "host1"),
	)
	// cpu_usage series: ref=5 (host1), ref=6 (host2), ref=10 (host6)
	// Excluding host1 leaves ref=6 and ref=10.
	assert.ElementsMatch(t, []uint32{6, 10}, bm.ToArray())
}

// --- Cross-segment correctness ---

func TestPostingsForMatchers_AcrossFlushAndBuffer(t *testing.T) {
	idx := NewIndex()

	// Segment 1.
	idx.Add(1, []Label{{"env", "prod"}, {"region", "us"}})
	idx.Add(2, []Label{{"env", "prod"}, {"region", "eu"}})
	idx.Flush()

	// Segment 2.
	idx.Add(3, []Label{{"env", "staging"}, {"region", "us"}})
	idx.Flush()

	// Buffer.
	idx.Add(4, []Label{{"env", "prod"}, {"region", "ap"}})

	// env=prod spans segment 1 and buffer.
	bm := idx.PostingsForMatchers(Equal("env", "prod"))
	assert.ElementsMatch(t, []uint32{1, 2, 4}, bm.ToArray())

	// env=prod, region!="us" -> exclude ref=1.
	bm = idx.PostingsForMatchers(
		Equal("env", "prod"),
		NotEqual("region", "us"),
	)
	assert.ElementsMatch(t, []uint32{2, 4}, bm.ToArray())
}

func TestPostingsForMatchers_AfterDropOldest(t *testing.T) {
	idx := NewIndex()
	idx.Add(1, []Label{{"job", "api"}})
	idx.Flush()
	idx.Add(2, []Label{{"job", "api"}})
	idx.Flush()
	idx.Add(3, []Label{{"job", "api"}})

	idx.DropOldestSegment()

	bm := idx.PostingsForMatchers(Equal("job", "api"))
	assert.ElementsMatch(t, []uint32{2, 3}, bm.ToArray())
}

// --- Scale ---

func TestManySeriesAcrossFlushes(t *testing.T) {
	idx := NewIndex()

	const (
		seriesPerFlush = 500
		numFlushes     = 4
	)

	var allRefs []uint32
	for flush := 0; flush < numFlushes; flush++ {
		for i := 0; i < seriesPerFlush; i++ {
			ref := uint32(flush*seriesPerFlush + i + 1)
			idx.Add(ref, []Label{
				{"__name__", "metric"},
				{"job", fmt.Sprintf("job_%d", flush)},
				{"instance", fmt.Sprintf("inst_%d", i)},
			})
			allRefs = append(allRefs, ref)
		}
		if flush < numFlushes-1 {
			idx.Flush()
		}
	}

	// All series have __name__=metric.
	bm := idx.Postings("__name__", "metric")
	got := bm.ToArray()
	sort.Slice(allRefs, func(i, j int) bool { return allRefs[i] < allRefs[j] })
	assert.Equal(t, allRefs, got)

	// Each job bucket has seriesPerFlush series.
	for flush := 0; flush < numFlushes; flush++ {
		jobBM := idx.Postings("job", fmt.Sprintf("job_%d", flush))
		assert.Equal(t, uint64(seriesPerFlush), jobBM.GetCardinality())
	}
}

func TestDropReducesPostingsCardinality(t *testing.T) {
	idx := NewIndex()
	for i := uint32(1); i <= 100; i++ {
		idx.Add(i, []Label{{"k", "v"}})
	}
	idx.Flush()
	for i := uint32(101); i <= 200; i++ {
		idx.Add(i, []Label{{"k", "v"}})
	}
	idx.Flush()

	before := idx.Postings("k", "v").GetCardinality()
	assert.Equal(t, uint64(200), before)

	idx.DropOldestSegment()

	after := idx.Postings("k", "v").GetCardinality()
	assert.Equal(t, uint64(100), after)
}

// --- allPostingsBitmap ---

func TestAllPostingsBitmap(t *testing.T) {
	idx := NewIndex()
	idx.Add(1, []Label{{"a", "1"}})
	idx.Flush()
	idx.Add(2, []Label{{"b", "2"}})

	bm := idx.allPostingsBitmap()
	assert.ElementsMatch(t, []uint32{1, 2}, bm.ToArray())
}

func TestAllPostingsBitmap_AfterDrop(t *testing.T) {
	idx := NewIndex()
	idx.Add(1, []Label{{"a", "1"}})
	idx.Flush()
	idx.Add(2, []Label{{"b", "2"}})
	idx.Flush()

	idx.DropOldestSegment()

	bm := idx.allPostingsBitmap()
	assert.ElementsMatch(t, []uint32{2}, bm.ToArray())
}

// --- helpers ---

// buildStandardIndex creates the same dataset used in main.go:
// 7 series in segment 1, 5 series in the write buffer.
func buildStandardIndex() *Index {
	idx := NewIndex()
	for _, s := range []struct {
		ref uint32
		ls  []Label
	}{
		{1, []Label{{"__name__", "http_requests_total"}, {"job", "api"}, {"instance", "host1"}}},
		{2, []Label{{"__name__", "http_requests_total"}, {"job", "api"}, {"instance", "host2"}}},
		{3, []Label{{"__name__", "http_requests_total"}, {"job", "web"}, {"instance", "host1"}}},
		{4, []Label{{"__name__", "http_requests_total"}, {"job", "web"}, {"instance", "host3"}}},
		{5, []Label{{"__name__", "cpu_usage"}, {"job", "api"}, {"instance", "host1"}}},
		{6, []Label{{"__name__", "cpu_usage"}, {"job", "web"}, {"instance", "host2"}}},
		{7, []Label{{"__name__", "memory_bytes"}, {"job", "db"}, {"instance", "host4"}}},
	} {
		idx.Add(s.ref, s.ls)
	}
	idx.Flush()
	for _, s := range []struct {
		ref uint32
		ls  []Label
	}{
		{8, []Label{{"__name__", "http_requests_total"}, {"job", "api"}, {"instance", "host5"}}},
		{9, []Label{{"__name__", "http_requests_total"}, {"job", "batch"}, {"instance", "host6"}}},
		{10, []Label{{"__name__", "cpu_usage"}, {"job", "batch"}, {"instance", "host6"}}},
		{11, []Label{{"__name__", "disk_io"}, {"job", "db"}, {"instance", "host4"}}},
		{12, []Label{{"__name__", "memory_bytes"}, {"job", "api"}, {"instance", "host5"}}},
	} {
		idx.Add(s.ref, s.ls)
	}
	return idx
}
