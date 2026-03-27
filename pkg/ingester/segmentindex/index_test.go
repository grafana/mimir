// SPDX-License-Identifier: AGPL-3.0-only

package segmentindex

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectPostings drains a Postings iterator into a sorted slice.
func collectPostings(t *testing.T, p index.Postings) []storage.SeriesRef {
	t.Helper()
	var refs []storage.SeriesRef
	for p.Next() {
		refs = append(refs, p.At())
	}
	require.NoError(t, p.Err())
	return refs
}

// referenceOracle wraps MemPostings to provide the same query interface
// as our Reader, so we can compare results.
type referenceOracle struct {
	mp     *index.MemPostings
	series map[storage.SeriesRef]labels.Labels
}

func newOracle() *referenceOracle {
	return &referenceOracle{
		mp:     index.NewMemPostings(),
		series: make(map[storage.SeriesRef]labels.Labels),
	}
}

func (o *referenceOracle) add(ref storage.SeriesRef, lset labels.Labels) {
	o.mp.Add(ref, lset)
	o.series[ref] = lset
}

// testSeries is a small helper for building label sets.
func testSeries(namevals ...string) labels.Labels {
	b := labels.NewBuilder(labels.EmptyLabels())
	for i := 0; i < len(namevals); i += 2 {
		b.Set(namevals[i], namevals[i+1])
	}
	return b.Labels()
}

// addStandardSeries inserts a standard set of series into both the index
// and the oracle, returning the oracle for assertions.
func addStandardSeries(t *testing.T, idx *Index) *referenceOracle {
	t.Helper()
	oracle := newOracle()

	series := []struct {
		ref  storage.SeriesRef
		lset labels.Labels
	}{
		{1, testSeries("__name__", "http_requests_total", "job", "api", "instance", "host1")},
		{2, testSeries("__name__", "http_requests_total", "job", "api", "instance", "host2")},
		{3, testSeries("__name__", "http_requests_total", "job", "web", "instance", "host1")},
		{4, testSeries("__name__", "cpu_usage", "job", "api", "instance", "host1")},
		{5, testSeries("__name__", "cpu_usage", "job", "web", "instance", "host3")},
		{6, testSeries("__name__", "memory_bytes", "job", "db", "instance", "host4")},
	}
	for _, s := range series {
		idx.Add(s.ref, s.lset)
		oracle.add(s.ref, s.lset)
	}
	return oracle
}

// ---------- Phase 1: buffer-only (no flush) ----------
// These tests verify that before any flush, the index behaves identically
// to MemPostings.

func TestPostings_ExactMatch(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(dir)
	require.NoError(t, err)
	defer idx.Close()

	oracle := addStandardSeries(t, idx)
	ctx := context.Background()
	reader := idx.Reader()

	cases := []struct {
		name   string
		label  string
		values []string
	}{
		{"single value", "job", []string{"api"}},
		{"multiple values", "job", []string{"api", "web"}},
		{"__name__", "__name__", []string{"http_requests_total"}},
		{"no match", "job", []string{"nonexistent"}},
		{"instance single", "instance", []string{"host1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := reader.Postings(ctx, tc.label, tc.values...)
			require.NoError(t, err)
			gotRefs := collectPostings(t, got)

			want := collectPostings(t, oracle.mp.Postings(ctx, tc.label, tc.values...))

			sort.Slice(gotRefs, func(i, j int) bool { return gotRefs[i] < gotRefs[j] })
			sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
			assert.Equal(t, want, gotRefs)
		})
	}
}

func TestPostingsForLabelMatching(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(dir)
	require.NoError(t, err)
	defer idx.Close()

	oracle := addStandardSeries(t, idx)
	ctx := context.Background()
	reader := idx.Reader()

	cases := []struct {
		name  string
		label string
		match func(string) bool
	}{
		{"prefix match", "__name__", func(v string) bool { return len(v) > 4 && v[:4] == "http" }},
		{"exact", "job", func(v string) bool { return v == "api" }},
		{"match all", "job", func(v string) bool { return true }},
		{"match none", "job", func(v string) bool { return false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotRefs := collectPostings(t, reader.PostingsForLabelMatching(ctx, tc.label, tc.match))
			wantRefs := collectPostings(t, oracle.mp.PostingsForLabelMatching(ctx, tc.label, tc.match))

			sort.Slice(gotRefs, func(i, j int) bool { return gotRefs[i] < gotRefs[j] })
			sort.Slice(wantRefs, func(i, j int) bool { return wantRefs[i] < wantRefs[j] })
			assert.Equal(t, wantRefs, gotRefs)
		})
	}
}

func TestPostingsForAllLabelValues(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(dir)
	require.NoError(t, err)
	defer idx.Close()

	oracle := addStandardSeries(t, idx)
	ctx := context.Background()
	reader := idx.Reader()

	for _, labelName := range []string{"__name__", "job", "instance", "nonexistent"} {
		t.Run(labelName, func(t *testing.T) {
			gotRefs := collectPostings(t, reader.PostingsForAllLabelValues(ctx, labelName))
			wantRefs := collectPostings(t, oracle.mp.PostingsForAllLabelValues(ctx, labelName))

			sort.Slice(gotRefs, func(i, j int) bool { return gotRefs[i] < gotRefs[j] })
			sort.Slice(wantRefs, func(i, j int) bool { return wantRefs[i] < wantRefs[j] })
			assert.Equal(t, wantRefs, gotRefs)
		})
	}
}

func TestLabelValues(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(dir)
	require.NoError(t, err)
	defer idx.Close()

	oracle := addStandardSeries(t, idx)
	ctx := context.Background()
	reader := idx.Reader()

	for _, labelName := range []string{"__name__", "job", "instance", "nonexistent"} {
		t.Run(labelName, func(t *testing.T) {
			got, err := reader.LabelValues(ctx, labelName, nil)
			require.NoError(t, err)

			want := oracle.mp.LabelValues(ctx, labelName, nil)

			sort.Strings(got)
			sort.Strings(want)
			assert.Equal(t, want, got)
		})
	}
}

func TestLabelNames(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(dir)
	require.NoError(t, err)
	defer idx.Close()

	oracle := addStandardSeries(t, idx)
	ctx := context.Background()
	reader := idx.Reader()

	got, err := reader.LabelNames(ctx)
	require.NoError(t, err)

	want := oracle.mp.LabelNames()

	sort.Strings(got)
	sort.Strings(want)
	assert.Equal(t, want, got)
}

func TestSeries(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(dir)
	require.NoError(t, err)
	defer idx.Close()

	oracle := addStandardSeries(t, idx)
	reader := idx.Reader()

	for ref, wantLabels := range oracle.series {
		t.Run(fmt.Sprintf("ref=%d", ref), func(t *testing.T) {
			var builder labels.ScratchBuilder
			err := reader.Series(ref, &builder, nil)
			require.NoError(t, err)
			assert.Equal(t, wantLabels, builder.Labels())
		})
	}

	t.Run("missing ref", func(t *testing.T) {
		var builder labels.ScratchBuilder
		err := reader.Series(999, &builder, nil)
		assert.ErrorIs(t, err, storage.ErrNotFound)
	})
}

// ---------- Phase 2: flush ----------
// After flushing the write buffer to an on-disk segment, all queries must
// still return the same results.

func TestFlush_QueriesStillWork(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(dir)
	require.NoError(t, err)
	defer idx.Close()

	oracle := addStandardSeries(t, idx)

	require.NoError(t, idx.Flush())

	ctx := context.Background()
	reader := idx.Reader()

	// Postings for a specific label value should match oracle.
	gotRefs, err := reader.Postings(ctx, "job", "api")
	require.NoError(t, err)
	got := collectPostings(t, gotRefs)
	want := collectPostings(t, oracle.mp.Postings(ctx, "job", "api"))
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	assert.Equal(t, want, got)

	// LabelValues should match.
	gotLV, err := reader.LabelValues(ctx, "job", nil)
	require.NoError(t, err)
	wantLV := oracle.mp.LabelValues(ctx, "job", nil)
	sort.Strings(gotLV)
	sort.Strings(wantLV)
	assert.Equal(t, wantLV, gotLV)

	// LabelNames should match.
	gotLN, err := reader.LabelNames(ctx)
	require.NoError(t, err)
	wantLN := oracle.mp.LabelNames()
	sort.Strings(gotLN)
	sort.Strings(wantLN)
	assert.Equal(t, wantLN, gotLN)

	// Series lookups should still work.
	var builder labels.ScratchBuilder
	require.NoError(t, reader.Series(1, &builder, nil))
	assert.Equal(t, oracle.series[1], builder.Labels())
}

// After flushing, new series added to the buffer should be merged with
// the flushed segment on reads.
func TestFlush_ThenAddMore(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(dir)
	require.NoError(t, err)
	defer idx.Close()

	// Phase 1: add some series and flush.
	idx.Add(1, testSeries("__name__", "metric_a", "job", "x"))
	idx.Add(2, testSeries("__name__", "metric_a", "job", "y"))
	require.NoError(t, idx.Flush())

	// Phase 2: add more series (they go into a fresh buffer).
	idx.Add(3, testSeries("__name__", "metric_a", "job", "x"))
	idx.Add(4, testSeries("__name__", "metric_b", "job", "z"))

	ctx := context.Background()
	reader := idx.Reader()

	// "job=x" should return refs from both the flushed segment and the buffer.
	gotRefs, err := reader.Postings(ctx, "job", "x")
	require.NoError(t, err)
	got := collectPostings(t, gotRefs)
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	assert.Equal(t, []storage.SeriesRef{1, 3}, got)

	// "__name__" label values should include both metric_a and metric_b.
	gotLV, err := reader.LabelValues(ctx, "__name__", nil)
	require.NoError(t, err)
	sort.Strings(gotLV)
	assert.Equal(t, []string{"metric_a", "metric_b"}, gotLV)
}

// Multiple flushes should produce multiple segments, and all queries should
// merge across all of them plus the current buffer.
func TestMultipleFlushes(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(dir)
	require.NoError(t, err)
	defer idx.Close()

	// Segment 1.
	idx.Add(1, testSeries("__name__", "m", "env", "prod"))
	require.NoError(t, idx.Flush())

	// Segment 2.
	idx.Add(2, testSeries("__name__", "m", "env", "staging"))
	require.NoError(t, idx.Flush())

	// Buffer (not yet flushed).
	idx.Add(3, testSeries("__name__", "m", "env", "dev"))

	ctx := context.Background()
	reader := idx.Reader()

	// All three series have __name__=m.
	gotRefs, err := reader.Postings(ctx, "__name__", "m")
	require.NoError(t, err)
	got := collectPostings(t, gotRefs)
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	assert.Equal(t, []storage.SeriesRef{1, 2, 3}, got)

	// env should have three values.
	gotLV, err := reader.LabelValues(ctx, "env", nil)
	require.NoError(t, err)
	sort.Strings(gotLV)
	assert.Equal(t, []string{"dev", "prod", "staging"}, gotLV)
}

// ---------- Phase 3: drop ----------
// Dropping the oldest segment removes its data from query results.

func TestDropOldestSegment(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(dir)
	require.NoError(t, err)
	defer idx.Close()

	// Segment 1: series 1 and 2.
	idx.Add(1, testSeries("__name__", "m", "wave", "first"))
	idx.Add(2, testSeries("__name__", "m", "wave", "first"))
	require.NoError(t, idx.Flush())

	// Segment 2: series 3.
	idx.Add(3, testSeries("__name__", "m", "wave", "second"))
	require.NoError(t, idx.Flush())

	// Buffer: series 4.
	idx.Add(4, testSeries("__name__", "m", "wave", "third"))

	// Drop segment 1.
	require.NoError(t, idx.DropOldestSegment())

	ctx := context.Background()
	reader := idx.Reader()

	// Series 1 and 2 should be gone.
	gotRefs, err := reader.Postings(ctx, "__name__", "m")
	require.NoError(t, err)
	got := collectPostings(t, gotRefs)
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	assert.Equal(t, []storage.SeriesRef{3, 4}, got)

	// "wave=first" should have no postings.
	firstRefs, err := reader.Postings(ctx, "wave", "first")
	require.NoError(t, err)
	assert.Empty(t, collectPostings(t, firstRefs))

	// LabelValues("wave") should no longer include "first".
	gotLV, err := reader.LabelValues(ctx, "wave", nil)
	require.NoError(t, err)
	sort.Strings(gotLV)
	assert.Equal(t, []string{"second", "third"}, gotLV)

	// Series(1) should return ErrNotFound.
	var builder labels.ScratchBuilder
	assert.ErrorIs(t, reader.Series(1, &builder, nil), storage.ErrNotFound)

	// Series(3) should still work.
	require.NoError(t, reader.Series(3, &builder, nil))
}

// ---------- Phase 4: scale ----------
// A light stress test with many series to catch obvious scaling issues.

func TestManySeriesAcrossFlushes(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(dir)
	require.NoError(t, err)
	defer idx.Close()

	const (
		seriesPerFlush = 1000
		numFlushes     = 5
	)

	oracle := newOracle()

	for flush := 0; flush < numFlushes; flush++ {
		for i := 0; i < seriesPerFlush; i++ {
			ref := storage.SeriesRef(flush*seriesPerFlush + i + 1)
			lset := testSeries(
				"__name__", "metric",
				"job", fmt.Sprintf("job_%d", flush),
				"instance", fmt.Sprintf("inst_%d", i),
			)
			idx.Add(ref, lset)
			oracle.add(ref, lset)
		}
		if flush < numFlushes-1 {
			require.NoError(t, idx.Flush())
		}
	}

	ctx := context.Background()
	reader := idx.Reader()

	// All series have __name__=metric.
	gotRefs, err := reader.Postings(ctx, "__name__", "metric")
	require.NoError(t, err)
	got := collectPostings(t, gotRefs)
	wantRefs := collectPostings(t, oracle.mp.Postings(ctx, "__name__", "metric"))

	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	sort.Slice(wantRefs, func(i, j int) bool { return wantRefs[i] < wantRefs[j] })
	assert.Equal(t, wantRefs, got)
	assert.Len(t, got, seriesPerFlush*numFlushes)

	// Each job should have seriesPerFlush series.
	for flush := 0; flush < numFlushes; flush++ {
		jobRefs, err := reader.Postings(ctx, "job", fmt.Sprintf("job_%d", flush))
		require.NoError(t, err)
		assert.Len(t, collectPostings(t, jobRefs), seriesPerFlush)
	}
}
