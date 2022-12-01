// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"sort"
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeriesChunkRef_Compare(t *testing.T) {
	input := []seriesChunkRef{
		{blockID: ulid.MustNew(0, nil), minTime: 2, maxTime: 5},
		{blockID: ulid.MustNew(1, nil), minTime: 1, maxTime: 5},
		{blockID: ulid.MustNew(2, nil), minTime: 1, maxTime: 3},
		{blockID: ulid.MustNew(3, nil), minTime: 4, maxTime: 7},
		{blockID: ulid.MustNew(4, nil), minTime: 3, maxTime: 6},
	}

	expected := []seriesChunkRef{
		{blockID: ulid.MustNew(2, nil), minTime: 1, maxTime: 3},
		{blockID: ulid.MustNew(1, nil), minTime: 1, maxTime: 5},
		{blockID: ulid.MustNew(0, nil), minTime: 2, maxTime: 5},
		{blockID: ulid.MustNew(4, nil), minTime: 3, maxTime: 6},
		{blockID: ulid.MustNew(3, nil), minTime: 4, maxTime: 7},
	}

	sort.Slice(input, func(i, j int) bool {
		return input[i].Compare(input[j]) > 0
	})

	assert.Equal(t, expected, input)
}

func TestSeriesChunkRefsIterator(t *testing.T) {
	c := generateSeriesChunkRef(5)
	series1 := labels.FromStrings(labels.MetricName, "metric_1")
	series2 := labels.FromStrings(labels.MetricName, "metric_2")
	series3 := labels.FromStrings(labels.MetricName, "metric_3")
	series4 := labels.FromStrings(labels.MetricName, "metric_4")

	t.Run("should iterate an empty set", func(t *testing.T) {
		it := newSeriesChunkRefsIterator(seriesChunkRefsSet{
			series: []seriesChunkRefs{},
		})

		require.True(t, it.Done())
		require.Zero(t, it.At())

		require.False(t, it.Next())
		require.True(t, it.Done())
		require.Zero(t, it.At())
	})

	t.Run("should iterate a set with some items", func(t *testing.T) {
		it := newSeriesChunkRefsIterator(seriesChunkRefsSet{
			series: []seriesChunkRefs{
				{lset: series1, chunks: []seriesChunkRef{c[0], c[1]}},
				{lset: series2, chunks: []seriesChunkRef{c[2]}},
				{lset: series3, chunks: []seriesChunkRef{c[3], c[4]}},
			},
		})

		require.False(t, it.Done())
		require.Zero(t, it.At())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series1, chunks: []seriesChunkRef{c[0], c[1]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series2, chunks: []seriesChunkRef{c[2]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series3, chunks: []seriesChunkRef{c[3], c[4]}}, it.At())
		require.False(t, it.Done())

		require.False(t, it.Next())
		require.True(t, it.Done())
		require.Zero(t, it.At())
	})

	t.Run("should re-initialize the internal state on reset()", func(t *testing.T) {
		it := newSeriesChunkRefsIterator(seriesChunkRefsSet{
			series: []seriesChunkRefs{
				{lset: series1, chunks: []seriesChunkRef{c[0], c[1]}},
				{lset: series2, chunks: []seriesChunkRef{c[2]}},
				{lset: series3, chunks: []seriesChunkRef{c[3], c[4]}},
			},
		})

		require.False(t, it.Done())
		require.Zero(t, it.At())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series1, chunks: []seriesChunkRef{c[0], c[1]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series2, chunks: []seriesChunkRef{c[2]}}, it.At())
		require.False(t, it.Done())

		// Reset.
		it.reset(seriesChunkRefsSet{
			series: []seriesChunkRefs{
				{lset: series1, chunks: []seriesChunkRef{c[3]}},
				{lset: series4, chunks: []seriesChunkRef{c[4]}},
			},
		})

		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series1, chunks: []seriesChunkRef{c[3]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series4, chunks: []seriesChunkRef{c[4]}}, it.At())
		require.False(t, it.Done())

		require.False(t, it.Next())
		require.True(t, it.Done())
		require.Zero(t, it.At())
	})
}

// sliceSeriesChunkRefsSetIterator implements seriesChunkRefsSetIterator and
// returns the provided err when the sets are exhausted.
//
//nolint:unused // dead code while we are working on PR 3355
type sliceSeriesChunkRefsSetIterator struct {
	current int
	sets    []seriesChunkRefsSet
	err     error
}

//nolint:unused // dead code while we are working on PR 3355
func newSliceSeriesChunkRefsSetIterator(err error, sets ...seriesChunkRefsSet) seriesChunkRefsSetIterator {
	return &sliceSeriesChunkRefsSetIterator{
		current: -1,
		sets:    sets,
		err:     err,
	}
}

//nolint:unused // dead code while we are working on PR 3355
func (s *sliceSeriesChunkRefsSetIterator) Next() bool {
	s.current++
	return s.current < len(s.sets)
}

//nolint:unused // dead code while we are working on PR 3355
func (s *sliceSeriesChunkRefsSetIterator) At() seriesChunkRefsSet {
	return s.sets[s.current]
}

//nolint:unused // dead code while we are working on PR 3355
func (s *sliceSeriesChunkRefsSetIterator) Err() error {
	if s.current >= len(s.sets) {
		return s.err
	}
	return nil
}

func generateSeriesChunkRef(num int) []seriesChunkRef {
	out := make([]seriesChunkRef, 0, num)

	for i := 0; i < num; i++ {
		out = append(out, seriesChunkRef{
			blockID: ulid.MustNew(uint64(i), nil),
			ref:     chunks.ChunkRef(i),
			minTime: int64(i),
			maxTime: int64(i),
		})
	}

	return out
}
