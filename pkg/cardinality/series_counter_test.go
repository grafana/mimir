// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"maps"
	"slices"
	"testing"
	"unsafe"

	"github.com/axiomhq/hyperloglog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
)

func TestSeriesCounter(t *testing.T) {
	a1 := labels.FromStrings("__name__", "a", "pod", "1")
	a2 := labels.FromStrings("__name__", "a", "pod", "2")
	b1 := labels.FromStrings("__name__", "b", "pod", "1")

	t.Run("one count per group, only series with a chunk in the window", func(t *testing.T) {
		c, err := NewSeriesCounter(CounterOptions{MinT: 100, MaxT: 200})
		require.NoError(t, err)
		assert.True(t, c.Add(a1, []ChunkRange{{MinTime: 50, MaxTime: 100}}), "a chunk ending at the window start overlaps it")
		assert.True(t, c.Add(a2, []ChunkRange{{MinTime: 150, MaxTime: 300}}))
		assert.False(t, c.Add(b1, []ChunkRange{{MinTime: 200, MaxTime: 250}}), "the window end is excluded")
		assert.False(t, c.Add(b1, nil))
		groups, err := c.Groups()
		require.NoError(t, err)
		assert.Equal(t, []*cardinalitypb.SeriesCountGroup{{Value: "a", Counts: []int64{2}}}, groups)
		assert.Equal(t, int64(2), c.Counted())
		assert.Equal(t, 1, c.GroupCount())
	})

	t.Run("group_by another label", func(t *testing.T) {
		c, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 10, GroupBy: "pod"})
		require.NoError(t, err)
		for _, l := range []labels.Labels{a1, a2, b1} {
			c.Add(l, []ChunkRange{{MinTime: 0, MaxTime: 5}})
		}
		groups, err := c.Groups()
		require.NoError(t, err)
		assert.Equal(t, []*cardinalitypb.SeriesCountGroup{{Value: "1", Counts: []int64{2}}, {Value: "2", Counts: []int64{1}}}, groups)
	})

	t.Run("buckets count a series once each, from its chunks", func(t *testing.T) {
		c, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 40, Step: 10})
		require.NoError(t, err)
		c.Add(a1, []ChunkRange{{MinTime: 0, MaxTime: 4}, {MinTime: 5, MaxTime: 12}, {MinTime: 35, MaxTime: 50}})
		c.Add(a2, []ChunkRange{{MinTime: 20, MaxTime: 29}})
		groups, err := c.Groups()
		require.NoError(t, err)
		assert.Equal(t, []*cardinalitypb.SeriesCountGroup{{Value: "a", Counts: []int64{1, 1, 1, 1}}}, groups)
	})

	t.Run("buckets gathered across sources count the same as the chunks", func(t *testing.T) {
		chunks := [][]ChunkRange{
			{{MinTime: 0, MaxTime: 4}, {MinTime: 5, MaxTime: 12}},
			{{MinTime: 35, MaxTime: 50}},
			{{MinTime: -10, MaxTime: -1}}, // outside the window
		}
		for _, step := range []int64{0, 10} {
			fromChunks, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 40, Step: step})
			require.NoError(t, err)
			gathered, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 40, Step: step})
			require.NoError(t, err)
			var all []ChunkRange
			var touched SeriesBuckets
			for _, c := range chunks {
				all = append(all, c...)
				touched = gathered.MarkBuckets(touched, c)
			}
			assert.True(t, fromChunks.Add(a1, all))
			assert.True(t, gathered.AddGroupBuckets("a", 0, touched))
			assert.False(t, gathered.AddGroupBuckets("a", 0, gathered.MarkBuckets(nil, chunks[2])), "no chunk in the window")
			assert.Nil(t, gathered.MarkBuckets(nil, chunks[2]), "nothing allocated for a series with no chunk in the window")
			want, err := fromChunks.Groups()
			require.NoError(t, err)
			got, err := gathered.Groups()
			require.NoError(t, err)
			assert.Equal(t, want, got, "step %d", step)
			assert.Equal(t, fromChunks.Counted(), gathered.Counted())
		}
		c, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 40, Step: 10})
		require.NoError(t, err)
		assert.True(t, c.InWindow([]ChunkRange{{MinTime: 39, MaxTime: 60}}))
		assert.False(t, c.InWindow([]ChunkRange{{MinTime: 40, MaxTime: 60}}), "the window end is excluded")
		assert.Len(t, c.MarkBuckets(nil, []ChunkRange{{MinTime: 0, MaxTime: 1}}), 1, "one word for up to 64 buckets")
		wide, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 1000, Step: 10})
		require.NoError(t, err)
		touched := wide.MarkBuckets(nil, []ChunkRange{{MinTime: 995, MaxTime: 999}})
		assert.Len(t, touched, 2)
		assert.True(t, wide.AddGroupBuckets("a", 0, touched))
		groups, err := wide.Groups()
		require.NoError(t, err)
		assert.Equal(t, int64(1), groups[0].Counts[99])
		assert.Equal(t, int64(0), groups[0].Counts[98])
	})

	t.Run("hashes, and a sketch above the threshold", func(t *testing.T) {
		c, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 10, Hashes: true, SketchAbove: 1})
		require.NoError(t, err)
		for _, l := range []labels.Labels{a1, a2, a1, b1} {
			c.Add(l, []ChunkRange{{MinTime: 1, MaxTime: 2}})
		}
		groups, err := c.Groups()
		require.NoError(t, err)
		require.Len(t, groups, 2)
		assert.Equal(t, "a", groups[0].Value)
		require.NotEmpty(t, groups[0].Sketch, "two series is above sketch_above 1")
		sk, err := hyperloglog.NewSketch(SketchPrecision, false)
		require.NoError(t, err)
		require.NoError(t, sk.UnmarshalBinary(groups[0].Sketch))
		assert.Equal(t, uint64(2), sk.Estimate())
		assert.Equal(t, []uint64{labels.StableHash(b1)}, groups[1].Hashes)
	})

	t.Run("a group's key doesn't keep the series' label set alive", func(t *testing.T) {
		for name, opts := range map[string]CounterOptions{
			"counts": {MinT: 0, MaxT: 10, GroupBy: "pod"},
			"hashes": {MinT: 0, MaxT: 10, GroupBy: "pod", Hashes: true, SketchAbove: 1},
		} {
			c, err := NewSeriesCounter(opts)
			require.NoError(t, err)
			lset := labels.FromStrings("__name__", "a", "pod", "a-long-pod-name")
			require.True(t, c.Add(lset, []ChunkRange{{MinTime: 1, MaxTime: 2}}))
			keys := slices.Collect(maps.Keys(c.counts))
			if opts.Hashes {
				keys = slices.Collect(maps.Keys(c.sets))
			}
			require.Equal(t, []string{"a-long-pod-name"}, keys, name)
			assert.NotSame(t, unsafe.StringData(lset.Get("pod")), unsafe.StringData(keys[0]), "%s: the key is a copy of the value, not a slice of the label set", name)
		}
	})

	t.Run("invalid options", func(t *testing.T) {
		for name, opts := range map[string]CounterOptions{
			"empty window":                {MinT: 10, MaxT: 10},
			"negative step":               {MinT: 0, MaxT: 10, Step: -1},
			"step doesn't divide":         {MinT: 0, MaxT: 10, Step: 3},
			"too many buckets":            {MinT: 0, MaxT: 2000, Step: 1},
			"hashes with a step":          {MinT: 0, MaxT: 10, Step: 5, Hashes: true},
			"negative sketch_above":       {MinT: 0, MaxT: 10, SketchAbove: -1},
			"hashes without sketch_above": {MinT: 0, MaxT: 10, Hashes: true},
		} {
			_, err := NewSeriesCounter(opts)
			assert.Error(t, err, name)
		}
	})
}
