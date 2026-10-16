// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"testing"

	"github.com/axiomhq/hyperloglog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		assert.Equal(t, []CounterGroup{{Value: "a", Counts: []int64{2}}}, groups)
		assert.Equal(t, int64(2), c.Counted())
	})

	t.Run("group_by another label", func(t *testing.T) {
		c, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 10, GroupBy: "pod"})
		require.NoError(t, err)
		for _, l := range []labels.Labels{a1, a2, b1} {
			c.Add(l, []ChunkRange{{MinTime: 0, MaxTime: 5}})
		}
		groups, err := c.Groups()
		require.NoError(t, err)
		assert.Equal(t, []CounterGroup{{Value: "1", Counts: []int64{2}}, {Value: "2", Counts: []int64{1}}}, groups)
	})

	t.Run("buckets count a series once each, from its chunks", func(t *testing.T) {
		c, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 40, Step: 10})
		require.NoError(t, err)
		c.Add(a1, []ChunkRange{{MinTime: 0, MaxTime: 4}, {MinTime: 5, MaxTime: 12}, {MinTime: 35, MaxTime: 50}})
		c.Add(a2, []ChunkRange{{MinTime: 20, MaxTime: 29}})
		groups, err := c.Groups()
		require.NoError(t, err)
		assert.Equal(t, []CounterGroup{{Value: "a", Counts: []int64{1, 1, 1, 1}}}, groups)
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

	t.Run("invalid options", func(t *testing.T) {
		for name, opts := range map[string]CounterOptions{
			"empty window":          {MinT: 10, MaxT: 10},
			"negative step":         {MinT: 0, MaxT: 10, Step: -1},
			"step doesn't divide":   {MinT: 0, MaxT: 10, Step: 3},
			"too many buckets":      {MinT: 0, MaxT: 2000, Step: 1},
			"hashes with a step":    {MinT: 0, MaxT: 10, Step: 5, Hashes: true},
			"negative sketch_above": {MinT: 0, MaxT: 10, SketchAbove: -1},
		} {
			_, err := NewSeriesCounter(opts)
			assert.Error(t, err, name)
		}
	})
}
