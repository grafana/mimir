// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"maps"
	"slices"
	"testing"
	"unsafe"

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
		groups := c.Groups()
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
		groups := c.Groups()
		assert.Equal(t, []*cardinalitypb.SeriesCountGroup{{Value: "1", Counts: []int64{2}}, {Value: "2", Counts: []int64{1}}}, groups)
	})

	t.Run("a chunk overlaps the window when it ends at or after its start and starts before its end", func(t *testing.T) {
		c, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 40})
		require.NoError(t, err)
		assert.True(t, c.InWindow([]ChunkRange{{MinTime: 39, MaxTime: 60}}))
		assert.False(t, c.InWindow([]ChunkRange{{MinTime: 40, MaxTime: 60}}), "the window end is excluded")
		assert.True(t, c.InWindow([]ChunkRange{{MinTime: -10, MaxTime: -1}, {MinTime: -5, MaxTime: 0}}), "one chunk in the window is enough")
	})

	t.Run("counts added in bulk", func(t *testing.T) {
		c, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 10})
		require.NoError(t, err)
		require.NoError(t, c.AddCount("a", 3))
		require.NoError(t, c.AddCount("a", 2))
		require.NoError(t, c.AddCount("b", 0))
		assert.Equal(t, []*cardinalitypb.SeriesCountGroup{{Value: "a", Counts: []int64{5}}}, c.Groups())
		assert.Equal(t, int64(5), c.Counted())
		hashes, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 10, Hashes: true})
		require.NoError(t, err)
		assert.Error(t, hashes.AddCount("a", 1))
	})

	t.Run("hashes", func(t *testing.T) {
		c, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 10, Hashes: true})
		require.NoError(t, err)
		for _, l := range []labels.Labels{a1, a2, a1, b1} {
			c.Add(l, []ChunkRange{{MinTime: 1, MaxTime: 2}})
		}
		groups := c.Groups()
		require.Len(t, groups, 2)
		assert.Equal(t, "a", groups[0].Value)
		want := []uint64{labels.StableHash(a1), labels.StableHash(a2)}
		slices.Sort(want)
		assert.Equal(t, want, groups[0].Hashes)
		assert.Equal(t, []uint64{labels.StableHash(b1)}, groups[1].Hashes)
	})

	t.Run("a group's key doesn't keep the series' label set alive", func(t *testing.T) {
		for name, opts := range map[string]CounterOptions{
			"counts": {MinT: 0, MaxT: 10, GroupBy: "pod"},
			"hashes": {MinT: 0, MaxT: 10, GroupBy: "pod", Hashes: true},
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
			"empty window":    {MinT: 10, MaxT: 10},
			"reversed window": {MinT: 10, MaxT: 5},
		} {
			_, err := NewSeriesCounter(opts)
			assert.Error(t, err, name)
		}
	})
}
