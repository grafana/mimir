// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

func TestMatchersFindSortedLabelsAndTreatMissingLabelsAsEmpty(t *testing.T) {
	stored := labels.FromSorted([][2]string{{"__name__", "metric"}, {"first", "one"}, {"last", "two"}})
	for _, c := range []struct {
		kind        int32
		name, value string
		expected    bool
	}{
		{0, "last", "two", true},
		{0, "last", "one", false},
		{0, "absent", "", true},
		{1, "absent", "", false},
		{1, "absent", "two", true},
		{2, "absent", ".*", true},
		{3, "absent", ".+", true},
	} {
		compiled, err := compileMatchers([]LabelMatcher{matcher(c.kind, c.name, c.value)})
		require.NoError(t, err)
		require.Equal(t, c.expected, matches(stored, compiled), "%+v", c)
	}
}

// A group that lost half its series to head compaction gives back their slots: an entry's slot
// costs more than most series' other memory.
func TestRetainGivesBackTheSlotsOfRemovedSeries(t *testing.T) {
	b := newSeriesByName()
	for n := range 1000 {
		stored := labelsFromStrings("__name__", "m", "pod", fmt.Sprint(n))
		b.insert(stored.Hash(), stored, Series{ref: uint64(n + 1)})
	}
	b.retain(func(entry *seriesEntry) bool { return entry.series.ref%2 == 0 })
	g := b.groups[0]
	require.Len(t, g.entries, 500)
	require.LessOrEqual(t, cap(g.entries), 625)
	// Lookups still find every kept series.
	for n := range 1000 {
		stored := labelsFromStrings("__name__", "m", "pod", fmt.Sprint(n))
		found := g.lookup(stored.Hash(), func(entry *seriesEntry) bool { return entry.labels == stored })
		require.Equal(t, (n+1)%2 == 0, found, n)
	}
}

// BenchmarkRetainShard is what a head compaction holds a shard's lock for when series leave it: a shard of a 2M series
// tenant with 19 labels each, of which a share leaves.
func BenchmarkRetainShard(b *testing.B) {
	for _, leaving := range []int{2, 20, 50} {
		b.Run(fmt.Sprintf("leaving=%d%%", leaving), func(b *testing.B) {
			for range b.N {
				b.StopTimer()
				index := newSeriesByName()
				for n := range 31_000 {
					pairs := make([]string, 0, 38)
					pairs = append(pairs, "__name__", fmt.Sprintf("metric_%d", n%200))
					for label := 1; label < 19; label++ {
						value := fmt.Sprintf("shared_%d", (n+label)%40)
						if label >= 12 {
							value = fmt.Sprintf("unique_%d_%d", label, n)
						}
						pairs = append(pairs, fmt.Sprintf("label_%02d", label), value)
					}
					stored := labelsFromStrings(pairs...)
					index.insert(stored.Hash(), stored, Series{ref: uint64(n + 1)})
				}
				b.StartTimer()
				index.retain(func(entry *seriesEntry) bool { return int(entry.series.ref%100) >= leaving })
			}
		})
	}
}
