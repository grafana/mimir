// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

func TestHeadEvictionsTrackInsertReviveAndRemoval(t *testing.T) {
	b := newSeriesByName()
	insert := func(pod string, evicted bool) {
		stored := labels.FromSorted([][2]string{{"__name__", "up"}, {"pod", pod}})
		require.True(t, b.insert(labels.StableHash(stored), stored, Series{headEvicted: evicted}))
	}
	insert("active", false)
	insert("restored-evicted", true)
	_, ok := b.headLabelNames(0)
	require.False(t, ok)
	check := func() {
		count := 0
		b.forEach(func(entry *seriesEntry) {
			if entry.series.headEvicted {
				count++
			}
		})
		require.Equal(t, count, b.headEvicted)
	}
	b.forEach(func(entry *seriesEntry) {
		b.setHeadEvicted(&entry.series, true)
		b.setHeadEvicted(&entry.series, true)
	})
	check()
	b.forEach(func(entry *seriesEntry) { b.setHeadEvicted(&entry.series, false) })
	check()
	_, ok = b.headLabelNames(0)
	require.True(t, ok)
	b.forEach(func(entry *seriesEntry) { b.setHeadEvicted(&entry.series, true) })
	b.retainGroups(nil, func(*seriesEntry) bool { return false })
	check()
	require.Zero(t, b.headEvicted)
	insert("new", false)
	_, ok = b.headLabelNames(1)
	require.True(t, ok)
}
