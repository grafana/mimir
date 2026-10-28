// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// Shrinking the groups' entries to their series keeps every series where appends, lookups by ref and queries find it,
// and gives back most of their room.
func TestShrinkingEntriesKeepsTheSeries(t *testing.T) {
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	const count = 5000
	lset := func(n int) promlabels.Labels {
		return promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%37), "pod", fmt.Sprintf("pod-%d", n))
	}
	refs := make([]storage.SeriesRef, count)
	appendAt := func(ts int64) {
		app := engine.Appender(context.Background())
		for n := range count {
			ref, err := app.Append(refs[n], lset(n), 0, ts, float64(n), nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
			refs[n] = ref
		}
		require.NoError(t, app.Commit())
	}
	appendAt(1000)
	room := func() (series, room int) {
		for _, shard := range engine.store.shards {
			shard.RLock()
			s, r := shard.tenants["tenant"].series.entriesSlack()
			shard.RUnlock()
			series, room = series+s, room+r
		}
		return series, room
	}
	before, roomBefore := room()
	// A budget of one group at a time, so the pass goes on from where it left.
	defer func(budget int) { shrinkBudget = budget }(shrinkBudget)
	shrinkBudget = 1
	engine.shrinkEntries()
	after, roomAfter := room()
	require.Equal(t, before, after)
	require.Less(t, roomAfter, roomBefore/2, "most of the room is given back")

	// The refs still find their series, the same ones, and appends by labels too.
	previous := slicesClone(refs)
	appendAt(2000)
	require.Equal(t, previous, refs, "appends by ref and labels find the same series")
	q, err := engine.ChunkQuerier(0, 3000)
	require.NoError(t, err)
	defer q.Close()
	set := q.Select(context.Background(), true, nil, promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "metric_.*"))
	series := 0
	for set.Next() {
		samples := 0
		chunks := set.At().Iterator(nil)
		for chunks.Next() {
			it := chunks.At().Chunk.Iterator(nil)
			for it.Next() != 0 {
				samples++
			}
		}
		require.Equal(t, 2, samples, set.At().Labels().String())
		series++
	}
	require.NoError(t, set.Err())
	require.Equal(t, count, series)
	// New series after a shrink grow the groups again.
	app := engine.Appender(context.Background())
	_, err = app.Append(0, promlabels.FromStrings("__name__", "metric_1", "pod", "new"), 0, 2000, 1, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	grown, _ := room()
	require.Equal(t, count+1, grown)
}

// Shrinking the shared postings to their series keeps every series an equality matcher selects by them, and gives back
// most of their room.
func TestShrinkingPostingsKeepsTheSeries(t *testing.T) {
	ctx := t.Context()
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	const count = 5000
	app := engine.Appender(ctx)
	for n := range count {
		_, err := app.Append(0, promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%37), "pod", fmt.Sprintf("pod-%d", n%7), "series", fmt.Sprint(n)), 0, 1000, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	room := func() (ids, room int) {
		for _, shard := range engine.store.shards {
			shard.RLock()
			for _, list := range shard.tenants["tenant"].series.sharedPostings {
				ids, room = ids+len(list), room+cap(list)-len(list)
			}
			shard.RUnlock()
		}
		return ids, room
	}
	selectPod := func(pod string) int {
		q, err := engine.ChunkQuerier(0, 2000)
		require.NoError(t, err)
		defer func() { require.NoError(t, q.Close()) }()
		set := q.Select(ctx, true, nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "pod", pod))
		series := 0
		for set.Next() {
			series++
		}
		require.NoError(t, set.Err())
		return series
	}
	before, roomBefore := room()
	// A budget of one list at a time, so the pass goes on from where it left.
	defer func(budget int) { shrinkBudget = budget }(shrinkBudget)
	shrinkBudget = 1
	engine.shrinkPostings()
	after, roomAfter := room()
	require.Equal(t, before, after)
	require.Less(t, roomAfter, roomBefore/2, "most of the room is given back")
	for pod := range 7 {
		require.Equal(t, count/7+min(1, max(0, count%7-pod)), selectPod(fmt.Sprintf("pod-%d", pod)))
	}
}

func slicesClone[T any](s []T) []T { return append([]T(nil), s...) }
