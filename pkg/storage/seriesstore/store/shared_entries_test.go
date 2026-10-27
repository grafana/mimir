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

func slicesClone[T any](s []T) []T { return append([]T(nil), s...) }
