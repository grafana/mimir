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

// A select of a name visits only the shards that hold it, which series added to other shards since, and series that
// left for cold blocks, are still found in.
func TestEngineSelectOfANameFindsTheShardsThatGotItSince(t *testing.T) {
	ctx := context.Background()
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 16, IdleEvictionMs: 30 * 60_000, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	appendAt := func(ts int64, from, to int) {
		app := engine.Appender(ctx)
		for n := from; n < to; n++ {
			_, err := app.Append(0, promlabels.FromStrings("__name__", "metric", "pod", fmt.Sprintf("pod-%d", n)), 0, ts, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	}
	selected := func() int {
		q, err := engine.ChunkQuerier(0, 1<<40)
		require.NoError(t, err)
		defer func() { require.NoError(t, q.Close()) }()
		set := q.Select(ctx, true, nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric"))
		count := 0
		for set.Next() {
			count++
		}
		require.NoError(t, set.Err())
		return count
	}
	appendAt(0, 0, 1)
	require.Equal(t, 1, selected())
	bits, ok := engine.shardsOfName("metric")
	require.True(t, ok)
	require.Equal(t, 1, popcount(bits), "the one shard of the series")
	// Into most of the shards.
	appendAt(0, 1, 200)
	require.Equal(t, 200, selected())
	// The first series stop, and leave the head for cold blocks.
	appendAt(3*3_600_000, 100, 200)
	require.NoError(t, engine.Compact(ctx))
	require.Less(t, int(engine.NumSeries()), 200, "series left the head")
	require.Equal(t, 200, selected())
	// The other shards have no series of other names.
	_, ok = engine.shardsOfName("absent")
	require.True(t, ok)
	bits, _ = engine.shardsOfName("absent")
	require.Zero(t, bits)
}

func popcount(bits uint64) int {
	count := 0
	for ; bits != 0; bits &= bits - 1 {
		count++
	}
	return count
}
