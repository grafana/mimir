// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"math"
	"slices"
	"testing"

	"github.com/grafana/dskit/ring"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
)

func TestPrometheusEngineHead(t *testing.T) {
	db := newHeadTestDB(t, 8)
	head := db.Head()
	raw := db.db.Head()
	require.Equal(t, uint64(8), head.NumSeries())
	require.Equal(t, timeBounds{raw.MinTime(), raw.MaxTime(), raw.MinOOOTime(), raw.MaxOOOTime()}, head.TimeBounds())
	wantTime, wantAvailable := raw.AppendableMinValidTime()
	gotTime, gotAvailable := head.OldestAppendableTime()
	require.Equal(t, wantTime, gotTime)
	require.Equal(t, wantAvailable, gotAvailable)
	require.Equal(t, raw.LabelValuesBytes(), head.LabelValuesBytes())
	require.NoError(t, head.Sync())

	idx, err := head.Index()
	require.NoError(t, err)
	defer func() { require.NoError(t, idx.Close()) }()
	all, err := idx.Postings(t.Context(), "", "")
	require.NoError(t, err)
	refs, err := index.ExpandPostings(all)
	require.NoError(t, err)
	require.Len(t, refs, 8)

	for _, tc := range []struct {
		name  string
		owned ring.TokenRanges
		count int
	}{
		{"none", nil, 0},
		{"half", ring.TokenRanges{0, 3}, 4},
		{"all", ring.TokenRanges{0, math.MaxUint32}, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var nonOwned []storage.SeriesRef
			count := head.Ownership(tc.owned, func(ref storage.SeriesRef) { nonOwned = append(nonOwned, ref) })
			require.Equal(t, tc.count, count)
			require.Len(t, nonOwned, len(refs)-count)
			for _, ref := range nonOwned {
				require.Contains(t, refs, ref)
			}
		})
	}

	var sharded []storage.SeriesRef
	for shard := uint64(0); shard < 3; shard++ {
		refs, err := index.ExpandPostings(head.ShardSeries(shard, 3))
		require.NoError(t, err)
		require.True(t, slices.IsSorted(refs))
		sharded = append(sharded, refs...)
	}
	require.ElementsMatch(t, refs, sharded)
}
