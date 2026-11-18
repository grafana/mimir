// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

// A count request holds expanded postings, groups and hashes, and the tenant's memory limit refuses
// it before the postings are read. The offsets-table count reads no postings, so the same limit
// answers it.
func TestBucketStore_SeriesCountsMemoryLimit(t *testing.T) {
	const series = 200
	specs := make(block.SeriesSpecs, 0, series)
	for i := range series {
		specs = append(specs, &block.SeriesSpec{
			Labels: labels.FromStrings("__name__", "m", "pod", fmt.Sprintf("p%03d", i)),
			Chunks: []chunks.Meta{chunkAt(0, 4*hourMs-1)},
		})
	}
	// 200 series need 2,400 bytes of postings to walk, one group of counts 65.
	const limit = 1000
	newStore := func(t *testing.T, opts ...prepareStoreConfigOption) (*BucketStore, ulid.ULID) {
		bkt, err := filesystem.NewBucket(t.TempDir())
		require.NoError(t, err)
		store, ids := prepareSeriesCountsStoreFromSpecs(t, bkt, t.TempDir(), []block.SeriesSpecs{specs}, nil, opts...)
		store.countMaxMemoryBytes = func() uint64 { return limit }
		return store, ids[0]
	}
	fetched := func(store *BucketStore, dataType string) float64 {
		return testutil.ToFloat64(store.metrics.cardinalityEstimateFetchedBytes.WithLabelValues(dataType))
	}
	requireRefusedByMemory := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		assert.Equal(t, cardinality.LimitMemory, cardinality.RefusedBy(err), err)
	}

	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			t.Run("a walk with no matchers is refused before the all-postings list is read", func(t *testing.T) {
				store, id := newStore(t, opts...)
				// The window cuts the block, so its series entries are needed.
				_, err := store.countSeries(t.Context(), &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id.String()}, StartMs: 0, EndMs: 4*hourMs - 1})
				requireRefusedByMemory(t, err)
				assert.Contains(t, err.Error(), "from the series counts in meta.json")
				assert.Zero(t, fetched(store, "postings"))
				assert.Zero(t, fetched(store, "series"))
			})

			t.Run("a walk with matchers is refused before its postings are expanded", func(t *testing.T) {
				store, id := newStore(t, opts...)
				m := []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "m"}}
				_, err := store.countSeries(t.Context(), &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id.String()}, Matchers: m, StartMs: 0, EndMs: 4*hourMs - 1})
				requireRefusedByMemory(t, err)
				assert.Contains(t, err.Error(), "postings lists to expand")
				assert.Zero(t, fetched(store, "postings"))
				assert.Zero(t, fetched(store, "series"))
			})

			t.Run("the offsets-table count of the same block is answered under the same limit", func(t *testing.T) {
				store, id := newStore(t, opts...)
				resp, err := store.countSeries(t.Context(), &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id.String()}, StartMs: 0, EndMs: 4 * hourMs})
				require.NoError(t, err)
				assert.Equal(t, map[string][]int64{"m": {series}}, countsByGroup(resp))
				assert.Equal(t, int64(1), resp.BlocksFromOffsetsTable)
				assert.Zero(t, fetched(store, "postings"))
			})

			t.Run("without the limit the walk is answered", func(t *testing.T) {
				store, id := newStore(t, opts...)
				store.countMaxMemoryBytes = func() uint64 { return 0 }
				resp, err := store.countSeries(t.Context(), &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id.String()}, StartMs: 0, EndMs: 4*hourMs - 1})
				require.NoError(t, err)
				assert.Equal(t, map[string][]int64{"m": {series}}, countsByGroup(resp))
			})

			t.Run("the hashes a walk holds are charged as they are counted", func(t *testing.T) {
				store, id := newStore(t, opts...)
				// The postings fit, the 200 hashes at 24 bytes each do not.
				store.countMaxMemoryBytes = func() uint64 { return 4000 }
				m := []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "m"}}
				_, err := store.countSeries(t.Context(), &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id.String()}, Matchers: m, StartMs: 0, EndMs: 4*hourMs - 1, Hashes: true})
				requireRefusedByMemory(t, err)
				assert.Contains(t, err.Error(), "the groups and hashes counted")

				store.countMaxMemoryBytes = func() uint64 { return 8000 }
				resp, err := store.countSeries(t.Context(), &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id.String()}, Matchers: m, StartMs: 0, EndMs: 4*hourMs - 1, Hashes: true})
				require.NoError(t, err)
				require.Len(t, resp.Groups, 1)
				assert.Len(t, resp.Groups[0].Hashes, series)
			})
		})
	}
}

// What a request takes is given back once each block is counted, so blocks walked one after the
// other need room for one block's postings, not for all of them.
func TestCountMemory(t *testing.T) {
	m := countMemory{limit: 100}
	require.NoError(t, m.take(60, "a"))
	err := m.take(41, "b")
	require.Error(t, err)
	assert.Equal(t, cardinality.LimitMemory, cardinality.RefusedBy(err))
	assert.Contains(t, err.Error(), "with b")
	assert.Equal(t, uint64(60), m.held.Load(), "a refused take holds nothing")
	m.release(60)
	require.NoError(t, m.take(100, "c"), "up to the limit")

	unlimited := countMemory{}
	require.NoError(t, unlimited.take(1<<40, "d"))
}
