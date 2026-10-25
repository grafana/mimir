// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

func TestEstimateMerger_CountsWhenHashesWereAsked(t *testing.T) {
	m := newEstimateMerger(context.Background(), true, 0)
	require.NoError(t, m.add("store-gateway 1.1.1.1", "up", nil, []uint64{1, 2}, nil))
	// An older store-gateway ignores the hashes field and sends counts.
	err := m.add("store-gateway 2.2.2.2", "up", []int64{3}, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store-gateway 2.2.2.2 sent counts instead of the label-set hashes")
	assert.Contains(t, err.Error(), "once every store-gateway is updated")

	// Without dedup, counts are what is asked for.
	m = newEstimateMerger(context.Background(), false, 0)
	require.NoError(t, m.add("ingester", "up", []int64{3}, nil, nil))
}

// A window held by blocks and ingesters is deduplicated by hash. A
// store-gateway that answers with counts must fail the estimate, not count
// its groups as 0.
func TestCardinalityEstimator_StoreGatewayIgnoresHashes(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	b1 := ulid.MustNew(1, nil)
	day := bucketindex.Blocks{{ID: b1, MinTime: 52 * hour, MaxTime: 76 * hour}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(day, &bucketindex.Metadata{}, nil)
	gw := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{3}}}})
	q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
	}}}}, queryStoreAfter: 12 * time.Hour}
	ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
		{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{1}}}, MinTimeMs: 87 * hour},
	}}}
	e := NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
	e.now = func() time.Time { return time.UnixMilli(100 * hour) }

	_, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 52 * hour, MaxT: 100 * hour})
	require.ErrorContains(t, err, "sent counts instead of the label-set hashes")
	assert.Equal(t, http.StatusInternalServerError, estimateHTTPStatus(err))
}

func TestEstimateMerger(t *testing.T) {
	// An HLL can't tell small integers apart, which label-set hashes never are.
	spread := func(n uint64) uint64 {
		n += 0x9e3779b97f4a7c15
		n = (n ^ (n >> 30)) * 0xbf58476d1ce4e5b9
		n = (n ^ (n >> 27)) * 0x94d049bb133111eb
		return n ^ (n >> 31)
	}
	spreadAll := func(ns ...uint64) []uint64 {
		out := make([]uint64, len(ns))
		for i, n := range ns {
			out[i] = spread(n)
		}
		return out
	}
	sketchOf := func(hashes ...uint64) []byte {
		sk, err := hyperloglog.NewSketch(cardinality.SketchPrecision, false)
		require.NoError(t, err)
		for _, h := range hashes {
			sk.InsertHash(spread(h))
		}
		b, err := sk.MarshalBinary()
		require.NoError(t, err)
		return b
	}
	type add struct {
		source, value string
		counts        []int64
		hashes        []uint64
		sketch        []byte
	}
	for name, tc := range map[string]struct {
		dedup     bool
		maxHeld   int64
		buckets   int
		adds      []add
		wantCount map[string][]int64
		estimated []string
		wantErr   string
	}{
		"counts of sources that share no series are added up": {
			adds: []add{
				{source: "store-gateway a", value: "up", counts: []int64{1, 2}},
				{source: "store-gateway b", value: "up", counts: []int64{3, 4}},
				{source: "store-gateway b", value: "x", counts: []int64{5, 6}},
			},
			wantCount: map[string][]int64{"up": {4, 6}, "x": {5, 6}},
		},
		"sources that disagree on the number of buckets fail": {
			adds: []add{
				{source: "store-gateway a", value: "up", counts: []int64{1, 2}},
				{source: "store-gateway b", value: "up", counts: []int64{3}},
			},
			wantErr: "store-gateway b returned 1 buckets for \"up\", expected 2",
		},
		"a source that ignored step_ms fails against the asked buckets": {
			buckets: 3,
			adds:    []add{{source: "store-gateway a", value: "up", counts: []int64{6}}},
			wantErr: "store-gateway a returned 1 buckets for \"up\" instead of the 3 asked for with step_ms",
		},
		"hashes are unioned, so a series in two sources counts once": {
			dedup: true,
			adds: []add{
				{source: "store-gateway a", value: "up", hashes: []uint64{1, 2, 3}},
				{source: "ingester", value: "up", hashes: []uint64{3, 4}},
			},
			wantCount: map[string][]int64{"up": {4}},
		},
		"a sketch from one source takes in the hashes of another and marks the group estimated": {
			dedup: true,
			adds: []add{
				{source: "store-gateway a", value: "up", sketch: sketchOf(1, 2, 3)},
				{source: "ingester", value: "up", hashes: spreadAll(3, 4)},
				{source: "ingester", value: "exact", hashes: spreadAll(9)},
			},
			wantCount: map[string][]int64{"up": {4}, "exact": {1}},
			estimated: []string{"up"},
		},
		"sketches of two sources are merged": {
			dedup: true,
			adds: []add{
				{source: "store-gateway a", value: "up", sketch: sketchOf(1, 2, 3)},
				{source: "store-gateway b", value: "up", sketch: sketchOf(3, 4)},
			},
			wantCount: map[string][]int64{"up": {4}},
			estimated: []string{"up"},
		},
		"a bad sketch is refused with the source named": {
			dedup:   true,
			adds:    []add{{source: "store-gateway a", value: "up", sketch: []byte("not a sketch")}},
			wantErr: "store-gateway a returned a bad sketch for \"up\"",
		},
		"holding more hashes than max-dedup-series is refused": {
			dedup:   true,
			maxHeld: 3,
			adds: []add{
				{source: "store-gateway a", value: "up", hashes: []uint64{1, 2}},
				{source: "store-gateway b", value: "up", hashes: []uint64{3, 4}},
			},
			wantErr: "more than 3 series hashes",
		},
		"hashes already held are not counted again against the limit": {
			dedup:   true,
			maxHeld: 2,
			adds: []add{
				{source: "store-gateway a", value: "up", hashes: []uint64{1, 2}},
				{source: "ingester", value: "up", hashes: []uint64{2, 1}},
			},
			wantCount: map[string][]int64{"up": {2}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := newEstimateMerger(context.Background(), tc.dedup, tc.maxHeld)
			m.buckets = tc.buckets
			var err error
			for _, a := range tc.adds {
				if err = m.add(a.source, a.value, a.counts, a.hashes, a.sketch); err != nil {
					break
				}
			}
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			var res CardinalityEstimateResult
			require.NoError(t, m.finish(&res))
			assert.Equal(t, tc.wantCount, res.Counts)
			var got []string
			for v := range res.Estimated {
				got = append(got, v)
			}
			assert.ElementsMatch(t, tc.estimated, got)
		})
	}
}
