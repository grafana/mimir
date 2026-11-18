// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

// A per-block count answers every block from its offsets table alone, whatever the window, with
// each group tagged with its block, and reads no postings and no series entries.
func TestBucketStore_SeriesCountsPerBlock(t *testing.T) {
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			store, b1, b2 := prepareSeriesCountsStore(t, opts...)
			ctx := t.Context()
			fetched := func(dataType string) float64 {
				return testutil.ToFloat64(store.metrics.cardinalityEstimateFetchedBytes.WithLabelValues(dataType))
			}
			byBlock := func(resp *storegatewaypb.SeriesCountsResponse) map[string]map[string]int64 {
				out := map[string]map[string]int64{}
				for _, g := range resp.Groups {
					if out[g.BlockId] == nil {
						out[g.BlockId] = map[string]int64{}
					}
					require.Len(t, g.Counts, 1)
					out[g.BlockId][g.Value] = g.Counts[0]
				}
				return out
			}

			// The window cuts both blocks, and upA is in both: per block it counts in each.
			resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, StartMs: hourMs, EndMs: 5 * hourMs, PerBlock: true})
			require.NoError(t, err)
			assert.Equal(t, map[string]map[string]int64{
				b1.String(): {"up": 3, "errors_total": 1},
				b2.String(): {"up": 2},
			}, byBlock(resp))
			assert.ElementsMatch(t, []string{b1.String(), b2.String()}, resp.BlockIds)
			assert.Equal(t, int64(2), resp.BlocksFromOffsetsTable)
			assert.Positive(t, resp.PostingsOffsetsTableBytes)
			assert.Zero(t, resp.IndexBytes)
			assert.Zero(t, fetched("postings"))
			assert.Zero(t, fetched("series"))

			// Another label: the series without it count under the empty value.
			resp, err = store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 1, GroupBy: "pod", PerBlock: true})
			require.NoError(t, err)
			assert.Equal(t, map[string]map[string]int64{b1.String(): {"p1": 2, "p2": 1, "p3": 1}}, byBlock(resp))

			// max_groups applies to each block.
			_, err = store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, StartMs: 0, EndMs: 8 * hourMs, PerBlock: true, MaxGroups: 1})
			require.Error(t, err)
			assert.Equal(t, cardinality.LimitMaxGroups, cardinality.RefusedBy(err))
			resp, err = store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b2.String()}, StartMs: 0, EndMs: 8 * hourMs, PerBlock: true, MaxGroups: 1})
			require.NoError(t, err)
			assert.Len(t, resp.Groups, 1)

			// The groups are charged to the memory limit; the postings a walk would read are not.
			store.countMaxMemoryBytes = func() uint64 { return 10 }
			_, err = store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 1, PerBlock: true})
			require.Error(t, err)
			assert.Equal(t, cardinality.LimitMemory, cardinality.RefusedBy(err))
			store.countMaxMemoryBytes = nil

			for name, req := range map[string]*storegatewaypb.SeriesCountsRequest{
				"matchers": {BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 1, PerBlock: true, Matchers: []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "up"}}},
				"hashes":   {BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 1, PerBlock: true, Hashes: true},
			} {
				_, err := store.countSeries(ctx, req)
				assert.Equal(t, codes.InvalidArgument, status.Code(err), name)
			}
		})
	}
}

// A block counted from postings for a group-by label it does not hold puts every matching series in
// the empty value.
func TestBucketStore_SeriesCountsFromPostingsWithoutTheGroupByLabel(t *testing.T) {
	store, b1, _ := prepareSeriesCountsStore(t)
	up := []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "up"}}
	resp, err := store.countSeries(t.Context(), &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "absent"})
	require.NoError(t, err)
	assert.Equal(t, map[string][]int64{"": {3}}, countsByGroup(resp))
}
