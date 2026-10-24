// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
)

func TestDistributor_SeriesCounts(t *testing.T) {
	fixtures := [][]mimirpb.LabelAdapter{
		labelAdapters(model.MetricNameLabel, "test_1", "status", "200"),
		labelAdapters(model.MetricNameLabel, "test_1", "status", "500"),
		labelAdapters(model.MetricNameLabel, "test_2"),
	}

	// The test partitions turned ACTIVE two hours ago (preparePartitionsRing).
	sinceActive := time.Now().Add(-time.Hour).UnixMilli()
	beforeActive := time.Now().Add(-3 * time.Hour).UnixMilli()
	withinGracePeriod := time.Now().Add(-2*time.Hour + 5*time.Minute).UnixMilli()
	writeShardSmaller := prepareDefaultLimits()
	writeShardSmaller.IngestionPartitionsTenantShardSize = 4
	writeShardSmaller.IngestionPartitionsTenantWriteShardSize = 2

	for name, tc := range map[string]struct {
		cfg        prepConfig
		wantHashed bool
		// startMs is the window's start, 0 by default.
		startMs int64
		// shardSizeSinceMs is since when the ingesters have seen the
		// tenant's shard size unchanged, beforeActive by default.
		shardSizeSinceMs *int64
	}{
		"classic replication, three zones": {
			cfg: prepConfig{numIngesters: 3, happyIngesters: 3, numDistributors: 1, replicationFactor: 3, ingesterZones: []string{"zone-a", "zone-b", "zone-c"}},
		},
		"classic replication, one zone, replication factor 3": {
			cfg:        prepConfig{numIngesters: 6, happyIngesters: 6, numDistributors: 1, replicationFactor: 3},
			wantHashed: true,
		},
		"ingest storage, one partition": {
			cfg: prepConfig{numIngesters: 1, happyIngesters: 1, numDistributors: 1, ingestStorageEnabled: true, ingesterZones: []string{"zone-a", "zone-b"}, limits: prepareDefaultLimits()},
		},
		"ingest storage, several partitions": {
			cfg:        prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
			wantHashed: true,
		},
		"ingest storage, several partitions that changed inside the window": {
			cfg:        prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
			startMs:    beforeActive,
			wantHashed: true,
		},
		"ingest storage, several partitions that changed within the creation grace period before the window": {
			cfg:        prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
			startMs:    withinGracePeriod,
			wantHashed: true,
		},
		// Three series don't fill four partitions, and an ingester that
		// holds nothing answers without a shard size, as one with no TSDB
		// for the tenant does: it can't count a series twice.
		"ingest storage, several partitions active since before the window": {
			cfg:     prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
			startMs: sinceActive,
		},
		"ingest storage, several partitions active since before the window, shard size changed inside it": {
			cfg:              prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
			startMs:          sinceActive,
			shardSizeSinceMs: &sinceActive,
			wantHashed:       true,
		},
		"ingest storage, several partitions active since before the window, shard size not known": {
			cfg:              prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
			startMs:          sinceActive,
			shardSizeSinceMs: new(int64),
			wantHashed:       true,
		},
		"ingest storage, several partitions, written to fewer partitions than read": {
			cfg:        prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: writeShardSmaller},
			startMs:    sinceActive,
			wantHashed: true,
		},
		"classic replication, three zones, two ingesters each": {
			cfg:        prepConfig{numIngesters: 6, happyIngesters: 6, numDistributors: 1, replicationFactor: 3, ingesterZones: []string{"zone-a", "zone-b", "zone-c"}},
			wantHashed: true,
		},
		"classic replication, three zones, replication factor 2": {
			cfg:        prepConfig{numIngesters: 3, happyIngesters: 3, numDistributors: 1, replicationFactor: 2, ingesterZones: []string{"zone-a", "zone-b", "zone-c"}},
			wantHashed: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			for _, minimize := range []bool{false, true} {
				t.Run(fmt.Sprintf("minimize ingester requests: %t", minimize), func(t *testing.T) {
					cfg := tc.cfg
					cfg.configure = func(config *Config) { config.MinimizeIngesterRequests = minimize }
					ds, ingesters, _, _ := prepare(t, cfg)
					for _, ing := range ingesters {
						ing.seriesCountsShardSizeUnchangedSinceMs = beforeActive
						if tc.shardSizeSinceMs != nil {
							ing.seriesCountsShardSizeUnchangedSinceMs = *tc.shardSizeSinceMs
						}
					}
					ctx := user.InjectOrgID(context.Background(), "series-counts")
					ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
					for _, lbls := range fixtures {
						_, err := ds[0].Push(ctx, mockWriteRequest(lbls, 1, 100000))
						require.NoError(t, err)
					}

					// The mock ingesters count every series whatever the window.
					start, end := tc.startMs, max(tc.startMs, 100000)+100000
					res, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{StartTimestampMs: start, EndTimestampMs: end})
					require.NoError(t, err)
					assert.Equal(t, tc.wantHashed, res.Hashed)
					assert.Equal(t, map[string]int64{"test_1": 2, "test_2": 1}, combineSeriesCounts(res))

					byStatus, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{StartTimestampMs: start, EndTimestampMs: end, GroupBy: "status"})
					require.NoError(t, err)
					assert.Equal(t, map[string]int64{"200": 1, "500": 1, "": 1}, combineSeriesCounts(byStatus))

					_, err = ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{StartTimestampMs: start, EndTimestampMs: end, StepMs: 100000})
					if tc.wantHashed {
						require.ErrorIs(t, err, ErrSeriesCountsNeedOneIngesterPerZone)
					} else {
						require.NoError(t, err, "counts that add up carry buckets")
					}
				})
			}
		})
	}
}

// A series can be in more than one replication set, or on more than one
// ingester of a zone: after a tenant's partitions change inside the
// lookback, the old partition's head still holds a series whose new samples
// go to another partition, and after a classic ring change the old owner
// still holds it. Each such series must count once.
func TestDistributor_SeriesCounts_SeriesOnTwoIngestersOfAZone(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg prepConfig
		// moved reports whether to, which doesn't hold the series, holds it
		// after the move from from.
		moved func(from, to *mockIngester) bool
	}{
		"ingest storage, series in two partitions": {
			cfg: prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
			moved: func(from, to *mockIngester) bool {
				return to.partitionID() != from.partitionID()
			},
		},
		"classic replication, three zones, series on two ingesters of each zone": {
			cfg: prepConfig{numIngesters: 6, happyIngesters: 6, numDistributors: 1, replicationFactor: 3, ingesterZones: []string{"zone-a", "zone-b", "zone-c"}},
			moved: func(from, to *mockIngester) bool {
				return to.zone == from.zone
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ds, ingesters, _, _ := prepare(t, tc.cfg)
			ctx := user.InjectOrgID(context.Background(), "series-counts")
			ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
			_, err := ds[0].Push(ctx, mockWriteRequest(labelAdapters(model.MetricNameLabel, "test_1"), 1, 100000))
			require.NoError(t, err)
			req := &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 200000}
			_, err = ds[0].SeriesCounts(ctx, nil, req)
			require.NoError(t, err)

			// Copy the series to the ingesters that don't hold it, the
			// way it stays in the head it was written to.
			var holders, others []*mockIngester
			for _, ing := range ingesters {
				if len(ing.series()) > 0 {
					holders = append(holders, ing)
				} else {
					others = append(others, ing)
				}
			}
			require.NotEmpty(t, holders)
			copied := 0
			for _, to := range others {
				for _, from := range holders {
					if !tc.moved(from, to) {
						continue
					}
					to.Lock()
					if to.timeseries == nil {
						to.timeseries = map[uint32]*mimirpb.PreallocTimeseries{}
					}
					maps.Copy(to.timeseries, from.series())
					to.Unlock()
					copied++
					break
				}
			}
			require.NotZero(t, copied)

			res, err := ds[0].SeriesCounts(ctx, nil, req)
			require.NoError(t, err)
			assert.True(t, res.Hashed)
			assert.Equal(t, map[string]int64{"test_1": 1}, combineSeriesCounts(res))

			_, err = ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 200000, StepMs: 100000})
			require.ErrorIs(t, err, ErrSeriesCountsNeedOneIngesterPerZone)
		})
	}
}

// combineSeriesCounts adds up counts, or unions hashes, the way the querier
// does.
func combineSeriesCounts(res SeriesCountsResult) map[string]int64 {
	out := map[string]int64{}
	sets := map[string]map[uint64]struct{}{}
	for _, resp := range res.Responses {
		for _, g := range resp.Groups {
			if res.Hashed {
				if sets[g.Value] == nil {
					sets[g.Value] = map[uint64]struct{}{}
				}
				for _, h := range g.Hashes {
					sets[g.Value][h] = struct{}{}
				}
				continue
			}
			out[g.Value] += g.Counts[0]
		}
	}
	for v, set := range sets {
		out[v] = int64(len(set))
	}
	return out
}
