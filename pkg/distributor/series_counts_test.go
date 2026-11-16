// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util/limiter"
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
	// A window from ten minutes ago, which an INACTIVE partition's state
	// change of 50 minutes ago is before, grace period included.
	recentStart := time.Now().Add(-10 * time.Minute).UnixMilli()
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
		// ingestersIgnoreShardSizeCheck makes the ingesters answer like ones
		// too old to check the shard size themselves.
		ingestersIgnoreShardSizeCheck bool
	}{
		"classic replication, three zones": {
			cfg:        prepConfig{numIngesters: 3, happyIngesters: 3, numDistributors: 1, replicationFactor: 3, ingesterZones: []string{"zone-a", "zone-b", "zone-c"}},
			wantHashed: true,
		},
		"classic replication, one ingester": {
			cfg: prepConfig{numIngesters: 1, happyIngesters: 1, numDistributors: 1, replicationFactor: 1},
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
		// The distributor checks the shard size itself as well, for
		// ingesters that don't know the request field and so never refuse.
		"ingest storage, several partitions active since before the window, ingesters that leave the shard size check to the distributor": {
			cfg:                           prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
			startMs:                       sinceActive,
			ingestersIgnoreShardSizeCheck: true,
		},
		"ingest storage, several partitions active since before the window, shard size changed inside it, ingesters that leave the check to the distributor": {
			cfg:                           prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
			startMs:                       sinceActive,
			shardSizeSinceMs:              &sinceActive,
			ingestersIgnoreShardSizeCheck: true,
			wantHashed:                    true,
		},
		"ingest storage, several partitions active since before the window, shard size not known, ingesters that leave the check to the distributor": {
			cfg:                           prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
			startMs:                       sinceActive,
			shardSizeSinceMs:              new(int64),
			ingestersIgnoreShardSizeCheck: true,
			wantHashed:                    true,
		},
		// A partition INACTIVE since 50 minutes ago is still read, and even
		// over a window it stopped changing before, its series may also be
		// in the partition that took over, so the counts can't be added up.
		"ingest storage, several partitions that stopped changing before the window, one of them INACTIVE": {
			cfg:        prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, ingestStorageInactivePartitions: []int32{3}, limits: prepareDefaultLimits()},
			startMs:    recentStart,
			wantHashed: true,
		},
		"ingest storage, several partitions that stopped changing before the window, all ACTIVE": {
			cfg:     prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
			startMs: recentStart,
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
						ing.seriesCountsIgnoreShardSizeCheck = tc.ingestersIgnoreShardSizeCheck
					}
					ctx := user.InjectOrgID(context.Background(), "series-counts")
					ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
					for _, lbls := range fixtures {
						_, err := ds[0].Push(ctx, mockWriteRequest(lbls, 1, 100000))
						require.NoError(t, err)
					}

					// The mock ingesters count every series whatever the window.
					start, end := tc.startMs, max(tc.startMs, 100000)+100000
					res, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: start, EndMs: end})
					require.NoError(t, err)
					assert.Equal(t, tc.wantHashed, res.Hashed)
					assert.Equal(t, map[string]int64{"test_1": 2, "test_2": 1}, combineSeriesCounts(res))
					// A switch to hashes is counted once, by what showed the counts don't add up.
					wantSwitches := map[string]float64{"replication_sets": 0, "shard_size_changed": 0}
					switch {
					case tc.wantHashed && tc.shardSizeSinceMs != nil:
						wantSwitches["shard_size_changed"] = 1
					case tc.wantHashed:
						wantSwitches["replication_sets"] = 1
					}
					for reason, want := range wantSwitches {
						assert.Equal(t, want, testutil.ToFloat64(ds[0].seriesCountsSwitchedToHashes.WithLabelValues(reason)), reason)
					}

					byStatus, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: start, EndMs: end, GroupBy: "status"})
					require.NoError(t, err)
					assert.Equal(t, map[string]int64{"200": 1, "500": 1, "": 1}, combineSeriesCounts(byStatus))

					_, err = ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: start, EndMs: end, StepMs: 100000})
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

// The answers the distributor holds are charged to the memory consumption tracker of the request's
// context until the caller releases them, and answers over its limit fail the request.
func TestDistributor_SeriesCounts_ChargesAnswersToTheMemoryTracker(t *testing.T) {
	for name, cfg := range map[string]prepConfig{
		"classic replication, three zones":   {numIngesters: 3, happyIngesters: 3, numDistributors: 1, replicationFactor: 3, ingesterZones: []string{"zone-a", "zone-b", "zone-c"}},
		"ingest storage, several partitions": {numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
	} {
		t.Run(name, func(t *testing.T) {
			ds, _, _, _ := prepare(t, cfg)
			ctx := user.InjectOrgID(context.Background(), "series-counts")
			ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
			for n := range 20 {
				_, err := ds[0].Push(ctx, mockWriteRequest(labelAdapters(model.MetricNameLabel, fmt.Sprintf("test_%d", n)), 1, 100000))
				require.NoError(t, err)
			}
			req := func() *client.SeriesCountsRequest {
				return &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 200000, Hashes: true}
			}
			held := func(tracker *limiter.MemoryConsumptionTracker) uint64 {
				return tracker.CurrentEstimatedMemoryConsumptionBytesBySource(limiter.CardinalityEstimateGroups)
			}

			tracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			res, err := ds[0].SeriesCounts(limiter.AddMemoryTrackerToContext(ctx, tracker), nil, req())
			require.NoError(t, err)
			// Each of the 20 groups is held at least once, with its value and one hash.
			assert.GreaterOrEqual(t, held(tracker), uint64(20*(seriesCountsGroupBytes+len("test_0")+8)))
			res.Release()
			assert.Zero(t, held(tracker))
			res.Release()
			assert.Zero(t, held(tracker), "releasing twice gives back nothing more")

			tracker = limiter.NewMemoryConsumptionTracker(ctx, 100, promauto.With(nil).NewCounter(prometheus.CounterOpts{}), "")
			_, err = ds[0].SeriesCounts(limiter.AddMemoryTrackerToContext(ctx, tracker), nil, req())
			require.ErrorIs(t, err, ErrSeriesCountsOverMemoryLimit)
			assert.Zero(t, held(tracker), "nothing stays charged for a failed request")

			// Without a tracker nothing is charged.
			res, err = ds[0].SeriesCounts(ctx, nil, req())
			require.NoError(t, err)
			res.Release()
		})
	}
}

// The second round of requests, sent for hashes when the ingesters' answers show the shard size
// changed, gets what the first round left of max_index_bytes.
func TestDistributor_SeriesCounts_ShardSizeRetryKeepsWithinMaxIndexBytes(t *testing.T) {
	const maxIndexBytes = 8000
	sinceActive := time.Now().Add(-time.Hour).UnixMilli()

	for name, tc := range map[string]struct {
		// walkFirst is how many ingesters walk in the first round, by leaving the shard size check to the distributor.
		walkFirst   int
		wantRefused bool
		// wantAgain is each ingester's max_index_bytes in the second round.
		wantAgain int64
	}{
		"every ingester saw the change before walking": {walkFirst: 0, wantAgain: maxIndexBytes / 4},
		"one ingester walked in the first round":       {walkFirst: 1, wantAgain: (maxIndexBytes - maxIndexBytes/4) / 4},
		"every ingester walked in the first round":     {walkFirst: 4, wantRefused: true},
	} {
		t.Run(name, func(t *testing.T) {
			ds, ingesters, _, _ := prepare(t, prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()})
			ctx := user.InjectOrgID(context.Background(), "series-counts")
			ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
			// Every ingester holds a series, so each checks the shard size.
			series := map[uint32]*mimirpb.PreallocTimeseries{1: {TimeSeries: &mimirpb.TimeSeries{Labels: labelAdapters(model.MetricNameLabel, "test_1")}}}
			for n, ing := range ingesters {
				ing.Lock()
				ing.timeseries = maps.Clone(series)
				ing.Unlock()
				ing.seriesCountsShardSizeUnchangedSinceMs = sinceActive
				ing.seriesCountsIgnoreShardSizeCheck = n < tc.walkFirst
			}

			_, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: sinceActive, EndMs: sinceActive + 200000, MaxIndexBytes: maxIndexBytes})
			if tc.wantRefused {
				require.Error(t, err)
				assert.Equal(t, cardinality.LimitMaxIndexBytes, cardinality.RefusedBy(err))
			} else {
				require.NoError(t, err)
			}

			var walked, again int64
			for n, ing := range ingesters {
				ing.Lock()
				for _, call := range ing.calls["SeriesCounts"] {
					req := call[1].(*client.SeriesCountsRequest)
					if req.Hashes {
						again++
						assert.Equal(t, tc.wantAgain, req.MaxIndexBytes)
					} else if n >= tc.walkFirst {
						// Answered that the shard size changed, before walking.
						continue
					}
					walked += req.MaxIndexBytes
				}
				ing.Unlock()
			}
			assert.LessOrEqual(t, walked, int64(maxIndexBytes), "the walks of both rounds together")
			if tc.wantRefused {
				assert.Zero(t, again)
			} else {
				assert.Equal(t, int64(4), again)
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
			req := &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 200000}
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

			_, err = ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 200000, StepMs: 100000})
			require.ErrorIs(t, err, ErrSeriesCountsNeedOneIngesterPerZone)
		})
	}
}

// With classic replication a write succeeds at quorum, so each zone can miss
// a series the others hold. Whichever zones answer, every series counts once.
func TestDistributor_SeriesCounts_ClassicZonesMissDifferentSeries(t *testing.T) {
	for _, minimize := range []bool{false, true} {
		t.Run(fmt.Sprintf("minimize ingester requests: %t", minimize), func(t *testing.T) {
			cfg := prepConfig{numIngesters: 3, happyIngesters: 3, numDistributors: 1, replicationFactor: 3, ingesterZones: []string{"zone-a", "zone-b", "zone-c"}}
			cfg.configure = func(config *Config) { config.MinimizeIngesterRequests = minimize }
			ds, ingesters, _, _ := prepare(t, cfg)
			ctx := user.InjectOrgID(context.Background(), "series-counts")
			for _, job := range []string{"all", "not-in-a", "not-in-b", "not-in-c"} {
				_, err := ds[0].Push(ctx, mockWriteRequest(labelAdapters(model.MetricNameLabel, "test_1", "job", job), 1, 100000))
				require.NoError(t, err)
			}
			// A push returns at quorum.
			require.Eventually(t, func() bool {
				for _, ing := range ingesters {
					if len(ing.series()) != 4 {
						return false
					}
				}
				return true
			}, time.Second, 10*time.Millisecond)
			for _, ing := range ingesters {
				missing := "not-in-" + strings.TrimPrefix(ing.zone, "zone-")
				ing.Lock()
				for ref, ts := range ing.timeseries {
					if mimirpb.FromLabelAdaptersToLabels(ts.Labels).Get("job") == missing {
						delete(ing.timeseries, ref)
					}
				}
				ing.Unlock()
				require.Len(t, ing.series(), 3)
			}

			res, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 200000})
			require.NoError(t, err)
			assert.True(t, res.Hashed)
			assert.Equal(t, map[string]int64{"test_1": 4}, combineSeriesCounts(res))

			_, err = ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 200000, StepMs: 100000})
			require.ErrorIs(t, err, ErrSeriesCountsNeedOneIngesterPerZone)
			assert.NotContains(t, err.Error(), "need one ingester per zone", "this ring has one ingester per zone, and its counts still can't be added up")
			assert.Contains(t, err.Error(), "with ingest storage, one ingester per zone")
		})
	}
}

// A request for counts that the distributor must turn into one for hashes
// needs a sketch_above, since a group's hashes are sent in one message.
func TestDistributor_SeriesCounts_HashesNeedSketchAbove(t *testing.T) {
	ds, _, _, _ := prepare(t, prepConfig{numIngesters: 6, happyIngesters: 6, numDistributors: 1, replicationFactor: 3})
	ctx := user.InjectOrgID(context.Background(), "series-counts")
	ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
	_, err := ds[0].Push(ctx, mockWriteRequest(labelAdapters(model.MetricNameLabel, "test_1"), 1, 100000))
	require.NoError(t, err)

	res, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 200000})
	require.NoError(t, err)
	assert.True(t, res.Hashed)

	_, err = ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{StartMs: 0, EndMs: 200000})
	st, ok := grpcutil.ErrorToStatus(err)
	require.True(t, ok, err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "sketch_above")
}

func TestDistributor_SeriesCounts_Window(t *testing.T) {
	ds, _, _, _ := prepare(t, prepConfig{numIngesters: 2, happyIngesters: 2, numDistributors: 1, ingestStorageEnabled: true, ingestStoragePartitions: 1, ingesterZones: []string{"zone-a", "zone-b"}, limits: prepareDefaultLimits()})
	ctx := user.InjectOrgID(context.Background(), "series-counts")
	ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
	_, err := ds[0].Push(ctx, mockWriteRequest(labelAdapters(model.MetricNameLabel, "test_1"), 1, 100000))
	require.NoError(t, err)
	req := &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 300000, StepMs: 100000, WindowMs: 200000}

	res, err := ds[0].SeriesCounts(ctx, nil, req)
	require.NoError(t, err)
	require.NotEmpty(t, res.Responses)
	for _, resp := range res.Responses {
		for _, g := range resp.Groups {
			// Buckets [0, 200s) and [100s, 300s).
			assert.Len(t, g.Counts, 2, "the window is passed on")
		}
	}
}

func TestDistributor_SeriesCounts_Step(t *testing.T) {
	ds, ingesters, _, _ := prepare(t, prepConfig{numIngesters: 2, happyIngesters: 2, numDistributors: 1, ingestStorageEnabled: true, ingestStoragePartitions: 1, ingesterZones: []string{"zone-a", "zone-b"}, limits: prepareDefaultLimits()})
	ctx := user.InjectOrgID(context.Background(), "series-counts")
	ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
	_, err := ds[0].Push(ctx, mockWriteRequest(labelAdapters(model.MetricNameLabel, "test_1"), 1, 100000))
	require.NoError(t, err)
	// Three buckets of 100s.
	req := &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 300000, StepMs: 100000}

	res, err := ds[0].SeriesCounts(ctx, nil, req)
	require.NoError(t, err)
	require.NotEmpty(t, res.Responses)
	for _, resp := range res.Responses {
		for _, g := range resp.Groups {
			assert.Len(t, g.Counts, 3, "one count per bucket")
		}
	}

	// One zone answering one count per group is a failed replica: the other
	// zone answers, and the one-bucket counts are not merged with its own.
	// Calls that returned at quorum may still have requests running.
	ingesters[0].Lock()
	ingesters[0].seriesCountsIgnoreStep = true
	ingesters[0].Unlock()
	res, err = ds[0].SeriesCounts(ctx, nil, req)
	require.NoError(t, err)
	require.NotEmpty(t, res.Responses)
	for _, resp := range res.Responses {
		for _, g := range resp.Groups {
			assert.Len(t, g.Counts, 3, "the zone with one count per group is left out")
		}
	}

	// When every zone does, the request fails and says why.
	for _, ing := range ingesters {
		ing.Lock()
		ing.seriesCountsIgnoreStep = true
		ing.Unlock()
	}
	_, err = ds[0].SeriesCounts(ctx, nil, req)
	require.ErrorContains(t, err, `returned 1 buckets for "test_1" instead of the 3 asked for with step_ms`)
	for _, ing := range ingesters {
		assert.NotContains(t, err.Error(), ing.address(), "the error names the kind of component, not its address")
	}

	// Without a step one count per group is right.
	res, err = ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 300000})
	require.NoError(t, err)
	for _, resp := range res.Responses {
		for _, g := range resp.Groups {
			assert.Len(t, g.Counts, 1)
		}
	}
}

// A group with no counts is a malformed answer, which the distributor
// refuses rather than passing on as an empty group.
func TestDistributor_SeriesCounts_GroupWithoutCounts(t *testing.T) {
	ds, ingesters, _, _ := prepare(t, prepConfig{numIngesters: 2, happyIngesters: 2, numDistributors: 1, ingestStorageEnabled: true, ingestStoragePartitions: 1, ingesterZones: []string{"zone-a", "zone-b"}, limits: prepareDefaultLimits()})
	ctx := user.InjectOrgID(context.Background(), "series-counts")
	ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
	_, err := ds[0].Push(ctx, mockWriteRequest(labelAdapters(model.MetricNameLabel, "test_1"), 1, 100000))
	require.NoError(t, err)
	for _, ing := range ingesters {
		ing.Lock()
		ing.seriesCountsNoCounts = true
		ing.Unlock()
	}
	_, err = ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 300000})
	require.ErrorContains(t, err, `an ingester returned no counts for "test_1"`)
	for _, ing := range ingesters {
		assert.NotContains(t, err.Error(), ing.address(), "the error names the kind of component, not its address")
	}
}

// When a replication set can't reach quorum and one of the failed owners is
// too old for the request, the set fails with the older-version error,
// whatever failed with it and in whatever order, so the querier answers 422.
func TestDistributor_SeriesCounts_OlderOwnerDecidesTheError(t *testing.T) {
	unimplemented := status.Error(codes.Unimplemented, "unknown method SeriesCounts")
	unavailable := status.Error(codes.Unavailable, "connection refused")
	// Two owners of one replication set: the same partition under ingest
	// storage, the one token range of classic replication.
	owners := func(cfg prepConfig, ingesters []*mockIngester) (*mockIngester, *mockIngester) {
		for i, a := range ingesters {
			for _, b := range ingesters[i+1:] {
				if a.zone != b.zone && (!cfg.ingestStorageEnabled || a.partitionID() == b.partitionID()) {
					return a, b
				}
			}
		}
		require.FailNow(t, "no two owners of one replication set")
		return nil, nil
	}
	fail := func(ing *mockIngester, err error, delay time.Duration) {
		ing.Lock()
		defer ing.Unlock()
		ing.seriesCountsErr, ing.seriesCountsDelay = err, delay
	}
	for setup, cfg := range map[string]prepConfig{
		"classic replication": {numIngesters: 3, happyIngesters: 3, numDistributors: 1, replicationFactor: 3, ingesterZones: []string{"zone-a", "zone-b", "zone-c"}},
		"ingest storage":      {numIngesters: 2, happyIngesters: 2, numDistributors: 1, ingestStorageEnabled: true, ingestStoragePartitions: 1, ingesterZones: []string{"zone-a", "zone-b"}, limits: prepareDefaultLimits()},
	} {
		for _, minimize := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, minimize ingester requests: %t", setup, minimize), func(t *testing.T) {
				cfg := cfg
				cfg.configure = func(config *Config) { config.MinimizeIngesterRequests = minimize }
				for name, delays := range map[string][2]time.Duration{
					"the older ingester answers first": {0, 200 * time.Millisecond},
					"the older ingester answers last":  {200 * time.Millisecond, 0},
				} {
					t.Run(name, func(t *testing.T) {
						ds, ingesters, _, _ := prepare(t, cfg)
						ctx := user.InjectOrgID(context.Background(), "series-counts")
						older, other := owners(cfg, ingesters)
						fail(older, unimplemented, delays[0])
						fail(other, unavailable, delays[1])
						_, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 300000})
						require.Error(t, err)
						assert.Equal(t, codes.Unimplemented, status.Code(err), err)
					})
				}

				t.Run("other failures keep their own error", func(t *testing.T) {
					ds, ingesters, _, _ := prepare(t, cfg)
					ctx := user.InjectOrgID(context.Background(), "series-counts")
					first, second := owners(cfg, ingesters)
					fail(first, unavailable, 0)
					fail(second, unavailable, 0)
					_, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 300000})
					assert.Equal(t, codes.Unavailable, status.Code(err), err)
				})
			})
		}
	}
}

// An ingester too busy to count, at its -ingester.cardinality-estimate-max-concurrent,
// answers Unavailable, and another zone answers for it.
func TestDistributor_SeriesCounts_BusyZone(t *testing.T) {
	busy := cardinality.Busyf("too many cardinality estimate count walks running")
	for setup, cfg := range map[string]prepConfig{
		"classic replication": {numIngesters: 3, happyIngesters: 3, numDistributors: 1, replicationFactor: 3, ingesterZones: []string{"zone-a", "zone-b", "zone-c"}},
		"ingest storage":      {numIngesters: 2, happyIngesters: 2, numDistributors: 1, ingestStorageEnabled: true, ingestStoragePartitions: 1, ingesterZones: []string{"zone-a", "zone-b"}, limits: prepareDefaultLimits()},
	} {
		for _, minimize := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, minimize ingester requests: %t", setup, minimize), func(t *testing.T) {
				cfg := cfg
				cfg.configure = func(config *Config) { config.MinimizeIngesterRequests = minimize }
				ds, ingesters, _, _ := prepare(t, cfg)
				ctx := user.InjectOrgID(context.Background(), "series-counts")
				ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
				_, err := ds[0].Push(ctx, mockWriteRequest(labelAdapters(model.MetricNameLabel, "test_1"), 1, 100000))
				require.NoError(t, err)
				for _, ing := range ingesters {
					if ing.zone == "zone-a" {
						ing.Lock()
						ing.seriesCountsErr = busy
						ing.Unlock()
					}
				}
				res, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 200000})
				require.NoError(t, err)
				assert.Equal(t, map[string]int64{"test_1": 1}, combineSeriesCounts(res))

				// With every zone busy the caller gets the busy refusal, to answer 503.
				for _, ing := range ingesters {
					ing.Lock()
					ing.seriesCountsErr = busy
					ing.Unlock()
				}
				_, err = ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 200000})
				require.Error(t, err)
				assert.True(t, cardinality.IsBusy(err), err)
				assert.False(t, isSeriesCountsRefusal(err), "a busy ingester is not a terminal refusal")
			})
		}
	}
}

// An ingester's refusal ends the request with that refusal: no other owner of
// the replication set is asked, and a later failure doesn't replace it.
func TestDistributor_SeriesCounts_RefusalIsNotRetried(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "too many cardinality estimate count walks running")
	fail := func(ing *mockIngester, err error, delay time.Duration) {
		ing.Lock()
		defer ing.Unlock()
		ing.seriesCountsErr, ing.seriesCountsDelay = err, delay
	}
	req := &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 300000}
	for setup, cfg := range map[string]prepConfig{
		"classic replication": {numIngesters: 3, happyIngesters: 3, numDistributors: 1, replicationFactor: 3, ingesterZones: []string{"zone-a", "zone-b", "zone-c"}},
		"ingest storage":      {numIngesters: 2, happyIngesters: 2, numDistributors: 1, ingestStorageEnabled: true, ingestStoragePartitions: 1, ingesterZones: []string{"zone-a", "zone-b"}, limits: prepareDefaultLimits()},
	} {
		for _, minimize := range []bool{false, true} {
			cfg := cfg
			cfg.configure = func(config *Config) { config.MinimizeIngesterRequests = minimize }
			for name, refusal := range map[string]error{
				"limit refusal":        cardinality.Refusalf(cardinality.LimitMaxIndexBytes, "the request would read more than max_index_bytes"),
				"invalid request":      status.Error(codes.InvalidArgument, "max_groups can't be negative"),
				"estimate is disabled": status.Error(codes.FailedPrecondition, "cardinality estimate is disabled for the tenant"),
			} {
				t.Run(fmt.Sprintf("%s, minimize ingester requests: %t, %s", setup, minimize, name), func(t *testing.T) {
					t.Run("another owner fails later", func(t *testing.T) {
						ds, ingesters, _, _ := prepare(t, cfg)
						ctx := user.InjectOrgID(context.Background(), "series-counts")
						fail(ingesters[0], refusal, 0)
						fail(ingesters[1], unavailable, 200*time.Millisecond)
						_, err := ds[0].SeriesCounts(ctx, nil, req)
						require.Error(t, err)
						assert.Equal(t, status.Code(refusal), status.Code(err), err)
						assert.Equal(t, cardinality.RefusedBy(refusal), cardinality.RefusedBy(err))
					})

					t.Run("every owner refuses", func(t *testing.T) {
						ds, ingesters, _, _ := prepare(t, cfg)
						ctx := user.InjectOrgID(context.Background(), "series-counts")
						for _, ing := range ingesters {
							fail(ing, refusal, 0)
						}
						_, err := ds[0].SeriesCounts(ctx, nil, req)
						require.Error(t, err)
						assert.Equal(t, status.Code(refusal), status.Code(err), err)
						if !minimize {
							return
						}
						asked := 0
						for _, ing := range ingesters {
							ing.Lock()
							asked += len(ing.calls["SeriesCounts"])
							ing.Unlock()
						}
						// Classic replication starts with two of three zones.
						assert.LessOrEqual(t, asked, len(ingesters)-1, "the last owner is not asked")
					})
				})
			}
		}
	}
}

// With no zones and a replication factor above 1, an ingester that fails
// leaves every series on another, so the union of the others' hashes is the
// answer, within the failures the ring allows.
func TestDistributor_SeriesCounts_OneIngesterDownInOneZone(t *testing.T) {
	for _, minimize := range []bool{false, true} {
		t.Run(fmt.Sprintf("minimize ingester requests: %t", minimize), func(t *testing.T) {
			cfg := prepConfig{numIngesters: 6, happyIngesters: 6, numDistributors: 1, replicationFactor: 3}
			cfg.configure = func(config *Config) { config.MinimizeIngesterRequests = minimize }
			ds, ingesters, _, _ := prepare(t, cfg)
			ctx := user.InjectOrgID(context.Background(), "series-counts")
			for _, lbls := range [][]mimirpb.LabelAdapter{
				labelAdapters(model.MetricNameLabel, "test_1", "status", "200"),
				labelAdapters(model.MetricNameLabel, "test_1", "status", "500"),
				labelAdapters(model.MetricNameLabel, "test_2"),
			} {
				_, err := ds[0].Push(ctx, mockWriteRequest(lbls, 1, 100000))
				require.NoError(t, err)
			}
			ingesters[0].Lock()
			ingesters[0].seriesCountsErr = status.Error(codes.Unavailable, "connection refused")
			ingesters[0].Unlock()

			res, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 200000})
			require.NoError(t, err)
			assert.True(t, res.Hashed)
			assert.Equal(t, map[string]int64{"test_1": 2, "test_2": 1}, combineSeriesCounts(res))

			// Two failures are more than the ring allows for a replication
			// factor of 3.
			ingesters[1].Lock()
			ingesters[1].seriesCountsErr = status.Error(codes.Unavailable, "connection refused")
			ingesters[1].Unlock()
			_, err = ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 200000})
			assert.Equal(t, codes.Unavailable, status.Code(err), err)
		})
	}
}

// A slow count walk is not hedged onto another zone.
func TestDistributor_SeriesCounts_NoHedging(t *testing.T) {
	cfg := prepConfig{numIngesters: 3, happyIngesters: 3, numDistributors: 1, replicationFactor: 3, ingesterZones: []string{"zone-a", "zone-b", "zone-c"}}
	cfg.configure = func(config *Config) {
		config.MinimizeIngesterRequests = true
		config.MinimiseIngesterRequestsHedgingDelay = 10 * time.Millisecond
	}
	ds, ingesters, _, _ := prepare(t, cfg)
	ctx := user.InjectOrgID(context.Background(), "series-counts")
	_, err := ds[0].Push(ctx, mockWriteRequest(labelAdapters(model.MetricNameLabel, "test_1"), 1, 100000))
	require.NoError(t, err)
	for _, ing := range ingesters {
		ing.Lock()
		ing.seriesCountsDelay = 200 * time.Millisecond
		ing.Unlock()
	}
	res, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 200000})
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{"test_1": 1}, combineSeriesCounts(res))
	asked := 0
	for _, ing := range ingesters {
		ing.Lock()
		asked += len(ing.calls["SeriesCounts"])
		ing.Unlock()
	}
	// Three zones tolerate one unavailable, so the fewest requests are two
	// zones' ingesters; a hedged request would have asked the third.
	assert.Equal(t, 2, asked, "no hedged request to the third zone")
}

// max_index_bytes and max_hashes are split across the ingesters asked.
func TestDistributor_SeriesCounts_SplitsLimits(t *testing.T) {
	threeZones := []string{"zone-a", "zone-b", "zone-c"}
	for name, tc := range map[string]struct {
		cfg      prepConfig
		minimize bool
		// wantAsked is how many ingesters the limits are split across.
		wantAsked int64
	}{
		"ingest storage, four partitions": {
			cfg:       prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
			minimize:  true,
			wantAsked: 4,
		},
		"ingest storage, one partition with two owners": {
			cfg:       prepConfig{numIngesters: 2, happyIngesters: 2, numDistributors: 1, ingestStorageEnabled: true, ingestStoragePartitions: 1, ingesterZones: []string{"zone-a", "zone-b"}, limits: prepareDefaultLimits()},
			minimize:  true,
			wantAsked: 1,
		},
		"ingest storage, one partition with two owners, every owner asked": {
			cfg:       prepConfig{numIngesters: 2, happyIngesters: 2, numDistributors: 1, ingestStorageEnabled: true, ingestStoragePartitions: 1, ingesterZones: []string{"zone-a", "zone-b"}, limits: prepareDefaultLimits()},
			wantAsked: 2,
		},
		"classic replication, three zones of two ingesters": {
			cfg:       prepConfig{numIngesters: 6, happyIngesters: 6, numDistributors: 1, replicationFactor: 3, ingesterZones: threeZones},
			minimize:  true,
			wantAsked: 4,
		},
		"classic replication, three zones of two ingesters, every zone asked": {
			cfg:       prepConfig{numIngesters: 6, happyIngesters: 6, numDistributors: 1, replicationFactor: 3, ingesterZones: threeZones},
			wantAsked: 6,
		},
		"classic replication, no zones": {
			cfg:       prepConfig{numIngesters: 6, happyIngesters: 6, numDistributors: 1, replicationFactor: 3},
			minimize:  true,
			wantAsked: 5,
		},
		"classic replication, no zones, every ingester asked": {
			cfg:       prepConfig{numIngesters: 6, happyIngesters: 6, numDistributors: 1, replicationFactor: 3},
			wantAsked: 6,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.configure = func(config *Config) { config.MinimizeIngesterRequests = tc.minimize }
			ds, ingesters, _, _ := prepare(t, cfg)
			ctx := user.InjectOrgID(context.Background(), "series-counts")
			_, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 200000, Hashes: true, MaxIndexBytes: 6000, MaxHashes: 120})
			require.NoError(t, err)
			var asked int64
			for _, ing := range ingesters {
				ing.Lock()
				for _, call := range ing.calls["SeriesCounts"] {
					req := call[1].(*client.SeriesCountsRequest)
					assert.Equal(t, 6000/tc.wantAsked, req.MaxIndexBytes)
					assert.Equal(t, 120/tc.wantAsked, req.MaxHashes)
					asked++
				}
				ing.Unlock()
			}
			if tc.minimize {
				assert.Equal(t, tc.wantAsked, asked)
			} else {
				// A request not started by the time of quorum is not sent.
				assert.LessOrEqual(t, asked, tc.wantAsked)
			}
		})
	}
}

func TestDistributor_SeriesCounts_FanOutSpan(t *testing.T) {
	spans := testSpanExporter(t)

	ds, _, _, _ := prepare(t, prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()})
	ctx := user.InjectOrgID(context.Background(), "series-counts")
	_, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{SketchAbove: 1_000_000, StartMs: 0, EndMs: 200000, Hashes: true})
	require.NoError(t, err)
	var found bool
	for _, s := range spans.GetSpans() {
		if s.Name != "Distributor.SeriesCounts fan-out" {
			continue
		}
		found = true
		attrs := map[string]int64{}
		for _, a := range s.Attributes {
			attrs[string(a.Key)] = a.Value.AsInt64()
		}
		assert.Equal(t, int64(4), attrs["replication_sets"], "one per partition")
		assert.Equal(t, int64(4), attrs["ingesters_answered"])
	}
	assert.True(t, found)
}

// A zone holds the replication set from the latest start of its ingesters,
// and the merged answer from the latest zone; an ingester that holds nothing
// for the tenant, reporting 0, is left out.
func TestMaxOverZones_MinTime(t *testing.T) {
	up := func(n int64) []*cardinalitypb.SeriesCountGroup {
		return []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{n}}}
	}
	out := maxOverZones(map[string][]*client.SeriesCountsResponse{
		"zone-a": {{Groups: up(1), MinTimeMs: 50}, {Groups: up(1), MinTimeMs: 90}, {MinTimeMs: 0}},
		"zone-b": {{Groups: up(2), MinTimeMs: 70}},
	})
	assert.Equal(t, int64(90), out.MinTimeMs, "zone-a holds everything only from 90")
	assert.Equal(t, []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}, out.Groups)

	out = maxOverZones(map[string][]*client.SeriesCountsResponse{
		"zone-a": {{MinTimeMs: 0}},
		"zone-b": {{Groups: up(2), MinTimeMs: 70}},
	})
	assert.Equal(t, int64(70), out.MinTimeMs)
}

// Zones that hold different series, or whose ingesters add up differently,
// merge to the largest count per group and bucket, with every group of
// every zone kept.
func TestMaxOverZones_DifferentZones(t *testing.T) {
	g := func(v string, counts ...int64) *cardinalitypb.SeriesCountGroup {
		return &cardinalitypb.SeriesCountGroup{Value: v, Counts: counts}
	}
	out := maxOverZones(map[string][]*client.SeriesCountsResponse{
		// One ingester, with a group zone-b does not have.
		"zone-a": {{Groups: []*cardinalitypb.SeriesCountGroup{g("errors", 1, 0), g("up", 3, 1)}, SeriesCounted: 5}},
		// Two ingesters whose counts add up to up 2,5.
		"zone-b": {{Groups: []*cardinalitypb.SeriesCountGroup{g("up", 2, 4)}, SeriesCounted: 6}, {Groups: []*cardinalitypb.SeriesCountGroup{g("up", 0, 1)}, SeriesCounted: 1}},
	})
	assert.Equal(t, []*cardinalitypb.SeriesCountGroup{g("errors", 1, 0), g("up", 3, 5)}, out.Groups)
	assert.Equal(t, int64(7), out.SeriesCounted, "the largest zone's series")
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
