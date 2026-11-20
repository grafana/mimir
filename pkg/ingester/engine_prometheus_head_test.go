// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
)

func TestHeadOwnership(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ranges ring.TokenRanges
		count  int
	}{
		{"all", ring.TokenRanges{0, math.MaxUint32}, 1000},
		{"half", ring.TokenRanges{0, 499}, 500},
		{"none", nil, 0},
		{"outside", ring.TokenRanges{1000, 2000}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, tracked := range []bool{false, true} {
				t.Run(strconv.FormatBool(tracked), func(t *testing.T) {
					db := newOwnershipTestDB(t, 1000)
					db.ownedTokenRanges = tc.ranges
					db.cfg.EarlyCompactionNonOwnedSeriesEnabled = tracked
					idx, err := db.Head().Index()
					require.NoError(t, err)
					defer func() { require.NoError(t, idx.Close()) }()
					nonOwned := map[storage.SeriesRef]bool{}
					count := db.Head().Ownership(tc.ranges, func(ref storage.SeriesRef) {
						require.False(t, nonOwned[ref], "duplicate callback")
						nonOwned[ref] = true
					})
					require.Equal(t, tc.count, count)
					require.Len(t, nonOwned, 1000-tc.count)

					db.Head().Ownership(nil, func(ref storage.SeriesRef) {
						builder := labels.NewScratchBuilder(2)
						var metas []chunks.Meta
						require.NoError(t, idx.Series(ref, &builder, &metas))
						lbls := builder.Labels()
						id, err := strconv.Atoi(lbls.Get("id"))
						require.NoError(t, err)
						owned := len(tc.ranges) > 0 && tc.ranges.IncludesKey(uint32(id))
						require.Equal(t, !owned, nonOwned[ref])
						db.activeSeries.UpdateSeries(lbls, ref, time.Now(), -1, false, idx)
					})
					require.Equal(t, tc.count, db.computeOwnedSeries())
					active, _, _, _ := db.activeSeries.Active()
					require.Equal(t, tc.count, active)
					db.Head().Ownership(nil, func(ref storage.SeriesRef) {
						require.Equal(t, !nonOwned[ref], db.activeSeries.ContainsRef(ref))
					})
					pending := db.takePendingNonOwnedRefs(time.Now().Add(time.Hour))
					if tracked {
						require.Len(t, pending, len(nonOwned))
						for _, ref := range pending {
							require.True(t, nonOwned[ref])
						}
					} else {
						require.Empty(t, pending)
					}
				})
			}
		})
	}
}

func BenchmarkHeadOwnership(b *testing.B) {
	db := newOwnershipTestDB(b, 10000)
	for _, dist := range []struct {
		name   string
		ranges ring.TokenRanges
		count  int
	}{
		{"all", ring.TokenRanges{0, math.MaxUint32}, 10000},
		{"half", ring.TokenRanges{0, 4999}, 5000},
		{"none", nil, 0},
	} {
		for _, op := range []string{"Compute", "ComputeTracked"} {
			b.Run(dist.name+"/"+op, func(b *testing.B) {
				db.ownedTokenRanges = dist.ranges
				db.cfg.EarlyCompactionNonOwnedSeriesEnabled = op == "ComputeTracked"
				b.ReportAllocs()
				var count int
				for b.Loop() {
					count = db.computeOwnedSeries()
				}
				require.Equal(b, dist.count, count)
			})
		}
	}
}

func newOwnershipTestDB(tb testing.TB, series int) *userTSDB {
	tb.Helper()
	opts := tsdb.DefaultOptions()
	opts.EnableSharding = true
	opts.OutOfOrderTimeWindow = time.Hour.Milliseconds()
	opts.SecondaryHashFunction = func(lbls labels.Labels) uint32 {
		id, err := strconv.Atoi(lbls.Get("id"))
		require.NoError(tb, err)
		return uint32(id)
	}
	raw, err := tsdb.Open(tb.TempDir(), promslog.NewNopLogger(), nil, opts, nil)
	require.NoError(tb, err)
	raw.DisableCompactions()
	tb.Cleanup(func() { require.NoError(tb, raw.Close()) })
	app := raw.AppenderV2(tb.Context())
	for n := 0; n < series; n++ {
		lbls := labels.FromStrings("__name__", "head_test", "id", strconv.Itoa(n))
		ref, err := app.Append(0, lbls, 0, 1000, 1, nil, nil, storage.AOptions{})
		require.NoError(tb, err)
		_, err = app.Append(ref, lbls, 0, 1001, 2, nil, nil, storage.AOptions{})
		require.NoError(tb, err)
	}
	require.NoError(tb, app.Commit())
	app = raw.AppenderV2(tb.Context())
	_, err = app.Append(0, labels.FromStrings("__name__", "head_test", "id", "0"), 0, 999, 0, nil, nil, storage.AOptions{})
	require.NoError(tb, err)
	require.NoError(tb, app.Commit())
	return &userTSDB{
		db:           raw,
		cfg:          &Config{},
		activeSeries: activeseries.NewActiveSeries(asmodel.NewMatchers(asmodel.CustomTrackersConfig{}), time.Minute, nil),
	}
}
