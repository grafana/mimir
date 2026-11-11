// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
)

func BenchmarkEngineHead(b *testing.B) {
	for _, operation := range []string{"Head", "NumSeries", "TimeBounds", "OldestAppendableTime", "Index", "LabelValuesBytes", "Sync", "Ownership", "ShardSeries", "ComputeOwnedSeries", "ComputeOwnedSeriesTracked"} {
		b.Run(operation, func(b *testing.B) {
			db := newHeadTestDB(b, 1000)
			head := db.Head()
			owned := ring.TokenRanges{0, 499}
			db.ownedTokenRanges = owned
			db.cfg.EarlyCompactionNonOwnedSeriesEnabled = operation == "ComputeOwnedSeriesTracked"
			visited := 0
			visit := func(storage.SeriesRef) { visited++ }
			var err error
			b.ReportAllocs()
			for b.Loop() {
				switch operation {
				case "Head":
					_ = db.Head()
				case "NumSeries":
					runtime.KeepAlive(head.NumSeries())
				case "TimeBounds":
					runtime.KeepAlive(head.TimeBounds())
				case "OldestAppendableTime":
					timestamp, available := head.OldestAppendableTime()
					runtime.KeepAlive(timestamp)
					runtime.KeepAlive(available)
				case "Index":
					var idx tsdb.IndexReader
					idx, err = head.Index()
					if err == nil {
						err = idx.Close()
					}
				case "LabelValuesBytes":
					runtime.KeepAlive(head.LabelValuesBytes())
				case "Sync":
					err = head.Sync()
				case "Ownership":
					runtime.KeepAlive(head.Ownership(owned, visit))
				case "ShardSeries":
					var refs []storage.SeriesRef
					refs, err = index.ExpandPostings(head.ShardSeries(0, 3))
					runtime.KeepAlive(refs)
				case "ComputeOwnedSeries", "ComputeOwnedSeriesTracked":
					runtime.KeepAlive(db.computeOwnedSeries())
				}
			}
			require.NoError(b, err)
			runtime.KeepAlive(visited)
		})
	}
}

func newHeadTestDB(tb testing.TB, series int) *userTSDB {
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
