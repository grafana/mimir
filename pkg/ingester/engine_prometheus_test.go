// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
)

type failingOOOCompactor struct {
	tsdb.Compactor
	err error
}

func (c failingOOOCompactor) Write(dest string, block tsdb.BlockReader, mint, maxt int64, meta *tsdb.BlockMeta) ([]ulid.ULID, error) {
	if meta != nil && meta.Compaction.FromOutOfOrder() {
		return nil, c.err
	}
	return c.Compactor.Write(dest, block, mint, maxt, meta)
}

func TestPrometheusEngine_EvictOOOFailure(t *testing.T) {
	for _, progress := range []bool{false, true} {
		name := "without_progress"
		if progress {
			name = "with_progress"
		}
		t.Run(name, func(t *testing.T) {
			for _, caller := range []string{"engine", "ingester"} {
				t.Run(caller, func(t *testing.T) {
					i, db, refs, cause := newEvictionFailureFixture(t, progress)
					before := db.Head().NumSeries()
					if caller == "engine" {
						require.ErrorIs(t, db.engine().Evict(t.Context(), refs), cause)
					} else {
						db.requiresOwnedSeriesUpdate.Store("")
						i.compactBlocksDueToNonOwnedSeries(t.Context(), 0)
						wantReason, wantTriggers := "", float64(0)
						if progress {
							wantReason, wantTriggers = recomputeOwnedSeriesReasonEarlyCompaction, 1
						}
						require.Equal(t, wantReason, db.requiresOwnedSeriesUpdate.Load())
						require.Equal(t, wantTriggers, testutil.ToFloat64(i.metrics.earlyCompactionNonOwnedSeriesTriggered.WithLabelValues(userID)))
					}

					wantSeries, wantBlocks := before, 0
					if progress {
						wantSeries--
						wantBlocks = 1
					}
					require.Equal(t, wantSeries, db.Head().NumSeries())
					require.Len(t, db.Blocks(), wantBlocks)
					require.True(t, db.getLastEarlyCompaction().IsZero())

					// A failed OOO write must retain those samples, while successfully evicted samples come from a block.
					querier, err := db.Querier(0, 2000)
					require.NoError(t, err)
					defer func() { require.NoError(t, querier.Close()) }()
					series := querier.Select(t.Context(), true, nil, labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"))
					actual := map[string][]int64{}
					for series.Next() {
						s := series.At()
						it := s.Iterator(nil)
						for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
							require.Equal(t, chunkenc.ValFloat, typ)
							ts, value := it.At()
							require.Equal(t, float64(ts-999), value)
							metric := s.Labels().Get(model.MetricNameLabel)
							actual[metric] = append(actual[metric], ts)
						}
						require.NoError(t, it.Err())
					}
					require.NoError(t, series.Err())
					expected := map[string][]int64{"ooo": {999, 1000, 1001}}
					if progress {
						expected["in_order"] = []int64{1000, 1001}
					}
					require.Equal(t, expected, actual)
				})
			}
		})
	}
}

func newEvictionFailureFixture(t *testing.T, progress bool) (*Ingester, *userTSDB, []storage.SeriesRef, error) {
	t.Helper()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.EarlyCompactionNonOwnedSeriesEnabled = true
	cfg.EarlyCompactionNonOwnedSeriesMinGracePeriod = 0
	cfg.EarlyCompactionNonOwnedSeriesMaxGracePeriod = time.Nanosecond
	limits := defaultLimitsTestConfig()
	limits.EarlyHeadCompactionOwnedSeriesThreshold = 1
	limits.OutOfOrderTimeWindow = model.Duration(time.Hour)
	i, _, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", nil)
	require.NoError(t, err)
	t.Cleanup(func() { i.lifecycler.StopAsync() })
	db, err := i.createTSDB(userID, 0)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	cause := errors.New("injected OOO block write failure")
	opts := tsdb.DefaultOptions()
	opts.OutOfOrderTimeWindow = time.Hour.Milliseconds()
	opts.SeriesLifecycleCallback = db
	opts.NewCompactorFunc = func(ctx context.Context, reg prometheus.Registerer, logger *slog.Logger, ranges []int64, pool chunkenc.Pool, _ *tsdb.Options) (tsdb.Compactor, error) {
		compactor, err := tsdb.NewLeveledCompactor(ctx, reg, logger, ranges, pool, nil)
		return failingOOOCompactor{Compactor: compactor, err: cause}, err
	}
	db.db, err = tsdb.Open(db.db.Dir(), promslog.NewNopLogger(), nil, opts, nil)
	require.NoError(t, err)
	db.db.DisableCompactions()
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	i.tsdbs[userID] = db

	app := db.db.AppenderV2(t.Context())
	refs := make([]storage.SeriesRef, 0, 2)
	names := []string{"ooo"}
	if progress {
		names = append(names, "in_order")
	}
	for _, name := range names {
		lbls := labels.FromStrings(model.MetricNameLabel, name)
		ref, err := app.Append(0, lbls, 0, 1000, 1, nil, nil, storage.AOptions{})
		require.NoError(t, err)
		_, err = app.Append(ref, lbls, 0, 1001, 2, nil, nil, storage.AOptions{})
		require.NoError(t, err)
		refs = append(refs, ref)
	}
	require.NoError(t, app.Commit())
	app = db.db.AppenderV2(t.Context())
	_, err = app.Append(refs[0], labels.FromStrings(model.MetricNameLabel, "ooo"), 0, 999, 0, nil, nil, storage.AOptions{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	db.pendingNonOwnedRefs = map[storage.SeriesRef]time.Time{}
	for _, ref := range refs {
		db.pendingNonOwnedRefs[ref] = time.Unix(0, 0)
	}
	return i, db, refs, cause
}
