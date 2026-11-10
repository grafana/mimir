// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func BenchmarkIngesterCompaction(b *testing.B) {
	for _, operation := range []string{
		"EngineCompact", "UserCompact", "EngineFlush", "UserFlush", "EngineEvict", "EngineEvictOOO",
		"CompactBlocksRegular", "CompactBlocksForced", "CompactBlocksIdle", "CompactNonOwned", "CreateTSDBReplay",
	} {
		b.Run(operation, func(b *testing.B) {
			b.StopTimer()
			i := newCompactionBenchmarkIngester(b)
			if operation == "CompactBlocksIdle" {
				i.compactionIdleTimeout = time.Nanosecond
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				db, err := i.createTSDB(userID, 0)
				require.NoError(b, err)
				refs := seedCompactionBenchmark(b, db, operation == "EngineEvictOOO")
				i.tsdbs[userID] = db
				if operation == "CompactNonOwned" {
					db.pendingNonOwnedRefs = make(map[storage.SeriesRef]time.Time, len(refs))
					for _, ref := range refs {
						db.pendingNonOwnedRefs[ref] = time.Unix(0, 0)
					}
				}
				if operation == "CreateTSDBReplay" {
					require.NoError(b, db.Close())
					delete(i.tsdbs, userID)
				}

				// Rebuild the same head outside timing so later iterations cannot benchmark an already-compacted DB.
				b.StartTimer()
				switch operation {
				case "EngineCompact":
					err = db.engine().Compact(b.Context())
				case "UserCompact":
					err = db.Compact()
				case "EngineFlush":
					err = db.engine().Flush(b.Context(), math.MaxInt64)
				case "UserFlush":
					err = db.compactHead(math.MaxInt64)
				case "EngineEvict", "EngineEvictOOO":
					err = db.engine().Evict(b.Context(), refs)
				case "CompactBlocksRegular":
					i.compactBlocks(b.Context(), false, 0, nil)
				case "CompactBlocksForced", "CompactBlocksIdle":
					i.compactBlocks(b.Context(), operation == "CompactBlocksForced", math.MaxInt64, nil)
				case "CompactNonOwned":
					i.compactBlocksDueToNonOwnedSeries(b.Context(), 0)
				case "CreateTSDBReplay":
					db, err = i.createTSDB(userID, 0)
				}
				b.StopTimer()
				require.NoError(b, err)
				require.NotEmpty(b, db.Blocks(), "the measured operation must persist data")
				switch operation {
				case "EngineFlush", "UserFlush", "CompactBlocksForced", "CompactBlocksIdle":
					require.Zero(b, db.Head().NumSeries())
				case "EngineEvict", "EngineEvictOOO", "CompactNonOwned":
					require.Equal(b, uint64(500), db.Head().NumSeries())
				default:
					require.Greater(b, db.Head().MinTime(), int64(0))
				}
				require.NoError(b, db.Close())
				delete(i.tsdbs, userID)
				require.NoError(b, os.RemoveAll(i.cfg.BlocksStorageConfig.TSDB.BlocksDir(userID)))
			}
		})
	}
}

func BenchmarkIngesterCompactionNoop(b *testing.B) {
	for _, operation := range []string{"EngineFactory", "EngineCompact", "UserCompact", "EngineFlush", "UserFlush", "EngineEvict", "NextRange"} {
		b.Run(operation, func(b *testing.B) {
			i := newCompactionBenchmarkIngester(b)
			db, err := i.createTSDB(userID, 0)
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, db.Close()) })
			b.ReportAllocs()
			for b.Loop() {
				switch operation {
				case "EngineFactory":
					_ = db.engine()
				case "EngineCompact":
					err = db.engine().Compact(b.Context())
				case "UserCompact":
					err = db.Compact()
				case "EngineFlush":
					err = db.engine().Flush(b.Context(), math.MaxInt64)
				case "UserFlush":
					err = db.compactHead(math.MaxInt64)
				case "EngineEvict":
					err = db.engine().Evict(b.Context(), nil)
				case "NextRange":
					nextForcedHeadCompactionRange(2*time.Hour.Milliseconds(), 0, 3*time.Hour.Milliseconds(), math.MaxInt64)
				}
			}
			if operation != "EngineFactory" && operation != "NextRange" {
				require.NoError(b, err)
			}
		})
	}
}

func newCompactionBenchmarkIngester(b *testing.B) *Ingester {
	b.Helper()
	cfg := defaultIngesterTestConfig(b)
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 0
	cfg.EarlyCompactionNonOwnedSeriesEnabled = true
	cfg.EarlyCompactionNonOwnedSeriesMinGracePeriod = 0
	cfg.EarlyCompactionNonOwnedSeriesMaxGracePeriod = time.Nanosecond
	limits := defaultLimitsTestConfig()
	limits.EarlyHeadCompactionOwnedSeriesThreshold = 1
	limits.OutOfOrderTimeWindow = model.Duration(time.Hour)
	i, _, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, nil, "", nil)
	require.NoError(b, err)
	b.Cleanup(func() { i.lifecycler.StopAsync() })
	return i
}

func seedCompactionBenchmark(b *testing.B, db *userTSDB, ooo bool) []storage.SeriesRef {
	b.Helper()
	const seriesCount, samplesPerSeries = 1000, 12
	refs := make([]storage.SeriesRef, seriesCount)
	seriesLabels := make([]labels.Labels, seriesCount)
	for n := range seriesLabels {
		seriesLabels[n] = labels.FromStrings(model.MetricNameLabel, "compaction_benchmark", "series", strconv.Itoa(n))
	}
	app := db.db.AppenderV2(b.Context())
	for sample := 0; sample < samplesPerSeries; sample++ {
		for n, lbls := range seriesLabels {
			ref, err := app.Append(refs[n], lbls, 0, int64(sample)*20*time.Minute.Milliseconds(), float64(sample), nil, nil, storage.AOptions{})
			require.NoError(b, err)
			refs[n] = ref
		}
	}
	require.NoError(b, app.Commit())
	if ooo {
		app = db.db.AppenderV2(b.Context())
		for n, lbls := range seriesLabels {
			_, err := app.Append(refs[n], lbls, 0, 219*time.Minute.Milliseconds(), 1, nil, nil, storage.AOptions{})
			require.NoError(b, err)
		}
		require.NoError(b, app.Commit())
	}
	return refs[:seriesCount/2]
}
