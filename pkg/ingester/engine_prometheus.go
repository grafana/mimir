// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"time"

	"github.com/go-kit/log/level"
	promcfg "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// NewestDataTime returns the max time of the head (in-order and out-of-order) and of the local blocks,
// or math.MinInt64 if the TSDB has no data.
//
// Head().MaxTime() alone is not enough. On open, it is raised to the max time of the newest in-order block,
// but blocks written from out-of-order data or by CompactSelectedSeries are skipped. After early compaction
// of non-owned series and a restart, the head can have series without any in-order sample, so its max time is
// math.MinInt64 while local blocks hold recent data. Block max times are aligned to the block range, so the
// result can be up to one block range newer than the newest sample.
func (e prometheusEngine) NewestDataTime() int64 {
	t := max(e.db.Head().MaxTime(), e.db.Head().MaxOOOTime())
	for _, b := range e.db.Blocks() {
		t = max(t, b.Meta().MaxTime)
	}
	return t
}

func (e prometheusEngine) Querier(mint, maxt int64) (storage.Querier, error) {
	return e.db.Querier(mint, maxt)
}

func (e prometheusEngine) ChunkQuerier(mint, maxt int64, unordered bool) (storage.ChunkQuerier, error) {
	if unordered {
		return e.db.UnorderedChunkQuerier(mint, maxt)
	}
	return e.db.ChunkQuerier(mint, maxt)
}

func (e prometheusEngine) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return e.db.ExemplarQuerier(ctx)
}

func (e prometheusEngine) Ingest(ctx *ingestContext, timeseries []mimirpb.PreallocTimeseries) error {
	i, userID, startAppend, stats := ctx.i, ctx.userID, ctx.startAppend, ctx.stats
	spanlog := spanlogger.FromContext(ctx, i.logger)
	app := e.db.AppenderV2(ctx).(extendedAppender)
	spanlog.DebugLog("event", "got appender for timeseries", "series", len(timeseries))

	minAppendTime, minAppendTimeAvailable := e.db.Head().AppendableMinValidTime()

	if pushSamplesToAppenderErr := i.pushSamplesToAppender(
		userID,
		timeseries,
		app,
		startAppend,
		stats,
		ctx.errProcessor,
		ctx.updateFirstPartial,
		ctx.activeSeries,
		i.limits.OutOfOrderTimeWindow(userID),
		minAppendTimeAvailable,
		minAppendTime,
		ctx.isOTLP,
	); pushSamplesToAppenderErr != nil {
		if err := app.Rollback(); err != nil {
			level.Warn(i.logger).Log("msg", "failed to rollback appender on error", "user", userID, "err", err)
		}

		return pushSamplesToAppenderErr
	}

	// At this point all samples have been added to the appender, so we can track the time it took.
	i.metrics.appenderAddDuration.Observe(time.Since(startAppend).Seconds())

	spanlog.DebugLog(
		"event", "start commit",
		"succeededSamplesCount", stats.succeededSamplesCount,
		"failedSamplesCount", stats.failedSamplesCount,
		"succeededExemplarsCount", stats.succeededExemplarsCount,
		"failedExemplarsCount", stats.failedExemplarsCount,
	)

	startCommit := time.Now()
	if err := app.Commit(); err != nil {
		return err
	}
	stats.commitDroppedSamples = app.CommitStats().DiscardedSamples

	commitDuration := time.Since(startCommit)
	i.metrics.appenderCommitDuration.Observe(commitDuration.Seconds())
	spanlog.DebugLog("event", "complete commit", "commitDuration", commitDuration.String())

	return nil
}

func (e prometheusEngine) Close() error {
	return e.db.Close()
}

func (e prometheusEngine) Compact(ctx context.Context) error {
	return e.db.Compact(ctx)
}

func (e prometheusEngine) Flush(ctx context.Context, upTo int64) error {
	h := e.db.Head()
	for {
		blockMinTime, blockMaxTime, isValid, isLast := nextForcedHeadCompactionRange(e.blockDuration, h.MinTime(), h.MaxTime(), upTo)
		if !isValid {
			break
		}

		if err := e.db.CompactHead(tsdb.NewRangeHead(h, blockMinTime, blockMaxTime)); err != nil {
			return err
		}

		// Do not check again if it was the last range.
		if isLast {
			break
		}
	}

	return e.db.CompactOOOHead(ctx)
}

func (e prometheusEngine) Configure(settings engineSettings) error {
	// DB.ApplyConfig only uses the storage settings.
	return e.db.ApplyConfig(&promcfg.Config{
		StorageConfig: promcfg.StorageConfig{
			ExemplarsConfig: &promcfg.ExemplarsConfig{MaxExemplars: settings.MaxExemplars},
			TSDBConfig: &promcfg.TSDBConfig{
				OutOfOrderTimeWindow: settings.OutOfOrderTimeWindow,
				// The caller resolves the default so ApplyConfig can reset the encoding when the tenant clears its limit.
				ChunkEncoding: promcfg.ChunkEncodingConfig{Floats: settings.FloatChunkEncoding},
			},
		},
	})
}
