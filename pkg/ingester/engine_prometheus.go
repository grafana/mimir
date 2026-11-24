// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"log/slog"
	"time"

	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	promcfg "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

func openPrometheusEngine(dir string, logger *slog.Logger, reg prometheus.Registerer, opts *tsdb.Options) (tenantEngine, error) {
	db, err := tsdb.Open(dir, logger, reg, opts, nil)
	if err != nil {
		return nil, err
	}
	// The ingester schedules compaction alongside its other tenant maintenance.
	db.DisableCompactions()
	return prometheusEngine{db: db, blockDuration: opts.MinBlockDuration}, nil
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

	commitDuration := time.Since(startCommit)
	i.metrics.appenderCommitDuration.Observe(commitDuration.Seconds())
	spanlog.DebugLog("event", "complete commit", "commitDuration", commitDuration.String())

	return nil
}

func (e prometheusEngine) Blocks() []*tsdb.Block {
	return e.db.Blocks()
}

func (e prometheusEngine) BlocksToDelete(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
	return tsdb.DefaultBlocksToDelete(e.db)(blocks)
}

func (e prometheusEngine) CompactOOOHead(ctx context.Context) error {
	return e.db.CompactOOOHead(ctx)
}

func (e prometheusEngine) CompactSelectedSeries(refs []storage.SeriesRef) error {
	return e.db.CompactSelectedSeries(refs)
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
