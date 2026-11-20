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

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

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

func (e prometheusEngine) Ingest(ctx *ingestContext, batch ingestBatch) (ingestOutcome, error) {
	i := ctx.i
	spanlog := spanlogger.FromContext(ctx, i.logger)
	app := e.db.AppenderV2(ctx).(extendedAppender)
	spanlog.DebugLog("event", "got appender for timeseries", "series", len(batch.Series))

	minAppendTime, minAppendTimeAvailable := e.db.Head().AppendableMinValidTime()
	outcome, err := i.pushSamplesToAppender(ctx, batch, app, minAppendTimeAvailable, minAppendTime)
	if err != nil {
		if rollbackErr := app.Rollback(); rollbackErr != nil {
			level.Warn(i.logger).Log("msg", "failed to rollback appender on error", "user", ctx.userID, "err", rollbackErr)
		}
		return ingestOutcome{}, err
	}

	i.metrics.appenderAddDuration.Observe(time.Since(batch.IngestedAt).Seconds())

	spanlog.DebugLog(
		"event", "start commit",
		"succeededSamplesCount", outcome.Samples,
		"failedSamplesCount", ctx.stats.failedSamplesCount,
		"succeededExemplarsCount", outcome.Exemplars,
		"failedExemplarsCount", ctx.stats.failedExemplarsCount,
	)

	startCommit := time.Now()
	if err := app.Commit(); err != nil {
		return ingestOutcome{}, err
	}
	outcome.CommitDuration = time.Since(startCommit)
	return outcome, nil
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
