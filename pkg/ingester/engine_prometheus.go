// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"time"

	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
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
func (e *prometheusEngine) NewestDataTime() int64 {
	t := max(e.db.Head().MaxTime(), e.db.Head().MaxOOOTime())
	for _, b := range e.db.Blocks() {
		t = max(t, b.Meta().MaxTime)
	}
	return t
}

func (e *prometheusEngine) Querier(mint, maxt int64) (storage.Querier, error) {
	return e.db.Querier(mint, maxt)
}

func (e *prometheusEngine) ChunkQuerier(mint, maxt int64, unordered bool) (storage.ChunkQuerier, error) {
	if unordered {
		return e.db.UnorderedChunkQuerier(mint, maxt)
	}
	return e.db.ChunkQuerier(mint, maxt)
}

func (e *prometheusEngine) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return e.db.ExemplarQuerier(ctx)
}

func (e *prometheusEngine) Ingest(ctx *ingestContext, timeseries []mimirpb.PreallocTimeseries) error {
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

func (e *prometheusEngine) Close() error {
	return e.db.Close()
}

func (e *prometheusEngine) Compact(ctx context.Context) error {
	return e.db.Compact(ctx)
}

func (e *prometheusEngine) Flush(ctx context.Context, upTo int64) error {
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

func (e *prometheusEngine) Configure(settings engineSettings) error {
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

func (e *prometheusEngine) BlockShippingEnabled() bool {
	return e.shipper != nil
}

func (e *prometheusEngine) ShipBlocks(ctx context.Context) {
	if !e.BlockShippingEnabled() {
		return
	}
	uploaded, err := e.shipper.Sync(ctx)
	if err != nil {
		level.Warn(e.shippingLogger).Log("msg", "shipper failed to synchronize TSDB blocks with the storage", "uploaded", uploaded, "err", err)
	} else {
		level.Debug(e.shippingLogger).Log("msg", "shipper successfully synchronized TSDB blocks with storage", "uploaded", uploaded)
	}

	// The shipper meta file could be updated even if the Sync() returned an error,
	// so it's safer to update it each time at least a block has been uploaded.
	// Moreover, the shipper meta file could be updated even if no blocks are uploaded
	// (eg. blocks removed due to retention) but doesn't cause any harm not updating
	// the cached list of blocks in such case, so we're not handling it.
	if uploaded > 0 {
		if err := e.updateCachedShippedBlocks(); err != nil {
			level.Error(e.shippingLogger).Log("msg", "failed to update cached shipped blocks after shipper synchronisation", "err", err)
		}
	}
}

// updateCachedShippedBlocks reads the shipper meta file and updates the cached shipped blocks.
func (e *prometheusEngine) updateCachedShippedBlocks() error {
	shippedBlocks, err := readShippedBlocks(e.db.Dir())
	if err != nil {
		return err
	}

	// Cache it.
	e.shippedBlocksMtx.Lock()
	e.shippedBlocks = shippedBlocks
	e.shippedBlocksMtx.Unlock()

	return nil
}

// getCachedShippedBlocks returns the cached shipped blocks.
func (e *prometheusEngine) getCachedShippedBlocks() map[ulid.ULID]time.Time {
	e.shippedBlocksMtx.Lock()
	defer e.shippedBlocksMtx.Unlock()

	// It's safe to directly return the map because it's never updated in-place.
	return e.shippedBlocks
}
