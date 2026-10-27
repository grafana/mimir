// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	promcfg "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
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

// Ingest appends the batch with an appender, commits it, and rolls it back when a hard error stops it.
func (e *prometheusEngine) Ingest(ctx *ingestContext, batch ingestBatch) (ingestOutcome, error) {
	// Require commit stats on the Prometheus appender so wrappers cannot silently drop its duplicate accounting.
	app := e.db.AppenderV2(ctx).(interface {
		extendedAppender
		storage.CommitStatsReporter
	})
	outcome, err := ingestThroughAppender(ctx, app, batch)
	if err == nil {
		ctx.stats.commitDroppedSamples = app.CommitStats().DiscardedSamples
	}
	return outcome, err
}

// ingestThroughAppender appends the batch with the appender, commits it, and rolls it back when a hard error stops it.
func ingestThroughAppender(ctx *ingestContext, app extendedAppender, batch ingestBatch) (ingestOutcome, error) {
	outcome, err := pushSamplesToAppender(ctx, app, batch, nil, false, nil)
	if err != nil {
		if rollbackErr := app.Rollback(); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf("roll back the appender: %w", rollbackErr))
		}
		return outcome, err
	}

	startCommit := time.Now()
	if err := app.Commit(); err != nil {
		return outcome, err
	}
	outcome.CommitDuration = time.Since(startCommit)
	return outcome, nil
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

// Evict compacts the out-of-order head first, which persists the out-of-order data of the series before they
// leave, then the series themselves. The first failure doesn't stop the second: series without out-of-order data
// can still leave.
func (e *prometheusEngine) Evict(ctx context.Context, refs []storage.SeriesRef) error {
	oooErr := e.db.CompactOOOHead(ctx)
	if oooErr != nil {
		oooErr = fmt.Errorf("compact the out-of-order head: %w", oooErr)
	}
	selectedErr := e.db.CompactSelectedSeries(refs)
	if selectedErr != nil {
		selectedErr = fmt.Errorf("compact the selected series: %w", selectedErr)
	}
	return errors.Join(oooErr, selectedErr)
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

func openPrometheusEngine(dir string, logger *slog.Logger, reg prometheus.Registerer, opts *tsdb.Options) (tenantEngine, error) {
	db, err := tsdb.Open(dir, logger, reg, opts, nil)
	if err != nil {
		return nil, err
	}
	// The ingester compacts on its own schedule.
	db.DisableCompactions()
	return &prometheusEngine{db: db, blockDuration: opts.MinBlockDuration}, nil
}

func (h prometheusHead) Ownership(owned ring.TokenRanges) (int, []storage.SeriesRef) {
	var (
		count    int
		nonOwned []storage.SeriesRef
	)
	h.ForEachSecondaryHash(func(refs []chunks.HeadSeriesRef, secondaryHashes []uint32) {
		for i, hash := range secondaryHashes {
			if owned.IncludesKey(hash) {
				count++
			} else {
				nonOwned = append(nonOwned, storage.SeriesRef(refs[i]))
			}
		}
	})
	return count, nonOwned
}
