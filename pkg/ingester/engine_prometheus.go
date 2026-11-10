// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

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

func (e prometheusEngine) Compact(ctx context.Context) error {
	return e.db.Compact(ctx)
}

func (e prometheusEngine) Flush(ctx context.Context, upTo int64) error {
	head := e.db.Head()
	for {
		blockMinTime, blockMaxTime, isValid, isLast := nextForcedHeadCompactionRange(e.blockDuration, head.MinTime(), head.MaxTime(), upTo)
		if !isValid {
			break
		}

		if err := e.db.CompactHead(tsdb.NewRangeHead(head, blockMinTime, blockMaxTime)); err != nil {
			return err
		}

		if isLast {
			break
		}
	}

	return e.db.CompactOOOHead(ctx)
}

func (e prometheusEngine) Evict(ctx context.Context, refs []storage.SeriesRef) error {
	// The selected-series writer skips series with in-memory OOO data. Persist it first, but still allow
	// series without OOO data to leave if that fails.
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

// nextForcedHeadCompactionRange computes the next TSDB head range to compact when a forced compaction
// is triggered. If the returned isValid is false, then the returned range should not be compacted.
func nextForcedHeadCompactionRange(blockDuration, headMinTime, headMaxTime, forcedMaxTime int64) (minTime, maxTime int64, isValid, isLast bool) {
	// Nothing to compact if the head is empty.
	if headMinTime == math.MaxInt64 || headMaxTime == math.MinInt64 {
		return 0, 0, false, true
	}

	// By default we try to compact the whole head, honoring the forcedMaxTime.
	minTime = headMinTime
	maxTime = min(headMaxTime, forcedMaxTime)

	// Due to the forcedMaxTime, the range may be empty. In that case we just skip it.
	if maxTime < minTime {
		return 0, 0, false, true
	}

	// Check whether the head compaction range would span across multiple block ranges.
	// If so, we break it to honor the block range period.
	if (minTime/blockDuration)*blockDuration != (maxTime/blockDuration)*blockDuration {
		// Block max time is exclusive, so we do a -1 here.
		maxTime = ((minTime/blockDuration)+1)*blockDuration - 1
		return minTime, maxTime, true, false
	}

	return minTime, maxTime, true, true
}
