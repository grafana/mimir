// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"time"

	"github.com/go-kit/log/level"

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
