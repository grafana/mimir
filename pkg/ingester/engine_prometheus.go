// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"

	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

func (e prometheusEngine) Ingest(ctx *ingestContext, timeseries []mimirpb.PreallocTimeseries) error {
	i, userID := ctx.i, ctx.userID
	spanlog := spanlogger.FromContext(ctx, i.logger)
	app := e.db.AppenderV2(ctx).(ExtendedAppender)
	spanlog.DebugLog("event", "got appender for timeseries", "series", len(timeseries))

	source := mimirpb.API
	if ctx.isOTLP {
		source = mimirpb.OTLP
	}
	res := PushWriteRequestTimeseries(ctx, WriteRequestTimeseriesPush{
		UserID:             userID,
		Timeseries:         timeseries,
		Source:             source,
		Limits:             i.limits,
		Logger:             i.logger,
		Samplers:           i.errorSamplers,
		Discard:            i.costAttributionMgr.SampleTracker(userID),
		MaxSeriesPerUser:   func() int { return i.limiter.limits.MaxGlobalSeriesPerUser(userID) },
		MaxSeriesPerMetric: func() int { return i.limiter.limits.MaxGlobalSeriesPerMetric(userID) },
		ActiveSeries:       ctx.activeSeries,
		App:                app,
		Head:               e.db.Head(),
	})
	*ctx.stats = res.Stats
	if res.Err != nil {
		return res.Err
	}
	ctx.firstPartialErr = res.FirstPartialErr

	i.metrics.appenderAddDuration.Observe(res.AppendDuration.Seconds())

	spanlog.DebugLog(
		"event", "push complete",
		"succeededSamplesCount", res.Stats.SucceededSamplesCount,
		"failedSamplesCount", res.Stats.FailedSamplesCount,
		"succeededExemplarsCount", res.Stats.SucceededExemplarsCount,
		"failedExemplarsCount", res.Stats.FailedExemplarsCount,
	)

	i.metrics.appenderCommitDuration.Observe(res.CommitDuration.Seconds())
	spanlog.DebugLog("event", "complete commit", "commitDuration", res.CommitDuration.String())

	return nil
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
