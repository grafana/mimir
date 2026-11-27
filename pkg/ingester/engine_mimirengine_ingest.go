// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/mimirengine/store"
)

func (e mimirengineEngine) Ingest(ctx *ingestContext, timeseries []mimirpb.PreallocTimeseries) error {
	minTime, maxTime := mimirengineAppendTimeBounds(ctx)
	histogramsEnabled := ctx.i.limits.NativeHistogramsIngestionEnabled(ctx.userID)
	exemplarsEnabled := ctx.i.limits.MaxGlobalExemplarsPerUser(ctx.userID) > 0
	var floats, remaining []int
	for index, series := range timeseries {
		if len(series.Samples) > 0 && series.CreatedTimestamp == 0 &&
			(!histogramsEnabled || len(series.Histograms) == 0) &&
			(!exemplarsEnabled || len(series.Exemplars) == 0) {
			floats = append(floats, index)
		} else {
			remaining = append(remaining, index)
		}
	}

	if len(floats) > 0 {
		sink := newMimirEngineFloatSink(ctx, func(index int) []mimirpb.LabelAdapter { return timeseries[index].Labels })
		defer sink.close()
		leftover, ingested, err := e.AppendFloats(timeseries, floats, minTime, maxTime, ctx.startAppend.UnixMilli(), ctx.isOTLP, sink)
		ctx.stats.succeededSamplesCount += ingested
		if err != nil {
			return err
		}
		remaining = append(remaining, leftover...)
	}
	if len(remaining) == 0 {
		return finishMimirEngineFastIngest(ctx)
	}
	// Exemplars can refer to series created by an earlier entry in the same request.
	slices.Sort(remaining)
	selected := make([]mimirpb.PreallocTimeseries, len(remaining))
	for i, index := range remaining {
		selected[i] = timeseries[index]
	}
	return e.ingestThroughAppender(ctx, selected)
}

func (e mimirengineEngine) ReadRecord(content []byte, version int) directIngestRecord {
	if version != 2 {
		return nil
	}
	record, err := store.ReadRW2(content, ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
	if err != nil {
		return nil
	}
	if !record.Complete() {
		record.Release()
		return nil
	}
	return mimirengineRecord{RW2Record: record, engine: e}
}

type mimirengineRecord struct {
	*store.RW2Record
	engine mimirengineEngine
}

func (r mimirengineRecord) NumSeries() int { return r.Len() }

func (r mimirengineRecord) FirstSeriesLabels() []mimirpb.LabelAdapter {
	labels, _ := r.Series(0)
	return labels
}

var mimirengineSubsetBuffers = sync.Pool{New: func() any { return new([]byte) }}

func (r mimirengineRecord) Ingest(ctx *ingestContext) error {
	minTime, maxTime := mimirengineAppendTimeBounds(ctx)
	sink := newMimirEngineFloatSink(ctx, func(index int) []mimirpb.LabelAdapter {
		labels, _ := r.Series(index)
		return labels
	})
	defer sink.close()

	leftover, ingested, err := r.engine.AppendRW2(r.RW2Record, minTime, maxTime, ctx.startAppend.UnixMilli(), ctx.isOTLP, sink)
	ctx.stats.succeededSamplesCount += ingested
	if err != nil {
		return err
	}
	if len(leftover) == 0 {
		return finishMimirEngineFastIngest(ctx)
	}

	slices.Sort(leftover)
	buffer := mimirengineSubsetBuffers.Get().(*[]byte)
	defer mimirengineSubsetBuffers.Put(buffer)
	*buffer = r.Subset(leftover, (*buffer)[:0])
	var request mimirpb.PreallocWriteRequest
	defer func() {
		request.FreeBuffer()
		mimirpb.ReuseSlice(request.Timeseries)
	}()
	if err := ingest.DeserializeRecordContent(*buffer, &request, 2); err != nil {
		return fmt.Errorf("decode remaining series: %w", err)
	}
	return r.engine.ingestThroughAppender(ctx, request.Timeseries)
}

func mimirengineAppendTimeBounds(ctx *ingestContext) (int64, int64) {
	limits := ctx.i.limits
	minTime := int64(math.MinInt64)
	if grace := limits.PastGracePeriod(ctx.userID); grace > 0 {
		minTime = ctx.startAppend.Add(-grace).Add(-limits.OutOfOrderTimeWindow(ctx.userID)).UnixMilli()
	}
	return minTime, ctx.startAppend.Add(limits.CreationGracePeriod(ctx.userID)).UnixMilli()
}

func finishMimirEngineFastIngest(ctx *ingestContext) error {
	ctx.i.metrics.appenderAddDuration.Observe(time.Since(ctx.startAppend).Seconds())
	ctx.i.metrics.appenderCommitDuration.Observe(0)
	return nil
}

func (e mimirengineEngine) ingestThroughAppender(ctx *ingestContext, timeseries []mimirpb.PreallocTimeseries) error {
	app := e.Appender(ctx)
	minTime, minTimeAvailable := e.Head().MinInOrderAppendTime()
	var ingested []store.IngestedSeries
	err := ctx.i.pushSamplesToAppender(
		ctx.userID, timeseries, app, ctx.startAppend, ctx.stats, ctx.errProcessor,
		ctx.updateFirstPartial, ctx.activeSeries, ctx.i.limits.OutOfOrderTimeWindow(ctx.userID),
		minTimeAvailable, minTime, ctx.isOTLP, func(ref storage.SeriesRef, buckets int) {
			ingested = append(ingested, store.IngestedSeries{Ref: ref, HistogramBuckets: buckets})
		},
	)
	if err != nil {
		if rollbackErr := app.Rollback(); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf("roll back the appender: %w", rollbackErr))
		}
		return err
	}

	startCommit := time.Now()
	ctx.i.metrics.appenderAddDuration.Observe(startCommit.Sub(ctx.startAppend).Seconds())
	if err := app.Commit(); err != nil {
		return err
	}
	ctx.i.metrics.appenderCommitDuration.Observe(time.Since(startCommit).Seconds())
	e.MarkIngested(ingested, ctx.startAppend.UnixMilli(), ctx.isOTLP)
	return nil
}

type mimirengineFloatSink struct {
	ctx    *ingestContext
	labels func(index int) []mimirpb.LabelAdapter
	index  tsdb.IndexReader
}

func newMimirEngineFloatSink(ctx *ingestContext, labels func(int) []mimirpb.LabelAdapter) mimirengineFloatSink {
	sink := mimirengineFloatSink{ctx: ctx, labels: labels}
	if ctx.activeSeries != nil {
		sink.index = mustIndex(ctx.i.getTSDB(ctx.userID).Head())
	}
	return sink
}

func (s mimirengineFloatSink) close() {
	if s.index != nil {
		_ = s.index.Close()
	}
}

func (s mimirengineFloatSink) Error(index int, err error, timestampMs int64) bool {
	return s.ctx.errProcessor.ProcessErr(err, timestampMs, s.labels(index))
}

func (s mimirengineFloatSink) NeedsLabels() bool { return s.ctx.activeSeries != nil }

func (s mimirengineFloatSink) Ingested(_ int, labels labels.Labels, ref storage.SeriesRef) {
	if s.ctx.activeSeries != nil {
		s.ctx.activeSeries.UpdateSeries(labels, ref, s.ctx.startAppend, -1, s.ctx.isOTLP, s.index)
	}
}

var _ tenantEngine = mimirengineEngine{}
var _ directIngestEngine = mimirengineEngine{}
var _ directIngestRecord = mimirengineRecord{}
