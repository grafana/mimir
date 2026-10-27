// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

// prometheusEngine is the Prometheus TSDB as a tenantEngine.
type prometheusEngine struct {
	*tsdb.DB
	// The time range of a block, which a flush compacts the head by.
	blockDuration int64
}

func (e prometheusEngine) ChunkQuerier(mint, maxt int64, unordered bool) (storage.ChunkQuerier, error) {
	if unordered {
		return e.UnorderedChunkQuerier(mint, maxt)
	}
	return e.DB.ChunkQuerier(mint, maxt)
}

// openPrometheusEngine opens the tenant's TSDB in dir, replaying its WAL.
func openPrometheusEngine(dir string, logger *slog.Logger, reg prometheus.Registerer, opts *tsdb.Options) (tenantEngine, error) {
	db, err := tsdb.Open(dir, logger, reg, opts, nil)
	if err != nil {
		return nil, err
	}
	// The ingester compacts on its own schedule.
	db.DisableCompactions()
	return prometheusEngine{DB: db, blockDuration: opts.MinBlockDuration}, nil
}

func (e prometheusEngine) Head() engineHead {
	return prometheusHead{e.DB.Head()}
}

// prometheusHead is the Prometheus TSDB's head as an engineHead.
type prometheusHead struct {
	*tsdb.Head
}

func (h prometheusHead) Sync() error {
	return h.FsyncWLSegments()
}

func (h prometheusHead) TimeBounds() timeBounds {
	return timeBounds{MinTime: h.MinTime(), MaxTime: h.MaxTime(), MinOOOTime: h.MinOOOTime(), MaxOOOTime: h.MaxOOOTime()}
}

func (h prometheusHead) OldestAppendableTime() (int64, bool) {
	return h.AppendableMinValidTime()
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

func (h prometheusHead) ShardSeries(shardIndex, shardCount uint64) index.Postings {
	out := make([]storage.SeriesRef, 0, 128)
	h.ForEachShardHash(func(refs []storage.SeriesRef, shardHashes []uint64) {
		for i := range refs {
			if shardHashes[i]%shardCount == shardIndex {
				out = append(out, refs[i])
			}
		}
	})
	slices.Sort(out) // The postings of an index are sorted.
	return index.NewListPostings(out)
}

// Flush compacts the head block by block up to upTo, then the out-of-order head.
func (e prometheusEngine) Flush(ctx context.Context, upTo int64) error {
	head := e.DB.Head()
	for {
		blockMinTime, blockMaxTime, isValid, isLast := nextForcedHeadCompactionRange(e.blockDuration, head.MinTime(), head.MaxTime(), upTo)
		if !isValid {
			break
		}

		if err := e.CompactHead(tsdb.NewRangeHead(head, blockMinTime, blockMaxTime)); err != nil {
			return err
		}

		// Do not check again if it was the last range.
		if isLast {
			break
		}
	}

	return e.CompactOOOHead(ctx)
}

// Evict compacts the out-of-order head first, which persists the out-of-order data of the series before they
// leave, then the series themselves. The first failure doesn't stop the second: series without out-of-order data
// can still leave.
func (e prometheusEngine) Evict(ctx context.Context, refs []storage.SeriesRef) error {
	oooErr := e.CompactOOOHead(ctx)
	if oooErr != nil {
		oooErr = fmt.Errorf("compact the out-of-order head: %w", oooErr)
	}
	selectedErr := e.CompactSelectedSeries(refs)
	if selectedErr != nil {
		selectedErr = fmt.Errorf("compact the selected series: %w", selectedErr)
	}
	return errors.Join(oooErr, selectedErr)
}

func (e prometheusEngine) Configure(settings engineSettings) error {
	// DB.ApplyConfig only looks at the TSDB settings of the config; the rest of its fields are for things like
	// rules and scrapes.
	return e.ApplyConfig(&config.Config{
		StorageConfig: config.StorageConfig{
			ExemplarsConfig: &config.ExemplarsConfig{MaxExemplars: settings.MaxExemplars},
			TSDBConfig: &config.TSDBConfig{
				OutOfOrderTimeWindow: settings.OutOfOrderTimeWindow,
				// ApplyConfig reads the empty string as "keep the encoding resolved at startup", so a
				// tenant clearing the limit needs an explicit value to fall back to the default.
				ChunkEncoding: config.ChunkEncodingConfig{Floats: settings.FloatChunkEncoding},
			},
		},
	})
}

func (e prometheusEngine) BlocksToDelete(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
	return tsdb.DefaultBlocksToDelete(e.DB)(blocks)
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

// GetRef() is an extra method added to TSDB to let Mimir check before calling Add()
type extendedAppender interface {
	storage.AppenderV2
	storage.GetRef
	storage.ExemplarAppenderV2
}

// Ingest appends the batch with an appender, commits it, and rolls it back when a hard error stops it.
func (e prometheusEngine) Ingest(ctx context.Context, batch ingestBatch, sink *pushSink) (ingestOutcome, error) {
	return ingestThroughAppender(e.AppenderV2(ctx).(extendedAppender), batch, sink)
}

// ingestThroughAppender appends the batch with the appender, commits it, and rolls it back when a hard error stops it.
func ingestThroughAppender(app extendedAppender, batch ingestBatch, sink *pushSink) (ingestOutcome, error) {
	outcome, err := appendBatch(app, batch, sink, nil, false, nil)
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

// appendBatch adds the batch to the appender, or with subset only the series at those indices, which the sink has
// been asked to skip or not already. Errors the sink calls soft are reported to it, and any other error is returned.
// The series that got samples are appended to ingested, unless it's nil.
func appendBatch(app extendedAppender, batch ingestBatch, sink *pushSink, subset []int, useSubset bool, ingested *[]store.IngestedSeries) (ingestOutcome, error) {
	var (
		outcome         ingestOutcome
		builder         labels.ScratchBuilder
		nonCopiedLabels labels.Labels
	)

	appOptions := storage.AppendV2Options{}
	// Zero samples for created timestamps must never go into the out-of-order head.
	zeroSampleOptions := storage.AppendV2Options{RejectOutOfOrder: true}

	count := len(batch.Series)
	if useSubset {
		count = len(subset)
	}
	for position := range count {
		si := position
		if useSubset {
			si = subset[position]
		} else if sink.Skip(si) {
			continue
		}
		ts := batch.Series[si]

		// MUST BE COPIED before being retained.
		mimirpb.FromLabelAdaptersOverwriteLabels(&builder, ts.Labels, &nonCopiedLabels)
		hash := nonCopiedLabels.Hash()
		// Look up a reference for this series. The hash passed should be the output of Labels.Hash()
		// and NOT the stable hashing because we use the stable hashing in ingesters only for query sharding.
		ref, copiedLabels := app.GetRef(nonCopiedLabels, hash)

		// The labels must be sorted. This is defensive programming; the distributor
		// sorts labels before forwarding to ingesters.
		if ref == 0 && !mimirpb.AreLabelNamesSortedAndUnique(ts.Labels) {
			for _, sample := range ts.Samples {
				sink.Error(si, globalerror.SeriesLabelsNotSorted, sample.TimestampMs)
			}
			for _, h := range ts.Histograms {
				sink.Error(si, globalerror.SeriesLabelsNotSorted, h.Timestamp)
			}
			for ei := range ts.Exemplars {
				sink.ExemplarFailed(si, ei, globalerror.SeriesLabelsNotSorted)
			}
			continue
		}

		// To find out if any sample was added to this series, we keep old value.
		oldSucceededSamplesCount := outcome.Samples

		ingestCreatedTimestamp := ts.CreatedTimestamp > 0

		for _, s := range ts.Samples {
			var err error

			// Ensure the sample is not too far in the future.
			if s.TimestampMs > batch.MaxTimestampMs {
				sink.Error(si, globalerror.SampleTooFarInFuture, s.TimestampMs)
				continue
			} else if s.TimestampMs < batch.MinTimestampMs {
				sink.Error(si, globalerror.SampleTooFarInPast, s.TimestampMs)
				continue
			}

			if ingestCreatedTimestamp && ts.CreatedTimestamp < s.TimestampMs && (!batch.NativeHistograms || len(ts.Histograms) == 0 || ts.Histograms[0].Timestamp >= s.TimestampMs) {
				if ref != 0 {
					_, err = app.Append(ref, copiedLabels, 0, ts.CreatedTimestamp, 0, nil, nil, zeroSampleOptions)
				} else {
					// Copy the label set because both TSDB and the active series tracker may retain it.
					copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
					ref, err = app.Append(0, copiedLabels, 0, ts.CreatedTimestamp, 0, nil, nil, zeroSampleOptions)
				}
				if err == nil {
					outcome.Samples++
				} else if !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) && !errors.Is(err, storage.ErrOutOfOrderSample) {
					// According to OTel spec: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#cumulative-streams-handling-unknown-start-time
					// if the start time is unknown, then it should equal to the timestamp of the first sample,
					// which will mean a created timestamp equal to the timestamp of the first sample for later
					// samples. Thus we ignore if zero sample would cause duplicate.
					// We also ignore out of order sample as created timestamp is out of order most of the time,
					// except when written before the first sample.
					sink.Error(si, err, ts.CreatedTimestamp)
				}
				ingestCreatedTimestamp = false // Only try to append created timestamp once per series.
			}

			// If the cached reference exists, we try to use it.
			if ref != 0 {
				if _, err = app.Append(ref, copiedLabels, 0, s.TimestampMs, s.Value, nil, nil, appOptions); err == nil {
					outcome.Samples++
					continue
				}
			} else {
				// Copy the label set because both TSDB and the active series tracker may retain it.
				copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)

				// Retain the reference in case there are multiple samples for the series.
				if ref, err = app.Append(0, copiedLabels, 0, s.TimestampMs, s.Value, nil, nil, appOptions); err == nil {
					outcome.Samples++
					continue
				}
			}

			// If it's a soft error it will be returned back to the distributor later as a 400.
			if sink.Error(si, err, s.TimestampMs) {
				continue
			}

			// Otherwise, return a 500.
			return outcome, err
		}

		numNativeHistogramBuckets := -1
		if batch.NativeHistograms {
			for _, h := range ts.Histograms {
				var (
					err error
					ih  *histogram.Histogram
					fh  *histogram.FloatHistogram
				)

				if h.Timestamp > batch.MaxTimestampMs {
					sink.Error(si, globalerror.SampleTooFarInFuture, h.Timestamp)
					continue
				} else if h.Timestamp < batch.MinTimestampMs {
					sink.Error(si, globalerror.SampleTooFarInPast, h.Timestamp)
					continue
				}

				if h.IsFloatHistogram() {
					fh = mimirpb.FromFloatHistogramProtoToFloatHistogram(&h)
				} else {
					ih = mimirpb.FromHistogramProtoToHistogram(&h)
				}

				if ingestCreatedTimestamp && ts.CreatedTimestamp < h.Timestamp {
					zih, zfh := mimir_tsdb.ZeroHistogramFor(ih, fh)
					if ref != 0 {
						_, err = app.Append(ref, copiedLabels, 0, ts.CreatedTimestamp, 0, zih, zfh, zeroSampleOptions)
					} else {
						// Copy the label set because both TSDB and the active series tracker may retain it.
						copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
						ref, err = app.Append(0, copiedLabels, 0, ts.CreatedTimestamp, 0, zih, zfh, zeroSampleOptions)
					}
					if err == nil {
						outcome.Samples++
					} else if !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) && !errors.Is(err, storage.ErrOutOfOrderSample) {
						// According to OTel spec: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#cumulative-streams-handling-unknown-start-time
						// if the start time is unknown, then it should equal to the timestamp of the first sample,
						// which will mean a created timestamp equal to the timestamp of the first sample for later
						// samples. Thus we ignore if zero sample would cause duplicate.
						// We also ignore out of order sample as created timestamp is out of order most of the time,
						// except when written before the first sample.
						sink.Error(si, err, ts.CreatedTimestamp)
					}
					ingestCreatedTimestamp = false // Only try to append created timestamp once per series.
				}

				// If the cached reference exists, we try to use it.
				if ref != 0 {
					if _, err = app.Append(ref, copiedLabels, 0, h.Timestamp, 0, ih, fh, appOptions); err == nil {
						outcome.Samples++
						continue
					}
				} else {
					// Copy the label set because both TSDB and the active series tracker may retain it.
					copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)

					// Retain the reference in case there are multiple samples for the series.
					if ref, err = app.Append(ref, copiedLabels, 0, h.Timestamp, 0, ih, fh, appOptions); err == nil {
						outcome.Samples++
						continue
					}
				}

				if sink.Error(si, err, h.Timestamp) {
					continue
				}

				return outcome, err
			}
			numNativeHistograms := len(ts.Histograms)
			if numNativeHistograms > 0 {
				lastNativeHistogram := ts.Histograms[numNativeHistograms-1]
				numFloats := len(ts.Samples)
				if numFloats == 0 || ts.Samples[numFloats-1].TimestampMs < lastNativeHistogram.Timestamp {
					numNativeHistogramBuckets = lastNativeHistogram.BucketCount()
				}
			}
		}

		if outcome.Samples > oldSucceededSamplesCount {
			sink.Ingested(si, nonCopiedLabels, ref, numNativeHistogramBuckets)
			if ingested != nil {
				*ingested = append(*ingested, store.IngestedSeries{Ref: ref, HistogramBuckets: numNativeHistogramBuckets})
			}
		}

		if len(ts.Exemplars) > 0 && batch.Exemplars {
			// app.AppendExemplar currently doesn't create the series, it must
			// already exist.  If it does not then drop.
			if ref == 0 {
				for ei := range ts.Exemplars {
					sink.ExemplarFailed(si, ei, errExemplarMissingSeries)
				}
			} else { // Note that else is explicit, rather than a continue in the above if, in case of additional logic post exemplar processing.
				exemplars := make([]exemplar.Exemplar, 1)
				for ei, ex := range ts.Exemplars {
					if ex.TimestampMs > batch.MaxTimestampMs {
						sink.ExemplarFailed(si, ei, errExemplarTooFarInFuture)
						continue
					} else if ex.TimestampMs < batch.MinTimestampMs {
						sink.ExemplarFailed(si, ei, errExemplarTooFarInPast)
						continue
					}

					exemplars[0] = exemplar.Exemplar{
						Value:  ex.Value,
						Ts:     ex.TimestampMs,
						HasTs:  true,
						Labels: mimirpb.FromLabelAdaptersToLabelsWithCopy(ex.Labels),
					}

					// TODO(krajorama): this is not taking advantage of passing
					// all exemplars to the appender at the same time, however
					// without getting the index of failed exemplars in the
					// returned partial error, we cannot report errors
					// correctly. Also this is the minimal change as first step.
					_, err := app.AppendExemplars(ref, labels.EmptyLabels(), exemplars)
					if err == nil {
						outcome.Exemplars++
						continue
					}

					var partialErr *storage.AppendPartialError
					if errors.As(err, &partialErr) && len(partialErr.ExemplarErrors) == 1 {
						err = partialErr.ExemplarErrors[0]
					}
					sink.ExemplarFailed(si, ei, err)
				}
			}
		}
	}
	return outcome, nil
}
