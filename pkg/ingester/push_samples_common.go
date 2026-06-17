// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_storage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/util/globalerror"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

// ExtendedAppender is the TSDB appender type used for Mimir push paths (Appender + GetRef).
type ExtendedAppender interface {
	storage.Appender
	storage.GetRef
}

// PushStats tracks per-request append outcomes (mirrors ingester push accounting).
type PushStats struct {
	SucceededSamplesCount       int
	FailedSamplesCount          int
	SucceededExemplarsCount     int
	FailedExemplarsCount        int
	SampleTimestampTooOldCount  int
	SampleOutOfOrderCount       int
	SampleTooOldCount           int
	SampleTooFarInFutureCount   int
	NewValueForTimestampCount   int
	PerUserSeriesLimitCount     int
	PerMetricSeriesLimitCount   int
	InvalidNativeHistogramCount int
	LabelsNotSortedCount        int
}

// PushDiscardedSamplesCounter optionally attributes discarded samples (e.g. cost attribution).
// If nil, discards are not attributed.
type PushDiscardedSamplesCounter interface {
	IncrementDiscardedSamples(labels []mimirpb.LabelAdapter, value float64, reason string, t time.Time)
}

func incrementDiscardedSamples(c PushDiscardedSamplesCounter, labels []mimirpb.LabelAdapter, value float64, reason string, t time.Time) {
	if c != nil {
		c.IncrementDiscardedSamples(labels, value, reason, t)
	}
}

// WriteRequestTimeseriesPush configures PushWriteRequestTimeseries.
type WriteRequestTimeseriesPush struct {
	UserID             string
	Timeseries         []mimirpb.PreallocTimeseries
	Source             mimirpb.WriteRequest_SourceEnum
	Limits             *validation.Overrides
	Logger             log.Logger
	Samplers           IngesterErrSamplers
	Discard            PushDiscardedSamplesCounter
	MaxSeriesPerUser   func() int
	MaxSeriesPerMetric func() int
	ActiveSeries       *activeseries.ActiveSeries
	App                ExtendedAppender
	Head               *tsdb.Head
}

// PushWriteRequestTimeseriesResult is the outcome of PushWriteRequestTimeseries.
type PushWriteRequestTimeseriesResult struct {
	Stats           PushStats
	AppendDuration  time.Duration
	CommitDuration  time.Duration
	Err             error // append failure, rollback failure, or commit failure (already user-annotated when applicable).
	FirstPartialErr error // set after a successful commit when a soft append error should be surfaced (already user-annotated).
}

// PushWriteRequestTimeseries appends samples with the same semantics as Ingester.PushWithCleanup
// (soft append errors, exemplar rules, created timestamps, GetRef). Caller must supply an
// appender from Head and hold any external synchronization (e.g. readcache partition mutex).
//
// When FirstPartialErr is non-nil, Err is nil and the TSDB commit has completed; callers that
// need ingester-style discarded-metrics accounting must still run updateMetricsFromPushStats
// (or equivalent) before returning FirstPartialErr to the client.
func PushWriteRequestTimeseries(ctx context.Context, p WriteRequestTimeseriesPush) PushWriteRequestTimeseriesResult {
	startAppend := time.Now()

	var stats PushStats
	var firstPartialErr error
	updateFirstPartial := func(sampler *util_log.Sampler, errFn softErrorFunction) {
		if firstPartialErr == nil {
			firstPartialErr = errFn()
			if sampler != nil {
				firstPartialErr = sampler.WrapError(firstPartialErr)
			}
		}
	}

	outOfOrderWindow := p.Limits.OutOfOrderTimeWindow(p.UserID)
	errProcessor := buildSoftAppendErrorProcessorForPush(
		startAppend,
		outOfOrderWindow,
		&stats,
		updateFirstPartial,
		p.Samplers,
		p.MaxSeriesPerUser,
		p.MaxSeriesPerMetric,
		p.Discard,
		p.Logger,
	)

	minAppendTime, minAppendTimeAvailable := p.Head.AppendableMinValidTime()

	if err := pushSamplesToExtendedAppender(
		p.Limits,
		p.Samplers,
		p.Discard,
		p.UserID,
		p.Timeseries,
		p.App,
		p.Head,
		startAppend,
		&stats,
		&errProcessor,
		updateFirstPartial,
		p.ActiveSeries,
		outOfOrderWindow,
		minAppendTimeAvailable,
		minAppendTime,
		p.Source == mimirpb.OTLP,
	); err != nil {
		if rbErr := p.App.Rollback(); rbErr != nil {
			level.Warn(p.Logger).Log("msg", "failed to rollback appender on error", "user", p.UserID, "err", rbErr)
		}
		return PushWriteRequestTimeseriesResult{
			Stats:          stats,
			AppendDuration: time.Since(startAppend),
			Err:            wrapOrAnnotateWithUser(err, p.UserID),
		}
	}

	appendDuration := time.Since(startAppend)
	startCommit := time.Now()
	if err := p.App.Commit(); err != nil {
		return PushWriteRequestTimeseriesResult{
			Stats:          stats,
			AppendDuration: appendDuration,
			CommitDuration: 0,
			Err:            wrapOrAnnotateWithUser(err, p.UserID),
		}
	}
	commitDuration := time.Since(startCommit)

	if firstPartialErr != nil {
		return PushWriteRequestTimeseriesResult{
			Stats:          stats,
			AppendDuration: appendDuration,
			CommitDuration: commitDuration,
			FirstPartialErr: wrapOrAnnotateWithUser(softErrorWithRejectedSamples{
				err:             firstPartialErr,
				rejectedSamples: int64(stats.FailedSamplesCount),
			}, p.UserID),
		}
	}

	_ = ctx // reserved for future tracing hooks
	return PushWriteRequestTimeseriesResult{
		Stats:          stats,
		AppendDuration: appendDuration,
		CommitDuration: commitDuration,
	}
}

func buildSoftAppendErrorProcessorForPush(
	startAppend time.Time,
	outOfOrderWindow time.Duration,
	stats *PushStats,
	updateFirstPartial func(sampler *util_log.Sampler, errFn softErrorFunction),
	samplers IngesterErrSamplers,
	maxSeriesPerUser func() int,
	maxSeriesPerMetric func() int,
	discard PushDiscardedSamplesCounter,
	logger log.Logger,
) mimir_storage.SoftAppendErrorProcessor {
	return mimir_storage.NewSoftAppendErrorProcessor(
		func() {
			stats.FailedSamplesCount++
		},
		func(timestamp int64, labels []mimirpb.LabelAdapter) {
			stats.SampleTimestampTooOldCount++
			incrementDiscardedSamples(discard, labels, 1, reasonSampleTimestampTooOld, startAppend)
			updateFirstPartial(samplers.SampleTimestampTooOld, func() softError {
				return newSampleTimestampTooOldError(model.Time(timestamp), labels)
			})
		},
		func(timestamp int64, labels []mimirpb.LabelAdapter) {
			stats.SampleOutOfOrderCount++
			incrementDiscardedSamples(discard, labels, 1, reasonSampleOutOfOrder, startAppend)
			updateFirstPartial(samplers.SampleOutOfOrder, func() softError {
				return newSampleOutOfOrderError(model.Time(timestamp), labels)
			})
		},
		func(timestamp int64, labels []mimirpb.LabelAdapter) {
			stats.SampleTooOldCount++
			incrementDiscardedSamples(discard, labels, 1, reasonSampleTooOld, startAppend)
			updateFirstPartial(samplers.SampleTimestampTooOldOOOEnabled, func() softError {
				return newSampleTimestampTooOldOOOEnabledError(model.Time(timestamp), labels, outOfOrderWindow)
			})
		},
		func(timestamp int64, labels []mimirpb.LabelAdapter) {
			stats.SampleTooFarInFutureCount++
			incrementDiscardedSamples(discard, labels, 1, reasonSampleTooFarInFuture, startAppend)
			updateFirstPartial(samplers.SampleTimestampTooFarInFuture, func() softError {
				return newSampleTimestampTooFarInFutureError(model.Time(timestamp), labels)
			})
		},
		func(errMsg string, timestamp int64, labels []mimirpb.LabelAdapter) {
			stats.NewValueForTimestampCount++
			incrementDiscardedSamples(discard, labels, 1, reasonNewValueForTimestamp, startAppend)
			updateFirstPartial(samplers.SampleDuplicateTimestamp, func() softError {
				return newSampleDuplicateTimestampError(errMsg, model.Time(timestamp), labels)
			})
		},
		func(labels []mimirpb.LabelAdapter) {
			stats.PerUserSeriesLimitCount++
			incrementDiscardedSamples(discard, labels, 1, reasonPerUserSeriesLimit, startAppend)
			updateFirstPartial(samplers.MaxSeriesPerUserLimitExceeded, func() softError {
				return newPerUserSeriesLimitReachedError(maxSeriesPerUser())
			})
		},
		func(labels []mimirpb.LabelAdapter) {
			stats.PerMetricSeriesLimitCount++
			incrementDiscardedSamples(discard, labels, 1, reasonPerMetricSeriesLimit, startAppend)
			updateFirstPartial(samplers.MaxSeriesPerMetricLimitExceeded, func() softError {
				return newPerMetricSeriesLimitReachedError(maxSeriesPerMetric(), labels)
			})
		},
		func(err error, timestamp int64, labels []mimirpb.LabelAdapter) bool {
			nativeHistogramErr, ok := newNativeHistogramValidationError(err, model.Time(timestamp), labels)

			if !ok {
				level.Warn(logger).Log("msg", "unknown histogram error", "err", err)
				return false
			}

			stats.InvalidNativeHistogramCount++
			incrementDiscardedSamples(discard, labels, 1, reasonInvalidNativeHistogram, startAppend)

			updateFirstPartial(samplers.NativeHistogramValidationError, func() softError {
				return nativeHistogramErr
			})

			return true
		},
		func(labels []mimirpb.LabelAdapter) {
			stats.LabelsNotSortedCount++
			incrementDiscardedSamples(discard, labels, 1, reasonLabelsNotSorted, startAppend)
			updateFirstPartial(samplers.LabelsNotSorted, func() softError {
				return newLabelsNotSortedError(labels)
			})
		},
	)
}

func pushSamplesToExtendedAppender(
	limits *validation.Overrides,
	samplers IngesterErrSamplers,
	discard PushDiscardedSamplesCounter,
	userID string,
	timeseries []mimirpb.PreallocTimeseries,
	app ExtendedAppender,
	head *tsdb.Head,
	startAppend time.Time,
	stats *PushStats,
	errProcessor *mimir_storage.SoftAppendErrorProcessor,
	updateFirstPartial func(sampler *util_log.Sampler, errFn softErrorFunction),
	activeSeries *activeseries.ActiveSeries,
	outOfOrderWindow time.Duration,
	minAppendTimeAvailable bool,
	minAppendTime int64,
	isOTLP bool,
) error {
	var (
		nativeHistogramsIngestionEnabled = limits.NativeHistogramsIngestionEnabled(userID)
		maxTimestampMs                   = startAppend.Add(limits.CreationGracePeriod(userID)).UnixMilli()
		minTimestampMs                   = int64(math.MinInt64)
	)
	if limits.PastGracePeriod(userID) > 0 {
		minTimestampMs = startAppend.Add(-limits.PastGracePeriod(userID)).Add(-limits.OutOfOrderTimeWindow(userID)).UnixMilli()
	}

	var builder labels.ScratchBuilder
	var nonCopiedLabels labels.Labels

	idx := head.MustIndex()
	defer idx.Close()

	for _, ts := range timeseries {
		if nativeHistogramsIngestionEnabled {
			if outOfOrderWindow <= 0 && minAppendTimeAvailable && len(ts.Exemplars) == 0 &&
				(len(ts.Samples) > 0 || len(ts.Histograms) > 0) &&
				allOutOfBoundsFloats(ts.Samples, minAppendTime) &&
				allOutOfBoundsHistograms(ts.Histograms, minAppendTime) {

				stats.FailedSamplesCount += len(ts.Samples) + len(ts.Histograms)
				stats.SampleTimestampTooOldCount += len(ts.Samples) + len(ts.Histograms)
				incrementDiscardedSamples(discard, ts.Labels, float64(len(ts.Samples)+len(ts.Histograms)), reasonSampleTimestampTooOld, startAppend)
				var firstTimestamp int64
				if len(ts.Samples) > 0 {
					firstTimestamp = ts.Samples[0].TimestampMs
				}
				if len(ts.Histograms) > 0 && (firstTimestamp == 0 || ts.Histograms[0].Timestamp < firstTimestamp) {
					firstTimestamp = ts.Histograms[0].Timestamp
				}

				updateFirstPartial(samplers.SampleTimestampTooOld, func() softError {
					return newSampleTimestampTooOldError(model.Time(firstTimestamp), ts.Labels)
				})
				continue
			}
		} else {
			if outOfOrderWindow <= 0 && minAppendTimeAvailable && len(ts.Exemplars) == 0 &&
				len(ts.Samples) > 0 && allOutOfBoundsFloats(ts.Samples, minAppendTime) {
				stats.FailedSamplesCount += len(ts.Samples)
				stats.SampleTimestampTooOldCount += len(ts.Samples)
				incrementDiscardedSamples(discard, ts.Labels, float64(len(ts.Samples)), reasonSampleTimestampTooOld, startAppend)
				firstTimestamp := ts.Samples[0].TimestampMs

				updateFirstPartial(samplers.SampleTimestampTooOld, func() softError {
					return newSampleTimestampTooOldError(model.Time(firstTimestamp), ts.Labels)
				})
				continue
			}
		}

		mimirpb.FromLabelAdaptersOverwriteLabels(&builder, ts.Labels, &nonCopiedLabels)
		hash := nonCopiedLabels.Hash()
		ref, copiedLabels := app.GetRef(nonCopiedLabels, hash)

		if ref == 0 && !mimirpb.AreLabelNamesSortedAndUnique(ts.Labels) {
			for _, sample := range ts.Samples {
				errProcessor.ProcessErr(globalerror.SeriesLabelsNotSorted, sample.TimestampMs, ts.Labels)
			}
			for _, h := range ts.Histograms {
				errProcessor.ProcessErr(globalerror.SeriesLabelsNotSorted, h.Timestamp, ts.Labels)
			}
			stats.FailedExemplarsCount += len(ts.Exemplars)
			continue
		}

		oldSucceededSamplesCount := stats.SucceededSamplesCount

		ingestCreatedTimestamp := ts.CreatedTimestamp > 0

		for _, s := range ts.Samples {
			var err error

			if s.TimestampMs > maxTimestampMs {
				errProcessor.ProcessErr(globalerror.SampleTooFarInFuture, s.TimestampMs, ts.Labels)
				continue
			} else if s.TimestampMs < minTimestampMs {
				errProcessor.ProcessErr(globalerror.SampleTooFarInPast, s.TimestampMs, ts.Labels)
				continue
			}

			if ingestCreatedTimestamp && ts.CreatedTimestamp < s.TimestampMs && (!nativeHistogramsIngestionEnabled || len(ts.Histograms) == 0 || ts.Histograms[0].Timestamp >= s.TimestampMs) {
				if ref != 0 {
					_, err = app.AppendSTZeroSample(ref, copiedLabels, s.TimestampMs, ts.CreatedTimestamp)
				} else {
					copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
					ref, err = app.AppendSTZeroSample(0, copiedLabels, s.TimestampMs, ts.CreatedTimestamp)
				}
				if err == nil {
					stats.SucceededSamplesCount++
				} else if !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) && !errors.Is(err, storage.ErrOutOfOrderST) && !errors.Is(err, storage.ErrOutOfOrderSample) {
					errProcessor.ProcessErr(err, ts.CreatedTimestamp, ts.Labels)
				}
				ingestCreatedTimestamp = false
			}

			if ref != 0 {
				if _, err = app.Append(ref, copiedLabels, s.TimestampMs, s.Value); err == nil {
					stats.SucceededSamplesCount++
					continue
				}
			} else {
				copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)

				if ref, err = app.Append(0, copiedLabels, s.TimestampMs, s.Value); err == nil {
					stats.SucceededSamplesCount++
					continue
				}
			}

			if errProcessor.ProcessErr(err, s.TimestampMs, ts.Labels) {
				continue
			}

			return err
		}

		numNativeHistogramBuckets := -1
		if nativeHistogramsIngestionEnabled {
			for _, h := range ts.Histograms {
				var (
					err error
					ih  *histogram.Histogram
					fh  *histogram.FloatHistogram
				)

				if h.Timestamp > maxTimestampMs {
					errProcessor.ProcessErr(globalerror.SampleTooFarInFuture, h.Timestamp, ts.Labels)
					continue
				} else if h.Timestamp < minTimestampMs {
					errProcessor.ProcessErr(globalerror.SampleTooFarInPast, h.Timestamp, ts.Labels)
					continue
				}

				if h.IsFloatHistogram() {
					fh = mimirpb.FromFloatHistogramProtoToFloatHistogram(&h)
				} else {
					ih = mimirpb.FromHistogramProtoToHistogram(&h)
				}

				if ingestCreatedTimestamp && ts.CreatedTimestamp < h.Timestamp {
					if ref != 0 {
						_, err = app.AppendHistogramSTZeroSample(ref, copiedLabels, h.Timestamp, ts.CreatedTimestamp, ih, fh)
					} else {
						copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
						ref, err = app.AppendHistogramSTZeroSample(0, copiedLabels, h.Timestamp, ts.CreatedTimestamp, ih, fh)
					}
					if err == nil {
						stats.SucceededSamplesCount++
					} else if !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) && !errors.Is(err, storage.ErrOutOfOrderST) && !errors.Is(err, storage.ErrOutOfOrderSample) {
						errProcessor.ProcessErr(err, ts.CreatedTimestamp, ts.Labels)
					}
					ingestCreatedTimestamp = false
				}

				if ref != 0 {
					if _, err = app.AppendHistogram(ref, copiedLabels, h.Timestamp, ih, fh); err == nil {
						stats.SucceededSamplesCount++
						continue
					}
				} else {
					copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)

					if ref, err = app.AppendHistogram(0, copiedLabels, h.Timestamp, ih, fh); err == nil {
						stats.SucceededSamplesCount++
						continue
					}
				}

				if errProcessor.ProcessErr(err, h.Timestamp, ts.Labels) {
					continue
				}

				return err
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

		if activeSeries != nil && stats.SucceededSamplesCount > oldSucceededSamplesCount {
			activeSeries.UpdateSeries(nonCopiedLabels, ref, startAppend, numNativeHistogramBuckets, isOTLP, idx)
		}

		if len(ts.Exemplars) > 0 && limits.MaxGlobalExemplarsPerUser(userID) > 0 {
			if ref == 0 {
				updateFirstPartial(nil, func() softError {
					return newExemplarMissingSeriesError(model.Time(ts.Exemplars[0].TimestampMs), ts.Labels, ts.Exemplars[0].Labels)
				})
				stats.FailedExemplarsCount += len(ts.Exemplars)
			} else {
				outOfOrderExemplars := 0
				for _, ex := range ts.Exemplars {
					if ex.TimestampMs > maxTimestampMs {
						stats.FailedExemplarsCount++
						updateFirstPartial(nil, func() softError {
							return newExemplarTimestampTooFarInFutureError(model.Time(ex.TimestampMs), ts.Labels, ex.Labels)
						})
						continue
					} else if ex.TimestampMs < minTimestampMs {
						stats.FailedExemplarsCount++
						updateFirstPartial(nil, func() softError {
							return newExemplarTimestampTooFarInPastError(model.Time(ex.TimestampMs), ts.Labels, ex.Labels)
						})
						continue
					}

					e := exemplar.Exemplar{
						Value:  ex.Value,
						Ts:     ex.TimestampMs,
						HasTs:  true,
						Labels: mimirpb.FromLabelAdaptersToLabelsWithCopy(ex.Labels),
					}

					var err error
					if _, err = app.AppendExemplar(ref, labels.EmptyLabels(), e); err == nil {
						stats.SucceededExemplarsCount++
						continue
					}

					stats.FailedExemplarsCount++

					isOOOExemplar := errors.Is(err, storage.ErrOutOfOrderExemplar)
					if isOOOExemplar {
						outOfOrderExemplars++
						if outOfOrderExemplars < len(ts.Exemplars) {
							continue
						}
					}

					if !isOOOExemplar || !limits.IgnoreOOOExemplars(userID) {
						updateFirstPartial(nil, func() softError {
							return newTSDBIngestExemplarErr(err, model.Time(ex.TimestampMs), ts.Labels, ex.Labels)
						})
					}
				}
			}
		}
	}
	return nil
}
