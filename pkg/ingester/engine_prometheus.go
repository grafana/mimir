// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

// prometheusEngine is the Prometheus TSDB as a tenantEngine.
type prometheusEngine struct {
	*tsdb.DB
}

// GetRef() is an extra method added to TSDB to let Mimir check before calling Add()
type extendedAppender interface {
	storage.AppenderV2
	storage.GetRef
	storage.ExemplarAppenderV2
}

// Ingest appends the batch with an appender, commits it, and rolls it back when a hard error stops it.
func (e prometheusEngine) Ingest(ctx context.Context, batch ingestBatch, sink *pushSink) (ingestOutcome, error) {
	app := e.AppenderV2(ctx).(extendedAppender)
	outcome, err := appendBatch(app, batch, sink)
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

// appendBatch adds the batch to the appender. Errors the sink calls soft are reported to it, and any other error is
// returned.
func appendBatch(app extendedAppender, batch ingestBatch, sink *pushSink) (ingestOutcome, error) {
	var (
		outcome         ingestOutcome
		builder         labels.ScratchBuilder
		nonCopiedLabels labels.Labels
	)

	appOptions := storage.AppendV2Options{}
	// Zero samples for created timestamps must never go into the out-of-order head.
	zeroSampleOptions := storage.AppendV2Options{RejectOutOfOrder: true}

	for si, ts := range batch.Series {
		if sink.Skip(si) {
			continue
		}

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
