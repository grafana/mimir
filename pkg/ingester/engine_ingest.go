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

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

// ingestBatch is what the ingester asks an engine to store: the series of one write request.
type ingestBatch struct {
	Series []mimirpb.PreallocTimeseries
	// Samples, histograms and exemplars outside [MinTimestampMs, MaxTimestampMs] are rejected.
	MinTimestampMs, MaxTimestampMs int64
	// Histograms are ingested only if NativeHistograms is set, and exemplars only if Exemplars is.
	NativeHistograms bool
	Exemplars        bool
}

// ingestSink is what the ingester wants to know while an engine ingests a batch: series are identified by their
// index in the batch, and nothing the sink is given may be retained unless said otherwise.
type ingestSink interface {
	// Skip reports whether the series is dropped before the engine looks at it, which the sink accounts for.
	Skip(series int) bool
	// Error reports a sample, a histogram or a created-timestamp zero sample that wasn't ingested. It returns
	// whether the error is soft: the ingestion goes on after a soft error, and stops, rolled back, after another.
	Error(series int, err error, timestampMs int64) (soft bool)
	// ExemplarFailed reports an exemplar that wasn't ingested: err is one of the errors below, or the engine's.
	ExemplarFailed(series, exemplar int, err error)
	// NeedsLabels reports whether Ingested uses the labels of the series, which an engine may not have built.
	NeedsLabels() bool
	// Ingested reports a series that got a sample or a histogram, with its labels, which are valid only during the
	// call, its reference, and the bucket count of its last histogram or -1.
	Ingested(series int, lbls labels.Labels, ref storage.SeriesRef, histogramBuckets int)
}

// The exemplar errors that don't come from an engine.
var (
	errExemplarMissingSeries  = errors.New("exemplars can only be added to a series that exists")
	errExemplarTooFarInFuture = errors.New("the exemplar is too far in the future")
	errExemplarTooFarInPast   = errors.New("the exemplar is too far in the past")
)

// ingestOutcome is what an engine ingested of a batch.
type ingestOutcome struct {
	// Samples counts floats, histograms and created-timestamp zero samples.
	Samples   int
	Exemplars int
	// CommitDuration is how long making the batch visible took.
	CommitDuration time.Duration
}

// extendedAppender is a Prometheus appender with GetRef, which the Prometheus engine checks before appending.
type extendedAppender interface {
	storage.Appender
	storage.GetRef
}

// Ingest appends the batch with an appender, commits it, and rolls it back when a hard error stops it.
func (e prometheusEngine) Ingest(ctx context.Context, batch ingestBatch, sink ingestSink) (ingestOutcome, error) {
	return ingestThroughAppender(e.DB.Appender(ctx).(extendedAppender), batch, sink)
}

// ingestThroughAppender appends the batch with the appender, commits it, and rolls it back when a hard error stops it.
func ingestThroughAppender(app extendedAppender, batch ingestBatch, sink ingestSink) (ingestOutcome, error) {
	outcome, err := appendBatch(app, batch, sink, nil, false)
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
func appendBatch(app extendedAppender, batch ingestBatch, sink ingestSink, subset []int, useSubset bool) (ingestOutcome, error) {
	var (
		outcome         ingestOutcome
		builder         labels.ScratchBuilder
		nonCopiedLabels labels.Labels
	)

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
					_, err = app.AppendSTZeroSample(ref, copiedLabels, s.TimestampMs, ts.CreatedTimestamp)
				} else {
					// Copy the label set because both TSDB and the active series tracker may retain it.
					copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
					ref, err = app.AppendSTZeroSample(0, copiedLabels, s.TimestampMs, ts.CreatedTimestamp)
				}
				if err == nil {
					outcome.Samples++
				} else if !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) && !errors.Is(err, storage.ErrOutOfOrderST) && !errors.Is(err, storage.ErrOutOfOrderSample) {
					// According to OTEL spec: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#cumulative-streams-handling-unknown-start-time
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
				if _, err = app.Append(ref, copiedLabels, s.TimestampMs, s.Value); err == nil {
					outcome.Samples++
					continue
				}
			} else {
				// Copy the label set because both TSDB and the active series tracker may retain it.
				copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)

				// Retain the reference in case there are multiple samples for the series.
				if ref, err = app.Append(0, copiedLabels, s.TimestampMs, s.Value); err == nil {
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
					if ref != 0 {
						_, err = app.AppendHistogramSTZeroSample(ref, copiedLabels, h.Timestamp, ts.CreatedTimestamp, ih, fh)
					} else {
						// Copy the label set because both TSDB and the active series tracker may retain it.
						copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
						ref, err = app.AppendHistogramSTZeroSample(0, copiedLabels, h.Timestamp, ts.CreatedTimestamp, ih, fh)
					}
					if err == nil {
						outcome.Samples++
					} else if !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) && !errors.Is(err, storage.ErrOutOfOrderST) && !errors.Is(err, storage.ErrOutOfOrderSample) {
						// See the created timestamp of the floats above.
						sink.Error(si, err, ts.CreatedTimestamp)
					}
					ingestCreatedTimestamp = false // Only try to append created timestamp once per series.
				}

				// If the cached reference exists, we try to use it.
				if ref != 0 {
					if _, err = app.AppendHistogram(ref, copiedLabels, h.Timestamp, ih, fh); err == nil {
						outcome.Samples++
						continue
					}
				} else {
					// Copy the label set because both TSDB and the active series tracker may retain it.
					copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)

					// Retain the reference in case there are multiple samples for the series.
					if ref, err = app.AppendHistogram(0, copiedLabels, h.Timestamp, ih, fh); err == nil {
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
				for ei, ex := range ts.Exemplars {
					if ex.TimestampMs > batch.MaxTimestampMs {
						sink.ExemplarFailed(si, ei, errExemplarTooFarInFuture)
						continue
					} else if ex.TimestampMs < batch.MinTimestampMs {
						sink.ExemplarFailed(si, ei, errExemplarTooFarInPast)
						continue
					}

					e := exemplar.Exemplar{
						Value:  ex.Value,
						Ts:     ex.TimestampMs,
						HasTs:  true,
						Labels: mimirpb.FromLabelAdaptersToLabelsWithCopy(ex.Labels),
					}

					if _, err := app.AppendExemplar(ref, labels.EmptyLabels(), e); err != nil {
						sink.ExemplarFailed(si, ei, err)
						continue
					}
					outcome.Exemplars++
				}
			}
		}
	}
	return outcome, nil
}

// ingestSubsetThroughAppender is ingestThroughAppender for the series at the indices only, which the sink has been
// asked to skip already.
func ingestSubsetThroughAppender(app extendedAppender, batch ingestBatch, sink ingestSink, subset []int) (ingestOutcome, error) {
	outcome, err := appendBatch(app, batch, sink, subset, true)
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
