// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"math"
	"time"

	"github.com/failsafe-go/failsafe-go/adaptivelimiter"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_storage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/util/globalerror"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// GetRef() is an extra method added to TSDB to let Mimir check before calling Add()
type extendedAppender interface {
	storage.Appender
	storage.GetRef
}

type pushStats struct {
	succeededSamplesCount       int
	failedSamplesCount          int
	succeededExemplarsCount     int
	failedExemplarsCount        int
	sampleTimestampTooOldCount  int
	sampleOutOfOrderCount       int
	sampleTooOldCount           int
	sampleTooFarInFutureCount   int
	newValueForTimestampCount   int
	perUserSeriesLimitCount     int
	perMetricSeriesLimitCount   int
	invalidNativeHistogramCount int
	labelsNotSortedCount        int
}

type ctxKey int

var pushReqCtxKey ctxKey = 1
var readReqCtxKey ctxKey = 2

type pushRequestState struct {
	requestSize     int64
	requestDuration time.Duration
	requestFinish   func(time.Duration, error)
	pushErr         error
}

type readRequestState struct {
	requestFinish func(err error)
}

func getPushRequestState(ctx context.Context) *pushRequestState {
	if st, ok := ctx.Value(pushReqCtxKey).(*pushRequestState); ok {
		return st
	}
	return nil
}

func getReadRequestState(ctx context.Context) *readRequestState {
	if st, ok := ctx.Value(readReqCtxKey).(*readRequestState); ok {
		return st
	}
	return nil
}

// StartPushRequest implements pushReceiver and checks if ingester can start push request, incrementing relevant
// counters. If new push request cannot be started, errors convertible to gRPC status code are returned, and metrics are
// updated.
func (i *Ingester) StartPushRequest(ctx context.Context, reqSize int64) (context.Context, error) {
	ctx, _, err := i.startPushRequest(ctx, reqSize)
	return ctx, err
}

// PreparePushRequest implements pushReceiver.
func (i *Ingester) PreparePushRequest(ctx context.Context) (finishFn func(error), err error) {
	if i.reactiveLimiter.push != nil {
		// Acquire a permit, blocking if needed
		permit, err := i.reactiveLimiter.push.AcquirePermit(ctx)
		if err != nil {
			i.metrics.rejected.WithLabelValues(reasonIngesterMaxInflightPushRequests).Inc()
			return nil, newReactiveLimiterExceededError(err)
		}
		return func(err error) {
			if errors.Is(err, context.Canceled) {
				permit.Drop()
			} else {
				permit.Record()
			}
		}, nil
	}
	return nil, nil
}

// FinishPushRequest implements pushReceiver.
func (i *Ingester) FinishPushRequest(ctx context.Context) {
	st := getPushRequestState(ctx)
	if st == nil {
		return
	}
	i.inflightPushRequests.Dec()
	if st.requestSize > 0 {
		i.inflightPushRequestsBytes.Sub(st.requestSize)
	}
	st.requestFinish(st.requestDuration, st.pushErr)
}

// This method can be called in two ways: 1. Ingester.PushWithCleanup, or 2. Ingester.StartPushRequest via gRPC server's method limiter.
//
// In the first case, returned errors can be inspected/logged by middleware. Ingester.PushWithCleanup will wrap the error in util_log.DoNotLogError wrapper.
// In the second case, returned errors will not be logged, because request will not reach any middleware.
//
// If startPushRequest ends with no error, the resulting context includes a *pushRequestState object
// containing relevant information about the push request started by this method.
// The resulting boolean flag tells if the caller must call finish on this request. If not, there is already someone in the call stack who will do that.
func (i *Ingester) startPushRequest(ctx context.Context, reqSize int64) (context.Context, bool, error) {
	if err := i.checkAvailableForPush(); err != nil {
		return nil, false, err
	}

	if st := getPushRequestState(ctx); st != nil {
		// If state is already in context, this means we already passed through StartPushRequest for this request.
		return ctx, false, nil
	}

	// We try to acquire a push permit from the circuit breaker.
	// If it is not possible, it is because the circuit breaker is open, and a circuitBreakerOpenError is returned.
	// If it is possible, a permit has to be released by recording either a success or a failure with the circuit
	// breaker. This is done by FinishPushRequest().
	finish, err := i.circuitBreaker.tryAcquirePushPermit()
	if err != nil {
		return nil, false, err
	}
	st := &pushRequestState{
		requestSize:   reqSize,
		requestFinish: finish,
	}
	ctx = context.WithValue(ctx, pushReqCtxKey, st)

	if i.reactiveLimiter.push != nil && !i.reactiveLimiter.push.CanAcquirePermit() {
		i.metrics.rejected.WithLabelValues(reasonIngesterMaxInflightPushRequests).Inc()
		return nil, false, newReactiveLimiterExceededError(adaptivelimiter.ErrExceeded)
	}

	inflight := i.inflightPushRequests.Inc()
	inflightBytes := int64(0)
	rejectEqualInflightBytes := false
	if reqSize > 0 {
		inflightBytes = i.inflightPushRequestsBytes.Add(reqSize)
	} else {
		inflightBytes = i.inflightPushRequestsBytes.Load()
		rejectEqualInflightBytes = true // if inflightBytes == limit, reject new request
	}

	instanceLimitsErr := i.checkInstanceLimits(inflight, inflightBytes, rejectEqualInflightBytes)
	if instanceLimitsErr == nil {
		// In this case a pull request has been successfully started, and we return
		// the context enriched with the corresponding pushRequestState object.
		return ctx, true, nil
	}

	// In this case a per-instance limit has been hit, and the corresponding error has to be passed
	// to FinishPushRequest, which finishes the push request, records the error with the circuit breaker,
	// and gives it a possibly acquired permit back.
	st.pushErr = instanceLimitsErr
	i.FinishPushRequest(ctx)
	return nil, false, instanceLimitsErr
}

func (i *Ingester) checkInstanceLimits(inflight int64, inflightBytes int64, rejectEqualInflightBytes bool) error {
	il := i.getInstanceLimits()
	if il == nil {
		return nil
	}
	if il.MaxInflightPushRequests > 0 && inflight > il.MaxInflightPushRequests {
		i.metrics.rejected.WithLabelValues(reasonIngesterMaxInflightPushRequests).Inc()
		return errMaxInflightRequestsReached
	}

	if il.MaxInflightPushRequestsBytes > 0 {
		if (rejectEqualInflightBytes && inflightBytes >= il.MaxInflightPushRequestsBytes) || inflightBytes > il.MaxInflightPushRequestsBytes {
			i.metrics.rejected.WithLabelValues(reasonIngesterMaxInflightPushRequestsBytes).Inc()
			return errMaxInflightRequestsBytesReached
		}
	}

	if il.MaxIngestionRate > 0 {
		if rate := i.ingestionRate.Rate(); rate >= il.MaxIngestionRate {
			i.metrics.rejected.WithLabelValues(reasonIngesterMaxIngestionRate).Inc()
			return errMaxIngestionRateReached
		}
	}
	return nil
}

// PushWithCleanup is the Push() implementation for blocks storage and takes a WriteRequest and adds it to the TSDB head.
func (i *Ingester) PushWithCleanup(ctx context.Context, req *mimirpb.WriteRequest, cleanUp func()) (returnErr error) {
	// NOTE: because we use `unsafe` in deserialisation, we must not
	// retain anything from `req` past the exit from this function.
	defer cleanUp()

	start := time.Now()
	// Only start/finish request here when the request comes NOT from grpc handlers (i.e., from ingest.Store).
	// NOTE: request coming from grpc handler may end up calling start multiple times during its lifetime (e.g., when migrating to ingest storage).
	// startPushRequest handles this.
	if i.cfg.IngestStorageConfig.Enabled {
		reqSize := int64(req.Size())
		var (
			shouldFinish bool
			startPushErr error
		)
		// We need to replace the original context with the context returned by startPushRequest,
		// because the latter might store a new pushRequestState in the context.
		ctx, shouldFinish, startPushErr = i.startPushRequest(ctx, reqSize)
		if startPushErr != nil {
			return middleware.DoNotLogError{Err: startPushErr}
		}
		if shouldFinish {
			defer func() {
				i.FinishPushRequest(ctx)
			}()
		}
	}

	defer func() {
		// We enrich the pushRequestState contained in the context with this PushWithCleanUp()
		// call duration, and a possible error it returns. These data are needed during a
		// successive call to FinishPushRequest().
		if st := getPushRequestState(ctx); st != nil {
			st.requestDuration = time.Since(start)
			st.pushErr = returnErr
		}
	}()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	// Given metadata is a best-effort approach, and we don't halt on errors
	// process it before samples. Otherwise, we risk returning an error before ingestion.
	if ingestedMetadata := i.pushMetadata(ctx, userID, req.GetMetadata()); ingestedMetadata > 0 {
		// Distributor counts both samples and metadata, so for consistency ingester does the same.
		i.ingestionRate.Add(int64(ingestedMetadata))
	}

	// Early exit if no timeseries in request - don't create a TSDB or an appender.
	if len(req.Timeseries) == 0 {
		return nil
	}

	db, err := i.getOrCreateTSDB(userID)
	if err != nil {
		return wrapOrAnnotateWithUser(err, userID)
	}

	lockState, err := db.acquireAppendLock(req.MinTimestamp())
	if err != nil {
		return wrapOrAnnotateWithUser(err, userID)
	}
	defer db.releaseAppendLock(lockState)

	// Note that we don't .Finish() the span in this method on purpose
	spanlog := spanlogger.FromContext(ctx, i.logger)
	spanlog.DebugLog("event", "acquired append lock")

	var (
		startAppend = time.Now()

		// Keep track of some stats which are tracked only if the samples will be
		// successfully committed
		stats pushStats

		firstPartialErr error
		// updateFirstPartial is a function that, in case of a softError, stores that error
		// in firstPartialError, and makes PushWithCleanup proceed. This way all the valid
		// samples and exemplars will be we actually ingested, and the first softError that
		// was encountered will be returned. If a sampler is specified, the softError gets
		// wrapped by that sampler.
		updateFirstPartial = func(sampler *util_log.Sampler, errFn softErrorFunction) {
			if firstPartialErr == nil {
				firstPartialErr = errFn()
				if sampler != nil {
					firstPartialErr = sampler.WrapError(firstPartialErr)
				}
			}
		}

		outOfOrderWindow = i.limits.OutOfOrderTimeWindow(userID)

		cast         = i.costAttributionMgr.SampleTracker(userID)
		errProcessor = mimir_storage.NewSoftAppendErrorProcessor(
			func() {
				stats.failedSamplesCount++
			},
			func(timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.sampleTimestampTooOldCount++
				cast.IncrementDiscardedSamples(labels, 1, reasonSampleTimestampTooOld, startAppend)
				updateFirstPartial(i.errorSamplers.sampleTimestampTooOld, func() softError {
					return newSampleTimestampTooOldError(model.Time(timestamp), labels)
				})
			},
			func(timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.sampleOutOfOrderCount++
				cast.IncrementDiscardedSamples(labels, 1, reasonSampleOutOfOrder, startAppend)
				updateFirstPartial(i.errorSamplers.sampleOutOfOrder, func() softError {
					return newSampleOutOfOrderError(model.Time(timestamp), labels)
				})
			},
			func(timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.sampleTooOldCount++
				cast.IncrementDiscardedSamples(labels, 1, reasonSampleTooOld, startAppend)
				updateFirstPartial(i.errorSamplers.sampleTimestampTooOldOOOEnabled, func() softError {
					return newSampleTimestampTooOldOOOEnabledError(model.Time(timestamp), labels, outOfOrderWindow)
				})
			},
			func(timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.sampleTooFarInFutureCount++
				cast.IncrementDiscardedSamples(labels, 1, reasonSampleTooFarInFuture, startAppend)
				updateFirstPartial(i.errorSamplers.sampleTimestampTooFarInFuture, func() softError {
					return newSampleTimestampTooFarInFutureError(model.Time(timestamp), labels)
				})
			},
			func(errMsg string, timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.newValueForTimestampCount++
				cast.IncrementDiscardedSamples(labels, 1, reasonNewValueForTimestamp, startAppend)
				updateFirstPartial(i.errorSamplers.sampleDuplicateTimestamp, func() softError {
					return newSampleDuplicateTimestampError(errMsg, model.Time(timestamp), labels)
				})
			},
			func(labels []mimirpb.LabelAdapter) {
				stats.perUserSeriesLimitCount++
				cast.IncrementDiscardedSamples(labels, 1, reasonPerUserSeriesLimit, startAppend)
				updateFirstPartial(i.errorSamplers.maxSeriesPerUserLimitExceeded, func() softError {
					return newPerUserSeriesLimitReachedError(i.limiter.limits.MaxGlobalSeriesPerUser(userID))
				})
			},
			func(labels []mimirpb.LabelAdapter) {
				stats.perMetricSeriesLimitCount++
				cast.IncrementDiscardedSamples(labels, 1, reasonPerMetricSeriesLimit, startAppend)
				updateFirstPartial(i.errorSamplers.maxSeriesPerMetricLimitExceeded, func() softError {
					return newPerMetricSeriesLimitReachedError(i.limiter.limits.MaxGlobalSeriesPerMetric(userID), labels)
				})
			},
			func(err error, timestamp int64, labels []mimirpb.LabelAdapter) bool {
				nativeHistogramErr, ok := newNativeHistogramValidationError(err, model.Time(timestamp), labels)

				if !ok {
					level.Warn(i.logger).Log("msg", "unknown histogram error", "err", err)
					return false
				}

				stats.invalidNativeHistogramCount++
				cast.IncrementDiscardedSamples(labels, 1, reasonInvalidNativeHistogram, startAppend)

				updateFirstPartial(i.errorSamplers.nativeHistogramValidationError, func() softError {
					return nativeHistogramErr
				})

				return true
			},
			func(labels []mimirpb.LabelAdapter) {
				stats.labelsNotSortedCount++
				cast.IncrementDiscardedSamples(labels, 1, reasonLabelsNotSorted, startAppend)
				updateFirstPartial(i.errorSamplers.labelsNotSorted, func() softError {
					return newLabelsNotSortedError(labels)
				})
			},
		)
	)

	// Walk the samples, appending them to the users database
	app := db.Appender(ctx).(extendedAppender)
	spanlog.DebugLog("event", "got appender for timeseries", "series", len(req.Timeseries))

	var activeSeries *activeseries.ActiveSeries
	if i.cfg.ActiveSeriesMetrics.Enabled {
		activeSeries = db.activeSeries
	}

	minAppendTime, minAppendTimeAvailable := db.Head().AppendableMinValidTime()

	if pushSamplesToAppenderErr := i.pushSamplesToAppender(
		userID,
		req.Timeseries,
		app,
		startAppend,
		&stats,
		&errProcessor,
		updateFirstPartial,
		activeSeries,
		i.limits.OutOfOrderTimeWindow(userID),
		minAppendTimeAvailable,
		minAppendTime,
		req.Source == mimirpb.OTLP,
	); pushSamplesToAppenderErr != nil {
		if err := app.Rollback(); err != nil {
			level.Warn(i.logger).Log("msg", "failed to rollback appender on error", "user", userID, "err", err)
		}

		return wrapOrAnnotateWithUser(pushSamplesToAppenderErr, userID)
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
		return wrapOrAnnotateWithUser(err, userID)
	}

	commitDuration := time.Since(startCommit)
	i.metrics.appenderCommitDuration.Observe(commitDuration.Seconds())
	spanlog.DebugLog("event", "complete commit", "commitDuration", commitDuration.String())

	// If only invalid samples are pushed, don't change "last update", as TSDB was not modified.
	if stats.succeededSamplesCount > 0 {
		db.setLastUpdate(time.Now())
	}

	// Increment metrics only if the samples have been successfully committed.
	// If the code didn't reach this point, it means that we returned an error
	// which will be converted into an HTTP 5xx and the client should/will retry.
	i.metrics.ingestedSamples.WithLabelValues(userID).Add(float64(stats.succeededSamplesCount))
	i.metrics.ingestedSamplesFail.WithLabelValues(userID).Add(float64(stats.failedSamplesCount))
	i.metrics.ingestedExemplars.Add(float64(stats.succeededExemplarsCount))
	i.metrics.ingestedExemplarsFail.Add(float64(stats.failedExemplarsCount))
	appendedSamplesStats.Inc(int64(stats.succeededSamplesCount))
	appendedExemplarsStats.Inc(int64(stats.succeededExemplarsCount))

	group := i.activeGroups.UpdateActiveGroupTimestamp(userID, validation.GroupLabel(i.limits, userID, req.Timeseries), startAppend)

	i.updateMetricsFromPushStats(userID, group, &stats, req.Source, db, i.metrics.discarded)

	if firstPartialErr != nil {
		wrappedErr := softErrorWithRejectedSamples{
			err:             firstPartialErr,
			rejectedSamples: int64(stats.failedSamplesCount),
		}
		return wrapOrAnnotateWithUser(wrappedErr, userID)
	}

	return nil
}

func (i *Ingester) updateMetricsFromPushStats(userID string, group string, stats *pushStats, samplesSource mimirpb.WriteRequest_SourceEnum, db *userTSDB, discarded *discardedMetrics) {
	if stats.sampleTimestampTooOldCount > 0 {
		discarded.sampleTimestampTooOld.WithLabelValues(userID, group).Add(float64(stats.sampleTimestampTooOldCount))
	}
	if stats.sampleOutOfOrderCount > 0 {
		discarded.sampleOutOfOrder.WithLabelValues(userID, group).Add(float64(stats.sampleOutOfOrderCount))
	}
	if stats.sampleTooOldCount > 0 {
		discarded.sampleTooOld.WithLabelValues(userID, group).Add(float64(stats.sampleTooOldCount))
	}
	if stats.sampleTooFarInFutureCount > 0 {
		discarded.sampleTooFarInFuture.WithLabelValues(userID, group).Add(float64(stats.sampleTooFarInFutureCount))
	}
	if stats.newValueForTimestampCount > 0 {
		discarded.newValueForTimestamp.WithLabelValues(userID, group).Add(float64(stats.newValueForTimestampCount))
	}
	if stats.perUserSeriesLimitCount > 0 {
		discarded.perUserSeriesLimit.WithLabelValues(userID, group).Add(float64(stats.perUserSeriesLimitCount))
	}
	if stats.perMetricSeriesLimitCount > 0 {
		discarded.perMetricSeriesLimit.WithLabelValues(userID, group).Add(float64(stats.perMetricSeriesLimitCount))
	}
	if stats.invalidNativeHistogramCount > 0 {
		discarded.invalidNativeHistogram.WithLabelValues(userID, group).Add(float64(stats.invalidNativeHistogramCount))
	}
	if stats.labelsNotSortedCount > 0 {
		discarded.labelsNotSorted.WithLabelValues(userID, group).Add(float64(stats.labelsNotSortedCount))
	}
	if stats.succeededSamplesCount > 0 {
		i.ingestionRate.Add(int64(stats.succeededSamplesCount))

		if samplesSource == mimirpb.RULE {
			db.ingestedRuleSamples.Add(int64(stats.succeededSamplesCount))
		} else {
			db.ingestedAPISamples.Add(int64(stats.succeededSamplesCount))
		}
	}
}

// pushSamplesToAppender appends samples and exemplars to the appender. Most errors are handled via updateFirstPartial function,
// but in case of unhandled errors, appender is rolled back and such error is returned. Errors handled by updateFirstPartial
// must be of type softError.
func (i *Ingester) pushSamplesToAppender(
	userID string,
	timeseries []mimirpb.PreallocTimeseries,
	app extendedAppender,
	startAppend time.Time,
	stats *pushStats,
	errProcessor *mimir_storage.SoftAppendErrorProcessor,
	updateFirstPartial func(sampler *util_log.Sampler, errFn softErrorFunction),
	activeSeries *activeseries.ActiveSeries,
	outOfOrderWindow time.Duration,
	minAppendTimeAvailable bool,
	minAppendTime int64,
	isOTLP bool,
) error {
	// Fetch limits once per push request both to avoid processing half the request differently.
	var (
		nativeHistogramsIngestionEnabled = i.limits.NativeHistogramsIngestionEnabled(userID)
		maxTimestampMs                   = startAppend.Add(i.limits.CreationGracePeriod(userID)).UnixMilli()
		minTimestampMs                   = int64(math.MinInt64)
	)
	if i.limits.PastGracePeriod(userID) > 0 {
		minTimestampMs = startAppend.Add(-i.limits.PastGracePeriod(userID)).Add(-i.limits.OutOfOrderTimeWindow(userID)).UnixMilli()
	}

	var builder labels.ScratchBuilder
	var nonCopiedLabels labels.Labels

	// idx is used to decrease active series count in case of error for cost attribution.
	idx := i.getTSDB(userID).Head().MustIndex()
	defer idx.Close()

	for _, ts := range timeseries {
		// Fast path in case we only have samples and they are all out of bound
		// and out-of-order support is not enabled.
		// TODO(jesus.vazquez) If we had too many old samples we might want to
		// extend the fast path to fail early.
		if nativeHistogramsIngestionEnabled {
			if outOfOrderWindow <= 0 && minAppendTimeAvailable && len(ts.Exemplars) == 0 &&
				(len(ts.Samples) > 0 || len(ts.Histograms) > 0) &&
				allOutOfBoundsFloats(ts.Samples, minAppendTime) &&
				allOutOfBoundsHistograms(ts.Histograms, minAppendTime) {

				stats.failedSamplesCount += len(ts.Samples) + len(ts.Histograms)
				stats.sampleTimestampTooOldCount += len(ts.Samples) + len(ts.Histograms)
				i.costAttributionMgr.SampleTracker(userID).IncrementDiscardedSamples(ts.Labels, float64(len(ts.Samples)+len(ts.Histograms)), reasonSampleTimestampTooOld, startAppend)
				var firstTimestamp int64
				if len(ts.Samples) > 0 {
					firstTimestamp = ts.Samples[0].TimestampMs
				}
				if len(ts.Histograms) > 0 && (firstTimestamp == 0 || ts.Histograms[0].Timestamp < firstTimestamp) {
					firstTimestamp = ts.Histograms[0].Timestamp
				}

				updateFirstPartial(i.errorSamplers.sampleTimestampTooOld, func() softError {
					return newSampleTimestampTooOldError(model.Time(firstTimestamp), ts.Labels)
				})
				continue
			}
		} else {
			// ignore native histograms in the condition and statitics as well
			if outOfOrderWindow <= 0 && minAppendTimeAvailable && len(ts.Exemplars) == 0 &&
				len(ts.Samples) > 0 && allOutOfBoundsFloats(ts.Samples, minAppendTime) {
				stats.failedSamplesCount += len(ts.Samples)
				stats.sampleTimestampTooOldCount += len(ts.Samples)
				i.costAttributionMgr.SampleTracker(userID).IncrementDiscardedSamples(ts.Labels, float64(len(ts.Samples)), reasonSampleTimestampTooOld, startAppend)
				firstTimestamp := ts.Samples[0].TimestampMs

				updateFirstPartial(i.errorSamplers.sampleTimestampTooOld, func() softError {
					return newSampleTimestampTooOldError(model.Time(firstTimestamp), ts.Labels)
				})
				continue
			}
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
				errProcessor.ProcessErr(globalerror.SeriesLabelsNotSorted, sample.TimestampMs, ts.Labels)
			}
			for _, h := range ts.Histograms {
				errProcessor.ProcessErr(globalerror.SeriesLabelsNotSorted, h.Timestamp, ts.Labels)
			}
			stats.failedExemplarsCount += len(ts.Exemplars)
			continue
		}

		// To find out if any sample was added to this series, we keep old value.
		oldSucceededSamplesCount := stats.succeededSamplesCount

		ingestCreatedTimestamp := ts.CreatedTimestamp > 0

		for _, s := range ts.Samples {
			var err error

			// Ensure the sample is not too far in the future.
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
					// Copy the label set because both TSDB and the active series tracker may retain it.
					copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
					ref, err = app.AppendSTZeroSample(0, copiedLabels, s.TimestampMs, ts.CreatedTimestamp)
				}
				if err == nil {
					stats.succeededSamplesCount++
				} else if !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) && !errors.Is(err, storage.ErrOutOfOrderST) && !errors.Is(err, storage.ErrOutOfOrderSample) {
					// According to OTEL spec: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#cumulative-streams-handling-unknown-start-time
					// if the start time is unknown, then it should equal to the timestamp of the first sample,
					// which will mean a created timestamp equal to the timestamp of the first sample for later
					// samples. Thus we ignore if zero sample would cause duplicate.
					// We also ignore out of order sample as created timestamp is out of order most of the time,
					// except when written before the first sample.
					errProcessor.ProcessErr(err, ts.CreatedTimestamp, ts.Labels)
				}
				ingestCreatedTimestamp = false // Only try to append created timestamp once per series.
			}

			// If the cached reference exists, we try to use it.
			if ref != 0 {
				if _, err = app.Append(ref, copiedLabels, s.TimestampMs, s.Value); err == nil {
					stats.succeededSamplesCount++
					continue
				}
			} else {
				// Copy the label set because both TSDB and the active series tracker may retain it.
				copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)

				// Retain the reference in case there are multiple samples for the series.
				if ref, err = app.Append(0, copiedLabels, s.TimestampMs, s.Value); err == nil {
					stats.succeededSamplesCount++
					continue
				}
			}

			// If it's a soft error it will be returned back to the distributor later as a 400.
			if errProcessor.ProcessErr(err, s.TimestampMs, ts.Labels) {
				continue
			}

			// Otherwise, return a 500.
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
						// Copy the label set because both TSDB and the active series tracker may retain it.
						copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
						ref, err = app.AppendHistogramSTZeroSample(0, copiedLabels, h.Timestamp, ts.CreatedTimestamp, ih, fh)
					}
					if err == nil {
						stats.succeededSamplesCount++
					} else if !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) && !errors.Is(err, storage.ErrOutOfOrderST) && !errors.Is(err, storage.ErrOutOfOrderSample) {
						// According to OTEL spec: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#cumulative-streams-handling-unknown-start-time
						// if the start time is unknown, then it should equal to the timestamp of the first sample,
						// which will mean a created timestamp equal to the timestamp of the first sample for later
						// samples. Thus we ignore if zero sample would cause duplicate.
						// We also ignore out of order sample as created timestamp is out of order most of the time,
						// except when written before the first sample.
						errProcessor.ProcessErr(err, ts.CreatedTimestamp, ts.Labels)
					}
					ingestCreatedTimestamp = false // Only try to append created timestamp once per series.
				}

				// If the cached reference exists, we try to use it.
				if ref != 0 {
					if _, err = app.AppendHistogram(ref, copiedLabels, h.Timestamp, ih, fh); err == nil {
						stats.succeededSamplesCount++
						continue
					}
				} else {
					// Copy the label set because both TSDB and the active series tracker may retain it.
					copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)

					// Retain the reference in case there are multiple samples for the series.
					if ref, err = app.AppendHistogram(0, copiedLabels, h.Timestamp, ih, fh); err == nil {
						stats.succeededSamplesCount++
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

		if activeSeries != nil && stats.succeededSamplesCount > oldSucceededSamplesCount {
			activeSeries.UpdateSeries(nonCopiedLabels, ref, startAppend, numNativeHistogramBuckets, isOTLP, idx)
		}

		if len(ts.Exemplars) > 0 && i.limits.MaxGlobalExemplarsPerUser(userID) > 0 {
			// app.AppendExemplar currently doesn't create the series, it must
			// already exist.  If it does not then drop.
			if ref == 0 {
				updateFirstPartial(nil, func() softError {
					return newExemplarMissingSeriesError(model.Time(ts.Exemplars[0].TimestampMs), ts.Labels, ts.Exemplars[0].Labels)
				})
				stats.failedExemplarsCount += len(ts.Exemplars)
			} else { // Note that else is explicit, rather than a continue in the above if, in case of additional logic post exemplar processing.
				outOfOrderExemplars := 0
				for _, ex := range ts.Exemplars {
					if ex.TimestampMs > maxTimestampMs {
						stats.failedExemplarsCount++
						updateFirstPartial(nil, func() softError {
							return newExemplarTimestampTooFarInFutureError(model.Time(ex.TimestampMs), ts.Labels, ex.Labels)
						})
						continue
					} else if ex.TimestampMs < minTimestampMs {
						stats.failedExemplarsCount++
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
						stats.succeededExemplarsCount++
						continue
					}

					// We track the failed exemplars ingestion, whatever is the reason. This way, the sum of successfully
					// and failed ingested exemplars is equal to the total number of processed ones.
					stats.failedExemplarsCount++

					isOOOExemplar := errors.Is(err, storage.ErrOutOfOrderExemplar)
					if isOOOExemplar {
						outOfOrderExemplars++
						// Only report out of order exemplars if all are out of order, otherwise this was a partial update
						// to some existing set of exemplars.
						if outOfOrderExemplars < len(ts.Exemplars) {
							continue
						}
					}

					// Error adding exemplar. Do not report to client if the error was out of order and we ignore such error.
					if !isOOOExemplar || !i.limits.IgnoreOOOExemplars(userID) {
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

// PushToStorageAndReleaseRequest implements ingest.Pusher interface for ingestion via ingest-storage.
func (i *Ingester) PushToStorageAndReleaseRequest(ctx context.Context, req *mimirpb.WriteRequest) error {
	err := i.PushWithCleanup(ctx, req, func() {
		req.FreeBuffer()
		mimirpb.ReuseSlice(req.Timeseries)
	})
	if err != nil {
		return mapPushErrorToErrorWithStatus(err)
	}
	return nil
}

// Push implements client.IngesterServer, which is registered into gRPC server.
func (i *Ingester) Push(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	if !i.cfg.PushGrpcMethodEnabled {
		return nil, errPushGrpcDisabled
	}

	err := i.PushToStorageAndReleaseRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return &mimirpb.WriteResponse{}, err
}

// pushMetadata returns number of ingested metadata.
func (i *Ingester) pushMetadata(ctx context.Context, userID string, metadata []*mimirpb.MetricMetadata) int {
	ingestedMetadata := 0
	failedMetadata := 0

	userMetadata := i.getOrCreateUserMetadata(userID)
	var firstMetadataErr error
	for _, m := range metadata {
		if m == nil {
			continue
		}
		err := userMetadata.add(m.MetricFamilyName, m)
		if err == nil {
			ingestedMetadata++
			continue
		}

		failedMetadata++
		if firstMetadataErr == nil {
			firstMetadataErr = err
		}
	}

	i.metrics.ingestedMetadata.Add(float64(ingestedMetadata))
	i.metrics.ingestedMetadataFail.Add(float64(failedMetadata))

	// If we have any error with regard to metadata we just log and no-op.
	// We consider metadata a best effort approach, errors here should not stop processing.
	if firstMetadataErr != nil {
		logger := util_log.WithContext(ctx, i.logger)
		level.Warn(logger).Log("msg", "failed to ingest some metadata", "err", firstMetadataErr)
	}

	return ingestedMetadata
}

func (i *Ingester) getOrCreateUserMetadata(userID string) *userMetricsMetadata {
	userMetadata := i.getUserMetadata(userID)
	if userMetadata != nil {
		return userMetadata
	}

	i.usersMetadataMtx.Lock()
	defer i.usersMetadataMtx.Unlock()

	// Ensure it was not created between switching locks.
	userMetadata, ok := i.usersMetadata[userID]
	if !ok {
		userMetadata = newMetadataMap(i.limiter, i.metrics, i.errorSamplers, userID)
		i.usersMetadata[userID] = userMetadata
	}
	return userMetadata
}

func (i *Ingester) getUserMetadata(userID string) *userMetricsMetadata {
	i.usersMetadataMtx.RLock()
	defer i.usersMetadataMtx.RUnlock()
	return i.usersMetadata[userID]
}

func (i *Ingester) deleteUserMetadata(userID string) {
	i.usersMetadataMtx.Lock()
	um := i.usersMetadata[userID]
	delete(i.usersMetadata, userID)
	i.usersMetadataMtx.Unlock()

	if um != nil {
		// We need call purge to update i.metrics.memMetadata correctly (it counts number of metrics with metadata in memory).
		// Passing zero time means purge everything.
		um.purge(time.Time{})
	}
}
func (i *Ingester) getUsersWithMetadata() []string {
	i.usersMetadataMtx.RLock()
	defer i.usersMetadataMtx.RUnlock()

	userIDs := make([]string, 0, len(i.usersMetadata))
	for userID := range i.usersMetadata {
		userIDs = append(userIDs, userID)
	}

	return userIDs
}

func (i *Ingester) purgeUserMetricsMetadata() {
	deadline := time.Now().Add(-i.cfg.MetadataRetainPeriod)

	for _, userID := range i.getUsersWithMetadata() {
		metadata := i.getUserMetadata(userID)
		if metadata == nil {
			continue
		}

		// Remove all metadata that we no longer need to retain.
		metadata.purge(deadline)
	}
}

// allOutOfBounds returns whether all the provided (float) samples are out of bounds.
func allOutOfBoundsFloats(samples []mimirpb.Sample, minValidTime int64) bool {
	for _, s := range samples {
		if s.TimestampMs >= minValidTime {
			return false
		}
	}
	return true
}

// allOutOfBoundsHistograms returns whether all the provided histograms are out of bounds.
func allOutOfBoundsHistograms(histograms []mimirpb.Histogram, minValidTime int64) bool {
	for _, s := range histograms {
		if s.Timestamp >= minValidTime {
			return false
		}
	}
	return true
}
