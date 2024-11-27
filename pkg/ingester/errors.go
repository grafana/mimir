// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/errors.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"errors"
	"fmt"
	"time"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/common/model"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	integerUnavailableMsgFormat = "ingester is unavailable (current state: %s)"
	ingesterTooBusyMsg          = "ingester is currently too busy to process queries, try again later"
	ingesterPushGrpcDisabledMsg = "ingester is configured with Push gRPC method disabled"
)

var (
	errTooBusy          = ingesterTooBusyError{}
	errPushGrpcDisabled = newErrorWithStatus(ingesterPushGrpcDisabledError{}, codes.Unimplemented)
)

// newErrorWithStatus creates a new ErrorWithStatus backed by the given error,
// and containing the given gRPC code. If the given error is an ingesterError,
// the resulting ErrorWithStatus will be enriched by the details backed by
// ingesterError.errorCause. These details are of type mimirpb.ErrorDetails.
func newErrorWithStatus(originalErr error, code codes.Code) globalerror.ErrorWithStatus {
	var (
		ingesterErr  ingesterError
		errorDetails *mimirpb.ErrorDetails
	)
	if errors.As(originalErr, &ingesterErr) {
		errorDetails = &mimirpb.ErrorDetails{Cause: ingesterErr.errorCause()}
	}
	return globalerror.WrapErrorWithGRPCStatus(originalErr, code, errorDetails)
}

// ingesterError is a marker interface for the errors returned by ingester, and that are safe to wrap.
type ingesterError interface {
	errorCause() mimirpb.ErrorCause
}

// softError is a marker interface for the errors on which ingester.Push should not stop immediately.
type softError interface {
	error
	soft()
}

type softErrorFunction func() softError

// wrapOrAnnotateWithUser prepends the given userID to the given error.
// If the given error matches one of the errors from this package, the
// returned error retains a reference to the former.
func wrapOrAnnotateWithUser(err error, userID string) error {
	// If err is an ingesterError, we wrap it with userID and return it.
	var ingesterErr ingesterError
	if errors.As(err, &ingesterErr) {
		return fmt.Errorf("user=%s: %w", userID, err)
	}

	// Otherwise, we just annotate it with userID and return it.
	return fmt.Errorf("user=%s: %s", userID, err)
}

// sampleError is an ingesterError indicating a problem with a histSample.
type sampleError struct {
	errID     globalerror.ID
	errMsg    string
	timestamp model.Time
	series    string
}

func (e sampleError) Error() string {
	return fmt.Sprintf(
		"%s. The affected histSample has timestamp %s and is from series %s",
		e.errID.Message(e.errMsg),
		e.timestamp.Time().UTC().Format(time.RFC3339Nano),
		e.series,
	)
}

func (e sampleError) errorCause() mimirpb.ErrorCause {
	return mimirpb.BAD_DATA
}

func (e sampleError) soft() {}

// Ensure that sampleError is an ingesterError.
var _ ingesterError = sampleError{}

// Ensure that sampleError is a softError.
var _ softError = sampleError{}

func newSampleError(errID globalerror.ID, errMsg string, timestamp model.Time, labels []mimirpb.LabelAdapter) sampleError {
	return sampleError{
		errID:     errID,
		errMsg:    errMsg,
		timestamp: timestamp,
		series:    mimirpb.FromLabelAdaptersToString(labels),
	}
}

func newSampleTimestampTooOldError(timestamp model.Time, labels []mimirpb.LabelAdapter) sampleError {
	return newSampleError(globalerror.SampleTimestampTooOld, "the histSample has been rejected because its timestamp is too old", timestamp, labels)
}

func newSampleTimestampTooOldOOOEnabledError(timestamp model.Time, labels []mimirpb.LabelAdapter, oooTimeWindow time.Duration) sampleError {
	return newSampleError(globalerror.SampleTimestampTooOld, fmt.Sprintf("the histSample has been rejected because another histSample with a more recent timestamp has already been ingested and this histSample is beyond the out-of-order time window of %s", model.Duration(oooTimeWindow).String()), timestamp, labels)
}

func newSampleTimestampTooFarInFutureError(timestamp model.Time, labels []mimirpb.LabelAdapter) sampleError {
	return newSampleError(globalerror.SampleTooFarInFuture, "received a histSample whose timestamp is too far in the future", timestamp, labels)
}

func newSampleOutOfOrderError(timestamp model.Time, labels []mimirpb.LabelAdapter) sampleError {
	return newSampleError(globalerror.SampleOutOfOrder, "the histSample has been rejected because another histSample with a more recent timestamp has already been ingested and out-of-order samples are not allowed", timestamp, labels)
}

func newSampleDuplicateTimestampError(timestamp model.Time, labels []mimirpb.LabelAdapter) sampleError {
	return newSampleError(globalerror.SampleDuplicateTimestamp, "the histSample has been rejected because another histSample with the same timestamp, but a different value, has already been ingested", timestamp, labels)
}

// exemplarError is an ingesterError indicating a problem with an exemplar.
type exemplarError struct {
	errID          globalerror.ID
	errMsg         string
	timestamp      model.Time
	seriesLabels   string
	exemplarLabels string
}

func (e exemplarError) Error() string {
	return fmt.Sprintf(
		"%s. The affected exemplar is %s with timestamp %s for series %s",
		e.errID.Message(e.errMsg),
		e.exemplarLabels,
		e.timestamp.Time().UTC().Format(time.RFC3339Nano),
		e.seriesLabels,
	)
}

func (e exemplarError) errorCause() mimirpb.ErrorCause {
	return mimirpb.BAD_DATA
}

func (e exemplarError) soft() {}

// Ensure that exemplarError is an ingesterError.
var _ ingesterError = exemplarError{}

// Ensure that exemplarError is an softError.
var _ softError = exemplarError{}

func newExemplarError(errID globalerror.ID, errMsg string, timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) exemplarError {
	return exemplarError{
		errID:          errID,
		errMsg:         errMsg,
		timestamp:      timestamp,
		seriesLabels:   mimirpb.FromLabelAdaptersToString(seriesLabels),
		exemplarLabels: mimirpb.FromLabelAdaptersToString(exemplarLabels),
	}
}

func newExemplarMissingSeriesError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) exemplarError {
	return newExemplarError(globalerror.ExemplarSeriesMissing, "the exemplar has been rejected because the related series has not been ingested yet", timestamp, seriesLabels, exemplarLabels)
}

func newExemplarTimestampTooFarInFutureError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) exemplarError {
	return newExemplarError(globalerror.ExemplarTooFarInFuture, "received an exemplar whose timestamp is too far in the future", timestamp, seriesLabels, exemplarLabels)
}
func newExemplarTimestampTooFarInPastError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) exemplarError {
	return newExemplarError(globalerror.ExemplarTooFarInPast, "received an exemplar whose timestamp is too far in the past", timestamp, seriesLabels, exemplarLabels)
}

// tsdbIngestExemplarErr is an ingesterError indicating a problem with an exemplar.
type tsdbIngestExemplarErr struct {
	originalErr    error
	timestamp      model.Time
	seriesLabels   string
	exemplarLabels string
}

func (e tsdbIngestExemplarErr) Error() string {
	return fmt.Sprintf("err: %v. timestamp=%s, series=%s, exemplar=%s",
		e.originalErr,
		e.timestamp.Time().UTC().Format(time.RFC3339Nano),
		e.seriesLabels,
		e.exemplarLabels,
	)
}

func (e tsdbIngestExemplarErr) errorCause() mimirpb.ErrorCause {
	return mimirpb.BAD_DATA
}

func (e tsdbIngestExemplarErr) soft() {}

// Ensure that tsdbIngestExemplarErr is an ingesterError.
var _ ingesterError = tsdbIngestExemplarErr{}

// Ensure that tsdbIngestExemplarErr is an softError.
var _ softError = tsdbIngestExemplarErr{}

func newTSDBIngestExemplarErr(ingestErr error, timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) tsdbIngestExemplarErr {
	return tsdbIngestExemplarErr{
		originalErr:    ingestErr,
		timestamp:      timestamp,
		seriesLabels:   mimirpb.FromLabelAdaptersToString(seriesLabels),
		exemplarLabels: mimirpb.FromLabelAdaptersToString(exemplarLabels),
	}
}

// perUserSeriesLimitReachedError is an ingesterError indicating that a per-user series limit has been reached.
type perUserSeriesLimitReachedError struct {
	limit int
}

// newPerUserSeriesLimitReachedError creates a new perUserMetadataLimitReachedError indicating that a per-user series limit has been reached.
func newPerUserSeriesLimitReachedError(limit int) perUserSeriesLimitReachedError {
	return perUserSeriesLimitReachedError{
		limit: limit,
	}
}

func (e perUserSeriesLimitReachedError) Error() string {
	return globalerror.MaxSeriesPerUser.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("per-user series limit of %d exceeded", e.limit),
		validation.MaxSeriesPerUserFlag,
	)
}

func (e perUserSeriesLimitReachedError) errorCause() mimirpb.ErrorCause {
	return mimirpb.TENANT_LIMIT
}

func (e perUserSeriesLimitReachedError) soft() {}

// Ensure that perUserSeriesLimitReachedError is an ingesterError.
var _ ingesterError = perUserSeriesLimitReachedError{}

// Ensure that perUserSeriesLimitReachedError is an softError.
var _ softError = perUserSeriesLimitReachedError{}

// perUserMetadataLimitReachedError is an ingesterError indicating that a per-user metadata limit has been reached.
type perUserMetadataLimitReachedError struct {
	limit int
}

// newPerUserMetadataLimitReachedError creates a new perUserMetadataLimitReachedError indicating that a per-user metadata limit has been reached.
func newPerUserMetadataLimitReachedError(limit int) perUserMetadataLimitReachedError {
	return perUserMetadataLimitReachedError{
		limit: limit,
	}
}

func (e perUserMetadataLimitReachedError) Error() string {
	return globalerror.MaxMetadataPerUser.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("per-user metric metadata limit of %d exceeded", e.limit),
		validation.MaxMetadataPerUserFlag,
	)
}

func (e perUserMetadataLimitReachedError) errorCause() mimirpb.ErrorCause {
	return mimirpb.TENANT_LIMIT
}

func (e perUserMetadataLimitReachedError) soft() {}

// Ensure that perUserMetadataLimitReachedError is an ingesterError.
var _ ingesterError = perUserMetadataLimitReachedError{}

// Ensure that perUserMetadataLimitReachedError is an softError.
var _ softError = perUserMetadataLimitReachedError{}

// perMetricSeriesLimitReachedError is an ingesterError indicating that a per-metric series limit has been reached.
type perMetricSeriesLimitReachedError struct {
	limit  int
	series string
}

// newPerMetricSeriesLimitReachedError creates a new perMetricSeriesLimitReachedError indicating that a per-metric series limit has been reached.
func newPerMetricSeriesLimitReachedError(limit int, labels []mimirpb.LabelAdapter) perMetricSeriesLimitReachedError {
	return perMetricSeriesLimitReachedError{
		limit:  limit,
		series: mimirpb.FromLabelAdaptersToString(labels),
	}
}

func (e perMetricSeriesLimitReachedError) Error() string {
	return fmt.Sprintf("%s This is for series %s",
		globalerror.MaxSeriesPerMetric.MessageWithPerTenantLimitConfig(
			fmt.Sprintf("per-metric series limit of %d exceeded", e.limit),
			validation.MaxSeriesPerMetricFlag,
		),
		e.series,
	)
}

func (e perMetricSeriesLimitReachedError) errorCause() mimirpb.ErrorCause {
	return mimirpb.TENANT_LIMIT
}

func (e perMetricSeriesLimitReachedError) soft() {}

// Ensure that perMetricSeriesLimitReachedError is an ingesterError.
var _ ingesterError = perMetricSeriesLimitReachedError{}

// Ensure that perMetricSeriesLimitReachedError is an softError.
var _ softError = perMetricSeriesLimitReachedError{}

// perMetricMetadataLimitReachedError is an ingesterError indicating that a per-metric metadata limit has been reached.
type perMetricMetadataLimitReachedError struct {
	limit  int
	family string
}

// newPerMetricMetadataLimitReachedError creates a new perMetricMetadataLimitReachedError indicating that a per-metric metadata limit has been reached.
func newPerMetricMetadataLimitReachedError(limit int, family string) perMetricMetadataLimitReachedError {
	return perMetricMetadataLimitReachedError{
		limit:  limit,
		family: family,
	}
}

func (e perMetricMetadataLimitReachedError) Error() string {
	return fmt.Sprintf("%s This is for metric %s",
		globalerror.MaxMetadataPerMetric.MessageWithPerTenantLimitConfig(
			fmt.Sprintf("per-metric metadata limit of %d exceeded", e.limit),
			validation.MaxMetadataPerMetricFlag,
		),
		e.family,
	)
}

func (e perMetricMetadataLimitReachedError) errorCause() mimirpb.ErrorCause {
	return mimirpb.TENANT_LIMIT
}

func (e perMetricMetadataLimitReachedError) soft() {}

// Ensure that perMetricMetadataLimitReachedError is an ingesterError.
var _ ingesterError = perMetricMetadataLimitReachedError{}

// Ensure that perMetricMetadataLimitReachedError is an softError.
var _ softError = perMetricMetadataLimitReachedError{}

// nativeHistogramValidationError indicates that native histogram bucket counts did not add up to the overall count.
type nativeHistogramValidationError struct {
	id           globalerror.ID
	originalErr  error
	seriesLabels []mimirpb.LabelAdapter
	timestamp    model.Time
}

func newNativeHistogramValidationError(id globalerror.ID, originalErr error, timestamp model.Time, seriesLabels []mimirpb.LabelAdapter) nativeHistogramValidationError {
	return nativeHistogramValidationError{
		id:           id,
		originalErr:  originalErr,
		seriesLabels: seriesLabels,
		timestamp:    timestamp,
	}
}

func (e nativeHistogramValidationError) Error() string {
	return e.id.Message(fmt.Sprintf("err: %v. timestamp=%s, series=%s",
		e.originalErr,
		e.timestamp.Time().UTC().Format(time.RFC3339Nano),
		e.seriesLabels,
	))
}

func (e nativeHistogramValidationError) errorCause() mimirpb.ErrorCause {
	return mimirpb.BAD_DATA
}

func (e nativeHistogramValidationError) soft() {}

// Ensure that histogramBucketCountMismatchError is an ingesterError.
var _ ingesterError = nativeHistogramValidationError{}

// Ensure that histogramBucketCountMismatchError is an softError.
var _ softError = nativeHistogramValidationError{}

// unavailableError is an ingesterError indicating that the ingester is unavailable.
type unavailableError struct {
	state services.State
}

func (e unavailableError) Error() string {
	return fmt.Sprintf(integerUnavailableMsgFormat, e.state.String())
}

func (e unavailableError) errorCause() mimirpb.ErrorCause {
	return mimirpb.SERVICE_UNAVAILABLE
}

// Ensure that unavailableError is an ingesterError.
var _ ingesterError = unavailableError{}

func newUnavailableError(state services.State) unavailableError {
	return unavailableError{state: state}
}

type instanceLimitReachedError struct {
	message string
}

func newInstanceLimitReachedError(message string) instanceLimitReachedError {
	return instanceLimitReachedError{message: message}
}

func (e instanceLimitReachedError) Error() string {
	return e.message
}

func (e instanceLimitReachedError) errorCause() mimirpb.ErrorCause {
	return mimirpb.INSTANCE_LIMIT
}

// Ensure that instanceLimitReachedError is an ingesterError.
var _ ingesterError = instanceLimitReachedError{}

// tsdbUnavailableError is an ingesterError indicating that the TSDB is unavailable.
type tsdbUnavailableError struct {
	message string
}

func newTSDBUnavailableError(message string) tsdbUnavailableError {
	return tsdbUnavailableError{message: message}
}

func (e tsdbUnavailableError) Error() string {
	return e.message
}

func (e tsdbUnavailableError) errorCause() mimirpb.ErrorCause {
	return mimirpb.TSDB_UNAVAILABLE
}

// Ensure that tsdbUnavailableError is an ingesterError.
var _ ingesterError = tsdbUnavailableError{}

type ingesterTooBusyError struct{}

func (e ingesterTooBusyError) Error() string {
	return ingesterTooBusyMsg
}

func (e ingesterTooBusyError) errorCause() mimirpb.ErrorCause {
	return mimirpb.TOO_BUSY
}

// Ensure that ingesterTooBusyError is an ingesterError.
var _ ingesterError = ingesterTooBusyError{}

type ingesterPushGrpcDisabledError struct{}

func (e ingesterPushGrpcDisabledError) Error() string {
	return ingesterPushGrpcDisabledMsg
}

func (e ingesterPushGrpcDisabledError) errorCause() mimirpb.ErrorCause {
	return mimirpb.METHOD_NOT_ALLOWED
}

// Ensure that ingesterPushGrpcDisabledError is an ingesterError.
var _ ingesterError = ingesterPushGrpcDisabledError{}

type circuitBreakerOpenError struct {
	requestType    string
	remainingDelay time.Duration
}

func newCircuitBreakerOpenError(requestType string, remainingDelay time.Duration) circuitBreakerOpenError {
	return circuitBreakerOpenError{requestType: requestType, remainingDelay: remainingDelay}
}

func (e circuitBreakerOpenError) Error() string {
	return fmt.Sprintf("%s on %s request type with remaining delay %s", circuitbreaker.ErrOpen.Error(), e.requestType, e.remainingDelay.String())
}

func (e circuitBreakerOpenError) errorCause() mimirpb.ErrorCause {
	return mimirpb.CIRCUIT_BREAKER_OPEN
}

var _ ingesterError = circuitBreakerOpenError{}

type ingesterErrSamplers struct {
	sampleTimestampTooOld             *log.Sampler
	sampleTimestampTooOldOOOEnabled   *log.Sampler
	sampleTimestampTooFarInFuture     *log.Sampler
	sampleOutOfOrder                  *log.Sampler
	sampleDuplicateTimestamp          *log.Sampler
	maxSeriesPerMetricLimitExceeded   *log.Sampler
	maxMetadataPerMetricLimitExceeded *log.Sampler
	maxSeriesPerUserLimitExceeded     *log.Sampler
	maxMetadataPerUserLimitExceeded   *log.Sampler
	nativeHistogramValidationError    *log.Sampler
}

func newIngesterErrSamplers(freq int64) ingesterErrSamplers {
	return ingesterErrSamplers{
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
	}
}

// mapPushErrorToErrorWithStatus maps the given error to the corresponding error of type globalerror.ErrorWithStatus.
func mapPushErrorToErrorWithStatus(err error) error {
	var (
		ingesterErr ingesterError
		errCode     = codes.Internal
		wrappedErr  = err
	)
	if errors.As(err, &ingesterErr) {
		switch ingesterErr.errorCause() {
		case mimirpb.BAD_DATA:
			errCode = codes.InvalidArgument
		case mimirpb.TENANT_LIMIT:
			errCode = codes.FailedPrecondition
		case mimirpb.SERVICE_UNAVAILABLE:
			errCode = codes.Unavailable
		case mimirpb.INSTANCE_LIMIT:
			errCode = codes.Unavailable
			wrappedErr = middleware.DoNotLogError{Err: err}
		case mimirpb.TSDB_UNAVAILABLE:
			errCode = codes.Internal
		case mimirpb.METHOD_NOT_ALLOWED:
			errCode = codes.Unimplemented
		case mimirpb.CIRCUIT_BREAKER_OPEN:
			errCode = codes.Unavailable
		}
	}
	return newErrorWithStatus(wrappedErr, errCode)
}

// mapReadErrorToErrorWithStatus maps the given error to the corresponding error of type globalerror.ErrorWithStatus.
func mapReadErrorToErrorWithStatus(err error) error {
	var (
		ingesterErr ingesterError
		errCode     = codes.Internal
	)
	if errors.As(err, &ingesterErr) {
		switch ingesterErr.errorCause() {
		case mimirpb.TOO_BUSY:
			errCode = codes.ResourceExhausted
		case mimirpb.SERVICE_UNAVAILABLE:
			errCode = codes.Unavailable
		case mimirpb.METHOD_NOT_ALLOWED:
			return newErrorWithStatus(err, codes.Unimplemented)
		case mimirpb.CIRCUIT_BREAKER_OPEN:
			return newErrorWithStatus(err, codes.Unavailable)
		}
	}
	return newErrorWithStatus(err, errCode)
}
