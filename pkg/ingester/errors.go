// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/errors.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
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
	ErrTooBusy          IngesterError = ingesterTooBusyError{}
	ErrPushGrpcDisabled               = NewErrorWithStatus(ingesterPushGrpcDisabledError{}, codes.Unimplemented)
)

// NewErrorWithStatus creates a new ErrorWithStatus backed by the given error,
// and containing the given gRPC code. If the given error is an ingesterError,
// the resulting ErrorWithStatus will be enriched by the details backed by
// ingesterError.errorCause. These details are of type mimirpb.ErrorDetails.
func NewErrorWithStatus(originalErr error, code codes.Code) globalerror.ErrorWithStatus {
	var (
		ingesterErr  IngesterError
		errorDetails *mimirpb.ErrorDetails
	)
	if errors.As(originalErr, &ingesterErr) {
		errorDetails = &mimirpb.ErrorDetails{Cause: ingesterErr.errorCause()}
	}
	return globalerror.WrapErrorWithGRPCStatus(originalErr, code, errorDetails)
}

// NewErrorWithHTTPStatus creates a new ErrorWithStatus backed by the given error,
// and containing the given HTTP status code.
func NewErrorWithHTTPStatus(err error, code int) globalerror.ErrorWithStatus {
	errWithHTTPStatus := httpgrpc.Errorf(code, err.Error())
	stat, _ := grpcutil.ErrorToStatus(errWithHTTPStatus)
	return globalerror.ErrorWithStatus{
		UnderlyingErr: err,
		Status:        stat,
	}
}

// IngesterError is a marker interface for the errors returned by ingester, and that are safe to wrap.
type IngesterError interface {
	error
	errorCause() mimirpb.ErrorCause
}

// SoftError is a marker interface for the errors on which ingester.Push should not stop immediately.
type SoftError interface {
	error
	soft()
}

type SoftIngesterError interface {
	SoftError
	IngesterError
}

// WrapOrAnnotateWithUser prepends the given userID to the given error.
// If the given error matches one of the errors from this package, the
// returned error retains a reference to the former.
func WrapOrAnnotateWithUser(err error, userID string) error {
	// If err is an ingesterError, we wrap it with userID and return it.
	var ingesterErr IngesterError
	if errors.As(err, &ingesterErr) {
		return fmt.Errorf("user=%s: %w", userID, err)
	}

	// Otherwise, we just annotate it with userID and return it.
	return fmt.Errorf("user=%s: %s", userID, err)
}

// sampleError is an IngesterError indicating a problem with a sample.
type sampleError struct {
	errID     globalerror.ID
	errMsg    string
	timestamp model.Time
	series    string
}

func (e sampleError) Error() string {
	return fmt.Sprintf(
		"%s. The affected sample has timestamp %s and is from series %s",
		e.errID.Message(e.errMsg),
		e.timestamp.Time().UTC().Format(time.RFC3339Nano),
		e.series,
	)
}

func (e sampleError) errorCause() mimirpb.ErrorCause {
	return mimirpb.BAD_DATA
}

func (e sampleError) soft() {}

func NewSampleError(errID globalerror.ID, errMsg string, timestamp model.Time, labels []mimirpb.LabelAdapter) SoftIngesterError {
	return sampleError{
		errID:     errID,
		errMsg:    errMsg,
		timestamp: timestamp,
		series:    mimirpb.FromLabelAdaptersToString(labels),
	}
}

func NewSampleTimestampTooOldError(timestamp model.Time, labels []mimirpb.LabelAdapter) SoftIngesterError {
	return NewSampleError(globalerror.SampleTimestampTooOld, "the sample has been rejected because its timestamp is too old", timestamp, labels)
}

func NewSampleTimestampTooOldOOOEnabledError(timestamp model.Time, labels []mimirpb.LabelAdapter, oooTimeWindow time.Duration) SoftIngesterError {
	return NewSampleError(globalerror.SampleTimestampTooOld, fmt.Sprintf("the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window of %s", model.Duration(oooTimeWindow).String()), timestamp, labels)
}

func NewSampleTimestampTooFarInFutureError(timestamp model.Time, labels []mimirpb.LabelAdapter) SoftIngesterError {
	return NewSampleError(globalerror.SampleTooFarInFuture, "received a sample whose timestamp is too far in the future", timestamp, labels)
}

func NewSampleOutOfOrderError(timestamp model.Time, labels []mimirpb.LabelAdapter) SoftIngesterError {
	return NewSampleError(globalerror.SampleOutOfOrder, "the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed", timestamp, labels)
}

func NewSampleDuplicateTimestampError(timestamp model.Time, labels []mimirpb.LabelAdapter) SoftIngesterError {
	return NewSampleError(globalerror.SampleDuplicateTimestamp, "the sample has been rejected because another sample with the same timestamp, but a different value, has already been ingested", timestamp, labels)
}

// exemplarError is an IngesterError indicating a problem with an exemplar.
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

func NewExemplarError(errID globalerror.ID, errMsg string, timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) SoftIngesterError {
	return exemplarError{
		errID:          errID,
		errMsg:         errMsg,
		timestamp:      timestamp,
		seriesLabels:   mimirpb.FromLabelAdaptersToString(seriesLabels),
		exemplarLabels: mimirpb.FromLabelAdaptersToString(exemplarLabels),
	}
}

func NewExemplarMissingSeriesError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) SoftIngesterError {
	return NewExemplarError(globalerror.ExemplarSeriesMissing, "the exemplar has been rejected because the related series has not been ingested yet", timestamp, seriesLabels, exemplarLabels)
}

func NewExemplarTimestampTooFarInFutureError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) SoftIngesterError {
	return NewExemplarError(globalerror.ExemplarTooFarInFuture, "received an exemplar whose timestamp is too far in the future", timestamp, seriesLabels, exemplarLabels)
}
func NewExemplarTimestampTooFarInPastError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) SoftIngesterError {
	return NewExemplarError(globalerror.ExemplarTooFarInPast, "received an exemplar whose timestamp is too far in the past", timestamp, seriesLabels, exemplarLabels)
}

// tsdbIngestExemplarErr is an IngesterError indicating a problem with an exemplar.
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

func NewTSDBIngestExemplarErr(ingestErr error, timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) SoftIngesterError {
	return tsdbIngestExemplarErr{
		originalErr:    ingestErr,
		timestamp:      timestamp,
		seriesLabels:   mimirpb.FromLabelAdaptersToString(seriesLabels),
		exemplarLabels: mimirpb.FromLabelAdaptersToString(exemplarLabels),
	}
}

// perUserSeriesLimitReachedError is an IngesterError indicating that a per-user series limit has been reached.
type perUserSeriesLimitReachedError struct {
	limit int
}

// NewPerUserSeriesLimitReachedError creates a new perUserMetadataLimitReachedError indicating that a per-user series limit has been reached.
func NewPerUserSeriesLimitReachedError(limit int) SoftIngesterError {
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
	return mimirpb.BAD_DATA
}

func (e perUserSeriesLimitReachedError) soft() {}

// perUserMetadataLimitReachedError is an IngesterError indicating that a per-user metadata limit has been reached.
type perUserMetadataLimitReachedError struct {
	limit int
}

// NewPerUserMetadataLimitReachedError creates a new perUserMetadataLimitReachedError indicating that a per-user metadata limit has been reached.
func NewPerUserMetadataLimitReachedError(limit int) SoftIngesterError {
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
	return mimirpb.BAD_DATA
}

func (e perUserMetadataLimitReachedError) soft() {}

// perMetricSeriesLimitReachedError is an IngesterError indicating that a per-metric series limit has been reached.
type perMetricSeriesLimitReachedError struct {
	limit  int
	series string
}

// NewPerMetricSeriesLimitReachedError creates a new perMetricMetadataLimitReachedError indicating that a per-metric series limit has been reached.
func NewPerMetricSeriesLimitReachedError(limit int, labels []mimirpb.LabelAdapter) SoftIngesterError {
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
	return mimirpb.BAD_DATA
}

func (e perMetricSeriesLimitReachedError) soft() {}

// perMetricMetadataLimitReachedError is an IngesterError indicating that a per-metric metadata limit has been reached.
type perMetricMetadataLimitReachedError struct {
	limit  int
	family string
}

// NewPerMetricMetadataLimitReachedError creates a new perMetricMetadataLimitReachedError indicating that a per-metric metadata limit has been reached.
func NewPerMetricMetadataLimitReachedError(limit int, family string) SoftIngesterError {
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
	return mimirpb.BAD_DATA
}

func (e perMetricMetadataLimitReachedError) soft() {}

// nativeHistogramValidationError indicates that native histogram bucket counts did not add up to the overall count.
type nativeHistogramValidationError struct {
	id           globalerror.ID
	originalErr  error
	seriesLabels []mimirpb.LabelAdapter
	timestamp    model.Time
}

func NewNativeHistogramValidationError(id globalerror.ID, originalErr error, timestamp model.Time, seriesLabels []mimirpb.LabelAdapter) SoftIngesterError {
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

// unavailableError is an IngesterError indicating that the ingester is unavailable.
type unavailableError struct {
	state services.State
}

func (e unavailableError) Error() string {
	return fmt.Sprintf(integerUnavailableMsgFormat, e.state.String())
}

func (e unavailableError) errorCause() mimirpb.ErrorCause {
	return mimirpb.SERVICE_UNAVAILABLE
}

// Ensure that unavailableError is an IngesterError.
var _ IngesterError = unavailableError{}

func NewUnavailableError(state services.State) unavailableError {
	return unavailableError{state: state}
}

type instanceLimitReachedError struct {
	message string
}

func NewInstanceLimitReachedError(message string) IngesterError {
	return instanceLimitReachedError{message: message}
}

func (e instanceLimitReachedError) Error() string {
	return e.message
}

func (e instanceLimitReachedError) errorCause() mimirpb.ErrorCause {
	return mimirpb.INSTANCE_LIMIT
}

// tsdbUnavailableError is an IngesterError indicating that the TSDB is unavailable.
type tsdbUnavailableError struct {
	message string
}

func NewTSDBUnavailableError(message string) IngesterError {
	return tsdbUnavailableError{message: message}
}

func (e tsdbUnavailableError) Error() string {
	return e.message
}

func (e tsdbUnavailableError) errorCause() mimirpb.ErrorCause {
	return mimirpb.TSDB_UNAVAILABLE
}

type ingesterTooBusyError struct{}

func (e ingesterTooBusyError) Error() string {
	return ingesterTooBusyMsg
}

func (e ingesterTooBusyError) errorCause() mimirpb.ErrorCause {
	return mimirpb.TOO_BUSY
}

type ingesterPushGrpcDisabledError struct{}

func (e ingesterPushGrpcDisabledError) Error() string {
	return ingesterPushGrpcDisabledMsg
}

func (e ingesterPushGrpcDisabledError) errorCause() mimirpb.ErrorCause {
	return mimirpb.METHOD_NOT_ALLOWED
}

// Ensure that ingesterPushGrpcDisabledError is an IngesterError.
var _ IngesterError = ingesterPushGrpcDisabledError{}

type circuitBreakerOpenError struct {
	requestType    string
	remainingDelay time.Duration
}

func NewCircuitBreakerOpenError(requestType string, remainingDelay time.Duration) IngesterError {
	return circuitBreakerOpenError{requestType: requestType, remainingDelay: remainingDelay}
}

func (e circuitBreakerOpenError) Error() string {
	return fmt.Sprintf("%s on %s request type with remaining delay %s", circuitbreaker.ErrOpen.Error(), e.requestType, e.remainingDelay.String())
}

func (e circuitBreakerOpenError) errorCause() mimirpb.ErrorCause {
	return mimirpb.CIRCUIT_BREAKER_OPEN
}

type ErrSamplers struct {
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

func NewErrSamplers(freq int64) ErrSamplers {
	return ErrSamplers{
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

// MapPushErrorToErrorWithStatus maps the given error to the corresponding error of type globalerror.ErrorWithStatus.
func MapPushErrorToErrorWithStatus(err error) error {
	var (
		ingesterErr IngesterError
		errCode     = codes.Internal
		wrappedErr  = err
	)
	if errors.As(err, &ingesterErr) {
		switch ingesterErr.errorCause() {
		case mimirpb.BAD_DATA:
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
	return NewErrorWithStatus(wrappedErr, errCode)
}

// MapPushErrorToErrorWithHTTPOrGRPCStatus maps IngesterError objects to an appropriate
// globalerror.ErrorWithStatus, which may contain both HTTP and gRPC error codes.
func MapPushErrorToErrorWithHTTPOrGRPCStatus(err error) error {
	var ingesterErr IngesterError
	if errors.As(err, &ingesterErr) {
		switch ingesterErr.errorCause() {
		case mimirpb.BAD_DATA:
			return NewErrorWithHTTPStatus(err, http.StatusBadRequest)
		case mimirpb.SERVICE_UNAVAILABLE:
			return NewErrorWithStatus(err, codes.Unavailable)
		case mimirpb.INSTANCE_LIMIT:
			return NewErrorWithStatus(middleware.DoNotLogError{Err: err}, codes.Unavailable)
		case mimirpb.TSDB_UNAVAILABLE:
			return NewErrorWithHTTPStatus(err, http.StatusServiceUnavailable)
		case mimirpb.METHOD_NOT_ALLOWED:
			return NewErrorWithStatus(err, codes.Unimplemented)
		case mimirpb.CIRCUIT_BREAKER_OPEN:
			return NewErrorWithStatus(err, codes.Unavailable)
		}
	}
	return err
}

// MapReadErrorToErrorWithStatus maps the given error to the corresponding error of type globalerror.ErrorWithStatus.
func MapReadErrorToErrorWithStatus(err error) error {
	var (
		ingesterErr IngesterError
		errCode     = codes.Internal
	)
	if errors.As(err, &ingesterErr) {
		switch ingesterErr.errorCause() {
		case mimirpb.TOO_BUSY:
			errCode = codes.ResourceExhausted
		case mimirpb.SERVICE_UNAVAILABLE:
			errCode = codes.Unavailable
		case mimirpb.METHOD_NOT_ALLOWED:
			return NewErrorWithStatus(err, codes.Unimplemented)
		case mimirpb.CIRCUIT_BREAKER_OPEN:
			return NewErrorWithStatus(err, codes.Unavailable)
		}
	}
	return NewErrorWithStatus(err, errCode)
}

// MapReadErrorToErrorWithHTTPOrGRPCStatus maps IngesterError objects to an appropriate
// globalerror.ErrorWithStatus, which may contain both HTTP and gRPC error codes.
func MapReadErrorToErrorWithHTTPOrGRPCStatus(err error) error {
	var (
		ingesterErr IngesterError
	)
	if errors.As(err, &ingesterErr) {
		switch ingesterErr.errorCause() {
		case mimirpb.TOO_BUSY:
			return NewErrorWithHTTPStatus(err, http.StatusServiceUnavailable)
		case mimirpb.SERVICE_UNAVAILABLE:
			return NewErrorWithStatus(err, codes.Unavailable)
		case mimirpb.METHOD_NOT_ALLOWED:
			return NewErrorWithStatus(err, codes.Unimplemented)
		case mimirpb.CIRCUIT_BREAKER_OPEN:
			return NewErrorWithStatus(err, codes.Unavailable)
		}
	}
	return err
}
