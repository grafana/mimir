// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/errors.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"errors"
	"fmt"
	"time"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	integerUnavailableMsgFormat = "ingester is unavailable (current state: %s)"
)

// safeToWrapError is a marker interface for the errors that are safe to wrap.
type safeToWrapError interface {
	safeToWrap()
}

// badDataError is a marker interface for the errors representing bad data.
// It is a safeToWrapError.
type badDataError interface {
	safeToWrapError
	badData()
}

// errorWithStatus is used for wrapping errors returned by ingester.
type errorWithStatus struct {
	err    error // underlying error
	status *status.Status
}

func newErrorWithStatus(err error, code codes.Code) errorWithStatus {
	return errorWithStatus{
		err:    err,
		status: status.New(code, err.Error()),
	}
}

func newErrorWithHTTPStatus(err error, code int) errorWithStatus {
	errWithHTTPStatus := httpgrpc.Errorf(code, err.Error())
	stat, _ := status.FromError(errWithHTTPStatus)
	return errorWithStatus{
		err:    err,
		status: stat,
	}
}

func (e errorWithStatus) Error() string {
	return e.status.String()
}

func (e errorWithStatus) Unwrap() error {
	return e.err
}

func (e errorWithStatus) GRPCStatus() *status.Status {
	return e.status
}

// wrapOrAnnotateWithUser prepends the given userID to the given error.
// If the given error matches one of the errors from this package, the
// returned error retains a reference to the former.
func wrapOrAnnotateWithUser(err error, userID string) error {
	// If err is a safeToWrapError, we wrap it with userID and return it.
	var safeToWrap safeToWrapError
	if errors.As(err, &safeToWrap) {
		return fmt.Errorf("user=%s: %w", userID, err)
	}

	// Otherwise, we just annotate it with userID and return it.
	return fmt.Errorf("user=%s: %s", userID, err)
}

// validationError is an error indicating a problem with a sample or an exemplar.
// It is a badDataError.
type validationError struct {
	badDataError
	message string
}

func (e validationError) Error() string {
	return e.message
}

func newSampleError(prefixMsg string, timestamp model.Time, labels []mimirpb.LabelAdapter) validationError {
	return validationError{
		message: fmt.Sprintf(
			"%s. The affected sample has timestamp %s and is from series %s",
			prefixMsg,
			timestamp.Time().UTC().Format(time.RFC3339Nano),
			mimirpb.FromLabelAdaptersToLabels(labels).String(),
		),
	}
}

func newSampleTimestampTooOldError(timestamp model.Time, labels []mimirpb.LabelAdapter) validationError {
	return newSampleError(globalerror.SampleTimestampTooOld.Message("the sample has been rejected because its timestamp is too old"), timestamp, labels)
}

func newSampleTimestampTooOldOOOEnabledError(timestamp model.Time, labels []mimirpb.LabelAdapter, oooTimeWindow time.Duration) validationError {
	return newSampleError(globalerror.SampleTimestampTooOld.Message(fmt.Sprintf("the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window of %s", model.Duration(oooTimeWindow).String())), timestamp, labels)
}

func newSampleTimestampTooFarInFutureError(timestamp model.Time, labels []mimirpb.LabelAdapter) validationError {
	return newSampleError(globalerror.SampleTooFarInFuture.Message("received a sample whose timestamp is too far in the future"), timestamp, labels)
}

func newSampleOutOfOrderError(timestamp model.Time, labels []mimirpb.LabelAdapter) validationError {
	return newSampleError(globalerror.SampleOutOfOrder.Message("the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed"), timestamp, labels)
}

func newSampleDuplicateTimestampError(timestamp model.Time, labels []mimirpb.LabelAdapter) validationError {
	return newSampleError(globalerror.SampleDuplicateTimestamp.Message("the sample has been rejected because another sample with the same timestamp, but a different value, has already been ingested"), timestamp, labels)
}

func newExemplarError(prefixMsg string, timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) validationError {
	return validationError{
		message: fmt.Sprintf(
			"%s. The affected exemplar is %s with timestamp %s for series %s",
			prefixMsg,
			mimirpb.FromLabelAdaptersToLabels(exemplarLabels).String(),
			timestamp.Time().UTC().Format(time.RFC3339Nano),
			mimirpb.FromLabelAdaptersToLabels(seriesLabels).String(),
		),
	}
}

func newExemplarMissingSeriesError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) validationError {
	return newExemplarError(globalerror.ExemplarSeriesMissing.Message("the exemplar has been rejected because the related series has not been ingested yet"), timestamp, seriesLabels, exemplarLabels)
}

func newExemplarTimestampTooFarInFutureError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) validationError {
	return newExemplarError(globalerror.ExemplarTooFarInFuture.Message("received an exemplar whose timestamp is too far in the future"), timestamp, seriesLabels, exemplarLabels)
}

func newTSDBExemplarOtherErr(ingestErr error, timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) error {
	return validationError{
		message: fmt.Sprintf("err: %v. timestamp=%s, series=%s, exemplar=%s",
			ingestErr,
			timestamp.Time().UTC().Format(time.RFC3339Nano),
			mimirpb.FromLabelAdaptersToLabels(seriesLabels).String(),
			mimirpb.FromLabelAdaptersToLabels(exemplarLabels).String(),
		),
	}
}

// perUserSeriesLimitReachedError is an error indicating that a per-user series limit has been reached.
// It is a badDataError.
type perUserSeriesLimitReachedError struct {
	badDataError
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

// perUserMetadataLimitReachedError is an error indicating that a per-user metadata limit has been reached.
// It is a badDataError.
type perUserMetadataLimitReachedError struct {
	badDataError
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

// perMetricMetadataLimitReachedError is an error indicating that a per-metric series limit has been reached.
// It is a badDataError.
type perMetricSeriesLimitReachedError struct {
	badDataError
	limit  int
	series string
}

// newPerMetricSeriesLimitReachedError creates a new perMetricMetadataLimitReachedError indicating that a per-metric series limit has been reached.
func newPerMetricSeriesLimitReachedError(limit int, labels labels.Labels) perMetricSeriesLimitReachedError {
	return perMetricSeriesLimitReachedError{
		limit:  limit,
		series: labels.String(),
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

// perMetricMetadataLimitReachedError is an error indicating that a per-metric metadata limit has been reached.
// It is a badDataError.
type perMetricMetadataLimitReachedError struct {
	badDataError
	limit  int
	series string
}

// newPerMetricMetadataLimitReachedError creates a new perMetricMetadataLimitReachedError indicating that a per-metric metadata limit has been reached.
func newPerMetricMetadataLimitReachedError(limit int, labels labels.Labels) perMetricMetadataLimitReachedError {
	return perMetricMetadataLimitReachedError{
		limit:  limit,
		series: labels.String(),
	}
}

func (e perMetricMetadataLimitReachedError) Error() string {
	return fmt.Sprintf("%s This is for series %s",
		globalerror.MaxMetadataPerMetric.MessageWithPerTenantLimitConfig(
			fmt.Sprintf("per-metric metadata limit of %d exceeded", e.limit),
			validation.MaxMetadataPerMetricFlag,
		),
		e.series,
	)
}

// unavailableError is an error indicating that the ingester is unavailable.
// It is a safeToWrapError.
type unavailableError struct {
	safeToWrapError
	state string
}

func newUnavailableError(state string) unavailableError {
	return unavailableError{state: state}
}

func (e unavailableError) Error() string {
	return fmt.Sprintf(integerUnavailableMsgFormat, e.state)
}

// instanceLimitReachedError is an error indicating that an instance limit has been reached.
// It is a safeToWrapError.
type instanceLimitReachedError struct {
	safeToWrapError
	message string
}

func newInstanceLimitReachedError(message string) instanceLimitReachedError {
	return instanceLimitReachedError{message: message}
}

func (e instanceLimitReachedError) Error() string {
	return e.message
}

// tsdbUnavailableError is an error indicating that the TSDB is unavailable.
// It is a safeToWrapError.
type tsdbUnavailableError struct {
	safeToWrapError
	message string
}

func newTSDBUnavailableError(message string) tsdbUnavailableError {
	return tsdbUnavailableError{message: message}
}

func (e tsdbUnavailableError) Error() string {
	return e.message
}

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
	}
}
