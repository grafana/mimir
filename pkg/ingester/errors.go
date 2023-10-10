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

var (
	// This is the closest fitting Prometheus API error code for requests rejected due to limiting.
	tooBusyError = newErrorWithHTTPStatus(
		errors.New("the ingester is currently too busy to process queries, try again later"),
		http.StatusServiceUnavailable,
	)
)

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
	// If err matches one of the errors from this package,
	// we wrap it with userID and return it.
	if errors.As(err, &unavailableError{}) ||
		errors.As(err, &tsdbUnavailableError{}) ||
		errors.As(err, &instanceLimitReachedError{}) ||
		errors.As(err, &validationError{}) ||
		errors.As(err, &perUserLimitReachedError{}) ||
		errors.As(err, &perMetricLimitReachedError{}) {
		return fmt.Errorf("user=%s: %w", userID, err)
	}

	// Otherwise, we just annotate it with userID and return it.
	return fmt.Errorf("user=%s: %s", userID, err)
}

// validationError is an error indicating a problem with a sample or an exemplar.
type validationError struct {
	message string
}

func (e validationError) Error() string {
	return e.message
}

func newSampleError(errID globalerror.ID, errMsg string, timestamp model.Time, labels []mimirpb.LabelAdapter) validationError {
	return validationError{
		message: fmt.Sprintf(
			"%v. The affected sample has timestamp %s and is from series %s",
			errID.Message(errMsg),
			timestamp.Time().UTC().Format(time.RFC3339Nano),
			mimirpb.FromLabelAdaptersToLabels(labels).String(),
		),
	}
}

func newSampleTimestampTooOldError(timestamp model.Time, labels []mimirpb.LabelAdapter) validationError {
	return newSampleError(globalerror.SampleTimestampTooOld, "the sample has been rejected because its timestamp is too old", timestamp, labels)
}

func newSampleTimestampTooOldOOOEnabledError(timestamp model.Time, labels []mimirpb.LabelAdapter, oooTimeWindow time.Duration) validationError {
	return newSampleError(globalerror.SampleTimestampTooOld, fmt.Sprintf("the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window of %s", model.Duration(oooTimeWindow).String()), timestamp, labels)
}

func newSampleTimestampTooFarInFutureError(timestamp model.Time, labels []mimirpb.LabelAdapter) validationError {
	return newSampleError(globalerror.SampleTooFarInFuture, "received a sample whose timestamp is too far in the future", timestamp, labels)
}

func newSampleOutOfOrderError(timestamp model.Time, labels []mimirpb.LabelAdapter) validationError {
	return newSampleError(globalerror.SampleOutOfOrder, "the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed", timestamp, labels)
}

func newSampleDuplicateTimestampError(timestamp model.Time, labels []mimirpb.LabelAdapter) validationError {
	return newSampleError(globalerror.SampleDuplicateTimestamp, "the sample has been rejected because another sample with the same timestamp, but a different value, has already been ingested", timestamp, labels)
}

func newExemplarError(errID globalerror.ID, errMsg string, timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) validationError {
	return validationError{
		message: fmt.Sprintf(
			"%v. The affected exemplar is %s with timestamp %s for series %s",
			errID.Message(errMsg),
			mimirpb.FromLabelAdaptersToLabels(exemplarLabels).String(),
			timestamp.Time().UTC().Format(time.RFC3339Nano),
			mimirpb.FromLabelAdaptersToLabels(seriesLabels).String(),
		),
	}
}

func newExemplarMissingSeriesError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) validationError {
	return newExemplarError(globalerror.ExemplarSeriesMissing, "the exemplar has been rejected because the related series has not been ingested yet", timestamp, seriesLabels, exemplarLabels)
}

func newExemplarTimestampTooFarInFutureError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) validationError {
	return newExemplarError(globalerror.ExemplarTooFarInFuture, "received an exemplar whose timestamp is too far in the future", timestamp, seriesLabels, exemplarLabels)
}

func newTSDBExemplarOtherErr(ingestErr error, timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) error {
	if ingestErr == nil {
		return nil
	}

	return validationError{
		message: fmt.Sprintf("err: %v. timestamp=%s, series=%s, exemplar=%s",
			ingestErr,
			timestamp.Time().UTC().Format(time.RFC3339Nano),
			mimirpb.FromLabelAdaptersToLabels(seriesLabels).String(),
			mimirpb.FromLabelAdaptersToLabels(exemplarLabels).String(),
		),
	}
}

type limitKind int

const (
	seriesKind limitKind = iota
	metadataKind
)

// perUserLimitReachedError is an error indicating that a per-user limit has been reached.
type perUserLimitReachedError struct {
	limit int
	kind  limitKind
}

// newPerUserSeriesLimitReachedError creates a new perUserLimitReachedError indicating that a per-user series limit has been reached.
func newPerUserSeriesLimitReachedError(limit int) perUserLimitReachedError {
	return perUserLimitReachedError{
		limit: limit,
		kind:  seriesKind,
	}
}

// newPerUserMetadataLimitReachedError creates a new perUserLimitReachedError indicating that a per-user metadata limit has been reached.
func newPerUserMetadataLimitReachedError(limit int) perUserLimitReachedError {
	return perUserLimitReachedError{
		limit: limit,
		kind:  metadataKind,
	}
}

func (e perUserLimitReachedError) Error() string {
	switch e.kind {
	case seriesKind:
		return globalerror.MaxSeriesPerUser.MessageWithPerTenantLimitConfig(
			fmt.Sprintf("per-user series limit of %d exceeded", e.limit),
			validation.MaxSeriesPerUserFlag,
		)
	case metadataKind:
		return globalerror.MaxMetadataPerUser.MessageWithPerTenantLimitConfig(
			fmt.Sprintf("per-user metric metadata limit of %d exceeded", e.limit),
			validation.MaxMetadataPerUserFlag,
		)
	default:
		return fmt.Sprintf("per-user limit unknown error kind: %d", e.kind)
	}
}

// perMetricLimitReachedError is an error indicating that a per-metric limit has been reached.
type perMetricLimitReachedError struct {
	limit  int
	series string
	kind   limitKind
}

// newPerMetricSeriesLimitReachedError creates a new perMetricLimitReachedError indicating that a per-metric series limit has been reached.
func newPerMetricSeriesLimitReachedError(limit int, labels labels.Labels) perMetricLimitReachedError {
	return perMetricLimitReachedError{
		limit:  limit,
		series: labels.String(),
		kind:   seriesKind,
	}
}

// newPerMetricMetadataLimitReachedError creates a new perMetricLimitReachedError indicating that a per-metric metadata limit has been reached.
func newPerMetricMetadataLimitReachedError(limit int, labels labels.Labels) perMetricLimitReachedError {
	return perMetricLimitReachedError{
		limit:  limit,
		series: labels.String(),
		kind:   metadataKind,
	}
}

func (e perMetricLimitReachedError) Error() string {
	switch e.kind {
	case seriesKind:
		return fmt.Sprintf("%s This is for series %s",
			globalerror.MaxSeriesPerMetric.MessageWithPerTenantLimitConfig(
				fmt.Sprintf("per-metric series limit of %d exceeded", e.limit),
				validation.MaxSeriesPerMetricFlag,
			),
			e.series,
		)
	case metadataKind:
		return fmt.Sprintf("%s This is for series %s",
			globalerror.MaxMetadataPerMetric.MessageWithPerTenantLimitConfig(
				fmt.Sprintf("per-metric metadata limit of %d exceeded", e.limit),
				validation.MaxMetadataPerMetricFlag,
			),
			e.series,
		)
	default:
		return fmt.Sprintf("per-metric limit unknown error kind: %d", e.kind)
	}
}

// unavailableError is an error indicating that the ingester is unavailable.
type unavailableError struct {
	state string
}

func newUnavailableError(state string) unavailableError {
	return unavailableError{state: state}
}

func (e unavailableError) Error() string {
	return fmt.Sprintf(integerUnavailableMsgFormat, e.state)
}

// instanceLimitReachedError is an error indicating that an instance limit has been reached.
type instanceLimitReachedError struct {
	message string
}

func newInstanceLimitReachedError(message string) instanceLimitReachedError {
	return instanceLimitReachedError{message: message}
}

func (e instanceLimitReachedError) Error() string {
	return e.message
}

// tsdbUnavailableError is an error indicating that the TSDB is unavailable.
type tsdbUnavailableError struct {
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
