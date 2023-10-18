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

	"github.com/gogo/status"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	integerUnavailableMsgFormat = "ingester is unavailable (current state: %s)"
)

// errorWithStatus is used for wrapping errors returned by ingester.
type errorWithStatus struct {
	err    error // underlying error
	status *status.Status
}

// newErrorWithStatus creates a new errorWithStatus backed by the given error,
// and containing the given gRPC code. If the given error is an ingesterError,
// the resulting errorWithStatus will be enriched by the details backed by
// ingesterError.errorCause. These details are of type mimirpb.WriteErrorDetails.
func newErrorWithStatus(originalErr error, code codes.Code) errorWithStatus {
	var (
		ingesterErr  ingesterError
		errorDetails *mimirpb.WriteErrorDetails
	)
	if errors.As(originalErr, &ingesterErr) {
		errorDetails = &mimirpb.WriteErrorDetails{Cause: ingesterErr.errorCause()}
	}
	stat := status.New(code, originalErr.Error())

	if errorDetails != nil {
		if statWithDetails, err := stat.WithDetails(errorDetails); err == nil {
			return errorWithStatus{
				err:    originalErr,
				status: statWithDetails,
			}
		}
	}
	return errorWithStatus{
		err:    originalErr,
		status: stat,
	}
}

// newErrorWithHTTPStatus creates a new errorWithStatus backed by the given error,
// and containing the given HTTP status code.
// TODO this is needed for backwards compatibility only and should be removed
// once httpgrpc.Errorf() usages in ingester are removed.
func newErrorWithHTTPStatus(err error, code int) errorWithStatus {
	errWithHTTPStatus := httpgrpc.Errorf(code, err.Error())
	stat, _ := status.FromError(errWithHTTPStatus)
	return errorWithStatus{
		err:    err,
		status: stat,
	}
}

func (e errorWithStatus) Error() string {
	return e.status.Message()
}

func (e errorWithStatus) Unwrap() error {
	return e.err
}

func (e errorWithStatus) GRPCStatus() *grpcstatus.Status {
	if stat, ok := e.status.Err().(interface{ GRPCStatus() *grpcstatus.Status }); ok {
		return stat.GRPCStatus()
	}
	return nil
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

// sampleError is an ingesterError indicating a problem with a sample.
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

// Ensure that sampleError is an ingesterError.
var _ ingesterError = sampleError{}

// Ensure that sampleError is a softError.
var _ softError = sampleError{}

func newSampleError(errID globalerror.ID, errMsg string, timestamp model.Time, labels []mimirpb.LabelAdapter) sampleError {
	return sampleError{
		errID:     errID,
		errMsg:    errMsg,
		timestamp: timestamp,
		series:    mimirpb.FromLabelAdaptersToLabels(labels).String(),
	}
}

func newSampleTimestampTooOldError(timestamp model.Time, labels []mimirpb.LabelAdapter) sampleError {
	return newSampleError(globalerror.SampleTimestampTooOld, "the sample has been rejected because its timestamp is too old", timestamp, labels)
}

func newSampleTimestampTooOldOOOEnabledError(timestamp model.Time, labels []mimirpb.LabelAdapter, oooTimeWindow time.Duration) sampleError {
	return newSampleError(globalerror.SampleTimestampTooOld, fmt.Sprintf("the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window of %s", model.Duration(oooTimeWindow).String()), timestamp, labels)
}

func newSampleTimestampTooFarInFutureError(timestamp model.Time, labels []mimirpb.LabelAdapter) sampleError {
	return newSampleError(globalerror.SampleTooFarInFuture, "received a sample whose timestamp is too far in the future", timestamp, labels)
}

func newSampleOutOfOrderError(timestamp model.Time, labels []mimirpb.LabelAdapter) sampleError {
	return newSampleError(globalerror.SampleOutOfOrder, "the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed", timestamp, labels)
}

func newSampleDuplicateTimestampError(timestamp model.Time, labels []mimirpb.LabelAdapter) sampleError {
	return newSampleError(globalerror.SampleDuplicateTimestamp, "the sample has been rejected because another sample with the same timestamp, but a different value, has already been ingested", timestamp, labels)
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
		seriesLabels:   mimirpb.FromLabelAdaptersToLabels(seriesLabels).String(),
		exemplarLabels: mimirpb.FromLabelAdaptersToLabels(exemplarLabels).String(),
	}
}

func newExemplarMissingSeriesError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) exemplarError {
	return newExemplarError(globalerror.ExemplarSeriesMissing, "the exemplar has been rejected because the related series has not been ingested yet", timestamp, seriesLabels, exemplarLabels)
}

func newExemplarTimestampTooFarInFutureError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) exemplarError {
	return newExemplarError(globalerror.ExemplarTooFarInFuture, "received an exemplar whose timestamp is too far in the future", timestamp, seriesLabels, exemplarLabels)
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
		seriesLabels:   mimirpb.FromLabelAdaptersToLabels(seriesLabels).String(),
		exemplarLabels: mimirpb.FromLabelAdaptersToLabels(exemplarLabels).String(),
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
	return mimirpb.BAD_DATA
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
	return mimirpb.BAD_DATA
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

func (e perMetricSeriesLimitReachedError) errorCause() mimirpb.ErrorCause {
	return mimirpb.BAD_DATA
}

func (e perMetricSeriesLimitReachedError) soft() {}

// Ensure that perMetricSeriesLimitReachedError is an ingesterError.
var _ ingesterError = perMetricSeriesLimitReachedError{}

// Ensure that perMetricSeriesLimitReachedError is an softError.
var _ softError = perMetricSeriesLimitReachedError{}

// perMetricMetadataLimitReachedError is an ingesterError indicating that a per-metric metadata limit has been reached.
type perMetricMetadataLimitReachedError struct {
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

func (e perMetricMetadataLimitReachedError) errorCause() mimirpb.ErrorCause {
	return mimirpb.BAD_DATA
}

func (e perMetricMetadataLimitReachedError) soft() {}

// Ensure that perMetricMetadataLimitReachedError is an ingesterError.
var _ ingesterError = perMetricMetadataLimitReachedError{}

// Ensure that perMetricMetadataLimitReachedError is an softError.
var _ softError = perMetricMetadataLimitReachedError{}

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

func handlePushError(err error) error {
	var ingesterErr ingesterError
	if errors.As(err, &ingesterErr) {
		switch ingesterErr.errorCause() {
		case mimirpb.BAD_DATA:
			return newErrorWithHTTPStatus(err, http.StatusBadRequest)
		case mimirpb.SERVICE_UNAVAILABLE:
			return newErrorWithStatus(err, codes.Unavailable)
		case mimirpb.INSTANCE_LIMIT:
			return newErrorWithStatus(log.DoNotLogError{Err: err}, codes.Unavailable)
		case mimirpb.TSDB_UNAVAILABLE:
			return newErrorWithHTTPStatus(err, http.StatusServiceUnavailable)
		}
	}
	return err
}
