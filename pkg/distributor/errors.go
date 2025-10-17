// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gogo/status"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	deadlineExceededWrapMessage     = "exceeded configured distributor remote timeout"
	failedPushingToIngesterMessage  = "failed pushing to ingester"
	failedPushingToPartitionMessage = "failed pushing to partition"
)

var (
	errMissingRequestState                  = fmt.Errorf("context does not contain a request state")
	errReactiveLimiterPermitAlreadyAcquired = fmt.Errorf("reactive limiter permit already acquired")

	tooManyClustersMsgFormat = globalerror.TooManyHAClusters.MessageWithPerTenantLimitConfig(
		"the write request has been rejected because the maximum number of high-availability (HA) clusters has been reached for this tenant (limit: %d)",
		validation.HATrackerMaxClustersFlag,
	)

	ingestionRateLimitedMsgFormat = globalerror.IngestionRateLimited.MessageWithPerTenantLimitConfig(
		"the request has been rejected because the tenant exceeded the ingestion rate limit, set to %v items/s with a maximum allowed burst of %d. This limit is applied on the total number of samples, exemplars and metadata received across all distributors",
		validation.IngestionRateFlag,
		validation.IngestionBurstSizeFlag,
	)

	ingestionBurstSizeLimitedMsgFormat = globalerror.IngestionRateLimited.MessageWithPerTenantLimitConfig(
		"the request has been rejected because the tenant exceeded the ingestion burst size limit, set to %d, with %d items. This limit is applied on the total number of samples, exemplars and metadata received across all distributors",
		validation.IngestionBurstSizeFlag,
	)

	requestRateLimitedMsgFormat = globalerror.RequestRateLimited.MessageWithPerTenantLimitConfig(
		"the request has been rejected because the tenant exceeded the request rate limit, set to %v requests/s across all distributors with a maximum allowed burst of %d",
		validation.RequestRateFlag,
		validation.RequestBurstSizeFlag,
	)

	activeSeriesLimitedMsgFormat = globalerror.MaxActiveSeries.MessageWithPerTenantLimitConfig(
		"the request has been rejected because the tenant exceeded the active series limit, set to %d. %d series were rejected from this request of a total of %d",
		validation.MaxActiveSeriesPerUserFlag,
	)
)

// Error is a marker interface for the errors returned by distributor.
type Error interface {
	// Cause returns the cause of the error.
	Cause() mimirpb.ErrorCause
	// IsSoft returns whether it's a soft type of error (didn't halt ingestion).
	IsSoft() bool
}

// replicasDidNotMatchError is an error stating that replicas do not match.
type replicasDidNotMatchError struct {
	replica, elected string
}

// newReplicasDidNotMatchError creates a replicasDidNotMatchError error with the given parameters.
func newReplicasDidNotMatchError(replica, elected string) replicasDidNotMatchError {
	return replicasDidNotMatchError{
		replica: replica,
		elected: elected,
	}
}

func (e replicasDidNotMatchError) Error() string {
	return fmt.Sprintf("replicas did not match, rejecting sample: replica=%s, elected=%s", e.replica, e.elected)
}

func (e replicasDidNotMatchError) Cause() mimirpb.ErrorCause {
	return mimirpb.ERROR_CAUSE_REPLICAS_DID_NOT_MATCH
}

func (e replicasDidNotMatchError) IsSoft() bool {
	return false
}

// Ensure that replicasDidNotMatchError implements Error.
var _ Error = replicasDidNotMatchError{}

// tooManyClustersError is an error stating that there are too many HA clusters.
type tooManyClustersError struct {
	limit int
}

// newTooManyClustersError creates a tooManyClustersError error containing the given error message.
func newTooManyClustersError(limit int) tooManyClustersError {
	return tooManyClustersError{
		limit: limit,
	}
}

func (e tooManyClustersError) Error() string {
	return fmt.Sprintf(tooManyClustersMsgFormat, e.limit)
}

func (e tooManyClustersError) Cause() mimirpb.ErrorCause {
	return mimirpb.ERROR_CAUSE_TOO_MANY_CLUSTERS
}

func (e tooManyClustersError) IsSoft() bool {
	return false
}

// Ensure that tooManyClustersError implements Error.
var _ Error = tooManyClustersError{}

// validationError is an error, used to represent all validation errors from the validation package.
type validationError struct {
	error
}

// newValidationError wraps the given error into a validationError error.
func newValidationError(err error) validationError {
	return validationError{error: err}
}

func (e validationError) Cause() mimirpb.ErrorCause {
	return mimirpb.ERROR_CAUSE_BAD_DATA
}

func (e validationError) Unwrap() error {
	return e.error
}

func (e validationError) IsSoft() bool {
	return false
}

// Ensure that validationError implements Error.
var _ Error = validationError{}

type reactiveLimiterExceededError struct {
	error
}

func newReactiveLimiterExceededError(err error) reactiveLimiterExceededError {
	return reactiveLimiterExceededError{err}
}

func (e reactiveLimiterExceededError) Cause() mimirpb.ErrorCause {
	return mimirpb.ERROR_CAUSE_INSTANCE_LIMIT
}

func (e reactiveLimiterExceededError) IsSoft() bool {
	return false
}

var _ Error = reactiveLimiterExceededError{}

func newActiveSeriesLimitedError(totalSeriesInThisRequest, rejectedSeriesFromThisRequest, limit int) activeSeriesLimitedError {
	return activeSeriesLimitedError{
		totalSeriesInThisRequest:      totalSeriesInThisRequest,
		rejectedSeriesFromThisRequest: rejectedSeriesFromThisRequest,
		limit:                         limit,
	}
}

type activeSeriesLimitedError struct {
	totalSeriesInThisRequest      int
	rejectedSeriesFromThisRequest int
	limit                         int
}

func (e activeSeriesLimitedError) Error() string {
	return fmt.Sprintf(activeSeriesLimitedMsgFormat, e.limit, e.rejectedSeriesFromThisRequest, e.totalSeriesInThisRequest)
}

func (e activeSeriesLimitedError) Cause() mimirpb.ErrorCause {
	return mimirpb.ERROR_CAUSE_ACTIVE_SERIES_LIMITED
}

func (e activeSeriesLimitedError) IsSoft() bool {
	return false
}

// Ensure that activeSeriesLimitedError implements Error.
var _ Error = activeSeriesLimitedError{}

// ingestionRateLimitedError is an error used to represent the ingestion rate limited error.
type ingestionRateLimitedError struct {
	limit float64
	burst int
}

// newIngestionRateLimitedError creates an ingestionRateLimitedError error containing the given rate and burst size limits.
func newIngestionRateLimitedError(limit float64, burst int) ingestionRateLimitedError {
	return ingestionRateLimitedError{
		limit: limit,
		burst: burst,
	}
}

func (e ingestionRateLimitedError) Error() string {
	return fmt.Sprintf(ingestionRateLimitedMsgFormat, e.limit, e.burst)
}

func (e ingestionRateLimitedError) Cause() mimirpb.ErrorCause {
	return mimirpb.ERROR_CAUSE_INGESTION_RATE_LIMITED
}

func (e ingestionRateLimitedError) IsSoft() bool {
	return false
}

// Ensure that ingestionRateLimitedError implements Error.
var _ Error = ingestionRateLimitedError{}

// ingestionBurstSizeLimitedError represents the ingestion burst size limited error.
type ingestionBurstSizeLimitedError struct {
	burst int
	items int
}

// newIngestionBurstSizeLimitedError creates an ingestionBurstSizeLimitedError error containing the given burst size limit and number of items.
func newIngestionBurstSizeLimitedError(burst, items int) ingestionBurstSizeLimitedError {
	return ingestionBurstSizeLimitedError{
		burst: burst,
		items: items,
	}
}

func (e ingestionBurstSizeLimitedError) Error() string {
	return fmt.Sprintf(ingestionBurstSizeLimitedMsgFormat, e.burst, e.items)
}

func (e ingestionBurstSizeLimitedError) Cause() mimirpb.ErrorCause {
	return mimirpb.ERROR_CAUSE_INGESTION_RATE_LIMITED
}

func (e ingestionBurstSizeLimitedError) IsSoft() bool {
	return false
}

// Ensure that ingestionBurstSizeLimitedError implements Error.
var _ Error = ingestionBurstSizeLimitedError{}

// requestRateLimitedError is an error used to represent the request rate limited error.
type requestRateLimitedError struct {
	limit float64
	burst int
}

// newRequestRateLimitedError creates a requestRateLimitedError error containing the given error message.
func newRequestRateLimitedError(limit float64, burst int) requestRateLimitedError {
	return requestRateLimitedError{
		limit: limit,
		burst: burst,
	}
}

func (e requestRateLimitedError) Error() string {
	return fmt.Sprintf(requestRateLimitedMsgFormat, e.limit, e.burst)
}

func (e requestRateLimitedError) Cause() mimirpb.ErrorCause {
	return mimirpb.ERROR_CAUSE_REQUEST_RATE_LIMITED
}

func (e requestRateLimitedError) IsSoft() bool {
	return false
}

// Ensure that requestRateLimitedError implements Error.
var _ Error = requestRateLimitedError{}

// ingesterPushError is an error used to represent a failed attempt to push to the ingester.
type ingesterPushError struct {
	message string
	cause   mimirpb.ErrorCause
	soft    bool
}

// newIngesterPushError creates an ingesterPushError error representing the given status object.
func newIngesterPushError(stat *status.Status, ingesterID string) ingesterPushError {
	errorCause := mimirpb.ERROR_CAUSE_UNKNOWN
	softErr := false
	details := stat.Details()
	if len(details) == 1 {
		if errorDetails, ok := details[0].(*mimirpb.ErrorDetails); ok {
			errorCause = errorDetails.GetCause()
			softErr = errorDetails.GetSoft()
		}
	}
	message := fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, stat.Message())
	return ingesterPushError{
		message: message,
		cause:   errorCause,
		soft:    softErr,
	}
}

func (e ingesterPushError) Error() string {
	return e.message
}

func (e ingesterPushError) Cause() mimirpb.ErrorCause {
	return e.cause
}

func (e ingesterPushError) IsSoft() bool {
	return e.soft
}

// Ensure that ingesterPushError implements Error.
var _ Error = ingesterPushError{}

// partitionPushError is an error to represent a failed attempt to write to a partition.
type partitionPushError struct {
	err   error
	cause mimirpb.ErrorCause
}

func newPartitionPushError(err error, cause mimirpb.ErrorCause) partitionPushError {
	return partitionPushError{
		err:   err,
		cause: cause,
	}
}

func (e partitionPushError) Error() string {
	return e.err.Error()
}

func (e partitionPushError) Cause() mimirpb.ErrorCause {
	return e.cause
}

func (e partitionPushError) IsSoft() bool {
	return false
}

func (e partitionPushError) Unwrap() error {
	return e.err
}

// Ensure that partitionPushError implements Error.
var _ Error = partitionPushError{}

// toErrorWithGRPCStatus converts the given error into an appropriate gRPC error.
func toErrorWithGRPCStatus(pushErr error) error {
	var (
		distributorErr Error
		errDetails     *mimirpb.ErrorDetails
		errCode        = codes.Internal
	)
	if errors.As(pushErr, &distributorErr) {
		errDetails = &mimirpb.ErrorDetails{Cause: distributorErr.Cause()}
		errCode = errorCauseToGRPCStatusCode(distributorErr.Cause())
	}
	return globalerror.WrapErrorWithGRPCStatus(pushErr, errCode, errDetails).Err()
}

func errorCauseToGRPCStatusCode(errCause mimirpb.ErrorCause) codes.Code {
	switch errCause {
	case mimirpb.ERROR_CAUSE_BAD_DATA:
		return codes.InvalidArgument
	case mimirpb.ERROR_CAUSE_TENANT_LIMIT:
		return codes.FailedPrecondition
	case mimirpb.ERROR_CAUSE_INGESTION_RATE_LIMITED, mimirpb.ERROR_CAUSE_REQUEST_RATE_LIMITED, mimirpb.ERROR_CAUSE_ACTIVE_SERIES_LIMITED:
		return codes.ResourceExhausted
	case mimirpb.ERROR_CAUSE_REPLICAS_DID_NOT_MATCH:
		return codes.AlreadyExists
	case mimirpb.ERROR_CAUSE_TOO_MANY_CLUSTERS:
		return codes.FailedPrecondition
	case mimirpb.ERROR_CAUSE_CIRCUIT_BREAKER_OPEN, mimirpb.ERROR_CAUSE_INSTANCE_LIMIT:
		return codes.Unavailable
	case mimirpb.ERROR_CAUSE_METHOD_NOT_ALLOWED:
		return codes.Unimplemented
	}
	return codes.Internal
}

func errorCauseToHTTPStatusCode(errCause mimirpb.ErrorCause) int {
	switch errCause {
	case mimirpb.ERROR_CAUSE_BAD_DATA:
		return http.StatusBadRequest
	case mimirpb.ERROR_CAUSE_TENANT_LIMIT:
		return http.StatusBadRequest
	case mimirpb.ERROR_CAUSE_INGESTION_RATE_LIMITED, mimirpb.ERROR_CAUSE_REQUEST_RATE_LIMITED, mimirpb.ERROR_CAUSE_ACTIVE_SERIES_LIMITED:
		// Return a 429 to tell the client it is going too fast.
		// Client may discard the data or slow down and re-send.
		// Prometheus v2.26 added a remote-write option 'retry_on_http_429'.
		return http.StatusTooManyRequests
	case mimirpb.ERROR_CAUSE_REPLICAS_DID_NOT_MATCH:
		return http.StatusAccepted
	case mimirpb.ERROR_CAUSE_TOO_MANY_CLUSTERS:
		return http.StatusBadRequest
	case mimirpb.ERROR_CAUSE_TSDB_UNAVAILABLE:
		return http.StatusServiceUnavailable
	case mimirpb.ERROR_CAUSE_CIRCUIT_BREAKER_OPEN, mimirpb.ERROR_CAUSE_INSTANCE_LIMIT:
		return http.StatusServiceUnavailable
	case mimirpb.ERROR_CAUSE_METHOD_NOT_ALLOWED:
		// Return a 501 (and not 405) to explicitly signal a misconfiguration and to possibly track that amongst other 5xx errors.
		return http.StatusNotImplemented

	default:
		return http.StatusInternalServerError
	}
}

func wrapIngesterPushError(err error, ingesterID string) error {
	if err == nil {
		return nil
	}

	stat, ok := grpcutil.ErrorToStatus(err)
	if !ok {
		return errors.Wrap(err, fmt.Sprintf("%s %s", failedPushingToIngesterMessage, ingesterID))
	}
	statusCode := stat.Code()
	if util.IsHTTPStatusCode(statusCode) {
		// This code is needed for backwards compatibility, since ingesters may still return errors
		// created by httpgrpc.Errorf(). If pushErr is one of those errors, we just propagate it.
		// Wrap HTTP gRPC error with more explanatory message.
		return httpgrpc.Errorf(int(statusCode), "%s %s: %s", failedPushingToIngesterMessage, ingesterID, stat.Message())
	}

	return newIngesterPushError(stat, ingesterID)
}

func wrapPartitionPushError(err error, partitionID int32) error {
	if err == nil {
		return nil
	}

	// Add the partition ID to the error message.
	err = errors.Wrap(err, fmt.Sprintf("%s %d", failedPushingToPartitionMessage, partitionID))

	// Detect the cause.
	cause := mimirpb.ERROR_CAUSE_UNKNOWN
	if errors.Is(err, ingest.ErrWriteRequestDataItemTooLarge) {
		cause = mimirpb.ERROR_CAUSE_BAD_DATA
	}

	return newPartitionPushError(err, cause)
}

func wrapDeadlineExceededPushError(err error) error {
	if err != nil && errors.Is(err, context.DeadlineExceeded) {
		return errors.Wrap(err, deadlineExceededWrapMessage)
	}

	return err
}

func isIngestionClientError(err error) bool {
	var ingesterPushErr ingesterPushError
	if errors.As(err, &ingesterPushErr) {
		return ingesterPushErr.Cause() == mimirpb.ERROR_CAUSE_BAD_DATA
	}

	var partitionPushErr partitionPushError
	if errors.As(err, &partitionPushErr) {
		return partitionPushErr.Cause() == mimirpb.ERROR_CAUSE_BAD_DATA
	}

	// This code is needed for backwards compatibility, since ingesters may still return errors with HTTP status
	// code created by httpgrpc.Errorf(). If err is one of those errors, we treat 4xx errors as client errors.
	if code := grpcutil.ErrorToStatusCode(err); code/100 == 4 {
		return true
	}

	return false
}

type unavailableError struct {
	state services.State
}

var _ Error = unavailableError{}

func newUnavailableError(state services.State) unavailableError {
	return unavailableError{state: state}
}

func (e unavailableError) Error() string {
	return fmt.Sprintf("distributor is unavailable (current state: %s)", e.state.String())
}

func (e unavailableError) Cause() mimirpb.ErrorCause {
	return mimirpb.ERROR_CAUSE_SERVICE_UNAVAILABLE
}

func (e unavailableError) IsSoft() bool {
	return false
}
