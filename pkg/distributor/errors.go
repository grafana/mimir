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
	// 529 is non-standard status code used by some services to signal that "The service is overloaded".
	StatusServiceOverloaded         = 529
	deadlineExceededWrapMessage     = "exceeded configured distributor remote timeout"
	failedPushingToIngesterMessage  = "failed pushing to ingester"
	failedPushingToPartitionMessage = "failed pushing to partition"
)

var (
	tooManyClustersMsgFormat = globalerror.TooManyHAClusters.MessageWithPerTenantLimitConfig(
		"the write request has been rejected because the maximum number of high-availability (HA) clusters has been reached for this tenant (limit: %d)",
		validation.HATrackerMaxClustersFlag,
	)

	ingestionRateLimitedMsgFormat = globalerror.IngestionRateLimited.MessageWithPerTenantLimitConfig(
		"the request has been rejected because the tenant exceeded the ingestion rate limit, set to %v items/s with a maximum allowed burst of %d. This limit is applied on the total number of samples, exemplars and metadata received across all distributors",
		validation.IngestionRateFlag,
		validation.IngestionBurstSizeFlag,
	)

	requestRateLimitedMsgFormat = globalerror.RequestRateLimited.MessageWithPerTenantLimitConfig(
		"the request has been rejected because the tenant exceeded the request rate limit, set to %v requests/s across all distributors with a maximum allowed burst of %d",
		validation.RequestRateFlag,
		validation.RequestBurstSizeFlag,
	)
)

// Error is a marker interface for the errors returned by distributor.
type Error interface {
	// Cause returns the cause of the error.
	Cause() mimirpb.ErrorCause
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
	return mimirpb.REPLICAS_DID_NOT_MATCH
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
	return mimirpb.TOO_MANY_CLUSTERS
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
	return mimirpb.BAD_DATA
}

// Ensure that validationError implements Error.
var _ Error = validationError{}

// ingestionRateLimitedError is an error used to represent the ingestion rate limited error.
type ingestionRateLimitedError struct {
	limit float64
	burst int
}

// newIngestionRateLimitedError creates a ingestionRateLimitedError error containing the given error message.
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
	return mimirpb.INGESTION_RATE_LIMITED
}

// Ensure that ingestionRateLimitedError implements Error.
var _ Error = ingestionRateLimitedError{}

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
	return mimirpb.REQUEST_RATE_LIMITED
}

// Ensure that requestRateLimitedError implements Error.
var _ Error = requestRateLimitedError{}

// ingesterPushError is an error used to represent a failed attempt to push to the ingester.
type ingesterPushError struct {
	message string
	cause   mimirpb.ErrorCause
}

// newIngesterPushError creates a ingesterPushError error representing the given status object.
func newIngesterPushError(stat *status.Status, ingesterID string) ingesterPushError {
	errorCause := mimirpb.UNKNOWN_CAUSE
	details := stat.Details()
	if len(details) == 1 {
		if errorDetails, ok := details[0].(*mimirpb.ErrorDetails); ok {
			errorCause = errorDetails.GetCause()
		}
		if errorDetails, ok := details[0].(*grpcutil.ErrorDetails); ok {
			if errorDetails.Cause == grpcutil.WRONG_CLUSTER_NAME {
				errorCause = mimirpb.BAD_DATA
			}
		}
	}
	message := fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, stat.Message())
	return ingesterPushError{
		message: message,
		cause:   errorCause,
	}
}

func (e ingesterPushError) Error() string {
	return e.message
}

func (e ingesterPushError) Cause() mimirpb.ErrorCause {
	return e.cause
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

func (e partitionPushError) Unwrap() error {
	return e.err
}

// Ensure that partitionPushError implements Error.
var _ Error = partitionPushError{}

// toErrorWithGRPCStatus converts the given error into an appropriate gRPC error.
func toErrorWithGRPCStatus(pushErr error, serviceOverloadErrorEnabled bool) error {
	var (
		distributorErr Error
		errDetails     *mimirpb.ErrorDetails
		errCode        = codes.Internal
	)
	if errors.As(pushErr, &distributorErr) {
		errDetails = &mimirpb.ErrorDetails{Cause: distributorErr.Cause()}
		errCode = errorCauseToGRPCStatusCode(distributorErr.Cause(), serviceOverloadErrorEnabled)
	}
	return globalerror.WrapErrorWithGRPCStatus(pushErr, errCode, errDetails).Err()
}

func errorCauseToGRPCStatusCode(errCause mimirpb.ErrorCause, serviceOverloadErrorEnabled bool) codes.Code {
	switch errCause {
	case mimirpb.BAD_DATA:
		return codes.InvalidArgument
	case mimirpb.TENANT_LIMIT:
		return codes.FailedPrecondition
	case mimirpb.INGESTION_RATE_LIMITED, mimirpb.REQUEST_RATE_LIMITED:
		if serviceOverloadErrorEnabled {
			return codes.Unavailable
		}
		return codes.ResourceExhausted
	case mimirpb.REPLICAS_DID_NOT_MATCH:
		return codes.AlreadyExists
	case mimirpb.TOO_MANY_CLUSTERS:
		return codes.FailedPrecondition
	case mimirpb.CIRCUIT_BREAKER_OPEN:
		return codes.Unavailable
	case mimirpb.METHOD_NOT_ALLOWED:
		return codes.Unimplemented
	}
	return codes.Internal
}

func errorCauseToHTTPStatusCode(errCause mimirpb.ErrorCause, serviceOverloadErrorEnabled bool) int {
	switch errCause {
	case mimirpb.BAD_DATA:
		return http.StatusBadRequest
	case mimirpb.TENANT_LIMIT:
		return http.StatusBadRequest
	case mimirpb.INGESTION_RATE_LIMITED, mimirpb.REQUEST_RATE_LIMITED:
		// Return a 429 or a 529 here depending on configuration to tell the client it is going too fast.
		// Client may discard the data or slow down and re-send.
		// Prometheus v2.26 added a remote-write option 'retry_on_http_429'.
		if serviceOverloadErrorEnabled {
			return StatusServiceOverloaded
		}
		return http.StatusTooManyRequests
	case mimirpb.REPLICAS_DID_NOT_MATCH:
		return http.StatusAccepted
	case mimirpb.TOO_MANY_CLUSTERS:
		return http.StatusBadRequest
	case mimirpb.TSDB_UNAVAILABLE:
		return http.StatusServiceUnavailable
	case mimirpb.CIRCUIT_BREAKER_OPEN:
		return http.StatusServiceUnavailable
	case mimirpb.METHOD_NOT_ALLOWED:
		// Return a 501 (and not 405) to explicitly signal a misconfiguration and to possibly track that amongst other 5xx errors.
		return http.StatusNotImplemented
	}
	return http.StatusInternalServerError
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
	cause := mimirpb.UNKNOWN_CAUSE
	if errors.Is(err, ingest.ErrWriteRequestDataItemTooLarge) {
		cause = mimirpb.BAD_DATA
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
		return ingesterPushErr.Cause() == mimirpb.BAD_DATA
	}

	var partitionPushErr partitionPushError
	if errors.As(err, &partitionPushErr) {
		return partitionPushErr.Cause() == mimirpb.BAD_DATA
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
	return mimirpb.SERVICE_UNAVAILABLE
}
