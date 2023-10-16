// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"errors"
	"fmt"

	"github.com/gogo/status"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	// 529 is non-standard status code used by some services to signal that "The service is overloaded".
	StatusServiceOverloaded     = 529
	deadlineExceededWrapMessage = "exceeded configured distributor remote timeout"
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

// distributorError is a marker interface for the errors returned by distributor.
type distributorError interface {
	errorCause() mimirpb.ErrorCause
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

func (e replicasDidNotMatchError) errorCause() mimirpb.ErrorCause {
	return mimirpb.REPLICAS_DID_NOT_MATCH
}

var _ distributorError = replicasDidNotMatchError{}

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

func (e tooManyClustersError) errorCause() mimirpb.ErrorCause {
	return mimirpb.TOO_MANY_CLUSTERS
}

var _ distributorError = tooManyClustersError{}

// validationError is an error, used to represent all validation errors from the validation package.
type validationError struct {
	error
}

// newValidationError wraps the given error into a validationError error.
func newValidationError(err error) validationError {
	return validationError{error: err}
}

func (e validationError) errorCause() mimirpb.ErrorCause {
	return mimirpb.BAD_DATA
}

var _ distributorError = validationError{}

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

func (e ingestionRateLimitedError) errorCause() mimirpb.ErrorCause {
	return mimirpb.INGESTION_RATE_LIMITED
}

var _ distributorError = ingestionRateLimitedError{}

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

func (e requestRateLimitedError) errorCause() mimirpb.ErrorCause {
	return mimirpb.REQUEST_RATE_LIMITED
}

var _ distributorError = requestRateLimitedError{}

// toGRPCError converts the given error into an appropriate gRPC error.
func toGRPCError(pushErr error, serviceOverloadErrorEnabled bool) error {
	var (
		distributorErr distributorError
		errDetails     *mimirpb.WriteErrorDetails
		errCode        = codes.Internal
	)
	if errors.As(pushErr, &distributorErr) {
		errDetails = &mimirpb.WriteErrorDetails{Cause: distributorErr.errorCause()}
		switch distributorErr.errorCause() {
		case mimirpb.BAD_DATA:
			errCode = codes.FailedPrecondition
		case mimirpb.INGESTION_RATE_LIMITED:
			errCode = codes.ResourceExhausted
		case mimirpb.REQUEST_RATE_LIMITED:
			if serviceOverloadErrorEnabled {
				errCode = codes.Unavailable
			} else {
				errCode = codes.ResourceExhausted
			}
		case mimirpb.REPLICAS_DID_NOT_MATCH:
			errCode = codes.AlreadyExists
		case mimirpb.TOO_MANY_CLUSTERS:
			errCode = codes.FailedPrecondition
		}
	}
	stat := status.New(errCode, pushErr.Error())
	if errDetails != nil {
		statWithDetails, err := stat.WithDetails(errDetails)
		if err == nil {
			return statWithDetails.Err()
		}
	}
	return stat.Err()
}
