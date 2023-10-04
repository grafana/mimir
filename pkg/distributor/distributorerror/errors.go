// SPDX-License-Identifier: AGPL-3.0-only

package distributorerror

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/grafana/mimir/pkg/util/globalerror"
)

const (
	// 529 is non-standard status code used by some services to signal that "The service is overloaded".
	StatusServiceOverloaded = 529
)

// ReplicasNotMatch is an error stating that replicas do not match.
// This error is not exposed in the Error catalog.
type ReplicasNotMatch struct {
	replica, elected string
}

func NewReplicasNotMatchError(replica, elected string) ReplicasNotMatch {
	return ReplicasNotMatch{
		replica: replica,
		elected: elected,
	}
}

func (e ReplicasNotMatch) Error() string {
	return fmt.Sprintf("replicas did not match, rejecting sample: replica=%s, elected=%s", e.replica, e.elected)
}

// TooManyClusters is an error stating that there are too many HA clusters.
// In the Error catalog, the ID of this error is globalerror.TooManyHAClusters.
type TooManyClusters struct {
	limit int
	flag  string
}

func NewTooManyClustersError(limit int, flag string) TooManyClusters {
	return TooManyClusters{limit: limit, flag: flag}
}

func (e TooManyClusters) Error() string {
	return globalerror.TooManyHAClusters.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the write request has been rejected because the maximum number of high-availability (HA) clusters has been reached for this tenant (limit: %d)", e.limit),
		e.flag,
	)
}

// Validation is an error, used to represent all validation errors from the validation package.
// All of those errors have an ID exposed in the Error catalog.
type Validation struct {
	error
}

func NewValidationError(err error) Validation { return Validation{error: err} }

// IngestionRateLimited is an error used to represent the ingestion rate limited error.
type IngestionRateLimited struct {
	format string
	limit  float64
	burst  int
}

func NewIngestionRateLimitedError(format string, limit float64, burst int) IngestionRateLimited {
	return IngestionRateLimited{
		format: format,
		limit:  limit,
		burst:  burst,
	}
}

func (e IngestionRateLimited) Error() string {
	return fmt.Sprintf(e.format, e.limit, e.burst)
}

// RequestRateLimited is an error used to represent the request rate limited error.
type RequestRateLimited struct {
	format string
	limit  float64
	burst  int
}

func NewRequestRateLimitedError(format string, limit float64, burst int) RequestRateLimited {
	return RequestRateLimited{
		format: format,
		limit:  limit,
		burst:  burst,
	}
}

func (e RequestRateLimited) Error() string {
	return fmt.Sprintf(e.format, e.limit, e.burst)
}

// ToHTTPStatus converts the given error into an appropriate HTTP status corresponding
// to that error, if the error is one of the errors from this package. In that case,
// the resulting HTTP status is returned with status true. Otherwise, -1 and the status
// false are returned.
// TODO Remove this method once HTTP status codes are removed from distributor.Push.
// TODO This method should be moved into the push package.
func ToHTTPStatus(pushErr error, serviceOverloadErrorEnabled bool) (int, bool) {
	var (
		replicasNotMatchErr ReplicasNotMatch
		tooManyClustersErr  TooManyClusters
		validationErr       Validation
		ingestionRateErr    IngestionRateLimited
		requestRateErr      RequestRateLimited
	)

	switch {
	case errors.As(pushErr, &replicasNotMatchErr):
		return http.StatusAccepted, true
	case errors.As(pushErr, &tooManyClustersErr):
		return http.StatusBadRequest, true
	case errors.As(pushErr, &validationErr):
		return http.StatusBadRequest, true
	case errors.As(pushErr, &ingestionRateErr):
		// Return a 429 here to tell the client it is going too fast.
		// Client may discard the data or slow down and re-send.
		// Prometheus v2.26 added a remote-write option 'retry_on_http_429'.
		return http.StatusTooManyRequests, true
	case errors.As(pushErr, &requestRateErr):
		// Return a 429 or a 529 here depending on configuration to tell the client it is going too fast.
		// Client may discard the data or slow down and re-send.
		// Prometheus v2.26 added a remote-write option 'retry_on_http_429'.
		if serviceOverloadErrorEnabled {
			return StatusServiceOverloaded, true
		}
		return http.StatusTooManyRequests, true
	default:
		return -1, false
	}
}
