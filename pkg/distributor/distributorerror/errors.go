// SPDX-License-Identifier: AGPL-3.0-only

package distributorerror

import (
	"errors"
	"fmt"
	"net/http"
)

const (
	// 529 is non-standard status code used by some services to signal that "The service is overloaded".
	StatusServiceOverloaded = 529
)

// ReplicasNotMatch is an error stating that replicas do not match.
type ReplicasNotMatch struct {
	replica, elected, format string
}

// ReplicasNotMatchErrorf formats and returns a ReplicasNotMatch error.
// The error message is formatted according to the given format specifier, replica and elected.
// In order to print the limit and the burst, the format specifier must contain two %s verbs.
func ReplicasNotMatchErrorf(format, replica, elected string) ReplicasNotMatch {
	return ReplicasNotMatch{
		replica: replica,
		elected: elected,
		format:  format,
	}
}

func (e ReplicasNotMatch) Error() string {
	return fmt.Sprintf(e.format, e.replica, e.elected)
}

// TooManyClusters is an error stating that there are too many HA clusters.
type TooManyClusters struct {
	limit  int
	format string
}

// TooManyClustersErrorf formats and returns a TooManyClusters error.
// The error message is formatted according to the given format specifier and limit.
// In order to print the limit, the format specifier must contain a %d verb.
func TooManyClustersErrorf(format string, limit int) TooManyClusters {
	return TooManyClusters{limit: limit, format: format}
}

func (e TooManyClusters) Error() string {
	return fmt.Sprintf(e.format, e.limit)
}

// Validation is an error, used to represent all validation errors from the validation package.
type Validation struct {
	error
}

// ValidationErrorf formats and returns a Validation error.
// The error message is formatted according to the given format specifier and arguments.
// The new error does not retain any of the passed parameters.
func ValidationErrorf(format string, args ...any) Validation {
	// In order to ensure that no label is retained, we first create a string out of the
	// given format and args, and then create an error containing that message.
	msg := fmt.Sprintf(format, args...)
	return Validation{error: errors.New(msg)}
}

// IngestionRateLimited is an error used to represent the ingestion rate limited error.
type IngestionRateLimited struct {
	format string
	limit  float64
	burst  int
}

// IngestionRateLimitedErrorf formats and returns a IngestionRateLimited error.
// The error message is formatted according to the given format specifier, limit and burst.
// In order to print the limit and the burst, the format specifier must contain %v and %d verbs.
func IngestionRateLimitedErrorf(format string, limit float64, burst int) IngestionRateLimited {
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

// RequestRateLimitedErrorf formats and returns a RequestRateLimited error.
// The error message is formatted according to the given format specifier, limit and burst.
// In order to print the limit and the burst, the format specifier must contain %v and %d verbs.
func RequestRateLimitedErrorf(format string, limit float64, burst int) RequestRateLimited {
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
