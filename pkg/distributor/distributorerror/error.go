// SPDX-License-Identifier: AGPL-3.0-only

package distributorerror

import (
	"fmt"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

// DistributorPushError is a generic error returned on
// distributor's write path.
type DistributorPushError struct {
	err error
}

// Error makes DistributorPushError implement error interface.
func (e DistributorPushError) Error() string {
	return e.err.Error()
}

// Unwrap makes DistributorPushError implement Wrapper interface.
func (e DistributorPushError) Unwrap() error {
	return e.err
}

// NewDistributorPushError wraps the given error into a DistributorPushError.
func NewDistributorPushError(err error) DistributorPushError {
	return DistributorPushError{
		err: err,
	}
}

// ReplicasNotMatchError is an implementation of Error,
// meaning that replicas do not match.
type ReplicasNotMatchError struct {
	replica, elected string
}

func NewReplicasNotMatchError(replica, elected string) ReplicasNotMatchError {
	return ReplicasNotMatchError{
		replica: replica,
		elected: elected,
	}
}

// Error makes ReplicasNotMatchError implement error interface.
func (e ReplicasNotMatchError) Error() string {
	return fmt.Sprintf("replicas did not mach, rejecting sample: replica=%s, elected=%s", e.replica, e.elected)
}

// TooManyClustersError is an implementation of Error,
// meaning that there are too many HA clusters (globalerror.TooManyHAClusters).
type TooManyClustersError struct {
	limit int
}

func NewTooManyClustersError(limit int) TooManyClustersError {
	return TooManyClustersError{
		limit: limit,
	}
}

// Error makes TooManyClustersError implement error interface.
func (e TooManyClustersError) Error() string {
	return globalerror.TooManyHAClusters.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the write request has been rejected because the maximum number of high-availability (HA) clusters has been reached for this tenant (limit: %d)", e.limit),
		validation.HATrackerMaxClustersFlag)
}

// ValidationError is an implementation of Error,
// used to represent all implementations of validation.ValidationError.
type ValidationError struct {
	DistributorPushError
	err string
}

func NewValidationError(err validation.ValidationError) ValidationError {
	return ValidationError{
		DistributorPushError: DistributorPushError{
			err: err,
		},
		err: err.Error(),
	}
}

func (e ValidationError) Error() string {
	return e.err
}

// IngestionRateError is an implementation of Error,
// used to represent the ingestion rate limited error (globalerror.IngestionRateLimited).
type IngestionRateError struct {
	DistributorPushError
}

func NewIngestionRateError(limit float64, burst int) IngestionRateError {
	return IngestionRateError{
		DistributorPushError{
			err: validation.NewIngestionRateLimitedError(
				limit,
				burst,
			),
		},
	}
}

// RequestRateError is an implementation of Error,
// used to represent the request rate limited error (globalerror.RequestRateLimited).
type RequestRateError struct {
	DistributorPushError
	serviceOverloadErrorEnabled bool
}

func NewRequestRateError(limit float64, burst int, enableServiceOverloadError bool) RequestRateError {
	return RequestRateError{
		DistributorPushError: DistributorPushError{
			err: validation.NewRequestRateLimitedError(
				limit,
				burst,
			),
		},
		serviceOverloadErrorEnabled: enableServiceOverloadError,
	}
}

func (e RequestRateError) ServiceOverloadErrorEnabled() bool {
	return e.serviceOverloadErrorEnabled
}
