// SPDX-License-Identifier: AGPL-3.0-only

package distributorerror

import (
	"fmt"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

type Error interface {
	error
	DistributorError()
}

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

// Error makes DistributorPushError implement Error interface.
func (e DistributorPushError) DistributorError() {}

// NewDistributorPushError wraps the given error into a DistributorPushError.
func NewDistributorPushError(err error) DistributorPushError {
	return DistributorPushError{
		err: err,
	}
}

// ReplicasNotMatchDistributorPushError is an implementation of Error,
// meaning that replicas do not match.
type ReplicasNotMatchDistributorPushError struct {
	replica, elected string
}

func NewReplicasNotMatchError(replica, elected string) ReplicasNotMatchDistributorPushError {
	return ReplicasNotMatchDistributorPushError{
		replica: replica,
		elected: elected,
	}
}

// Error makes ReplicasNotMatchDistributorPushError implement error interface.
func (e ReplicasNotMatchDistributorPushError) Error() string {
	return fmt.Sprintf("replicas did not mach, rejecting sample: replica=%s, elected=%s", e.replica, e.elected)
}

// DistributorError makes ReplicasNotMatchDistributorPushError implement Error interface.
func (e ReplicasNotMatchDistributorPushError) DistributorError() {}

// TooManyClustersDistributorPushError is an implementation of Error,
// meaning that there are too many HA clusters (globalerror.TooManyHAClusters).
type TooManyClustersDistributorPushError struct {
	limit int
}

func NewTooManyClustersError(limit int) TooManyClustersDistributorPushError {
	return TooManyClustersDistributorPushError{
		limit: limit,
	}
}

// Error makes TooManyClustersDistributorPushError implement error interface.
func (e TooManyClustersDistributorPushError) Error() string {
	return globalerror.TooManyHAClusters.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the write request has been rejected because the maximum number of high-availability (HA) clusters has been reached for this tenant (limit: %d)", e.limit),
		validation.HATrackerMaxClustersFlag)
}

// DistributorError makes TooManyClustersDistributorPushError implement Error interface.
func (e TooManyClustersDistributorPushError) DistributorError() {}

// ValidationDistributorPushError is an implementation of Error,
// used to represent all implementations of validation.ValidationError.
type ValidationDistributorPushError struct {
	DistributorPushError
	err string
}

func NewValidationDistributorPushError(err validation.ValidationError) ValidationDistributorPushError {
	return ValidationDistributorPushError{
		DistributorPushError: DistributorPushError{
			err: err,
		},
		err: err.Error(),
	}
}

func (e ValidationDistributorPushError) Error() string {
	return e.err
}

// IngestionRateDistributorPushError is an implementation of Error,
// used to represent the ingestion rate limited error (globalerror.IngestionRateLimited).
type IngestionRateDistributorPushError struct {
	DistributorPushError
}

func NewIngestionRateDistributorPushError(limit float64, burst int) IngestionRateDistributorPushError {
	return IngestionRateDistributorPushError{
		DistributorPushError{
			err: validation.NewIngestionRateLimitedError(
				limit,
				burst,
			),
		},
	}
}

// RequestRateDistributorPushError is an implementation of Error,
// used to represent the request rate limited error (globalerror.RequestRateLimited).
type RequestRateDistributorPushError struct {
	DistributorPushError
	serviceOverloadErrorEnabled bool
}

func NewRequestRateDistributorPushError(limit float64, burst int, enableServiceOverloadError bool) RequestRateDistributorPushError {
	return RequestRateDistributorPushError{
		DistributorPushError: DistributorPushError{
			err: validation.NewRequestRateLimitedError(
				limit,
				burst,
			),
		},
		serviceOverloadErrorEnabled: enableServiceOverloadError,
	}
}

func (e RequestRateDistributorPushError) ServiceOverloadErrorEnabled() bool {
	return e.serviceOverloadErrorEnabled
}
