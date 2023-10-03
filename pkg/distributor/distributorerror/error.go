// SPDX-License-Identifier: AGPL-3.0-only

package distributorerror

import "github.com/grafana/mimir/pkg/util/validation"

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

// PushError makes DistributorPushError implement pusherror.Error interface.
func (e DistributorPushError) PushError() {}

// Error makes DistributorPushError implement Error interface.
func (e DistributorPushError) DistributorError() {}

// NewDistributorPushError wraps the given error into a DistributorPushError.
func NewDistributorPushError(err error) DistributorPushError {
	return DistributorPushError{
		err: err,
	}
}

// ReplicasNotMatchDistributorPushError is an implementation of Error,
// corresponding to distributor.replicasNotMatchError.
type ReplicasNotMatchDistributorPushError struct {
	DistributorPushError
}

func NewReplicasNotMatchDistributorPushError(err error) ReplicasNotMatchDistributorPushError {
	return ReplicasNotMatchDistributorPushError{
		DistributorPushError{
			err: err,
		},
	}
}

// TooManyClustersDistributorPushError is an implementation of Error,
// corresponding to distributor.tooManyClustersError.
type TooManyClustersDistributorPushError struct {
	DistributorPushError
}

func NewTooManyClustersDistributorPushError(err error) TooManyClustersDistributorPushError {
	return TooManyClustersDistributorPushError{
		DistributorPushError{
			err: err,
		},
	}
}

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
