// SPDX-License-Identifier: AGPL-3.0-only

package distributorerror

import (
	"fmt"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

type distributorPushError struct {
	err error
}

// Error makes distributorPushError implement error interface.
func (e distributorPushError) Error() string {
	return e.err.Error()
}

// Unwrap makes distributorPushError implement Wrapper interface.
func (e distributorPushError) Unwrap() error {
	return e.err
}

// ReplicasNotMatch is an error stating that replicas do not match.
// This error is not exposed in the Error catalog.
type ReplicasNotMatch struct {
	replica, elected string
}

func NewReplicasNotMatch(replica, elected string) ReplicasNotMatch {
	return ReplicasNotMatch{
		replica: replica,
		elected: elected,
	}
}

func (e ReplicasNotMatch) Error() string {
	return fmt.Sprintf("replicas did not mach, rejecting sample: replica=%s, elected=%s", e.replica, e.elected)
}

// TooManyClusters is an error stating that there are too many HA clusters.
// In the Error catalog, the ID of this error is globalerror.TooManyHAClusters.
type TooManyClusters struct {
	limit int
}

func NewTooManyClusters(limit int) TooManyClusters {
	return TooManyClusters{
		limit: limit,
	}
}

func (e TooManyClusters) Error() string {
	return globalerror.TooManyHAClusters.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the write request has been rejected because the maximum number of high-availability (HA) clusters has been reached for this tenant (limit: %d)", e.limit),
		validation.HATrackerMaxClustersFlag)
}

// Validation is an error, used to represent all validation errors from the validation package.
// All of those errors have an ID exposed in the Error catalog.
type Validation struct {
	distributorPushError
	err string
}

func NewValidation(err validation.ValidationError) Validation {
	return Validation{
		distributorPushError: distributorPushError{
			err: err,
		},
		err: err.Error(),
	}
}

func (e Validation) Error() string {
	return e.err
}

// IngestionRate is an error used to represent the ingestion rate limited error.
// In the error catalog, the ID of this error is globalerror.IngestionRateLimited.
type IngestionRate struct {
	distributorPushError
}

func NewIngestionRate(limit float64, burst int) IngestionRate {
	return IngestionRate{
		distributorPushError{
			err: validation.NewIngestionRateLimitedError(
				limit,
				burst,
			),
		},
	}
}

// RequestRate is an error used to represent the request rate limited error.
// In the error catalog, the ID of this error is globalerror.RequestRateLimited.
type RequestRate struct {
	distributorPushError
	serviceOverloadErrorEnabled bool
}

func NewRequestRate(limit float64, burst int, enableServiceOverloadError bool) RequestRate {
	return RequestRate{
		distributorPushError: distributorPushError{
			err: validation.NewRequestRateLimitedError(
				limit,
				burst,
			),
		},
		serviceOverloadErrorEnabled: enableServiceOverloadError,
	}
}

func (e RequestRate) ServiceOverloadErrorEnabled() bool {
	return e.serviceOverloadErrorEnabled
}
