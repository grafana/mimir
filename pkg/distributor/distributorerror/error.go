// SPDX-License-Identifier: AGPL-3.0-only

package distributorerror

import (
	"fmt"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

type errorMessage string

func (e errorMessage) Error() string {
	return string(e)
}

// ReplicasNotMatch is an error stating that replicas do not match.
// This error is not exposed in the Error catalog.
type ReplicasNotMatch struct {
	errorMessage
}

func NewReplicasNotMatchError(replica, elected string) ReplicasNotMatch {
	return ReplicasNotMatch{
		errorMessage(
			fmt.Sprintf("replicas did not mach, rejecting sample: replica=%s, elected=%s", replica, elected),
		),
	}
}

// TooManyClusters is an error stating that there are too many HA clusters.
// In the Error catalog, the ID of this error is globalerror.TooManyHAClusters.
type TooManyClusters struct {
	errorMessage
}

func NewTooManyClustersError(limit int) TooManyClusters {
	return TooManyClusters{
		errorMessage(
			globalerror.TooManyHAClusters.MessageWithPerTenantLimitConfig(
				fmt.Sprintf("the write request has been rejected because the maximum number of high-availability (HA) clusters has been reached for this tenant (limit: %d)", limit),
				validation.HATrackerMaxClustersFlag,
			),
		),
	}
}

// Validation is an error, used to represent all validation errors from the validation package.
// All of those errors have an ID exposed in the Error catalog.
type Validation struct {
	errorMessage
}

func NewValidationError(err error) Validation {
	return Validation{
		errorMessage(
			err.Error(),
		),
	}
}

// IngestionRate is an error used to represent the ingestion rate limited error.
// In the error catalog, the ID of this error is globalerror.IngestionRateLimited.
type IngestionRate struct {
	errorMessage
}

func NewIngestionRateError(limit float64, burst int) IngestionRate {
	return IngestionRate{
		errorMessage(
			validation.FormatIngestionRateLimitedError(
				limit,
				burst,
			),
		),
	}
}

// RequestRate is an error used to represent the request rate limited error.
// In the error catalog, the ID of this error is globalerror.RequestRateLimited.
type RequestRate struct {
	errorMessage
}

func NewRequestRateError(limit float64, burst int) RequestRate {
	return RequestRate{
		errorMessage(
			validation.FormatRequestRateLimitedError(
				limit,
				burst,
			),
		),
	}
}
