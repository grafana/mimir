// SPDX-License-Identifier: AGPL-3.0-only

package distributorerror

import (
	"errors"
	"fmt"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

// ReplicasNotMatch is an error stating that replicas do not match.
// This error is not exposed in the Error catalog.
type ReplicasNotMatch struct {
	error
}

func NewReplicasNotMatchError(replica, elected string) ReplicasNotMatch {
	return ReplicasNotMatch{
		fmt.Errorf("replicas did not mach, rejecting sample: replica=%s, elected=%s", replica, elected),
	}
}

// TooManyClusters is an error stating that there are too many HA clusters.
// In the Error catalog, the ID of this error is globalerror.TooManyHAClusters.
type TooManyClusters struct {
	error
}

func NewTooManyClustersError(limit int) TooManyClusters {
	return TooManyClusters{
		errors.New(
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
	error
}

func NewValidationError(err error) Validation {
	return Validation{
		errors.New(
			err.Error(),
		),
	}
}

// IngestionRateLimited is an error used to represent the ingestion rate limited error.
// In the error catalog, the ID of this error is globalerror.IngestionRateLimited.
type IngestionRateLimited struct {
	error
}

func NewIngestionRateLimitedError(limit float64, burst int) IngestionRateLimited {
	return IngestionRateLimited{
		errors.New(
			validation.FormatIngestionRateLimitedMessage(
				limit,
				burst,
			),
		),
	}
}

// RequestRateLimited is an error used to represent the request rate limited error.
// In the error catalog, the ID of this error is globalerror.RequestRateLimited.
type RequestRateLimited struct {
	error
}

func NewRequestRateLimitedError(limit float64, burst int) RequestRateLimited {
	return RequestRateLimited{
		errors.New(
			validation.FormatRequestRateLimitedMessage(
				limit,
				burst,
			),
		),
	}
}
