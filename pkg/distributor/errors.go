// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"fmt"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	// 529 is non-standard status code used by some services to signal that "The service is overloaded".
	StatusServiceOverloaded = 529
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

// ReplicasDidNotMatch is an error stating that replicas do not match.
type ReplicasDidNotMatch struct {
	replica, elected string
}

// NewReplicasDidNotMatch creates a ReplicasDidNotMatch error with the given parameters.
func NewReplicasDidNotMatch(replica, elected string) ReplicasDidNotMatch {
	return ReplicasDidNotMatch{
		replica: replica,
		elected: elected,
	}
}

func (e ReplicasDidNotMatch) Error() string {
	return fmt.Sprintf("replicas did not match, rejecting sample: replica=%s, elected=%s", e.replica, e.elected)
}

// TooManyClusters is an error stating that there are too many HA clusters.
type TooManyClusters struct {
	limit int
}

// NewTooManyClusters creates a TooManyClusters error containing the given error message.
func NewTooManyClusters(limit int) TooManyClusters {
	return TooManyClusters{
		limit: limit,
	}
}

func (e TooManyClusters) Error() string {
	return fmt.Sprintf(tooManyClustersMsgFormat, e.limit)
}

// Validation is an error, used to represent all validation errors from the validation package.
type Validation struct {
	error
}

// NewValidation wraps the given error into a Validation error.
func NewValidation(err error) Validation {
	return Validation{error: err}
}

// IngestionRateLimited is an error used to represent the ingestion rate limited error.
type IngestionRateLimited struct {
	limit float64
	burst int
}

// NewIngestionRateLimited creates a IngestionRateLimited error containing the given error message.
func NewIngestionRateLimited(limit float64, burst int) IngestionRateLimited {
	return IngestionRateLimited{
		limit: limit,
		burst: burst,
	}
}

func (e IngestionRateLimited) Error() string {
	return fmt.Sprintf(ingestionRateLimitedMsgFormat, e.limit, e.burst)
}

// RequestRateLimited is an error used to represent the request rate limited error.
type RequestRateLimited struct {
	limit float64
	burst int
}

// NewRequestRateLimited creates a RequestRateLimited error containing the given error message.
func NewRequestRateLimited(limit float64, burst int) RequestRateLimited {
	return RequestRateLimited{
		limit: limit,
		burst: burst,
	}
}

func (e RequestRateLimited) Error() string {
	return fmt.Sprintf(requestRateLimitedMsgFormat, e.limit, e.burst)
}
