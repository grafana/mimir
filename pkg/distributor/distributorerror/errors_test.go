// SPDX-License-Identifier: AGPL-3.0-only

package distributorerror

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestNewReplicasNotMatchError(t *testing.T) {
	replica := "a"
	elected := "b"
	err := NewReplicasNotMatchError(replica, elected)
	assert.Error(t, err)
	expectedMsg := fmt.Sprintf("replicas did not mach, rejecting sample: replica=%s, elected=%s", replica, elected)
	assert.EqualError(t, err, expectedMsg)

	anotherErr := NewReplicasNotMatchError("c", "d")
	assert.NotErrorIs(t, err, anotherErr)

	var replicasNotMatch ReplicasNotMatch
	var tooManyClusters TooManyClusters
	assert.True(t, errors.As(err, &replicasNotMatch))
	assert.False(t, errors.As(err, &tooManyClusters))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &replicasNotMatch))
}

func TestNewTooManyClustersError(t *testing.T) {
	limit := 1
	err := NewTooManyClustersError(limit)
	assert.Error(t, err)
	expectedMsg := globalerror.TooManyHAClusters.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the write request has been rejected because the maximum number of high-availability (HA) clusters has been reached for this tenant (limit: %d)", limit),
		validation.HATrackerMaxClustersFlag,
	)
	assert.EqualError(t, err, expectedMsg)

	anotherErr := NewTooManyClustersError(2)
	assert.NotErrorIs(t, err, anotherErr)

	var tooManyClusters TooManyClusters
	var replicasNotMatch ReplicasNotMatch
	assert.True(t, errors.As(err, &tooManyClusters))
	assert.False(t, errors.As(err, &replicasNotMatch))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &tooManyClusters))
}

func TestNewValidationError(t *testing.T) {
	validationMsg := "this is a validation error"
	validationErr := errors.New(validationMsg)
	err := NewValidationError(validationErr)
	assert.Error(t, err)
	assert.EqualError(t, err, validationMsg)

	anotherErr := NewValidationError(
		errors.New("this is another validation error"),
	)
	assert.NotErrorIs(t, err, anotherErr)

	var validation Validation
	var replicasNotMatch ReplicasNotMatch
	assert.True(t, errors.As(err, &validation))
	assert.False(t, errors.As(err, &replicasNotMatch))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &validation))
}

func TestNewIngestionRateError(t *testing.T) {
	limit := 10.0
	burst := 10
	err := NewIngestionRateLimitedError(limit, burst)
	assert.Error(t, err)
	expectedMsg := validation.FormatIngestionRateLimitedMessage(limit, burst)
	assert.EqualError(t, err, expectedMsg)

	anotherErr := NewIngestionRateLimitedError(15.0, 15)
	assert.NotErrorIs(t, err, anotherErr)

	var ingestionRateLimited IngestionRateLimited
	var replicasNotMatch ReplicasNotMatch
	assert.True(t, errors.As(err, &ingestionRateLimited))
	assert.False(t, errors.As(err, &replicasNotMatch))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &ingestionRateLimited))
}

func TestNewRequestRateError(t *testing.T) {
	limit := 10.0
	burst := 10
	err := NewRequestRateLimitedError(limit, burst)
	assert.Error(t, err)
	expectedMsg := validation.FormatRequestRateLimitedMessage(limit, burst)
	assert.EqualError(t, err, expectedMsg)

	anotherErr := NewRequestRateLimitedError(15.0, 15)
	assert.NotErrorIs(t, err, anotherErr)

	var requestRateLimited RequestRateLimited
	var replicasNotMatch ReplicasNotMatch
	assert.True(t, errors.As(err, &requestRateLimited))
	assert.False(t, errors.As(err, &replicasNotMatch))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &requestRateLimited))
}

func TestToHTTPStatusHandler(t *testing.T) {
	originalErr := errors.New("this is an error")
	testCases := []struct {
		name                        string
		err                         error
		serviceOverloadErrorEnabled bool
		expectedHTTPStatus          int
		expectedOutcome             bool
	}{
		{
			name:               "a generic error gets translated into -1, false",
			err:                originalErr,
			expectedHTTPStatus: -1,
			expectedOutcome:    false,
		},
		{
			name:               "a DoNotLog error of a generic error gets translated into a -1, false",
			err:                log.DoNotLogError{Err: originalErr},
			expectedHTTPStatus: -1,
			expectedOutcome:    false,
		},
		{
			name:               "a ReplicasNotMatch gets translated into 202, true",
			err:                NewReplicasNotMatchError("a", "b"),
			expectedHTTPStatus: http.StatusAccepted,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of a ReplicasNotMatch gets translated into 202, true",
			err:                log.DoNotLogError{Err: NewReplicasNotMatchError("a", "b")},
			expectedHTTPStatus: http.StatusAccepted,
			expectedOutcome:    true,
		},
		{
			name:               "a TooManyClusters gets translated into 400, true",
			err:                NewTooManyClustersError(1),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of a TooManyClusters gets translated into 400, true",
			err:                log.DoNotLogError{Err: NewTooManyClustersError(1)},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "a Validation gets translated into 400, true",
			err:                NewValidationError(validation.ValidationError(originalErr)),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of a Validation gets translated into 400, true",
			err:                log.DoNotLogError{Err: NewValidationError(validation.ValidationError(originalErr))},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "an IngestionRateLimited gets translated into an HTTP 429",
			err:                NewIngestionRateLimitedError(10, 10),
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of an IngestionRateLimited gets translated into an HTTP 429",
			err:                log.DoNotLogError{Err: NewIngestionRateLimitedError(10, 10)},
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedOutcome:    true,
		},
		{
			name:                        "a RequestRateLimited with serviceOverloadErrorEnabled gets translated into an HTTP 529",
			err:                         NewRequestRateLimitedError(10, 10),
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
			expectedOutcome:             true,
		},
		{
			name:                        "a DoNotLog error of a RequestRateLimited with serviceOverloadErrorEnabled gets translated into an HTTP 529",
			err:                         log.DoNotLogError{Err: NewRequestRateLimitedError(10, 10)},
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
			expectedOutcome:             true,
		},
		{
			name:                        "a RequestRateLimited without serviceOverloadErrorEnabled gets translated into an HTTP 429",
			err:                         NewRequestRateLimitedError(10, 10),
			serviceOverloadErrorEnabled: false,
			expectedHTTPStatus:          http.StatusTooManyRequests,
			expectedOutcome:             true,
		},
		{
			name:                        "a DoNotLog error of a RequestRateLimited without serviceOverloadErrorEnabled gets translated into an HTTP 429",
			err:                         log.DoNotLogError{Err: NewRequestRateLimitedError(10, 10)},
			serviceOverloadErrorEnabled: false,
			expectedHTTPStatus:          http.StatusTooManyRequests,
			expectedOutcome:             true,
		},
	}

	for tn, tc := range testCases {
		fmt.Println(tn)
		httpStatus, outcome := ToHTTPStatus(tc.err, tc.serviceOverloadErrorEnabled)
		require.Equal(t, tc.expectedHTTPStatus, httpStatus)
		require.Equal(t, tc.expectedOutcome, outcome)
	}
}
