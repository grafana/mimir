// SPDX-License-Identifier: AGPL-3.0-only

package distributorerror

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/log"
)

func TestNewReplicasNotMatchError(t *testing.T) {
	replica := "a"
	elected := "b"
	err := NewReplicasDidNotMatch(replica, elected)
	assert.Error(t, err)
	expectedMsg := fmt.Sprintf("replicas did not match, rejecting sample: replica=%s, elected=%s", replica, elected)
	assert.EqualError(t, err, expectedMsg)

	anotherErr := NewReplicasDidNotMatch("c", "d")
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &ReplicasDidNotMatch{}))
	assert.False(t, errors.As(err, &TooManyClusters{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &ReplicasDidNotMatch{}))
}

func TestNewTooManyClustersError(t *testing.T) {
	message := "this is an error"
	err := NewTooManyClusters(message)
	assert.Error(t, err)
	assert.EqualError(t, err, message)

	anotherErr := NewTooManyClusters(message)
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &TooManyClusters{}))
	assert.False(t, errors.As(err, &ReplicasDidNotMatch{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &TooManyClusters{}))
}

func TestNewValidationError(t *testing.T) {
	validationMsg := "this is a validation error"
	firstErr := errors.New(validationMsg)

	err := NewValidation(firstErr)
	assert.Error(t, err)
	assert.EqualError(t, err, validationMsg)

	anotherErr := NewValidation(errors.New("this is another validation error"))
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &Validation{}))
	assert.False(t, errors.As(err, &ReplicasDidNotMatch{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &Validation{}))
}

func TestNewIngestionRateError(t *testing.T) {
	errorMsg := "this is an ingestion rate limited error"
	err := NewIngestionRateLimited(errorMsg)
	assert.Error(t, err)
	assert.EqualError(t, err, errorMsg)

	anotherErr := NewIngestionRateLimited(errorMsg)
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &IngestionRateLimited{}))
	assert.False(t, errors.As(err, &ReplicasDidNotMatch{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &IngestionRateLimited{}))
}

func TestNewRequestRateError(t *testing.T) {
	errorMsg := "this is a request rate limited error"
	err := RequestRateLimitedErrorf(errorMsg)
	assert.Error(t, err)
	assert.EqualError(t, err, errorMsg)

	anotherErr := RequestRateLimitedErrorf(errorMsg)
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &RequestRateLimited{}))
	assert.False(t, errors.As(err, &ReplicasDidNotMatch{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &RequestRateLimited{}))
}

func TestToHTTPStatusHandler(t *testing.T) {
	originalMsg := "this is an error"
	originalErr := errors.New(originalMsg)
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
			name:               "a ReplicasDidNotMatch gets translated into 202, true",
			err:                NewReplicasDidNotMatch("a", "b"),
			expectedHTTPStatus: http.StatusAccepted,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of a ReplicasDidNotMatch gets translated into 202, true",
			err:                log.DoNotLogError{Err: NewReplicasDidNotMatch("a", "b")},
			expectedHTTPStatus: http.StatusAccepted,
			expectedOutcome:    true,
		},
		{
			name:               "a TooManyClusters gets translated into 400, true",
			err:                NewTooManyClusters("limit %d"),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of a TooManyClusters gets translated into 400, true",
			err:                log.DoNotLogError{Err: NewTooManyClusters("limit %d")},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "a Validation gets translated into 400, true",
			err:                NewValidation(originalErr),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of a Validation gets translated into 400, true",
			err:                log.DoNotLogError{Err: NewValidation(originalErr)},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "an IngestionRateLimited gets translated into an HTTP 429",
			err:                NewIngestionRateLimited("%v - %d"),
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of an IngestionRateLimited gets translated into an HTTP 429",
			err:                log.DoNotLogError{Err: NewIngestionRateLimited("%v - %d")},
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedOutcome:    true,
		},
		{
			name:                        "a RequestRateLimited with serviceOverloadErrorEnabled gets translated into an HTTP 529",
			err:                         RequestRateLimitedErrorf("%v - %d"),
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
			expectedOutcome:             true,
		},
		{
			name:                        "a DoNotLog error of a RequestRateLimited with serviceOverloadErrorEnabled gets translated into an HTTP 529",
			err:                         log.DoNotLogError{Err: RequestRateLimitedErrorf("%v - %d")},
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
			expectedOutcome:             true,
		},
		{
			name:                        "a RequestRateLimited without serviceOverloadErrorEnabled gets translated into an HTTP 429",
			err:                         RequestRateLimitedErrorf("%v - %d"),
			serviceOverloadErrorEnabled: false,
			expectedHTTPStatus:          http.StatusTooManyRequests,
			expectedOutcome:             true,
		},
		{
			name:                        "a DoNotLog error of a RequestRateLimited without serviceOverloadErrorEnabled gets translated into an HTTP 429",
			err:                         log.DoNotLogError{Err: RequestRateLimitedErrorf("%v - %d")},
			serviceOverloadErrorEnabled: false,
			expectedHTTPStatus:          http.StatusTooManyRequests,
			expectedOutcome:             true,
		},
	}

	for _, tc := range testCases {
		httpStatus, outcome := ToHTTPStatus(tc.err, tc.serviceOverloadErrorEnabled)
		require.Equal(t, tc.expectedHTTPStatus, httpStatus)
		require.Equal(t, tc.expectedOutcome, outcome)
	}
}
