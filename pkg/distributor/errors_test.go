// SPDX-License-Identifier: AGPL-3.0-only

package distributor

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
	err := newReplicasDidNotMatchError(replica, elected)
	assert.Error(t, err)
	expectedMsg := fmt.Sprintf("replicas did not match, rejecting sample: replica=%s, elected=%s", replica, elected)
	assert.EqualError(t, err, expectedMsg)

	anotherErr := newReplicasDidNotMatchError("c", "d")
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &replicasDidNotMatchError{}))
	assert.False(t, errors.As(err, &tooManyClustersError{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &replicasDidNotMatchError{}))
}

func TestNewTooManyClustersError(t *testing.T) {
	limit := 10
	err := newTooManyClustersError(limit)
	expectedErrorMsg := fmt.Sprintf(tooManyClustersMsgFormat, limit)
	assert.Error(t, err)
	assert.EqualError(t, err, expectedErrorMsg)

	anotherErr := newTooManyClustersError(20)
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &tooManyClustersError{}))
	assert.False(t, errors.As(err, &replicasDidNotMatchError{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &tooManyClustersError{}))
}

func TestNewValidationError(t *testing.T) {
	validationMsg := "this is a validation error"
	firstErr := errors.New(validationMsg)

	err := newValidationError(firstErr)
	assert.Error(t, err)
	assert.EqualError(t, err, validationMsg)

	anotherErr := newValidationError(errors.New("this is another validation error"))
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &validationError{}))
	assert.False(t, errors.As(err, &replicasDidNotMatchError{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &validationError{}))
}

func TestNewIngestionRateError(t *testing.T) {
	limit := 10.0
	burst := 10
	err := newIngestionRateLimitedError(limit, burst)
	expectedErrorMsg := fmt.Sprintf(ingestionRateLimitedMsgFormat, limit, burst)
	assert.Error(t, err)
	assert.EqualError(t, err, expectedErrorMsg)

	anotherErr := newIngestionRateLimitedError(20, 20)
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &ingestionRateLimitedError{}))
	assert.False(t, errors.As(err, &replicasDidNotMatchError{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &ingestionRateLimitedError{}))
}

func TestNewRequestRateError(t *testing.T) {
	limit := 10.0
	burst := 10
	err := newRequestRateLimitedError(limit, burst)
	expectedErrorMsg := fmt.Sprintf(requestRateLimitedMsgFormat, limit, burst)
	assert.Error(t, err)
	assert.EqualError(t, err, expectedErrorMsg)

	anotherErr := newRequestRateLimitedError(20, 20)
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &requestRateLimitedError{}))
	assert.False(t, errors.As(err, &replicasDidNotMatchError{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &requestRateLimitedError{}))
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
			name:               "a replicasDidNotMatchError gets translated into 202, true",
			err:                newReplicasDidNotMatchError("a", "b"),
			expectedHTTPStatus: http.StatusAccepted,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of a replicasDidNotMatchError gets translated into 202, true",
			err:                log.DoNotLogError{Err: newReplicasDidNotMatchError("a", "b")},
			expectedHTTPStatus: http.StatusAccepted,
			expectedOutcome:    true,
		},
		{
			name:               "a tooManyClustersError gets translated into 400, true",
			err:                newTooManyClustersError(10),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of a tooManyClustersError gets translated into 400, true",
			err:                log.DoNotLogError{Err: newTooManyClustersError(10)},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "a validationError gets translated into 400, true",
			err:                newValidationError(originalErr),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of a validationError gets translated into 400, true",
			err:                log.DoNotLogError{Err: newValidationError(originalErr)},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "an ingestionRateLimitedError gets translated into an HTTP 429",
			err:                newIngestionRateLimitedError(10, 10),
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of an ingestionRateLimitedError gets translated into an HTTP 429",
			err:                log.DoNotLogError{Err: newIngestionRateLimitedError(10, 10)},
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedOutcome:    true,
		},
		{
			name:                        "a requestRateLimitedError with serviceOverloadErrorEnabled gets translated into an HTTP 529",
			err:                         newRequestRateLimitedError(10, 10),
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
			expectedOutcome:             true,
		},
		{
			name:                        "a DoNotLog error of a requestRateLimitedError with serviceOverloadErrorEnabled gets translated into an HTTP 529",
			err:                         log.DoNotLogError{Err: newRequestRateLimitedError(10, 10)},
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
			expectedOutcome:             true,
		},
		{
			name:                        "a requestRateLimitedError without serviceOverloadErrorEnabled gets translated into an HTTP 429",
			err:                         newRequestRateLimitedError(10, 10),
			serviceOverloadErrorEnabled: false,
			expectedHTTPStatus:          http.StatusTooManyRequests,
			expectedOutcome:             true,
		},
		{
			name:                        "a DoNotLog error of a requestRateLimitedError without serviceOverloadErrorEnabled gets translated into an HTTP 429",
			err:                         log.DoNotLogError{Err: newRequestRateLimitedError(10, 10)},
			serviceOverloadErrorEnabled: false,
			expectedHTTPStatus:          http.StatusTooManyRequests,
			expectedOutcome:             true,
		},
	}

	for _, tc := range testCases {
		httpStatus, outcome := toHTTPStatus(tc.err, tc.serviceOverloadErrorEnabled)
		require.Equal(t, tc.expectedHTTPStatus, httpStatus)
		require.Equal(t, tc.expectedOutcome, outcome)
	}
}
