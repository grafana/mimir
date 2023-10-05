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
	format := "%s - %s"
	err := ReplicasNotMatchErrorf(format, replica, elected)
	assert.Error(t, err)
	expectedMsg := fmt.Sprintf(format, replica, elected)
	assert.EqualError(t, err, expectedMsg)

	anotherErr := ReplicasNotMatchErrorf("%s - %s", "c", "d")
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
	format := "this is a limit %d"
	err := TooManyClustersErrorf(format, limit)
	assert.Error(t, err)
	expectedMsg := fmt.Sprintf(format, limit)
	assert.EqualError(t, err, expectedMsg)

	anotherErr := TooManyClustersErrorf(format, 2)
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
	firstErr := errors.New(validationMsg)
	format := "this is a bad format %w"
	secondErr := fmt.Errorf(format, firstErr)
	assert.ErrorIs(t, secondErr, firstErr)

	err := ValidationErrorf(format, firstErr)
	assert.Error(t, err)
	assert.NotErrorIs(t, err, firstErr)
	assert.EqualError(t, err, fmt.Sprintf(format, firstErr))

	anotherErr := ValidationErrorf("this is another validation error")
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
	format := "values are limit: %v, burst: %d"
	limit := 10.0
	burst := 10
	err := IngestionRateLimitedErrorf(format, limit, burst)
	assert.Error(t, err)
	expectedMsg := fmt.Sprintf(format, limit, burst)
	assert.EqualError(t, err, expectedMsg)

	anotherErr := IngestionRateLimitedErrorf(format, 15.0, 15)
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
	format := "values are limit: %v, burst: %d"
	limit := 10.0
	burst := 10
	err := RequestRateLimitedErrorf(format, limit, burst)
	assert.Error(t, err)
	expectedMsg := fmt.Sprintf(format, limit, burst)
	assert.EqualError(t, err, expectedMsg)

	anotherErr := RequestRateLimitedErrorf(format, 15.0, 15)
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
			name:               "a ReplicasNotMatch gets translated into 202, true",
			err:                ReplicasNotMatchErrorf("%s - %s", "a", "b"),
			expectedHTTPStatus: http.StatusAccepted,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of a ReplicasNotMatch gets translated into 202, true",
			err:                log.DoNotLogError{Err: ReplicasNotMatchErrorf("%s - %s", "a", "b")},
			expectedHTTPStatus: http.StatusAccepted,
			expectedOutcome:    true,
		},
		{
			name:               "a TooManyClusters gets translated into 400, true",
			err:                TooManyClustersErrorf("limit %d", 1),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of a TooManyClusters gets translated into 400, true",
			err:                log.DoNotLogError{Err: TooManyClustersErrorf("limit %d", 1)},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "a Validation gets translated into 400, true",
			err:                ValidationErrorf(originalMsg),
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of a Validation gets translated into 400, true",
			err:                log.DoNotLogError{Err: ValidationErrorf(originalMsg)},
			expectedHTTPStatus: http.StatusBadRequest,
			expectedOutcome:    true,
		},
		{
			name:               "an IngestionRateLimited gets translated into an HTTP 429",
			err:                IngestionRateLimitedErrorf("%v - %d", 10, 10),
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedOutcome:    true,
		},
		{
			name:               "a DoNotLog error of an IngestionRateLimited gets translated into an HTTP 429",
			err:                log.DoNotLogError{Err: IngestionRateLimitedErrorf("%v - %d", 10, 10)},
			expectedHTTPStatus: http.StatusTooManyRequests,
			expectedOutcome:    true,
		},
		{
			name:                        "a RequestRateLimited with serviceOverloadErrorEnabled gets translated into an HTTP 529",
			err:                         RequestRateLimitedErrorf("%v - %d", 10, 10),
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
			expectedOutcome:             true,
		},
		{
			name:                        "a DoNotLog error of a RequestRateLimited with serviceOverloadErrorEnabled gets translated into an HTTP 529",
			err:                         log.DoNotLogError{Err: RequestRateLimitedErrorf("%v - %d", 10, 10)},
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
			expectedOutcome:             true,
		},
		{
			name:                        "a RequestRateLimited without serviceOverloadErrorEnabled gets translated into an HTTP 429",
			err:                         RequestRateLimitedErrorf("%v - %d", 10, 10),
			serviceOverloadErrorEnabled: false,
			expectedHTTPStatus:          http.StatusTooManyRequests,
			expectedOutcome:             true,
		},
		{
			name:                        "a DoNotLog error of a RequestRateLimited without serviceOverloadErrorEnabled gets translated into an HTTP 429",
			err:                         log.DoNotLogError{Err: RequestRateLimitedErrorf("%v - %d", 10, 10)},
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
