// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"fmt"
	"testing"

	"github.com/gogo/status"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/log"
)

func TestNewReplicasNotMatchError(t *testing.T) {
	replica := "a"
	elected := "b"
	err := newReplicasDidNotMatchError(replica, elected)
	assert.Error(t, err)
	expectedMsg := fmt.Sprintf("replicas did not match, rejecting sample: replica=%s, elected=%s", replica, elected)
	assert.EqualError(t, err, expectedMsg)
	checkDistributorError(t, err, mimirpb.REPLICAS_DID_NOT_MATCH)

	anotherErr := newReplicasDidNotMatchError("c", "d")
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &replicasDidNotMatchError{}))
	assert.False(t, errors.As(err, &tooManyClustersError{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &replicasDidNotMatchError{}))
	checkDistributorError(t, wrappedErr, mimirpb.REPLICAS_DID_NOT_MATCH)
}

func TestNewTooManyClustersError(t *testing.T) {
	limit := 10
	err := newTooManyClustersError(limit)
	expectedErrorMsg := fmt.Sprintf(tooManyClustersMsgFormat, limit)
	assert.Error(t, err)
	assert.EqualError(t, err, expectedErrorMsg)
	checkDistributorError(t, err, mimirpb.TOO_MANY_CLUSTERS)

	anotherErr := newTooManyClustersError(20)
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &tooManyClustersError{}))
	assert.False(t, errors.As(err, &replicasDidNotMatchError{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &tooManyClustersError{}))
	checkDistributorError(t, wrappedErr, mimirpb.TOO_MANY_CLUSTERS)
}

func TestNewValidationError(t *testing.T) {
	validationMsg := "this is a validation error"
	firstErr := errors.New(validationMsg)

	err := newValidationError(firstErr)
	assert.Error(t, err)
	assert.EqualError(t, err, validationMsg)
	checkDistributorError(t, err, mimirpb.BAD_DATA)

	anotherErr := newValidationError(errors.New("this is another validation error"))
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &validationError{}))
	assert.False(t, errors.As(err, &replicasDidNotMatchError{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &validationError{}))
	checkDistributorError(t, wrappedErr, mimirpb.BAD_DATA)
}

func TestNewIngestionRateError(t *testing.T) {
	limit := 10.0
	burst := 10
	err := newIngestionRateLimitedError(limit, burst)
	expectedErrorMsg := fmt.Sprintf(ingestionRateLimitedMsgFormat, limit, burst)
	assert.Error(t, err)
	assert.EqualError(t, err, expectedErrorMsg)
	checkDistributorError(t, err, mimirpb.INGESTION_RATE_LIMITED)

	anotherErr := newIngestionRateLimitedError(20, 20)
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &ingestionRateLimitedError{}))
	assert.False(t, errors.As(err, &replicasDidNotMatchError{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &ingestionRateLimitedError{}))
	checkDistributorError(t, wrappedErr, mimirpb.INGESTION_RATE_LIMITED)
}

func TestNewRequestRateError(t *testing.T) {
	limit := 10.0
	burst := 10
	err := newRequestRateLimitedError(limit, burst)
	expectedErrorMsg := fmt.Sprintf(requestRateLimitedMsgFormat, limit, burst)
	assert.Error(t, err)
	assert.EqualError(t, err, expectedErrorMsg)
	checkDistributorError(t, err, mimirpb.REQUEST_RATE_LIMITED)

	anotherErr := newRequestRateLimitedError(20, 20)
	assert.NotErrorIs(t, err, anotherErr)

	assert.True(t, errors.As(err, &requestRateLimitedError{}))
	assert.False(t, errors.As(err, &replicasDidNotMatchError{}))

	wrappedErr := fmt.Errorf("wrapped %w", err)
	assert.ErrorIs(t, wrappedErr, err)
	assert.True(t, errors.As(wrappedErr, &requestRateLimitedError{}))
	checkDistributorError(t, wrappedErr, mimirpb.REQUEST_RATE_LIMITED)
}

func TestToGRPCError(t *testing.T) {
	originalMsg := "this is an error"
	originalErr := errors.New(originalMsg)
	replicasDidNotMatchErr := newReplicasDidNotMatchError("a", "b")
	tooManyClustersErr := newTooManyClustersError(10)
	ingestionRateLimitedErr := newIngestionRateLimitedError(10, 10)
	requestRateLimitedErr := newRequestRateLimitedError(10, 10)
	testCases := []struct {
		name                        string
		err                         error
		serviceOverloadErrorEnabled bool
		expectedGRPCCode            codes.Code
		expectedErrorMsg            string
		expectedErrorDetails        *mimirpb.WriteErrorDetails
	}{
		{
			name:             "a generic error gets translated into an Internal error with no details",
			err:              originalErr,
			expectedGRPCCode: codes.Internal,
			expectedErrorMsg: originalMsg,
		},
		{
			name:             "a DoNotLog error of a generic error gets translated into an Internal error with no details",
			err:              log.DoNotLogError{Err: originalErr},
			expectedGRPCCode: codes.Internal,
			expectedErrorMsg: originalMsg,
		},
		{
			name:                 "a replicasDidNotMatchError gets translated into an AlreadyExists error with REPLICASE_DID_NOT_MATCH cause",
			err:                  replicasDidNotMatchErr,
			expectedGRPCCode:     codes.AlreadyExists,
			expectedErrorMsg:     replicasDidNotMatchErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.REPLICAS_DID_NOT_MATCH},
		},
		{
			name:                 "a DoNotLotError of a replicasDidNotMatchError gets translated into an AlreadyExists error with REPLICASE_DID_NOT_MATCH cause",
			err:                  log.DoNotLogError{Err: replicasDidNotMatchErr},
			expectedGRPCCode:     codes.AlreadyExists,
			expectedErrorMsg:     replicasDidNotMatchErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.REPLICAS_DID_NOT_MATCH},
		},
		{
			name:                 "a tooManyClustersError gets translated into a FailedPrecondition error with TOO_MANY_CLUSTERS cause",
			err:                  tooManyClustersErr,
			expectedGRPCCode:     codes.FailedPrecondition,
			expectedErrorMsg:     tooManyClustersErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.TOO_MANY_CLUSTERS},
		},
		{
			name:                 "a DoNotLogError of a tooManyClustersError gets translated into a FailedPrecondition error with TOO_MANY_CLUSTERS cause",
			err:                  log.DoNotLogError{Err: tooManyClustersErr},
			expectedGRPCCode:     codes.FailedPrecondition,
			expectedErrorMsg:     tooManyClustersErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.TOO_MANY_CLUSTERS},
		},
		{
			name:                 "a validationError gets translated into gets translated into a FailedPrecondition error with VALIDATION cause",
			err:                  newValidationError(originalErr),
			expectedGRPCCode:     codes.FailedPrecondition,
			expectedErrorMsg:     originalMsg,
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		{
			name:                 "a DoNotLogError of a validationError gets translated into gets translated into a FailedPrecondition error with VALIDATION cause",
			err:                  log.DoNotLogError{Err: newValidationError(originalErr)},
			expectedGRPCCode:     codes.FailedPrecondition,
			expectedErrorMsg:     originalMsg,
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		{
			name:                 "an ingestionRateLimitedError gets translated into gets translated into a ResourceExhausted error with INGESTION_RATE_LIMITED cause",
			err:                  ingestionRateLimitedErr,
			expectedGRPCCode:     codes.ResourceExhausted,
			expectedErrorMsg:     ingestionRateLimitedErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.INGESTION_RATE_LIMITED},
		},
		{
			name:                 "a DoNotLogError of an ingestionRateLimitedError gets translated into gets translated into a ResourceExhausted error with INGESTION_RATE_LIMITED cause",
			err:                  log.DoNotLogError{Err: ingestionRateLimitedErr},
			expectedGRPCCode:     codes.ResourceExhausted,
			expectedErrorMsg:     ingestionRateLimitedErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.INGESTION_RATE_LIMITED},
		},
		{
			name:                        "a requestRateLimitedError with serviceOverloadErrorEnabled gets translated into an Unavailable error with REQUEST_RATE_LIMITED cause",
			err:                         requestRateLimitedErr,
			serviceOverloadErrorEnabled: true,
			expectedGRPCCode:            codes.Unavailable,
			expectedErrorMsg:            requestRateLimitedErr.Error(),
			expectedErrorDetails:        &mimirpb.WriteErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED},
		},
		{
			name:                        "a DoNotLogError of a requestRateLimitedError with serviceOverloadErrorEnabled gets translated into an Unavailable error with REQUEST_RATE_LIMITED cause",
			err:                         log.DoNotLogError{Err: requestRateLimitedErr},
			serviceOverloadErrorEnabled: true,
			expectedGRPCCode:            codes.Unavailable,
			expectedErrorMsg:            requestRateLimitedErr.Error(),
			expectedErrorDetails:        &mimirpb.WriteErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED},
		},
		{
			name:                 "a requestRateLimitedError without serviceOverloadErrorEnabled gets translated into an ResourceExhausted error with REQUEST_RATE_LIMITED cause",
			err:                  requestRateLimitedErr,
			expectedGRPCCode:     codes.ResourceExhausted,
			expectedErrorMsg:     requestRateLimitedErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED},
		},
		{
			name:                 "a DoNotLogError of a requestRateLimitedError without serviceOverloadErrorEnabled gets translated into an ResourceExhausted error with REQUEST_RATE_LIMITED cause",
			err:                  log.DoNotLogError{Err: requestRateLimitedErr},
			expectedGRPCCode:     codes.ResourceExhausted,
			expectedErrorMsg:     requestRateLimitedErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED},
		},
	}

	for _, tc := range testCases {
		err := toGRPCError(tc.err, tc.serviceOverloadErrorEnabled)

		stat, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, tc.expectedGRPCCode, stat.Code())
		require.Equal(t, tc.expectedErrorMsg, stat.Message())
		if tc.expectedErrorDetails == nil {
			require.Len(t, stat.Details(), 0)
		} else {
			details := stat.Details()
			require.Len(t, details, 1)
			errDetails, ok := details[0].(*mimirpb.WriteErrorDetails)
			require.True(t, ok)
			require.Equal(t, tc.expectedErrorDetails, errDetails)
		}
	}
}

func checkDistributorError(t *testing.T, err error, expectedCause mimirpb.ErrorCause) {
	var distributorErr distributorError
	require.ErrorAs(t, err, &distributorErr)
	require.Equal(t, expectedCause, distributorErr.errorCause())
}
