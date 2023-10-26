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

func TestNewIngesterPushError(t *testing.T) {
	testMsg := "this is an error"
	anotherTestMsg := "this is another error"
	tests := map[string]struct {
		originalStatus *status.Status
		expectedCause  mimirpb.ErrorCause
	}{
		"a gRPC error with details give an ingesterPushError with the same details": {
			originalStatus: createStatusWithDetails(t, codes.Internal, testMsg, mimirpb.SERVICE_UNAVAILABLE),
			expectedCause:  mimirpb.SERVICE_UNAVAILABLE,
		},
		"a gRPC error without details give an ingesterPushError with UNKNOWN_CAUSE cause": {
			originalStatus: status.New(codes.Internal, testMsg),
			expectedCause:  mimirpb.UNKNOWN_CAUSE,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			err := newIngesterPushError(testData.originalStatus)
			expectedErrorMsg := fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, testMsg)
			assert.Error(t, err)
			assert.EqualError(t, err, expectedErrorMsg)
			checkDistributorError(t, err, testData.expectedCause)

			anotherErr := newIngesterPushError(createStatusWithDetails(t, codes.Internal, anotherTestMsg, testData.expectedCause))
			assert.NotErrorIs(t, err, anotherErr)

			assert.True(t, errors.As(err, &ingesterPushError{}))
			assert.False(t, errors.As(err, &replicasDidNotMatchError{}))

			wrappedErr := fmt.Errorf("wrapped %w", err)
			assert.ErrorIs(t, wrappedErr, err)
			assert.True(t, errors.As(wrappedErr, &ingesterPushError{}))
			checkDistributorError(t, wrappedErr, testData.expectedCause)
		})
	}
}

func TestToGRPCError(t *testing.T) {
	originalMsg := "this is an error"
	originalErr := errors.New(originalMsg)
	replicasDidNotMatchErr := newReplicasDidNotMatchError("a", "b")
	tooManyClustersErr := newTooManyClustersError(10)
	ingestionRateLimitedErr := newIngestionRateLimitedError(10, 10)
	requestRateLimitedErr := newRequestRateLimitedError(10, 10)
	type testStruct struct {
		err                         error
		serviceOverloadErrorEnabled bool
		expectedGRPCCode            codes.Code
		expectedErrorMsg            string
		expectedErrorDetails        *mimirpb.WriteErrorDetails
	}
	testCases := map[string]testStruct{
		"a generic error gets translated into an Internal error with no details": {
			err:              originalErr,
			expectedGRPCCode: codes.Internal,
			expectedErrorMsg: originalMsg,
		},
		"a DoNotLog error of a generic error gets translated into an Internal error with no details": {
			err:              log.DoNotLogError{Err: originalErr},
			expectedGRPCCode: codes.Internal,
			expectedErrorMsg: originalMsg,
		},
		"a replicasDidNotMatchError gets translated into an AlreadyExists error with REPLICAS_DID_NOT_MATCH cause": {
			err:                  replicasDidNotMatchErr,
			expectedGRPCCode:     codes.AlreadyExists,
			expectedErrorMsg:     replicasDidNotMatchErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.REPLICAS_DID_NOT_MATCH},
		},
		"a DoNotLotError of a replicasDidNotMatchError gets translated into an AlreadyExists error with REPLICAS_DID_NOT_MATCH cause": {
			err:                  log.DoNotLogError{Err: replicasDidNotMatchErr},
			expectedGRPCCode:     codes.AlreadyExists,
			expectedErrorMsg:     replicasDidNotMatchErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.REPLICAS_DID_NOT_MATCH},
		},
		"a tooManyClustersError gets translated into a FailedPrecondition error with TOO_MANY_CLUSTERS cause": {
			err:                  tooManyClustersErr,
			expectedGRPCCode:     codes.FailedPrecondition,
			expectedErrorMsg:     tooManyClustersErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.TOO_MANY_CLUSTERS},
		},
		"a DoNotLogError of a tooManyClustersError gets translated into a FailedPrecondition error with TOO_MANY_CLUSTERS cause": {
			err:                  log.DoNotLogError{Err: tooManyClustersErr},
			expectedGRPCCode:     codes.FailedPrecondition,
			expectedErrorMsg:     tooManyClustersErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.TOO_MANY_CLUSTERS},
		},
		"a validationError gets translated into gets translated into a FailedPrecondition error with VALIDATION cause": {
			err:                  newValidationError(originalErr),
			expectedGRPCCode:     codes.FailedPrecondition,
			expectedErrorMsg:     originalMsg,
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a DoNotLogError of a validationError gets translated into gets translated into a FailedPrecondition error with VALIDATION cause": {
			err:                  log.DoNotLogError{Err: newValidationError(originalErr)},
			expectedGRPCCode:     codes.FailedPrecondition,
			expectedErrorMsg:     originalMsg,
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"an ingestionRateLimitedError gets translated into gets translated into a ResourceExhausted error with INGESTION_RATE_LIMITED cause": {
			err:                  ingestionRateLimitedErr,
			expectedGRPCCode:     codes.ResourceExhausted,
			expectedErrorMsg:     ingestionRateLimitedErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.INGESTION_RATE_LIMITED},
		},
		"a DoNotLogError of an ingestionRateLimitedError gets translated into gets translated into a ResourceExhausted error with INGESTION_RATE_LIMITED cause": {
			err:                  log.DoNotLogError{Err: ingestionRateLimitedErr},
			expectedGRPCCode:     codes.ResourceExhausted,
			expectedErrorMsg:     ingestionRateLimitedErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.INGESTION_RATE_LIMITED},
		},
		"a requestRateLimitedError with serviceOverloadErrorEnabled gets translated into an Unavailable error with REQUEST_RATE_LIMITED cause": {
			err:                         requestRateLimitedErr,
			serviceOverloadErrorEnabled: true,
			expectedGRPCCode:            codes.Unavailable,
			expectedErrorMsg:            requestRateLimitedErr.Error(),
			expectedErrorDetails:        &mimirpb.WriteErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED},
		},
		"a DoNotLogError of a requestRateLimitedError with serviceOverloadErrorEnabled gets translated into an Unavailable error with REQUEST_RATE_LIMITED cause": {
			err:                         log.DoNotLogError{Err: requestRateLimitedErr},
			serviceOverloadErrorEnabled: true,
			expectedGRPCCode:            codes.Unavailable,
			expectedErrorMsg:            requestRateLimitedErr.Error(),
			expectedErrorDetails:        &mimirpb.WriteErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED},
		},
		"a requestRateLimitedError without serviceOverloadErrorEnabled gets translated into an ResourceExhausted error with REQUEST_RATE_LIMITED cause": {
			err:                  requestRateLimitedErr,
			expectedGRPCCode:     codes.ResourceExhausted,
			expectedErrorMsg:     requestRateLimitedErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED},
		},
		"a DoNotLogError of a requestRateLimitedError without serviceOverloadErrorEnabled gets translated into an ResourceExhausted error with REQUEST_RATE_LIMITED cause": {
			err:                  log.DoNotLogError{Err: requestRateLimitedErr},
			expectedGRPCCode:     codes.ResourceExhausted,
			expectedErrorMsg:     requestRateLimitedErr.Error(),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED},
		},
		"an ingesterPushError with BAD_DATA cause gets translated into a FailedPrecondition error with BAD_DATA cause": {
			err:                  newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.BAD_DATA)),
			expectedGRPCCode:     codes.FailedPrecondition,
			expectedErrorMsg:     fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a DoNotLogError of an ingesterPushError with BAD_DATA cause gets translated into a FailedPrecondition error with BAD_DATA cause": {
			err:                  log.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.BAD_DATA))},
			expectedGRPCCode:     codes.FailedPrecondition,
			expectedErrorMsg:     fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"an ingesterPushError with INSTANCE_LIMIT cause gets translated into a Internal error with INSTANCE_LIMIT cause": {
			err:                  newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.INSTANCE_LIMIT)),
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.INSTANCE_LIMIT},
		},
		"a DoNotLogError of an ingesterPushError with INSTANCE_LIMIT cause gets translated into a Internal error with INSTANCE_LIMIT cause": {
			err:                  log.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.INSTANCE_LIMIT))},
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.INSTANCE_LIMIT},
		},
		"an ingesterPushError with SERVICE_UNAVAILABLE cause gets translated into a Internal error with SERVICE_UNAVAILABLE cause": {
			err:                  newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.SERVICE_UNAVAILABLE)),
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"a DoNotLogError of an ingesterPushError with SERVICE_UNAVAILABLE cause gets translated into a Internal error with SERVICE_UNAVAILABLE cause": {
			err:                  log.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.SERVICE_UNAVAILABLE))},
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"an ingesterPushError with TSDB_UNAVAILABLE cause gets translated into a Internal error with TSDB_UNAVAILABLE cause": {
			err:                  newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.TSDB_UNAVAILABLE)),
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.TSDB_UNAVAILABLE},
		},
		"a DoNotLogError of an ingesterPushError with TSDB_UNAVAILABLE cause gets translated into a Internal error with TSDB_UNAVAILABLE cause": {
			err:                  log.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.TSDB_UNAVAILABLE))},
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.TSDB_UNAVAILABLE},
		},
		"an ingesterPushError with UNKNOWN_CAUSE cause gets translated into a Internal error with UNKNOWN_CAUSE cause": {
			err:                  newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.UNKNOWN_CAUSE)),
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.UNKNOWN_CAUSE},
		},
		"a DoNotLogError of an ingesterPushError with UNKNOWN_CAUSE cause gets translated into a Internal error with UNKNOWN_CAUSE cause": {
			err:                  log.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.UNKNOWN_CAUSE))},
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s: %s", failedPushingToIngesterMessage, originalMsg),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.UNKNOWN_CAUSE},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
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
		})
	}
}

func checkDistributorError(t *testing.T, err error, expectedCause mimirpb.ErrorCause) {
	var distributorErr distributorError
	require.ErrorAs(t, err, &distributorErr)
	require.Equal(t, expectedCause, distributorErr.errorCause())
}

type mockDistributorErr string

func (e mockDistributorErr) Error() string {
	return string(e)
}

func (e mockDistributorErr) errorCause() mimirpb.ErrorCause {
	return mimirpb.UNKNOWN_CAUSE
}
