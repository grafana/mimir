// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/gogo/status"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
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
	const (
		ingesterID     = "ingester-25"
		testMsg        = "this is an error"
		anotherTestMsg = "this is another error"
	)
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
			err := newIngesterPushError(testData.originalStatus, ingesterID)
			expectedErrorMsg := fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, testMsg)
			assert.Error(t, err)
			assert.EqualError(t, err, expectedErrorMsg)
			checkDistributorError(t, err, testData.expectedCause)

			anotherErr := newIngesterPushError(createStatusWithDetails(t, codes.Internal, anotherTestMsg, testData.expectedCause), ingesterID)
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

func TestPartitionPushError(t *testing.T) {
	origErr := errors.New("the original error")
	pushErr := newPartitionPushError(origErr, mimirpb.BAD_DATA)

	assert.ErrorIs(t, pushErr, origErr)
	assert.NotErrorIs(t, pushErr, context.Canceled)

	assert.Equal(t, mimirpb.BAD_DATA, pushErr.Cause())
}

func TestToErrorWithGRPCStatus(t *testing.T) {
	const (
		ingesterID  = "ingester-25"
		originalMsg = "this is an error"
	)
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
		expectedErrorDetails        *mimirpb.ErrorDetails
	}
	testCases := map[string]testStruct{
		"a generic error gets translated into an Internal error with no details": {
			err:              originalErr,
			expectedGRPCCode: codes.Internal,
			expectedErrorMsg: originalMsg,
		},
		"a DoNotLog error of a generic error gets translated into an Internal error with no details": {
			err:              middleware.DoNotLogError{Err: originalErr},
			expectedGRPCCode: codes.Internal,
			expectedErrorMsg: originalMsg,
		},
		"a replicasDidNotMatchError gets translated into an AlreadyExists error with REPLICAS_DID_NOT_MATCH cause": {
			err:                  replicasDidNotMatchErr,
			expectedGRPCCode:     codes.AlreadyExists,
			expectedErrorMsg:     replicasDidNotMatchErr.Error(),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.REPLICAS_DID_NOT_MATCH},
		},
		"a DoNotLotError of a replicasDidNotMatchError gets translated into an AlreadyExists error with REPLICAS_DID_NOT_MATCH cause": {
			err:                  middleware.DoNotLogError{Err: replicasDidNotMatchErr},
			expectedGRPCCode:     codes.AlreadyExists,
			expectedErrorMsg:     replicasDidNotMatchErr.Error(),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.REPLICAS_DID_NOT_MATCH},
		},
		"a tooManyClustersError gets translated into a FailedPrecondition error with TOO_MANY_CLUSTERS cause": {
			err:                  tooManyClustersErr,
			expectedGRPCCode:     codes.FailedPrecondition,
			expectedErrorMsg:     tooManyClustersErr.Error(),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TOO_MANY_CLUSTERS},
		},
		"a DoNotLogError of a tooManyClustersError gets translated into a FailedPrecondition error with TOO_MANY_CLUSTERS cause": {
			err:                  middleware.DoNotLogError{Err: tooManyClustersErr},
			expectedGRPCCode:     codes.FailedPrecondition,
			expectedErrorMsg:     tooManyClustersErr.Error(),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TOO_MANY_CLUSTERS},
		},
		"a validationError gets translated into an InvalidArgument error with BAD_DATA cause": {
			err:                  newValidationError(originalErr),
			expectedGRPCCode:     codes.InvalidArgument,
			expectedErrorMsg:     originalMsg,
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a DoNotLogError of a validationError gets translated into an InvalidArgument error with BAD_DATA cause": {
			err:                  middleware.DoNotLogError{Err: newValidationError(originalErr)},
			expectedGRPCCode:     codes.InvalidArgument,
			expectedErrorMsg:     originalMsg,
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"an ingestionRateLimitedError with serviceOverloadErrorEnabled gets translated into an Unavailable error with INGESTION_RATE_LIMITED cause": {
			err:                         ingestionRateLimitedErr,
			serviceOverloadErrorEnabled: true,
			expectedGRPCCode:            codes.Unavailable,
			expectedErrorMsg:            ingestionRateLimitedErr.Error(),
			expectedErrorDetails:        &mimirpb.ErrorDetails{Cause: mimirpb.INGESTION_RATE_LIMITED},
		},
		"a DoNotLogError of an ingestionRateLimitedError with serviceOverloadErrorEnabled gets translated into an Unavailable error with INGESTION_RATE_LIMITED cause": {
			err:                         middleware.DoNotLogError{Err: ingestionRateLimitedErr},
			serviceOverloadErrorEnabled: true,
			expectedGRPCCode:            codes.Unavailable,
			expectedErrorMsg:            ingestionRateLimitedErr.Error(),
			expectedErrorDetails:        &mimirpb.ErrorDetails{Cause: mimirpb.INGESTION_RATE_LIMITED},
		},
		"an ingestionRateLimitedError without serviceOverloadErrorEnabled gets translated into a ResourceExhausted error with INGESTION_RATE_LIMITED cause": {
			err:                  ingestionRateLimitedErr,
			expectedGRPCCode:     codes.ResourceExhausted,
			expectedErrorMsg:     ingestionRateLimitedErr.Error(),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.INGESTION_RATE_LIMITED},
		},
		"a DoNotLogError of an ingestionRateLimitedError without serviceOverloadErrorEnabled gets translated into a ResourceExhausted error with INGESTION_RATE_LIMITED cause": {
			err:                  middleware.DoNotLogError{Err: ingestionRateLimitedErr},
			expectedGRPCCode:     codes.ResourceExhausted,
			expectedErrorMsg:     ingestionRateLimitedErr.Error(),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.INGESTION_RATE_LIMITED},
		},
		"a requestRateLimitedError with serviceOverloadErrorEnabled gets translated into an Unavailable error with REQUEST_RATE_LIMITED cause": {
			err:                         requestRateLimitedErr,
			serviceOverloadErrorEnabled: true,
			expectedGRPCCode:            codes.Unavailable,
			expectedErrorMsg:            requestRateLimitedErr.Error(),
			expectedErrorDetails:        &mimirpb.ErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED},
		},
		"a DoNotLogError of a requestRateLimitedError with serviceOverloadErrorEnabled gets translated into an Unavailable error with REQUEST_RATE_LIMITED cause": {
			err:                         middleware.DoNotLogError{Err: requestRateLimitedErr},
			serviceOverloadErrorEnabled: true,
			expectedGRPCCode:            codes.Unavailable,
			expectedErrorMsg:            requestRateLimitedErr.Error(),
			expectedErrorDetails:        &mimirpb.ErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED},
		},
		"a requestRateLimitedError without serviceOverloadErrorEnabled gets translated into an ResourceExhausted error with REQUEST_RATE_LIMITED cause": {
			err:                  requestRateLimitedErr,
			expectedGRPCCode:     codes.ResourceExhausted,
			expectedErrorMsg:     requestRateLimitedErr.Error(),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED},
		},
		"a DoNotLogError of a requestRateLimitedError without serviceOverloadErrorEnabled gets translated into an ResourceExhausted error with REQUEST_RATE_LIMITED cause": {
			err:                  middleware.DoNotLogError{Err: requestRateLimitedErr},
			expectedGRPCCode:     codes.ResourceExhausted,
			expectedErrorMsg:     requestRateLimitedErr.Error(),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED},
		},
		"an ingesterPushError with BAD_DATA cause gets translated into an InvalidArgument error with BAD_DATA cause": {
			err:                  newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.BAD_DATA), ingesterID),
			expectedGRPCCode:     codes.InvalidArgument,
			expectedErrorMsg:     fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a DoNotLogError of an ingesterPushError with BAD_DATA cause gets translated into an InvalidArgument error with BAD_DATA cause": {
			err:                  middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.BAD_DATA), ingesterID)},
			expectedGRPCCode:     codes.InvalidArgument,
			expectedErrorMsg:     fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"an ingesterPushError with INSTANCE_LIMIT cause gets translated into a Internal error with INSTANCE_LIMIT cause": {
			err:                  newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.INSTANCE_LIMIT), ingesterID),
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.INSTANCE_LIMIT},
		},
		"a DoNotLogError of an ingesterPushError with INSTANCE_LIMIT cause gets translated into a Internal error with INSTANCE_LIMIT cause": {
			err:                  middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.INSTANCE_LIMIT), ingesterID)},
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.INSTANCE_LIMIT},
		},
		"an ingesterPushError with SERVICE_UNAVAILABLE cause gets translated into a Internal error with SERVICE_UNAVAILABLE cause": {
			err:                  newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.SERVICE_UNAVAILABLE), ingesterID),
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"a DoNotLogError of an ingesterPushError with SERVICE_UNAVAILABLE cause gets translated into a Internal error with SERVICE_UNAVAILABLE cause": {
			err:                  middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.SERVICE_UNAVAILABLE), ingesterID)},
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"an ingesterPushError with TSDB_UNAVAILABLE cause gets translated into a Internal error with TSDB_UNAVAILABLE cause": {
			err:                  newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.TSDB_UNAVAILABLE), ingesterID),
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TSDB_UNAVAILABLE},
		},
		"a DoNotLogError of an ingesterPushError with TSDB_UNAVAILABLE cause gets translated into a Internal error with TSDB_UNAVAILABLE cause": {
			err:                  middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.TSDB_UNAVAILABLE), ingesterID)},
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TSDB_UNAVAILABLE},
		},
		"an ingesterPushError with UNKNOWN_CAUSE cause gets translated into a Internal error with UNKNOWN_CAUSE cause": {
			err:                  newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.UNKNOWN_CAUSE), ingesterID),
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.UNKNOWN_CAUSE},
		},
		"a DoNotLogError of an ingesterPushError with UNKNOWN_CAUSE cause gets translated into a Internal error with UNKNOWN_CAUSE cause": {
			err:                  middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.UNKNOWN_CAUSE), ingesterID)},
			expectedGRPCCode:     codes.Internal,
			expectedErrorMsg:     fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.UNKNOWN_CAUSE},
		},
		"an ingesterPushError with CIRCUIT_BREAKER_OPEN cause gets translated into an Unavailable error with CIRCUIT_BREAKER_OPEN cause": {
			err:                  newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.CIRCUIT_BREAKER_OPEN), ingesterID),
			expectedGRPCCode:     codes.Unavailable,
			expectedErrorMsg:     fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.CIRCUIT_BREAKER_OPEN},
		},
		"a wrapped ingesterPushError with CIRCUIT_BREAKER_OPEN cause gets translated into an Unavailable error with CIRCUIT_BREAKER_OPEN cause": {
			err:                  errors.Wrap(newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.CIRCUIT_BREAKER_OPEN), ingesterID), "wrapped"),
			expectedGRPCCode:     codes.Unavailable,
			expectedErrorMsg:     fmt.Sprintf("%s: %s %s: %s", "wrapped", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.CIRCUIT_BREAKER_OPEN},
		},
		"an ingesterPushError with METHOD_NOT_ALLOWED cause gets translated into a Unimplemented error with METHOD_NOT_ALLOWED cause": {
			err:                  newIngesterPushError(createStatusWithDetails(t, codes.Unimplemented, originalMsg, mimirpb.METHOD_NOT_ALLOWED), ingesterID),
			expectedGRPCCode:     codes.Unimplemented,
			expectedErrorMsg:     fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.METHOD_NOT_ALLOWED},
		},
		"a DoNotLogError of an ingesterPushError with METHOD_NOT_ALLOWED cause gets translated into a Unimplemented error with METHOD_NOT_ALLOWED cause": {
			err:                  middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unimplemented, originalMsg, mimirpb.METHOD_NOT_ALLOWED), ingesterID)},
			expectedGRPCCode:     codes.Unimplemented,
			expectedErrorMsg:     fmt.Sprintf("%s %s: %s", failedPushingToIngesterMessage, ingesterID, originalMsg),
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.METHOD_NOT_ALLOWED},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := toErrorWithGRPCStatus(tc.err, tc.serviceOverloadErrorEnabled)

			stat, ok := grpcutil.ErrorToStatus(err)
			require.True(t, ok)
			require.Equal(t, tc.expectedGRPCCode, stat.Code())
			require.Equal(t, tc.expectedErrorMsg, stat.Message())
			if tc.expectedErrorDetails == nil {
				require.Len(t, stat.Details(), 0)
			} else {
				details := stat.Details()
				require.Len(t, details, 1)
				errDetails, ok := details[0].(*mimirpb.ErrorDetails)
				require.True(t, ok)
				require.Equal(t, tc.expectedErrorDetails, errDetails)
			}
		})
	}
}

func TestWrapIngesterPushError(t *testing.T) {
	const (
		ingesterID   = "ingester-25"
		testErrorMsg = "this is a test error message"
	)

	// Ensure that no error gets translated into no error.
	t.Run("no error gives no error", func(t *testing.T) {
		err := wrapIngesterPushError(nil, ingesterID)
		require.NoError(t, err)
	})

	// Ensure that the errors created by gogo/status package get translated
	// into ingesterPushError messages.
	statusTests := map[string]struct {
		ingesterPushError         error
		expectedIngesterPushError ingesterPushError
	}{
		"a gRPC error with details gives an ingesterPushError with the same details": {
			ingesterPushError:         createStatusWithDetails(t, codes.Unavailable, testErrorMsg, mimirpb.UNKNOWN_CAUSE).Err(),
			expectedIngesterPushError: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, testErrorMsg, mimirpb.UNKNOWN_CAUSE), ingesterID),
		},
		"a DeadlineExceeded gRPC ingester error gives an ingesterPushError with UNKNOWN_CAUSE cause": {
			// This is how context.DeadlineExceeded error is translated into a gRPC error.
			ingesterPushError:         status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()),
			expectedIngesterPushError: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, context.DeadlineExceeded.Error(), mimirpb.UNKNOWN_CAUSE), ingesterID),
		},
		"an Unavailable gRPC error without details gives an ingesterPushError with UNKNOWN_CAUSE cause": {
			ingesterPushError:         status.Error(codes.Unavailable, testErrorMsg),
			expectedIngesterPushError: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, testErrorMsg, mimirpb.UNKNOWN_CAUSE), ingesterID),
		},
		"an Internal gRPC ingester error without details gives an ingesterPushError with UNKNOWN_CAUSE cause": {
			ingesterPushError:         status.Error(codes.Internal, testErrorMsg),
			expectedIngesterPushError: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, testErrorMsg, mimirpb.UNKNOWN_CAUSE), ingesterID),
		},
		"an Unknown gRPC ingester error without details gives an ingesterPushError with UNKNOWN_CAUSE cause": {
			ingesterPushError:         status.Error(codes.Unknown, testErrorMsg),
			expectedIngesterPushError: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, testErrorMsg, mimirpb.UNKNOWN_CAUSE), ingesterID),
		},
	}
	for testName, testData := range statusTests {
		t.Run(testName, func(t *testing.T) {
			err := wrapIngesterPushError(testData.ingesterPushError, ingesterID)
			ingesterPushErr, ok := err.(ingesterPushError)
			require.True(t, ok)

			require.Equal(t, testData.expectedIngesterPushError.Error(), ingesterPushErr.Error())
			require.Equal(t, testData.expectedIngesterPushError.Cause(), ingesterPushErr.Cause())
		})
	}
}

func TestWrapPartitionPushError(t *testing.T) {
	tests := map[string]struct {
		err           error
		expectedMsg   string
		expectedCause mimirpb.ErrorCause
	}{
		"should wrap ingest.ErrWriteRequestDataItemTooLarge": {
			err:           wrapPartitionPushError(ingest.ErrWriteRequestDataItemTooLarge, 1),
			expectedMsg:   "failed pushing to partition 1: " + ingest.ErrWriteRequestDataItemTooLarge.Error(),
			expectedCause: mimirpb.BAD_DATA,
		},
		"should wrap context.Canceled": {
			err:           wrapPartitionPushError(context.Canceled, 1),
			expectedMsg:   "failed pushing to partition 1: context canceled",
			expectedCause: mimirpb.UNKNOWN_CAUSE,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectedMsg, testData.err.Error())

			partitionPushErr, ok := testData.err.(partitionPushError)
			require.True(t, ok)
			assert.Equal(t, testData.expectedCause, partitionPushErr.Cause())
		})
	}
}

func TestIsIngestionClientError(t *testing.T) {
	testCases := map[string]struct {
		err             error
		expectedOutcome bool
	}{
		"an ingesterPushError with error cause BAD_DATA is a client error": {
			err:             ingesterPushError{cause: mimirpb.BAD_DATA},
			expectedOutcome: true,
		},
		"an ingesterPushError with error cause METHOD_NOT_ALLOWED is not a client error": {
			err:             ingesterPushError{cause: mimirpb.METHOD_NOT_ALLOWED},
			expectedOutcome: false,
		},
		"an ingesterPushError with other error cause is not a client error": {
			err:             ingesterPushError{cause: mimirpb.SERVICE_UNAVAILABLE},
			expectedOutcome: false,
		},
		"an partitionPushError with error cause BAD_DATA is a client error": {
			err:             partitionPushError{cause: mimirpb.BAD_DATA},
			expectedOutcome: true,
		},
		"an partitionPushError with other error cause is not a client error": {
			err:             partitionPushError{cause: mimirpb.SERVICE_UNAVAILABLE},
			expectedOutcome: false,
		},
		"an gRPC error with status code 4xx built by httpgrpc package is a client error": {
			err:             httpgrpc.Errorf(http.StatusBadRequest, "this is an error"),
			expectedOutcome: true,
		},
		"an gRPC error with status code 4xx built by grpc's status package is a client error": {
			err:             grpcstatus.Error(http.StatusTooManyRequests, "this is an error"),
			expectedOutcome: true,
		},
		"an gRPC error with status code 4xx built by gogo's status package is a client error": {
			err:             status.Error(http.StatusTooManyRequests, "this is an error"),
			expectedOutcome: true,
		},
		"an gRPC error with status code different from 4xx built by httpgrpc package is a not client error": {
			err:             httpgrpc.Errorf(http.StatusInternalServerError, "this is an error"),
			expectedOutcome: false,
		},
		"an gRPC error with status code different from 4xx built by grpc's status package is not a client error": {
			err:             grpcstatus.Error(http.StatusServiceUnavailable, "this is an error"),
			expectedOutcome: false,
		},
		"an gRPC error with status code different from 4xx built by gogo's status package is not a client error": {
			err:             status.Error(codes.Internal, "this is an error"),
			expectedOutcome: false,
		},
		"a random non-gRPC and non-ingesterPushError error is not a client error": {
			err:             errors.New("this is a random error"),
			expectedOutcome: false,
		},
	}
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, testData.expectedOutcome, isIngestionClientError(testData.err))
		})
	}
}

func TestErrorCauseToGRPCStatusCode(t *testing.T) {
	type testStruct struct {
		errorCause                  mimirpb.ErrorCause
		serviceOverloadErrorEnabled bool
		expectedGRPCStatusCode      codes.Code
	}
	testCases := map[string]testStruct{
		"a REPLICAS_DID_NOT_MATCH error cause gets translated into an AlreadyExists": {
			errorCause:             mimirpb.REPLICAS_DID_NOT_MATCH,
			expectedGRPCStatusCode: codes.AlreadyExists,
		},
		"a TOO_MANY_CLUSTERS error cause gets translated into a FailedPrecondition": {
			errorCause:             mimirpb.TOO_MANY_CLUSTERS,
			expectedGRPCStatusCode: codes.FailedPrecondition,
		},
		"a BAD_DATA error cause gets translated into a InvalidArgument": {
			errorCause:             mimirpb.BAD_DATA,
			expectedGRPCStatusCode: codes.InvalidArgument,
		},
		"a TENANT_LIMIT error cause gets translated into a FailedPrecondition": {
			errorCause:             mimirpb.TENANT_LIMIT,
			expectedGRPCStatusCode: codes.FailedPrecondition,
		},
		"an INGESTION_RATE_LIMITED error cause without serviceOverloadErrorEnabled gets translated into a ResourceExhausted": {
			errorCause:                  mimirpb.INGESTION_RATE_LIMITED,
			serviceOverloadErrorEnabled: false,
			expectedGRPCStatusCode:      codes.ResourceExhausted,
		},
		"an INGESTION_RATE_LIMITED error cause with serviceOverloadErrorEnabled gets translated into an Unavailable": {
			errorCause:                  mimirpb.INGESTION_RATE_LIMITED,
			serviceOverloadErrorEnabled: true,
			expectedGRPCStatusCode:      codes.Unavailable,
		},
		"a REQUEST_RATE_LIMITED error cause without serviceOverloadErrorEnabled gets translated into a ResourceExhausted": {
			errorCause:                  mimirpb.REQUEST_RATE_LIMITED,
			serviceOverloadErrorEnabled: false,
			expectedGRPCStatusCode:      codes.ResourceExhausted,
		},
		"a REQUEST_RATE_LIMITED error cause with serviceOverloadErrorEnabled gets translated into an Unavailable": {
			errorCause:                  mimirpb.REQUEST_RATE_LIMITED,
			serviceOverloadErrorEnabled: true,
			expectedGRPCStatusCode:      codes.Unavailable,
		},
		"an UNKNOWN error cause gets translated into an Internal": {
			errorCause:             mimirpb.UNKNOWN_CAUSE,
			expectedGRPCStatusCode: codes.Internal,
		},
		"an INSTANCE_LIMIT error cause gets translated into an Internal": {
			errorCause:             mimirpb.INSTANCE_LIMIT,
			expectedGRPCStatusCode: codes.Internal,
		},
		"a SERVICE_UNAVAILABLE error cause gets translated into an Internal": {
			errorCause:             mimirpb.SERVICE_UNAVAILABLE,
			expectedGRPCStatusCode: codes.Internal,
		},
		"a TOO_BUSY error cause gets translated into an Internal": {
			errorCause:             mimirpb.TOO_BUSY,
			expectedGRPCStatusCode: codes.Internal,
		},
		"a CIRCUIT_BREAKER_OPEN error cause gets translated into an Unavailable": {
			errorCause:             mimirpb.CIRCUIT_BREAKER_OPEN,
			expectedGRPCStatusCode: codes.Unavailable,
		},
		"a METHOD_NOT_ALLOWED error cause gets translated into an Unimplemented": {
			errorCause:             mimirpb.METHOD_NOT_ALLOWED,
			expectedGRPCStatusCode: codes.Unimplemented,
		},
		"a TSDB_UNAVAILABLE error cause gets translated into an Internal": {
			errorCause:             mimirpb.TSDB_UNAVAILABLE,
			expectedGRPCStatusCode: codes.Internal,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			status := errorCauseToGRPCStatusCode(tc.errorCause, tc.serviceOverloadErrorEnabled)
			assert.Equal(t, tc.expectedGRPCStatusCode, status)
		})
	}
}

func TestErrorCauseToHTTPStatusCode(t *testing.T) {
	type testStruct struct {
		errorCause                  mimirpb.ErrorCause
		serviceOverloadErrorEnabled bool
		expectedHTTPStatus          int
	}
	testCases := map[string]testStruct{
		"a REPLICAS_DID_NOT_MATCH error cause gets translated into a HTTP 202": {
			errorCause:         mimirpb.REPLICAS_DID_NOT_MATCH,
			expectedHTTPStatus: http.StatusAccepted,
		},
		"a TOO_MANY_CLUSTERS error cause gets translated into a HTTP 400": {
			errorCause:         mimirpb.TOO_MANY_CLUSTERS,
			expectedHTTPStatus: http.StatusBadRequest,
		},
		"a BAD_DATA error cause gets translated into a HTTP 400": {
			errorCause:         mimirpb.BAD_DATA,
			expectedHTTPStatus: http.StatusBadRequest,
		},
		"a TENANT_LIMIT error cause gets translated into a HTTP 400": {
			errorCause:         mimirpb.TENANT_LIMIT,
			expectedHTTPStatus: http.StatusBadRequest,
		},
		"an INGESTION_RATE_LIMITED error cause without serviceOverloadErrorEnabled gets translated into a HTTP 429": {
			errorCause:                  mimirpb.INGESTION_RATE_LIMITED,
			serviceOverloadErrorEnabled: false,
			expectedHTTPStatus:          http.StatusTooManyRequests,
		},
		"an INGESTION_RATE_LIMITED error cause with serviceOverloadErrorEnabled gets translated into a HTTP 529": {
			errorCause:                  mimirpb.INGESTION_RATE_LIMITED,
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
		},
		"a REQUEST_RATE_LIMITED error cause without serviceOverloadErrorEnabled gets translated into a HTTP 429": {
			errorCause:                  mimirpb.REQUEST_RATE_LIMITED,
			serviceOverloadErrorEnabled: false,
			expectedHTTPStatus:          http.StatusTooManyRequests,
		},
		"a REQUEST_RATE_LIMITED error cause with serviceOverloadErrorEnabled gets translated into a HTTP 529": {
			errorCause:                  mimirpb.REQUEST_RATE_LIMITED,
			serviceOverloadErrorEnabled: true,
			expectedHTTPStatus:          StatusServiceOverloaded,
		},
		"an UNKNOWN error cause gets translated into a HTTP 500": {
			errorCause:         mimirpb.UNKNOWN_CAUSE,
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"an INSTANCE_LIMIT error cause gets translated into a HTTP 500": {
			errorCause:         mimirpb.INSTANCE_LIMIT,
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"a SERVICE_UNAVAILABLE error cause gets translated into a HTTP 500": {
			errorCause:         mimirpb.SERVICE_UNAVAILABLE,
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"a TOO_BUSY error cause gets translated into a HTTP 500": {
			errorCause:         mimirpb.TOO_BUSY,
			expectedHTTPStatus: http.StatusInternalServerError,
		},
		"a METHOD_NOT_ALLOWED error cause gets translated into a HTTP 501": {
			errorCause:         mimirpb.METHOD_NOT_ALLOWED,
			expectedHTTPStatus: http.StatusNotImplemented,
		},
		"a CIRCUIT_BREAKER_OPEN error cause gets translated into a HTTP 503": {
			errorCause:         mimirpb.CIRCUIT_BREAKER_OPEN,
			expectedHTTPStatus: http.StatusServiceUnavailable,
		},
		"a TSDB_UNAVAILABLE error cause gets translated into a HTTP 503": {
			errorCause:         mimirpb.TSDB_UNAVAILABLE,
			expectedHTTPStatus: http.StatusServiceUnavailable,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			status := errorCauseToHTTPStatusCode(tc.errorCause, tc.serviceOverloadErrorEnabled)
			assert.Equal(t, tc.expectedHTTPStatus, status)
		})
	}
}

func checkDistributorError(t *testing.T, err error, expectedCause mimirpb.ErrorCause) {
	var distributorErr Error
	require.ErrorAs(t, err, &distributorErr)
	require.Equal(t, expectedCause, distributorErr.Cause())
}

type mockDistributorErr string

func (e mockDistributorErr) Error() string {
	return string(e)
}

func (e mockDistributorErr) Cause() mimirpb.ErrorCause {
	return mimirpb.UNKNOWN_CAUSE
}
