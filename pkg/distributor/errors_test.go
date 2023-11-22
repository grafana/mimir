// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/gogo/status"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
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
			err:              middleware.DoNotLogError{Err: originalErr},
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
			err:                  middleware.DoNotLogError{Err: replicasDidNotMatchErr},
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
			err:                  middleware.DoNotLogError{Err: tooManyClustersErr},
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
			err:                  middleware.DoNotLogError{Err: newValidationError(originalErr)},
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
			err:                  middleware.DoNotLogError{Err: ingestionRateLimitedErr},
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
			err:                         middleware.DoNotLogError{Err: requestRateLimitedErr},
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
			err:                  middleware.DoNotLogError{Err: requestRateLimitedErr},
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
			err:                  middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.BAD_DATA))},
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
			err:                  middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.INSTANCE_LIMIT))},
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
			err:                  middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.SERVICE_UNAVAILABLE))},
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
			err:                  middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Internal, originalMsg, mimirpb.TSDB_UNAVAILABLE))},
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
			err:                  middleware.DoNotLogError{Err: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, originalMsg, mimirpb.UNKNOWN_CAUSE))},
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

func TestHandleIngesterPushError(t *testing.T) {
	testErrorMsg := "this is a test error message"
	outputErrorMsgPrefix := "failed pushing to ingester"

	// Ensure that no error gets translated into no error.
	t.Run("no error gives no error", func(t *testing.T) {
		err := handleIngesterPushError(nil)
		require.NoError(t, err)
	})

	// Ensure that the errors created by httpgrpc get translated into
	// other errors created by httpgrpc with the same code, and with
	// a more explanatory message.
	// TODO: this is needed for backwards compatibility and will be removed
	// in mimir 2.12.0.
	httpgrpcTests := map[string]struct {
		ingesterPushError error
		expectedStatus    int32
		expectedMessage   string
	}{
		"a 4xx HTTP gRPC error gives a 4xx HTTP gRPC error": {
			ingesterPushError: httpgrpc.Errorf(http.StatusBadRequest, testErrorMsg),
			expectedStatus:    http.StatusBadRequest,
			expectedMessage:   fmt.Sprintf("%s: %s", outputErrorMsgPrefix, testErrorMsg),
		},
		"a 5xx HTTP gRPC error gives a 5xx HTTP gRPC error": {
			ingesterPushError: httpgrpc.Errorf(http.StatusServiceUnavailable, testErrorMsg),
			expectedStatus:    http.StatusServiceUnavailable,
			expectedMessage:   fmt.Sprintf("%s: %s", outputErrorMsgPrefix, testErrorMsg),
		},
	}
	for testName, testData := range httpgrpcTests {
		t.Run(testName, func(t *testing.T) {
			err := handleIngesterPushError(testData.ingesterPushError)
			res, ok := httpgrpc.HTTPResponseFromError(err)
			require.True(t, ok)
			require.NotNil(t, res)
			require.Equal(t, testData.expectedStatus, res.GetCode())
			require.Equal(t, testData.expectedMessage, string(res.Body))
		})
	}

	// Ensure that the errors created by gogo/status package get translated
	// into ingesterPushError messages.
	statusTests := map[string]struct {
		ingesterPushError         error
		expectedIngesterPushError ingesterPushError
	}{
		"a gRPC error with details gives an ingesterPushError with the same details": {
			ingesterPushError:         createStatusWithDetails(t, codes.Unavailable, testErrorMsg, mimirpb.UNKNOWN_CAUSE).Err(),
			expectedIngesterPushError: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, testErrorMsg, mimirpb.UNKNOWN_CAUSE)),
		},
		"a DeadlineExceeded gRPC ingester error gives an ingesterPushError with UNKNOWN_CAUSE cause": {
			// This is how context.DeadlineExceeded error is translated into a gRPC error.
			ingesterPushError:         status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()),
			expectedIngesterPushError: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, context.DeadlineExceeded.Error(), mimirpb.UNKNOWN_CAUSE)),
		},
		"an Unavailable gRPC error without details gives an ingesterPushError with UNKNOWN_CAUSE cause": {
			ingesterPushError:         status.Error(codes.Unavailable, testErrorMsg),
			expectedIngesterPushError: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, testErrorMsg, mimirpb.UNKNOWN_CAUSE)),
		},
		"an Internal gRPC ingester error without details gives an ingesterPushError with UNKNOWN_CAUSE cause": {
			ingesterPushError:         status.Error(codes.Internal, testErrorMsg),
			expectedIngesterPushError: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, testErrorMsg, mimirpb.UNKNOWN_CAUSE)),
		},
		"an Unknown gRPC ingester error without details gives an ingesterPushError with UNKNOWN_CAUSE cause": {
			ingesterPushError:         status.Error(codes.Unknown, testErrorMsg),
			expectedIngesterPushError: newIngesterPushError(createStatusWithDetails(t, codes.Unavailable, testErrorMsg, mimirpb.UNKNOWN_CAUSE)),
		},
	}
	for testName, testData := range statusTests {
		t.Run(testName, func(t *testing.T) {
			err := handleIngesterPushError(testData.ingesterPushError)
			ingesterPushErr, ok := err.(ingesterPushError)
			require.True(t, ok)

			require.Equal(t, testData.expectedIngesterPushError.Error(), ingesterPushErr.Error())
			require.Equal(t, testData.expectedIngesterPushError.errorCause(), ingesterPushErr.errorCause())
		})
	}
}

func TestIsClientError(t *testing.T) {
	testCases := map[string]struct {
		err             error
		expectedOutcome bool
	}{
		"an ingesterPushError with error cause BAD_DATA is a client error": {
			err:             ingesterPushError{cause: mimirpb.BAD_DATA},
			expectedOutcome: true,
		},
		"an ingesterPushError with error cause different from BAD_DATA is not a client error": {
			err:             ingesterPushError{cause: mimirpb.SERVICE_UNAVAILABLE},
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
			require.Equal(t, testData.expectedOutcome, isClientError(testData.err))
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
