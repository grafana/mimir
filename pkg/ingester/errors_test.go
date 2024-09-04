// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gogo/status"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	timestamp = model.Time(1575043969)
)

func TestUnavailableError(t *testing.T) {
	state := services.Starting
	err := newUnavailableError(state)
	require.Error(t, err)
	expectedMsg := fmt.Sprintf(integerUnavailableMsgFormat, state)
	require.EqualError(t, err, expectedMsg)
	checkIngesterError(t, err, mimirpb.SERVICE_UNAVAILABLE, false)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &unavailableError{})
	checkIngesterError(t, wrappedErr, mimirpb.SERVICE_UNAVAILABLE, false)
}

func TestInstanceLimitReachedError(t *testing.T) {
	limitErrorMessage := "this is a limit error message"
	err := newInstanceLimitReachedError(limitErrorMessage)
	require.Error(t, err)
	require.EqualError(t, err, limitErrorMessage)
	checkIngesterError(t, err, mimirpb.INSTANCE_LIMIT, false)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &instanceLimitReachedError{})
	checkIngesterError(t, wrappedErr, mimirpb.INSTANCE_LIMIT, false)
}

func TestNewTSDBUnavailableError(t *testing.T) {
	tsdbErrMsg := "TSDB Head forced compaction in progress and no write request is currently allowed"
	err := newTSDBUnavailableError(tsdbErrMsg)
	require.Error(t, err)
	require.EqualError(t, err, tsdbErrMsg)
	checkIngesterError(t, err, mimirpb.TSDB_UNAVAILABLE, false)

	wrappedErr := fmt.Errorf("wrapped: %w", err)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &tsdbUnavailableError{})

	wrappedWithUserErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedWithUserErr, err)
	require.ErrorAs(t, wrappedWithUserErr, &tsdbUnavailableError{})
	checkIngesterError(t, wrappedErr, mimirpb.TSDB_UNAVAILABLE, false)
}

func TestNewPerUserSeriesLimitError(t *testing.T) {
	limit := 100
	err := newPerUserSeriesLimitReachedError(limit)
	expectedErrMsg := globalerror.MaxSeriesPerUser.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("per-user series limit of %d exceeded", limit),
		validation.MaxSeriesPerUserFlag,
	)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, mimirpb.TENANT_LIMIT, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perUserSeriesLimitReachedError{})
	checkIngesterError(t, wrappedErr, mimirpb.TENANT_LIMIT, true)
}

func TestNewPerUserMetadataLimitError(t *testing.T) {
	limit := 100
	err := newPerUserMetadataLimitReachedError(limit)
	expectedErrMsg := globalerror.MaxMetadataPerUser.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("per-user metric metadata limit of %d exceeded", limit),
		validation.MaxMetadataPerUserFlag,
	)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, mimirpb.TENANT_LIMIT, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perUserMetadataLimitReachedError{})
	checkIngesterError(t, wrappedErr, mimirpb.TENANT_LIMIT, true)
}

func TestNewPerMetricSeriesLimitError(t *testing.T) {
	limit := 100
	labels := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
	err := newPerMetricSeriesLimitReachedError(limit, labels)
	expectedErrMsg := fmt.Sprintf("%s This is for series %s",
		globalerror.MaxSeriesPerMetric.MessageWithPerTenantLimitConfig(
			fmt.Sprintf("per-metric series limit of %d exceeded", limit),
			validation.MaxSeriesPerMetricFlag,
		),
		mimirpb.FromLabelAdaptersToString(labels),
	)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, mimirpb.TENANT_LIMIT, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perMetricSeriesLimitReachedError{})
	checkIngesterError(t, wrappedErr, mimirpb.TENANT_LIMIT, true)
}

func TestNewPerMetricMetadataLimitError(t *testing.T) {
	limit := 100
	family := "testmetric"
	err := newPerMetricMetadataLimitReachedError(limit, family)
	expectedErrMsg := fmt.Sprintf("%s This is for metric %s",
		globalerror.MaxMetadataPerMetric.MessageWithPerTenantLimitConfig(
			fmt.Sprintf("per-metric metadata limit of %d exceeded", limit),
			validation.MaxMetadataPerMetricFlag,
		),
		family,
	)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, mimirpb.TENANT_LIMIT, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perMetricMetadataLimitReachedError{})
	checkIngesterError(t, wrappedErr, mimirpb.TENANT_LIMIT, true)
}

func TestNewSampleError(t *testing.T) {
	seriesLabels := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	tests := map[string]struct {
		err         error
		expectedMsg string
	}{
		"newSampleTimestampTooOldError": {
			err:         newSampleTimestampTooOldError(timestamp, seriesLabels),
			expectedMsg: `the sample has been rejected because its timestamp is too old (err-mimir-sample-timestamp-too-old). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test`,
		},
		"newSampleTimestampTooOldOOOEnabledError": {
			err:         newSampleTimestampTooOldOOOEnabledError(timestamp, seriesLabels, 2*time.Hour),
			expectedMsg: `the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window of 2h (err-mimir-sample-timestamp-too-old). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test`,
		},
		"newSampleTimestampTooFarInFutureError": {
			err:         newSampleTimestampTooFarInFutureError(timestamp, seriesLabels),
			expectedMsg: `received a sample whose timestamp is too far in the future (err-mimir-too-far-in-future). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test`,
		},
		"newSampleOutOfOrderError": {
			err:         newSampleOutOfOrderError(timestamp, seriesLabels),
			expectedMsg: `the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed (err-mimir-sample-out-of-order). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test`,
		},
		"newSampleDuplicateTimestampError": {
			err:         newSampleDuplicateTimestampError(timestamp, seriesLabels),
			expectedMsg: `the sample has been rejected because another sample with the same timestamp, but a different value, has already been ingested (err-mimir-sample-duplicate-timestamp). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test`,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, tc.expectedMsg, tc.err.Error())
			checkIngesterError(t, tc.err, mimirpb.BAD_DATA, true)

			wrappedErr := wrapOrAnnotateWithUser(tc.err, userID)
			require.ErrorIs(t, wrappedErr, tc.err)
			var sampleErr sampleError
			require.ErrorAs(t, wrappedErr, &sampleErr)
			checkIngesterError(t, wrappedErr, mimirpb.BAD_DATA, true)
		})
	}
}

func TestNewExemplarError(t *testing.T) {
	seriesLabels := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	exemplarsLabels := []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}}
	tests := map[string]struct {
		err         error
		expectedMsg string
	}{
		"newExemplarMissingSeriesError": {
			err:         newExemplarMissingSeriesError(timestamp, seriesLabels, exemplarsLabels),
			expectedMsg: `the exemplar has been rejected because the related series has not been ingested yet (err-mimir-exemplar-series-missing). The affected exemplar is {traceID="123"} with timestamp 1970-01-19T05:30:43.969Z for series test`,
		},
		"newExemplarTimestampTooFarInFutureError": {
			err:         newExemplarTimestampTooFarInFutureError(timestamp, seriesLabels, exemplarsLabels),
			expectedMsg: `received an exemplar whose timestamp is too far in the future (err-mimir-exemplar-too-far-in-future). The affected exemplar is {traceID="123"} with timestamp 1970-01-19T05:30:43.969Z for series test`,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, tc.expectedMsg, tc.err.Error())
			checkIngesterError(t, tc.err, mimirpb.BAD_DATA, true)

			wrappedErr := wrapOrAnnotateWithUser(tc.err, userID)
			require.ErrorIs(t, wrappedErr, tc.err)
			var exemplarErr exemplarError
			require.ErrorAs(t, wrappedErr, &exemplarErr)
			checkIngesterError(t, wrappedErr, mimirpb.BAD_DATA, true)
		})
	}
}

func TestNewTSDBIngestExemplarErr(t *testing.T) {
	seriesLabels := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	exemplarsLabels := []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}}
	anotherErr := errors.New("another error")
	err := newTSDBIngestExemplarErr(anotherErr, timestamp, seriesLabels, exemplarsLabels)
	expectedErrMsg := fmt.Sprintf("err: %v. timestamp=1970-01-19T05:30:43.969Z, series=test, exemplar={traceID=\"123\"}", anotherErr)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, mimirpb.BAD_DATA, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &tsdbIngestExemplarErr{})
	checkIngesterError(t, wrappedErr, mimirpb.BAD_DATA, true)
}

func TestTooBusyError(t *testing.T) {
	require.Error(t, errTooBusy)
	require.Equal(t, "ingester is currently too busy to process queries, try again later", errTooBusy.Error())
	checkIngesterError(t, errTooBusy, mimirpb.TOO_BUSY, false)

	wrappedErr := wrapOrAnnotateWithUser(errTooBusy, userID)
	require.ErrorIs(t, wrappedErr, errTooBusy)
	var anotherIngesterTooBusyErr ingesterTooBusyError
	require.ErrorAs(t, wrappedErr, &anotherIngesterTooBusyErr)
	checkIngesterError(t, wrappedErr, mimirpb.TOO_BUSY, false)
}

func TestNewCircuitBreakerOpenError(t *testing.T) {
	remainingDelay := 1 * time.Second
	expectedMsg := fmt.Sprintf("circuit breaker open on foo request type with remaining delay %s", remainingDelay.String())
	err := newCircuitBreakerOpenError("foo", remainingDelay)
	require.Error(t, err)
	require.EqualError(t, err, expectedMsg)
	checkIngesterError(t, err, mimirpb.CIRCUIT_BREAKER_OPEN, false)

	wrappedErr := fmt.Errorf("wrapped: %w", err)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &circuitBreakerOpenError{})

	wrappedWithUserErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedWithUserErr, err)
	require.ErrorAs(t, wrappedWithUserErr, &circuitBreakerOpenError{})
	checkIngesterError(t, wrappedErr, mimirpb.CIRCUIT_BREAKER_OPEN, false)
}

func TestNewErrorWithStatus(t *testing.T) {
	errMsg := "this is an error"
	ingesterErr := mockIngesterErr(errMsg)
	nonIngesterErr := errors.New(errMsg)
	tests := map[string]struct {
		originErr            error
		statusCode           codes.Code
		doNotLog             bool
		expectedErrorMessage string
		expectedErrorDetails *mimirpb.ErrorDetails
	}{
		"new ErrorWithStatus backed by an ingesterError contains ErrorDetails": {
			originErr:            ingesterErr,
			statusCode:           codes.Unimplemented,
			expectedErrorMessage: errMsg,
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: ingesterErr.errorCause()},
		},
		"new ErrorWithStatus backed by a DoNotLog error of ingesterError contains ErrorDetails": {
			originErr:            middleware.DoNotLogError{Err: ingesterErr},
			statusCode:           codes.Unimplemented,
			doNotLog:             true,
			expectedErrorMessage: errMsg,
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: ingesterErr.errorCause()},
		},
		"new ErrorWithStatus backed by a non-ingesterError doesn't contain ErrorDetails": {
			originErr:            nonIngesterErr,
			statusCode:           codes.Unimplemented,
			expectedErrorMessage: errMsg,
			expectedErrorDetails: nil,
		},
	}

	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			errWithStatus := newErrorWithStatus(data.originErr, data.statusCode)
			require.Error(t, errWithStatus)
			require.Errorf(t, errWithStatus, data.expectedErrorMessage)

			// Ensure gogo's status.FromError recognizes errWithStatus.
			//lint:ignore faillint We want to explicitly assert on status.FromError()
			stat, ok := status.FromError(errWithStatus)
			require.True(t, ok)
			require.Equal(t, codes.Unimplemented, stat.Code())
			require.Equal(t, stat.Message(), data.expectedErrorMessage)
			checkErrorWithStatusDetails(t, stat.Details(), data.expectedErrorDetails)

			// Ensure dskit's grpcutil.ErrorToStatus recognizes errWithHTTPStatus.
			stat, ok = grpcutil.ErrorToStatus(errWithStatus)
			require.True(t, ok)
			require.Equal(t, codes.Unimplemented, stat.Code())
			require.Equal(t, stat.Message(), data.expectedErrorMessage)
			checkErrorWithStatusDetails(t, stat.Details(), data.expectedErrorDetails)

			// Ensure grpc's status.FromError recognizes errWithStatus.
			//lint:ignore faillint We want to explicitly assert on status.FromError()
			st, ok := grpcstatus.FromError(errWithStatus)
			require.True(t, ok)
			require.Equal(t, codes.Unimplemented, st.Code())
			require.Equal(t, st.Message(), data.expectedErrorMessage)

			if data.doNotLog {
				var optional middleware.OptionalLogging
				require.ErrorAs(t, errWithStatus, &optional)

				shouldLog, _ := optional.ShouldLog(context.Background())
				require.False(t, shouldLog)
			}
		})
	}
}

func TestWrapOrAnnotateWithUser(t *testing.T) {
	userID := "1"
	annotatingErr := errors.New("this error will be annotated")
	expectedAnnotatedErrMsg := fmt.Sprintf("user=%s: %s", userID, annotatingErr.Error())
	annotatedUnsafeErr := wrapOrAnnotateWithUser(annotatingErr, userID)
	require.Error(t, annotatedUnsafeErr)
	require.EqualError(t, annotatedUnsafeErr, expectedAnnotatedErrMsg)
	require.NotErrorIs(t, annotatedUnsafeErr, annotatingErr)
	require.Nil(t, errors.Unwrap(annotatedUnsafeErr))

	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	wrappingErr := newSampleTimestampTooOldError(timestamp, metricLabelAdapters)
	expectedWrappedErrMsg := fmt.Sprintf("user=%s: %s", userID, wrappingErr.Error())
	wrappedSafeErr := wrapOrAnnotateWithUser(wrappingErr, userID)
	require.Error(t, wrappedSafeErr)
	require.EqualError(t, wrappedSafeErr, expectedWrappedErrMsg)
	require.ErrorIs(t, wrappedSafeErr, wrappingErr)
	require.Equal(t, wrappingErr, errors.Unwrap(wrappedSafeErr))
}

func TestMapPushErrorToErrorWithStatus(t *testing.T) {
	const originalMsg = "this is an error"
	originalErr := errors.New(originalMsg)
	family := "testmetric"
	labelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: family}, {Name: "foo", Value: "biz"}}
	timestamp := model.Time(1)

	testCases := map[string]struct {
		err              error
		doNotLogExpected bool
		expectedCode     codes.Code
		expectedMessage  string
		expectedDetails  *mimirpb.ErrorDetails
	}{
		"a generic error gets translated into an Internal gRPC error without details": {
			err:             originalErr,
			expectedCode:    codes.Internal,
			expectedMessage: originalMsg,
			expectedDetails: nil,
		},
		"a DoNotLog of a generic error gets translated into an Internal gRPC error without details": {
			err:              middleware.DoNotLogError{Err: originalErr},
			expectedCode:     codes.Internal,
			expectedMessage:  originalMsg,
			expectedDetails:  nil,
			doNotLogExpected: true,
		},
		"an unavailableError gets translated into an ErrorWithStatus Unavailable error with details": {
			err:             newUnavailableError(services.Stopping),
			expectedCode:    codes.Unavailable,
			expectedMessage: newUnavailableError(services.Stopping).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"a wrapped unavailableError gets translated into an ErrorWithStatus Unavailable error": {
			err:             fmt.Errorf("wrapped: %w", newUnavailableError(services.Stopping)),
			expectedCode:    codes.Unavailable,
			expectedMessage: fmt.Sprintf("wrapped: %s", newUnavailableError(services.Stopping).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"an ingesterPushGrpcDisabledError gets translated into an ErrorWithStatus Unimplemented error with details": {
			err:             ingesterPushGrpcDisabledError{},
			expectedCode:    codes.Unimplemented,
			expectedMessage: ingesterPushGrpcDisabledMsg,
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.METHOD_NOT_ALLOWED},
		},
		"a wrapped ingesterPushGrpcDisabledError gets translated into an ErrorWithStatus Unimplemented error": {
			err:             fmt.Errorf("wrapped: %w", ingesterPushGrpcDisabledError{}),
			expectedCode:    codes.Unimplemented,
			expectedMessage: fmt.Sprintf("wrapped: %s", ingesterPushGrpcDisabledMsg),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.METHOD_NOT_ALLOWED},
		},
		"an instanceLimitReachedError gets translated into a non-loggable ErrorWithStatus Unavailable error with details": {
			err:              newInstanceLimitReachedError("instance limit reached"),
			expectedCode:     codes.Unavailable,
			expectedMessage:  newInstanceLimitReachedError("instance limit reached").Error(),
			expectedDetails:  &mimirpb.ErrorDetails{Cause: mimirpb.INSTANCE_LIMIT},
			doNotLogExpected: true,
		},
		"a wrapped instanceLimitReachedError gets translated into an ErrorWithStatus Unavailable error with details": {
			err:              fmt.Errorf("wrapped: %w", newInstanceLimitReachedError("instance limit reached")),
			expectedCode:     codes.Unavailable,
			expectedMessage:  fmt.Sprintf("wrapped: %s", newInstanceLimitReachedError("instance limit reached").Error()),
			expectedDetails:  &mimirpb.ErrorDetails{Cause: mimirpb.INSTANCE_LIMIT},
			doNotLogExpected: true,
		},
		"a tsdbUnavailableError gets translated into an ErrorWithStatus Internal error with details": {
			err:             newTSDBUnavailableError("tsdb stopping"),
			expectedCode:    codes.Internal,
			expectedMessage: newTSDBUnavailableError("tsdb stopping").Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TSDB_UNAVAILABLE},
		},
		"a wrapped tsdbUnavailableError gets translated into an ErrorWithStatus Internal error with details": {
			err:             fmt.Errorf("wrapped: %w", newTSDBUnavailableError("tsdb stopping")),
			expectedCode:    codes.Internal,
			expectedMessage: fmt.Sprintf("wrapped: %s", newTSDBUnavailableError("tsdb stopping").Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TSDB_UNAVAILABLE},
		},
		"a sampleError gets translated into an ErrorWithStatus InvalidArgument error with details": {
			err:             newSampleError("id", "sample error", timestamp, labelAdapters),
			expectedCode:    codes.InvalidArgument,
			expectedMessage: newSampleError("id", "sample error", timestamp, labelAdapters).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped sampleError gets translated into an ErrorWithStatus InvalidArgument error with details": {
			err:             fmt.Errorf("wrapped: %w", newSampleError("id", "sample error", timestamp, labelAdapters)),
			expectedCode:    codes.InvalidArgument,
			expectedMessage: fmt.Sprintf("wrapped: %s", newSampleError("id", "sample error", timestamp, labelAdapters).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a exemplarError gets translated into an ErrorWithStatus InvalidArgument error with details": {
			err:             newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters),
			expectedCode:    codes.InvalidArgument,
			expectedMessage: newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped exemplarError gets translated into an ErrorWithStatus InvalidArgument error with details": {
			err:             fmt.Errorf("wrapped: %w", newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters)),
			expectedCode:    codes.InvalidArgument,
			expectedMessage: fmt.Sprintf("wrapped: %s", newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a tsdbIngestExemplarErr gets translated into an ErrorWithStatus InvalidArgument error with details": {
			err:             newTSDBIngestExemplarErr(originalErr, timestamp, labelAdapters, labelAdapters),
			expectedCode:    codes.InvalidArgument,
			expectedMessage: newTSDBIngestExemplarErr(originalErr, timestamp, labelAdapters, labelAdapters).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped tsdbIngestExemplarErr gets translated into an ErrorWithStatus InvalidArgument error with details": {
			err:             fmt.Errorf("wrapped: %w", newTSDBIngestExemplarErr(originalErr, timestamp, labelAdapters, labelAdapters)),
			expectedCode:    codes.InvalidArgument,
			expectedMessage: fmt.Sprintf("wrapped: %s", newTSDBIngestExemplarErr(originalErr, timestamp, labelAdapters, labelAdapters).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a perUserSeriesLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             newPerUserSeriesLimitReachedError(10),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: newPerUserSeriesLimitReachedError(10).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TENANT_LIMIT},
		},
		"a wrapped perUserSeriesLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", newPerUserSeriesLimitReachedError(10)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", newPerUserSeriesLimitReachedError(10).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TENANT_LIMIT},
		},
		"a perUserMetadataLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             newPerUserMetadataLimitReachedError(10),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: newPerUserMetadataLimitReachedError(10).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TENANT_LIMIT},
		},
		"a wrapped perUserMetadataLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", newPerUserMetadataLimitReachedError(10)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", newPerUserMetadataLimitReachedError(10).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TENANT_LIMIT},
		},
		"a perMetricSeriesLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             newPerMetricSeriesLimitReachedError(10, labelAdapters),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: newPerMetricSeriesLimitReachedError(10, labelAdapters).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TENANT_LIMIT},
		},
		"a wrapped perMetricSeriesLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", newPerMetricSeriesLimitReachedError(10, labelAdapters)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", newPerMetricSeriesLimitReachedError(10, labelAdapters).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TENANT_LIMIT},
		},
		"a perMetricMetadataLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             newPerMetricMetadataLimitReachedError(10, family),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: newPerMetricMetadataLimitReachedError(10, family).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TENANT_LIMIT},
		},
		"a wrapped perMetricMetadataLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", newPerMetricMetadataLimitReachedError(10, family)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", newPerMetricMetadataLimitReachedError(10, family).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TENANT_LIMIT},
		},
		"a circuitBreakerOpenError gets translated into an ErrorWithStatus Unavailable error with details": {
			err:             newCircuitBreakerOpenError("foo", 1*time.Second),
			expectedCode:    codes.Unavailable,
			expectedMessage: newCircuitBreakerOpenError("foo", 1*time.Second).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.CIRCUIT_BREAKER_OPEN},
		},
		"a wrapped circuitBreakerOpenError gets translated into an ErrorWithStatus Unavailable error with details": {
			err:             fmt.Errorf("wrapped: %w", newCircuitBreakerOpenError("foo", 1*time.Second)),
			expectedCode:    codes.Unavailable,
			expectedMessage: fmt.Sprintf("wrapped: %s", newCircuitBreakerOpenError("foo", 1*time.Second).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.CIRCUIT_BREAKER_OPEN},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			handledErr := mapPushErrorToErrorWithStatus(tc.err)
			stat, ok := grpcutil.ErrorToStatus(handledErr)
			require.True(t, ok)
			require.Equal(t, tc.expectedCode, stat.Code())
			require.Equal(t, tc.expectedMessage, stat.Message())
			checkErrorWithStatusDetails(t, stat.Details(), tc.expectedDetails)
			if tc.doNotLogExpected {
				var doNotLogError middleware.DoNotLogError
				require.ErrorAs(t, handledErr, &doNotLogError)

				shouldLog, _ := doNotLogError.ShouldLog(context.Background())
				require.False(t, shouldLog)
			}
		})
	}
}

func TestMapReadErrorToErrorWithStatus(t *testing.T) {
	const originalMsg = "this is an error"
	originalErr := errors.New(originalMsg)

	testCases := map[string]struct {
		err             error
		expectedCode    codes.Code
		expectedMessage string
		expectedDetails *mimirpb.ErrorDetails
	}{
		"a generic error gets translated into an Internal gRPC error without details": {
			err:             originalErr,
			expectedCode:    codes.Internal,
			expectedMessage: originalMsg,
			expectedDetails: nil,
		},
		"an unavailableError gets translated into an ErrorWithStatus Unavailable error with details": {
			err:             newUnavailableError(services.Stopping),
			expectedCode:    codes.Unavailable,
			expectedMessage: newUnavailableError(services.Stopping).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"a wrapped unavailableError gets translated into an ErrorWithStatus Unavailable error": {
			err:             fmt.Errorf("wrapped: %w", newUnavailableError(services.Stopping)),
			expectedCode:    codes.Unavailable,
			expectedMessage: fmt.Sprintf("wrapped: %s", newUnavailableError(services.Stopping).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"errTooBusy gets translated into an ErrorWithStatus ResourceExhausted error with details": {
			err:             errTooBusy,
			expectedCode:    codes.ResourceExhausted,
			expectedMessage: ingesterTooBusyMsg,
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TOO_BUSY},
		},
		"a wrapped errTooBusy gets translated into an ErrorWithStatus ResourceExhausted error with details": {
			err:             fmt.Errorf("wrapped: %w", errTooBusy),
			expectedCode:    codes.ResourceExhausted,
			expectedMessage: fmt.Sprintf("wrapped: %s", ingesterTooBusyMsg),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TOO_BUSY},
		},
		"a circuitBreakerOpenError gets translated into an ErrorWithStatus Unavailable error with details": {
			err:             newCircuitBreakerOpenError("foo", 1*time.Second),
			expectedCode:    codes.Unavailable,
			expectedMessage: newCircuitBreakerOpenError("foo", 1*time.Second).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.CIRCUIT_BREAKER_OPEN},
		},
		"a wrapped circuitBreakerOpenError gets translated into an ErrorWithStatus Unavailable error with details": {
			err:             fmt.Errorf("wrapped: %w", newCircuitBreakerOpenError("foo", 1*time.Second)),
			expectedCode:    codes.Unavailable,
			expectedMessage: fmt.Sprintf("wrapped: %s", newCircuitBreakerOpenError("foo", 1*time.Second)),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.CIRCUIT_BREAKER_OPEN},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			handledErr := mapReadErrorToErrorWithStatus(tc.err)
			stat, ok := grpcutil.ErrorToStatus(handledErr)
			require.True(t, ok)
			require.Equal(t, tc.expectedCode, stat.Code())
			require.Equal(t, tc.expectedMessage, stat.Message())
			checkErrorWithStatusDetails(t, stat.Details(), tc.expectedDetails)
		})
	}
}

type mockIngesterErr string

func (e mockIngesterErr) Error() string {
	return string(e)
}

func (e mockIngesterErr) errorCause() mimirpb.ErrorCause {
	return mimirpb.UNKNOWN_CAUSE
}

func checkIngesterError(t *testing.T, err error, expectedCause mimirpb.ErrorCause, isSoft bool) {
	var ingesterErr ingesterError
	require.ErrorAs(t, err, &ingesterErr)
	require.Equal(t, expectedCause, ingesterErr.errorCause())

	if isSoft {
		var softErr softError
		require.ErrorAs(t, err, &softErr)
	}
}

func checkErrorWithStatusDetails(t *testing.T, details []any, expectedDetails *mimirpb.ErrorDetails) {
	if expectedDetails == nil {
		require.Empty(t, details)
	} else {
		require.Len(t, details, 1)
		errDetails, ok := details[0].(*mimirpb.ErrorDetails)
		require.True(t, ok)
		require.Equal(t, expectedDetails, errDetails)
	}
}
