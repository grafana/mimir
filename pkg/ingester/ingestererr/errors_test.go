// SPDX-License-Identifier: AGPL-3.0-only

package ingestererr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gogo/status"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
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
	userID    = "test-user"
)

func TestUnavailableError(t *testing.T) {
	state := services.Starting
	err := NewUnavailableError(state)
	require.Error(t, err)
	expectedMsg := fmt.Sprintf(integerUnavailableMsgFormat, state)
	require.EqualError(t, err, expectedMsg)
	checkIngesterError(t, err, mimirpb.SERVICE_UNAVAILABLE, false)

	wrappedErr := WrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &UnavailableError{})
	checkIngesterError(t, wrappedErr, mimirpb.SERVICE_UNAVAILABLE, false)
}

func TestInstanceLimitReachedError(t *testing.T) {
	limitErrorMessage := "this is a limit error message"
	err := NewInstanceLimitReachedError(limitErrorMessage)
	require.Error(t, err)
	require.EqualError(t, err, limitErrorMessage)
	checkIngesterError(t, err, mimirpb.INSTANCE_LIMIT, false)

	wrappedErr := WrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &InstanceLimitReachedError{})
	checkIngesterError(t, wrappedErr, mimirpb.INSTANCE_LIMIT, false)
}

func TestNewTSDBUnavailableError(t *testing.T) {
	tsdbErrMsg := "TSDB Head forced compaction in progress and no write request is currently allowed"
	err := NewTSDBUnavailableError(tsdbErrMsg)
	require.Error(t, err)
	require.EqualError(t, err, tsdbErrMsg)
	checkIngesterError(t, err, mimirpb.TSDB_UNAVAILABLE, false)

	wrappedErr := fmt.Errorf("wrapped: %w", err)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &tsdbUnavailableError{})

	wrappedWithUserErr := WrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedWithUserErr, err)
	require.ErrorAs(t, wrappedWithUserErr, &tsdbUnavailableError{})
	checkIngesterError(t, wrappedErr, mimirpb.TSDB_UNAVAILABLE, false)
}

func TestNewPerUserSeriesLimitError(t *testing.T) {
	limit := 100
	err := NewPerUserSeriesLimitReachedError(limit)
	expectedErrMsg := globalerror.MaxSeriesPerUser.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("per-user series limit of %d exceeded", limit),
		validation.MaxSeriesPerUserFlag,
	)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, mimirpb.BAD_DATA, true)

	wrappedErr := WrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perUserSeriesLimitReachedError{})
	checkIngesterError(t, wrappedErr, mimirpb.BAD_DATA, true)
}

func TestNewPerUserMetadataLimitError(t *testing.T) {
	limit := 100
	err := NewPerUserMetadataLimitReachedError(limit)
	expectedErrMsg := globalerror.MaxMetadataPerUser.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("per-user metric metadata limit of %d exceeded", limit),
		validation.MaxMetadataPerUserFlag,
	)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, mimirpb.BAD_DATA, true)

	wrappedErr := WrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perUserMetadataLimitReachedError{})
	checkIngesterError(t, wrappedErr, mimirpb.BAD_DATA, true)
}

func TestNewPerMetricSeriesLimitError(t *testing.T) {
	limit := 100
	labels := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
	err := NewPerMetricSeriesLimitReachedError(limit, labels)
	expectedErrMsg := fmt.Sprintf("%s This is for series %s",
		globalerror.MaxSeriesPerMetric.MessageWithPerTenantLimitConfig(
			fmt.Sprintf("per-metric series limit of %d exceeded", limit),
			validation.MaxSeriesPerMetricFlag,
		),
		mimirpb.FromLabelAdaptersToString(labels),
	)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, mimirpb.BAD_DATA, true)

	wrappedErr := WrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perMetricSeriesLimitReachedError{})
	checkIngesterError(t, wrappedErr, mimirpb.BAD_DATA, true)
}

func TestNewPerMetricMetadataLimitError(t *testing.T) {
	limit := 100
	family := "testmetric"
	err := NewPerMetricMetadataLimitReachedError(limit, family)
	expectedErrMsg := fmt.Sprintf("%s This is for metric %s",
		globalerror.MaxMetadataPerMetric.MessageWithPerTenantLimitConfig(
			fmt.Sprintf("per-metric metadata limit of %d exceeded", limit),
			validation.MaxMetadataPerMetricFlag,
		),
		family,
	)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, mimirpb.BAD_DATA, true)

	wrappedErr := WrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perMetricMetadataLimitReachedError{})
	checkIngesterError(t, wrappedErr, mimirpb.BAD_DATA, true)
}

func TestNewSampleError(t *testing.T) {
	seriesLabels := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	tests := map[string]struct {
		err         error
		expectedMsg string
	}{
		"newSampleTimestampTooOldError": {
			err:         NewSampleTimestampTooOldError(timestamp, seriesLabels),
			expectedMsg: `the sample has been rejected because its timestamp is too old (err-mimir-sample-timestamp-too-old). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test`,
		},
		"newSampleTimestampTooOldOOOEnabledError": {
			err:         NewSampleTimestampTooOldOOOEnabledError(timestamp, seriesLabels, 2*time.Hour),
			expectedMsg: `the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window of 2h (err-mimir-sample-timestamp-too-old). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test`,
		},
		"newSampleTimestampTooFarInFutureError": {
			err:         NewSampleTimestampTooFarInFutureError(timestamp, seriesLabels),
			expectedMsg: `received a sample whose timestamp is too far in the future (err-mimir-too-far-in-future). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test`,
		},
		"newSampleOutOfOrderError": {
			err:         NewSampleOutOfOrderError(timestamp, seriesLabels),
			expectedMsg: `the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed (err-mimir-sample-out-of-order). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test`,
		},
		"newSampleDuplicateTimestampError": {
			err:         NewSampleDuplicateTimestampError(timestamp, seriesLabels),
			expectedMsg: `the sample has been rejected because another sample with the same timestamp, but a different value, has already been ingested (err-mimir-sample-duplicate-timestamp). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test`,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, tc.expectedMsg, tc.err.Error())
			checkIngesterError(t, tc.err, mimirpb.BAD_DATA, true)

			wrappedErr := WrapOrAnnotateWithUser(tc.err, userID)
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
			err:         NewExemplarMissingSeriesError(timestamp, seriesLabels, exemplarsLabels),
			expectedMsg: `the exemplar has been rejected because the related series has not been ingested yet (err-mimir-exemplar-series-missing). The affected exemplar is {traceID="123"} with timestamp 1970-01-19T05:30:43.969Z for series test`,
		},
		"newExemplarTimestampTooFarInFutureError": {
			err:         NewExemplarTimestampTooFarInFutureError(timestamp, seriesLabels, exemplarsLabels),
			expectedMsg: `received an exemplar whose timestamp is too far in the future (err-mimir-exemplar-too-far-in-future). The affected exemplar is {traceID="123"} with timestamp 1970-01-19T05:30:43.969Z for series test`,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, tc.expectedMsg, tc.err.Error())
			checkIngesterError(t, tc.err, mimirpb.BAD_DATA, true)

			wrappedErr := WrapOrAnnotateWithUser(tc.err, userID)
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
	err := NewTSDBIngestExemplarErr(anotherErr, timestamp, seriesLabels, exemplarsLabels)
	expectedErrMsg := fmt.Sprintf("err: %v. timestamp=1970-01-19T05:30:43.969Z, series=test, exemplar={traceID=\"123\"}", anotherErr)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, mimirpb.BAD_DATA, true)

	wrappedErr := WrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &tsdbIngestExemplarErr{})
	checkIngesterError(t, wrappedErr, mimirpb.BAD_DATA, true)
}

func TestTooBusyError(t *testing.T) {
	require.Error(t, ErrTooBusy)
	require.Equal(t, "ingester is currently too busy to process queries, try again later", ErrTooBusy.Error())
	checkIngesterError(t, ErrTooBusy, mimirpb.TOO_BUSY, false)

	wrappedErr := WrapOrAnnotateWithUser(ErrTooBusy, userID)
	require.ErrorIs(t, wrappedErr, ErrTooBusy)
	var anotherIngesterTooBusyErr ingesterTooBusyError
	require.ErrorAs(t, wrappedErr, &anotherIngesterTooBusyErr)
	checkIngesterError(t, wrappedErr, mimirpb.TOO_BUSY, false)
}

func TestNewCircuitBreakerOpenError(t *testing.T) {
	remainingDelay := 1 * time.Second
	expectedMsg := fmt.Sprintf("circuit breaker open on foo request type with remaining delay %s", remainingDelay.String())
	err := NewCircuitBreakerOpenError("foo", remainingDelay)
	require.Error(t, err)
	require.EqualError(t, err, expectedMsg)
	checkIngesterError(t, err, mimirpb.CIRCUIT_BREAKER_OPEN, false)

	wrappedErr := fmt.Errorf("wrapped: %w", err)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &CircuitBreakerOpenError{})

	wrappedWithUserErr := WrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedWithUserErr, err)
	require.ErrorAs(t, wrappedWithUserErr, &CircuitBreakerOpenError{})
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
			errWithStatus := NewErrorWithStatus(data.originErr, data.statusCode)
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

			// Ensure httpgrpc's HTTPResponseFromError doesn't recognize errWithStatus.
			resp, ok := httpgrpc.HTTPResponseFromError(errWithStatus)
			require.False(t, ok)
			require.Nil(t, resp)

			if data.doNotLog {
				var optional middleware.OptionalLogging
				require.ErrorAs(t, errWithStatus, &optional)

				shouldLog, _ := optional.ShouldLog(context.Background())
				require.False(t, shouldLog)
			}
		})
	}
}

func TestErrorWithHTTPStatus(t *testing.T) {
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	err := NewSampleTimestampTooOldError(timestamp, metricLabelAdapters)
	errWithHTTPStatus := NewErrorWithHTTPStatus(err, http.StatusBadRequest)
	require.Error(t, errWithHTTPStatus)

	// Ensure gogo's status.FromError recognizes errWithHTTPStatus.
	//lint:ignore faillint We want to explicitly assert on status.FromError()
	stat, ok := status.FromError(errWithHTTPStatus)
	require.True(t, ok)
	require.Equal(t, http.StatusBadRequest, int(stat.Code()))
	require.Errorf(t, err, stat.Message())
	require.NotEmpty(t, stat.Details())

	// Ensure dskit's grpcutil.ErrorToStatus recognizes errWithHTTPStatus.
	stat, ok = grpcutil.ErrorToStatus(errWithHTTPStatus)
	require.True(t, ok)
	require.Equal(t, http.StatusBadRequest, int(stat.Code()))
	require.Errorf(t, err, stat.Message())

	// Ensure grpc's status.FromError recognizes errWithHTTPStatus.
	//lint:ignore faillint We want to explicitly assert on status.FromError()
	st, ok := grpcstatus.FromError(errWithHTTPStatus)
	require.True(t, ok)
	require.Equal(t, http.StatusBadRequest, int(st.Code()))
	require.Errorf(t, err, st.Message())

	// Ensure httpgrpc's HTTPResponseFromError recognizes errWithHTTPStatus.
	resp, ok := httpgrpc.HTTPResponseFromError(errWithHTTPStatus)
	require.True(t, ok)
	require.Equal(t, int32(http.StatusBadRequest), resp.Code)
	require.Errorf(t, err, errWithHTTPStatus.Error())
}

func TestWrapOrAnnotateWithUser(t *testing.T) {
	userID := "1"
	annotatingErr := errors.New("this error will be annotated")
	expectedAnnotatedErrMsg := fmt.Sprintf("user=%s: %s", userID, annotatingErr.Error())
	annotatedUnsafeErr := WrapOrAnnotateWithUser(annotatingErr, userID)
	require.Error(t, annotatedUnsafeErr)
	require.EqualError(t, annotatedUnsafeErr, expectedAnnotatedErrMsg)
	require.NotErrorIs(t, annotatedUnsafeErr, annotatingErr)
	require.Nil(t, errors.Unwrap(annotatedUnsafeErr))

	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	wrappingErr := NewSampleTimestampTooOldError(timestamp, metricLabelAdapters)
	expectedWrappedErrMsg := fmt.Sprintf("user=%s: %s", userID, wrappingErr.Error())
	wrappedSafeErr := WrapOrAnnotateWithUser(wrappingErr, userID)
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
			err:             NewUnavailableError(services.Stopping),
			expectedCode:    codes.Unavailable,
			expectedMessage: NewUnavailableError(services.Stopping).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"a wrapped unavailableError gets translated into an ErrorWithStatus Unavailable error": {
			err:             fmt.Errorf("wrapped: %w", NewUnavailableError(services.Stopping)),
			expectedCode:    codes.Unavailable,
			expectedMessage: fmt.Sprintf("wrapped: %s", NewUnavailableError(services.Stopping).Error()),
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
			err:              NewInstanceLimitReachedError("instance limit reached"),
			expectedCode:     codes.Unavailable,
			expectedMessage:  NewInstanceLimitReachedError("instance limit reached").Error(),
			expectedDetails:  &mimirpb.ErrorDetails{Cause: mimirpb.INSTANCE_LIMIT},
			doNotLogExpected: true,
		},
		"a wrapped instanceLimitReachedError gets translated into an ErrorWithStatus Unavailable error with details": {
			err:              fmt.Errorf("wrapped: %w", NewInstanceLimitReachedError("instance limit reached")),
			expectedCode:     codes.Unavailable,
			expectedMessage:  fmt.Sprintf("wrapped: %s", NewInstanceLimitReachedError("instance limit reached").Error()),
			expectedDetails:  &mimirpb.ErrorDetails{Cause: mimirpb.INSTANCE_LIMIT},
			doNotLogExpected: true,
		},
		"a tsdbUnavailableError gets translated into an ErrorWithStatus Internal error with details": {
			err:             NewTSDBUnavailableError("tsdb stopping"),
			expectedCode:    codes.Internal,
			expectedMessage: NewTSDBUnavailableError("tsdb stopping").Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TSDB_UNAVAILABLE},
		},
		"a wrapped tsdbUnavailableError gets translated into an ErrorWithStatus Internal error with details": {
			err:             fmt.Errorf("wrapped: %w", NewTSDBUnavailableError("tsdb stopping")),
			expectedCode:    codes.Internal,
			expectedMessage: fmt.Sprintf("wrapped: %s", NewTSDBUnavailableError("tsdb stopping").Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TSDB_UNAVAILABLE},
		},
		"a sampleError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             NewSampleError("id", "sample error", timestamp, labelAdapters),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: NewSampleError("id", "sample error", timestamp, labelAdapters).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped sampleError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", NewSampleError("id", "sample error", timestamp, labelAdapters)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", NewSampleError("id", "sample error", timestamp, labelAdapters).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a exemplarError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             NewExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: NewExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped exemplarError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", NewExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", NewExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a tsdbIngestExemplarErr gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             NewTSDBIngestExemplarErr(originalErr, timestamp, labelAdapters, labelAdapters),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: NewTSDBIngestExemplarErr(originalErr, timestamp, labelAdapters, labelAdapters).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped tsdbIngestExemplarErr gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", NewTSDBIngestExemplarErr(originalErr, timestamp, labelAdapters, labelAdapters)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", NewTSDBIngestExemplarErr(originalErr, timestamp, labelAdapters, labelAdapters).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a perUserSeriesLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             NewPerUserSeriesLimitReachedError(10),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: NewPerUserSeriesLimitReachedError(10).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped perUserSeriesLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", NewPerUserSeriesLimitReachedError(10)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", NewPerUserSeriesLimitReachedError(10).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a perUserMetadataLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             NewPerUserMetadataLimitReachedError(10),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: NewPerUserMetadataLimitReachedError(10).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped perUserMetadataLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", NewPerUserMetadataLimitReachedError(10)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", NewPerUserMetadataLimitReachedError(10).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a perMetricSeriesLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             NewPerMetricSeriesLimitReachedError(10, labelAdapters),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: NewPerMetricSeriesLimitReachedError(10, labelAdapters).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped perMetricSeriesLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", NewPerMetricSeriesLimitReachedError(10, labelAdapters)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", NewPerMetricSeriesLimitReachedError(10, labelAdapters).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a perMetricMetadataLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             NewPerMetricMetadataLimitReachedError(10, family),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: NewPerMetricMetadataLimitReachedError(10, family).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped perMetricMetadataLimitReachedError gets translated into an ErrorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", NewPerMetricMetadataLimitReachedError(10, family)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", NewPerMetricMetadataLimitReachedError(10, family).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a circuitBreakerOpenError gets translated into an ErrorWithStatus Unavailable error with details": {
			err:             NewCircuitBreakerOpenError("foo", 1*time.Second),
			expectedCode:    codes.Unavailable,
			expectedMessage: NewCircuitBreakerOpenError("foo", 1*time.Second).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.CIRCUIT_BREAKER_OPEN},
		},
		"a wrapped circuitBreakerOpenError gets translated into an ErrorWithStatus Unavailable error with details": {
			err:             fmt.Errorf("wrapped: %w", NewCircuitBreakerOpenError("foo", 1*time.Second)),
			expectedCode:    codes.Unavailable,
			expectedMessage: fmt.Sprintf("wrapped: %s", NewCircuitBreakerOpenError("foo", 1*time.Second).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.CIRCUIT_BREAKER_OPEN},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			handledErr := MapPushErrorToErrorWithStatus(tc.err)
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

func TestMapPushErrorToErrorWithHTTPOrGRPCStatus(t *testing.T) {
	const originalMsg = "this is an error"
	originalErr := errors.New(originalMsg)
	family := "testmetric"
	labelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: family}, {Name: "foo", Value: "biz"}}
	timestamp := model.Time(1)

	testCases := map[string]struct {
		err                 error
		doNotLogExpected    bool
		expectedTranslation error
	}{
		"a generic error is not translated": {
			err:                 originalErr,
			expectedTranslation: originalErr,
		},
		"a DoNotLog error of a generic error is not translated": {
			err:                 middleware.DoNotLogError{Err: originalErr},
			expectedTranslation: middleware.DoNotLogError{Err: originalErr},
			doNotLogExpected:    true,
		},
		"an unavailableError gets translated into an ErrorWithStatus Unavailable error": {
			err:                 NewUnavailableError(services.Stopping),
			expectedTranslation: NewErrorWithStatus(NewUnavailableError(services.Stopping), codes.Unavailable),
		},
		"a wrapped unavailableError gets translated into a non-loggable ErrorWithStatus Unavailable error": {
			err: fmt.Errorf("wrapped: %w", NewUnavailableError(services.Stopping)),
			expectedTranslation: NewErrorWithStatus(
				fmt.Errorf("wrapped: %w", NewUnavailableError(services.Stopping)),
				codes.Unavailable,
			),
		},
		"an instanceLimitReachedError gets translated into a non-loggable ErrorWithStatus Unavailable error": {
			err: NewInstanceLimitReachedError("instance limit reached"),
			expectedTranslation: NewErrorWithStatus(
				middleware.DoNotLogError{Err: NewInstanceLimitReachedError("instance limit reached")},
				codes.Unavailable,
			),
			doNotLogExpected: true,
		},
		"a wrapped instanceLimitReachedError gets translated into a non-loggable ErrorWithStatus Unavailable error": {
			err: fmt.Errorf("wrapped: %w", NewInstanceLimitReachedError("instance limit reached")),
			expectedTranslation: NewErrorWithStatus(
				middleware.DoNotLogError{Err: fmt.Errorf("wrapped: %w", NewInstanceLimitReachedError("instance limit reached"))},
				codes.Unavailable,
			),
			doNotLogExpected: true,
		},
		"a tsdbUnavailableError gets translated into an errorWithHTTPStatus 503 error": {
			err: NewTSDBUnavailableError("tsdb stopping"),
			expectedTranslation: NewErrorWithHTTPStatus(
				NewTSDBUnavailableError("tsdb stopping"),
				http.StatusServiceUnavailable,
			),
		},
		"a wrapped tsdbUnavailableError gets translated into an errorWithHTTPStatus 503 error": {
			err: fmt.Errorf("wrapped: %w", NewTSDBUnavailableError("tsdb stopping")),
			expectedTranslation: NewErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", NewTSDBUnavailableError("tsdb stopping")),
				http.StatusServiceUnavailable,
			),
		},
		"a sampleError gets translated into an errorWithHTTPStatus 400 error": {
			err: NewSampleError("id", "sample error", timestamp, labelAdapters),
			expectedTranslation: NewErrorWithHTTPStatus(
				NewSampleError("id", "sample error", timestamp, labelAdapters),
				http.StatusBadRequest,
			),
		},
		"a wrapped sample gets translated into an errorWithHTTPStatus 400 error": {
			err: fmt.Errorf("wrapped: %w", NewSampleError("id", "sample error", timestamp, labelAdapters)),
			expectedTranslation: NewErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", NewSampleError("id", "sample error", timestamp, labelAdapters)),
				http.StatusBadRequest,
			),
		},
		"a exemplarError gets translated into an errorWithHTTPStatus 400 error": {
			err: NewExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters),
			expectedTranslation: NewErrorWithHTTPStatus(
				NewExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters),
				http.StatusBadRequest,
			),
		},
		"a wrapped exemplarError gets translated into an errorWithHTTPStatus 400 error": {
			err: fmt.Errorf("wrapped: %w", NewExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters)),
			expectedTranslation: NewErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", NewExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters)),
				http.StatusBadRequest,
			),
		},
		"a perUserSeriesLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: NewPerUserSeriesLimitReachedError(10),
			expectedTranslation: NewErrorWithHTTPStatus(
				NewPerUserSeriesLimitReachedError(10),
				http.StatusBadRequest,
			),
		},
		"a wrapped perUserSeriesLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: fmt.Errorf("wrapped: %w", NewPerUserSeriesLimitReachedError(10)),
			expectedTranslation: NewErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", NewPerUserSeriesLimitReachedError(10)),
				http.StatusBadRequest,
			),
		},
		"a perMetricSeriesLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: NewPerMetricSeriesLimitReachedError(10, labelAdapters),
			expectedTranslation: NewErrorWithHTTPStatus(
				NewPerMetricSeriesLimitReachedError(10, labelAdapters),
				http.StatusBadRequest,
			),
		},
		"a wrapped perMetricSeriesLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: fmt.Errorf("wrapped: %w", NewPerMetricSeriesLimitReachedError(10, labelAdapters)),
			expectedTranslation: NewErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", NewPerMetricSeriesLimitReachedError(10, labelAdapters)),
				http.StatusBadRequest,
			),
		},
		"a perUserMetadataLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: NewPerUserMetadataLimitReachedError(10),
			expectedTranslation: NewErrorWithHTTPStatus(
				NewPerUserMetadataLimitReachedError(10),
				http.StatusBadRequest,
			),
		},
		"a wrapped perUserMetadataLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: fmt.Errorf("wrapped: %w", NewPerUserMetadataLimitReachedError(10)),
			expectedTranslation: NewErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", NewPerUserMetadataLimitReachedError(10)),
				http.StatusBadRequest,
			),
		},
		"a perMetricMetadataLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: NewPerMetricMetadataLimitReachedError(10, family),
			expectedTranslation: NewErrorWithHTTPStatus(
				NewPerMetricMetadataLimitReachedError(10, family),
				http.StatusBadRequest,
			),
		},
		"a wrapped perMetricMetadataLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: fmt.Errorf("wrapped: %w", NewPerMetricMetadataLimitReachedError(10, family)),
			expectedTranslation: NewErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", NewPerMetricMetadataLimitReachedError(10, family)),
				http.StatusBadRequest,
			),
		},
		"an ingesterPushGrpcDisabledError gets translated into an ErrorWithStatus Unimplemented error": {
			err: ingesterPushGrpcDisabledError{},
			expectedTranslation: NewErrorWithStatus(
				ingesterPushGrpcDisabledError{},
				codes.Unimplemented,
			),
		},
		"a wrapped ingesterPushGrpcDisabledError gets translated into an ErrorWithStatus Unimplemented error": {
			err: fmt.Errorf("wrapped: %w", ingesterPushGrpcDisabledError{}),
			expectedTranslation: NewErrorWithStatus(
				fmt.Errorf("wrapped: %w", ingesterPushGrpcDisabledError{}),
				codes.Unimplemented,
			),
		},
		"a circuitBreakerOpenError gets translated into an ErrorWithStatus Unavailable error": {
			err: NewCircuitBreakerOpenError("foo", 1*time.Second),
			expectedTranslation: NewErrorWithStatus(
				NewCircuitBreakerOpenError("foo", 1*time.Second),
				codes.Unavailable,
			),
		},
		"a wrapped circuitBreakerOpenError gets translated into an ErrorWithStatus Unavailable error": {
			err: fmt.Errorf("wrapped: %w", NewCircuitBreakerOpenError("foo", 1*time.Second)),
			expectedTranslation: NewErrorWithStatus(
				fmt.Errorf("wrapped: %w", NewCircuitBreakerOpenError("foo", 1*time.Second)),
				codes.Unavailable,
			),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			handledErr := MapPushErrorToErrorWithHTTPOrGRPCStatus(tc.err)
			require.Equal(t, tc.expectedTranslation, handledErr)
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
			err:             NewUnavailableError(services.Stopping),
			expectedCode:    codes.Unavailable,
			expectedMessage: NewUnavailableError(services.Stopping).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"a wrapped unavailableError gets translated into an ErrorWithStatus Unavailable error": {
			err:             fmt.Errorf("wrapped: %w", NewUnavailableError(services.Stopping)),
			expectedCode:    codes.Unavailable,
			expectedMessage: fmt.Sprintf("wrapped: %s", NewUnavailableError(services.Stopping).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"ErrTooBusy gets translated into an ErrorWithStatus ResourceExhausted error with details": {
			err:             ErrTooBusy,
			expectedCode:    codes.ResourceExhausted,
			expectedMessage: ingesterTooBusyMsg,
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TOO_BUSY},
		},
		"a wrapped ErrTooBusy gets translated into an ErrorWithStatus ResourceExhausted error with details": {
			err:             fmt.Errorf("wrapped: %w", ErrTooBusy),
			expectedCode:    codes.ResourceExhausted,
			expectedMessage: fmt.Sprintf("wrapped: %s", ingesterTooBusyMsg),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TOO_BUSY},
		},
		"a circuitBreakerOpenError gets translated into an ErrorWithStatus Unavailable error with details": {
			err:             NewCircuitBreakerOpenError("foo", 1*time.Second),
			expectedCode:    codes.Unavailable,
			expectedMessage: NewCircuitBreakerOpenError("foo", 1*time.Second).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.CIRCUIT_BREAKER_OPEN},
		},
		"a wrapped circuitBreakerOpenError gets translated into an ErrorWithStatus Unavailable error with details": {
			err:             fmt.Errorf("wrapped: %w", NewCircuitBreakerOpenError("foo", 1*time.Second)),
			expectedCode:    codes.Unavailable,
			expectedMessage: fmt.Sprintf("wrapped: %s", NewCircuitBreakerOpenError("foo", 1*time.Second)),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.CIRCUIT_BREAKER_OPEN},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			handledErr := MapReadErrorToErrorWithStatus(tc.err)
			stat, ok := grpcutil.ErrorToStatus(handledErr)
			require.True(t, ok)
			require.Equal(t, tc.expectedCode, stat.Code())
			require.Equal(t, tc.expectedMessage, stat.Message())
			checkErrorWithStatusDetails(t, stat.Details(), tc.expectedDetails)
		})
	}
}

func TestMapReadErrorToErrorWithHTTPOrGRPCStatus(t *testing.T) {
	const originalMsg = "this is an error"
	originalErr := errors.New(originalMsg)

	testCases := map[string]struct {
		err                 error
		expectedTranslation error
	}{
		"a generic error is not translated": {
			err:                 originalErr,
			expectedTranslation: originalErr,
		},
		"an unavailableError gets translated into an ErrorWithStatus Unavailable error with details": {
			err:                 NewUnavailableError(services.Stopping),
			expectedTranslation: NewErrorWithStatus(NewUnavailableError(services.Stopping), codes.Unavailable),
		},
		"a wrapped unavailableError gets translated into an ErrorWithStatus Unavailable error": {
			err:                 fmt.Errorf("wrapped: %w", NewUnavailableError(services.Stopping)),
			expectedTranslation: NewErrorWithStatus(fmt.Errorf("wrapped: %w", NewUnavailableError(services.Stopping)), codes.Unavailable),
		},
		"ErrTooBusy gets translated into an errorWithHTTPStatus with status code 503": {
			err:                 ErrTooBusy,
			expectedTranslation: NewErrorWithHTTPStatus(ErrTooBusy, http.StatusServiceUnavailable),
		},
		"a wrapped ErrTooBusy gets translated into an errorWithHTTPStatus with status code 503": {
			err:                 fmt.Errorf("wrapped: %w", ErrTooBusy),
			expectedTranslation: NewErrorWithHTTPStatus(fmt.Errorf("wrapped: %w", ErrTooBusy), http.StatusServiceUnavailable),
		},
		"a circuitBreakerOpenError gets translated into an ErrorWithStatus Unavailable error": {
			err:                 NewCircuitBreakerOpenError("foo", 1*time.Second),
			expectedTranslation: NewErrorWithStatus(NewCircuitBreakerOpenError("foo", 1*time.Second), codes.Unavailable),
		},
		"a wrapped circuitBreakerOpenError gets translated into an ErrorWithStatus Unavailable": {
			err:                 fmt.Errorf("wrapped: %w", NewCircuitBreakerOpenError("foo", 1*time.Second)),
			expectedTranslation: NewErrorWithStatus(fmt.Errorf("wrapped: %w", NewCircuitBreakerOpenError("foo", 1*time.Second)), codes.Unavailable),
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			handledErr := MapReadErrorToErrorWithHTTPOrGRPCStatus(tc.err)
			require.Equal(t, tc.expectedTranslation, handledErr)
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

func checkIngesterError(t *testing.T, err error, expectedCause mimirpb.ErrorCause, shouldBeSoft bool) {
	t.Helper()
	var ingesterErr IngesterError
	require.ErrorAs(t, err, &ingesterErr)
	require.Equal(t, expectedCause, ingesterErr.ErrorCause())

	if shouldBeSoft {
		var softErr SoftError
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
