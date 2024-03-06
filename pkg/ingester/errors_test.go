// SPDX-License-Identifier: AGPL-3.0-only

package ingester

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
	checkIngesterError(t, err, mimirpb.BAD_DATA, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perUserSeriesLimitReachedError{})
	checkIngesterError(t, wrappedErr, mimirpb.BAD_DATA, true)
}

func TestNewPerUserMetadataLimitError(t *testing.T) {
	limit := 100
	err := newPerUserMetadataLimitReachedError(limit)
	expectedErrMsg := globalerror.MaxMetadataPerUser.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("per-user metric metadata limit of %d exceeded", limit),
		validation.MaxMetadataPerUserFlag,
	)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, mimirpb.BAD_DATA, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perUserMetadataLimitReachedError{})
	checkIngesterError(t, wrappedErr, mimirpb.BAD_DATA, true)
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
	checkIngesterError(t, err, mimirpb.BAD_DATA, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perMetricSeriesLimitReachedError{})
	checkIngesterError(t, wrappedErr, mimirpb.BAD_DATA, true)
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
	checkIngesterError(t, err, mimirpb.BAD_DATA, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
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
	require.Error(t, tooBusyError)
	require.Equal(t, "the ingester is currently too busy to process queries, try again later", tooBusyError.Error())
	checkIngesterError(t, tooBusyError, mimirpb.TOO_BUSY, false)

	wrappedErr := wrapOrAnnotateWithUser(tooBusyError, userID)
	require.ErrorIs(t, wrappedErr, tooBusyError)
	var anotherIngesterTooBusyErr ingesterTooBusyError
	require.ErrorAs(t, wrappedErr, &anotherIngesterTooBusyErr)
	checkIngesterError(t, wrappedErr, mimirpb.TOO_BUSY, false)
}

func TestErrorWithStatus(t *testing.T) {
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
		"new errorWithStatus backed by an ingesterError contains ErrorDetails": {
			originErr:            ingesterErr,
			statusCode:           codes.Unimplemented,
			expectedErrorMessage: errMsg,
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: ingesterErr.errorCause()},
		},
		"new errorWithStatus backed by a DoNotLog error of ingesterError contains ErrorDetails": {
			originErr:            middleware.DoNotLogError{Err: ingesterErr},
			statusCode:           codes.Unimplemented,
			doNotLog:             true,
			expectedErrorMessage: errMsg,
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: ingesterErr.errorCause()},
		},
		"new errorWithStatus backed by a non-ingesterError doesn't contain ErrorDetails": {
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

			// Ensure httpgrpc's HTTPResponseFromError doesn't recognize errWithStatus.
			resp, ok := httpgrpc.HTTPResponseFromError(errWithStatus)
			require.False(t, ok)
			require.Nil(t, resp)

			if data.doNotLog {
				var optional middleware.OptionalLogging
				require.ErrorAs(t, errWithStatus, &optional)
				require.False(t, optional.ShouldLog(context.Background(), 0))
			}
		})
	}
}

func TestErrorWithHTTPStatus(t *testing.T) {
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	err := newSampleTimestampTooOldError(timestamp, metricLabelAdapters)
	errWithHTTPStatus := newErrorWithHTTPStatus(err, http.StatusBadRequest)
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
		"an unavailableError gets translated into an errorWithStatus Unavailable error with details": {
			err:             newUnavailableError(services.Stopping),
			expectedCode:    codes.Unavailable,
			expectedMessage: newUnavailableError(services.Stopping).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"a wrapped unavailableError gets translated into an errorWithStatus Unavailable error": {
			err:             fmt.Errorf("wrapped: %w", newUnavailableError(services.Stopping)),
			expectedCode:    codes.Unavailable,
			expectedMessage: fmt.Sprintf("wrapped: %s", newUnavailableError(services.Stopping).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"an instanceLimitReachedError gets translated into a non-loggable errorWithStatus Unavailable error with details": {
			err:              newInstanceLimitReachedError("instance limit reached"),
			expectedCode:     codes.Unavailable,
			expectedMessage:  newInstanceLimitReachedError("instance limit reached").Error(),
			expectedDetails:  &mimirpb.ErrorDetails{Cause: mimirpb.INSTANCE_LIMIT},
			doNotLogExpected: true,
		},
		"a wrapped instanceLimitReachedError gets translated into an errorWithStatus Unavailable error with details": {
			err:              fmt.Errorf("wrapped: %w", newInstanceLimitReachedError("instance limit reached")),
			expectedCode:     codes.Unavailable,
			expectedMessage:  fmt.Sprintf("wrapped: %s", newInstanceLimitReachedError("instance limit reached").Error()),
			expectedDetails:  &mimirpb.ErrorDetails{Cause: mimirpb.INSTANCE_LIMIT},
			doNotLogExpected: true,
		},
		"a tsdbUnavailableError gets translated into an errorWithStatus Internal error with details": {
			err:             newTSDBUnavailableError("tsdb stopping"),
			expectedCode:    codes.Internal,
			expectedMessage: newTSDBUnavailableError("tsdb stopping").Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TSDB_UNAVAILABLE},
		},
		"a wrapped tsdbUnavailableError gets translated into an errorWithStatus Internal error with details": {
			err:             fmt.Errorf("wrapped: %w", newTSDBUnavailableError("tsdb stopping")),
			expectedCode:    codes.Internal,
			expectedMessage: fmt.Sprintf("wrapped: %s", newTSDBUnavailableError("tsdb stopping").Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TSDB_UNAVAILABLE},
		},
		"a sampleError gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             newSampleError("id", "sample error", timestamp, labelAdapters),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: newSampleError("id", "sample error", timestamp, labelAdapters).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped sampleError gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", newSampleError("id", "sample error", timestamp, labelAdapters)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", newSampleError("id", "sample error", timestamp, labelAdapters).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a exemplarError gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped exemplarError gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a tsdbIngestExemplarErr gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             newTSDBIngestExemplarErr(originalErr, timestamp, labelAdapters, labelAdapters),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: newTSDBIngestExemplarErr(originalErr, timestamp, labelAdapters, labelAdapters).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped tsdbIngestExemplarErr gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", newTSDBIngestExemplarErr(originalErr, timestamp, labelAdapters, labelAdapters)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", newTSDBIngestExemplarErr(originalErr, timestamp, labelAdapters, labelAdapters).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a perUserSeriesLimitReachedError gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             newPerUserSeriesLimitReachedError(10),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: newPerUserSeriesLimitReachedError(10).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped perUserSeriesLimitReachedError gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", newPerUserSeriesLimitReachedError(10)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", newPerUserSeriesLimitReachedError(10).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a perUserMetadataLimitReachedError gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             newPerUserMetadataLimitReachedError(10),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: newPerUserMetadataLimitReachedError(10).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped perUserMetadataLimitReachedError gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", newPerUserMetadataLimitReachedError(10)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", newPerUserMetadataLimitReachedError(10).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a perMetricSeriesLimitReachedError gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             newPerMetricSeriesLimitReachedError(10, labelAdapters),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: newPerMetricSeriesLimitReachedError(10, labelAdapters).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped perMetricSeriesLimitReachedError gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", newPerMetricSeriesLimitReachedError(10, labelAdapters)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", newPerMetricSeriesLimitReachedError(10, labelAdapters).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a perMetricMetadataLimitReachedError gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             newPerMetricMetadataLimitReachedError(10, family),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: newPerMetricMetadataLimitReachedError(10, family).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"a wrapped perMetricMetadataLimitReachedError gets translated into an errorWithStatus FailedPrecondition error with details": {
			err:             fmt.Errorf("wrapped: %w", newPerMetricMetadataLimitReachedError(10, family)),
			expectedCode:    codes.FailedPrecondition,
			expectedMessage: fmt.Sprintf("wrapped: %s", newPerMetricMetadataLimitReachedError(10, family).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
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
				require.False(t, doNotLogError.ShouldLog(context.Background(), 0))
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
		"an unavailableError gets translated into an errorWithStatus Unavailable error": {
			err:                 newUnavailableError(services.Stopping),
			expectedTranslation: newErrorWithStatus(newUnavailableError(services.Stopping), codes.Unavailable),
		},
		"a wrapped unavailableError gets translated into a non-loggable errorWithStatus Unavailable error": {
			err: fmt.Errorf("wrapped: %w", newUnavailableError(services.Stopping)),
			expectedTranslation: newErrorWithStatus(
				fmt.Errorf("wrapped: %w", newUnavailableError(services.Stopping)),
				codes.Unavailable,
			),
		},
		"an instanceLimitReachedError gets translated into a non-loggable errorWithStatus Unavailable error": {
			err: newInstanceLimitReachedError("instance limit reached"),
			expectedTranslation: newErrorWithStatus(
				middleware.DoNotLogError{Err: newInstanceLimitReachedError("instance limit reached")},
				codes.Unavailable,
			),
			doNotLogExpected: true,
		},
		"a wrapped instanceLimitReachedError gets translated into a non-loggable errorWithStatus Unavailable error": {
			err: fmt.Errorf("wrapped: %w", newInstanceLimitReachedError("instance limit reached")),
			expectedTranslation: newErrorWithStatus(
				middleware.DoNotLogError{Err: fmt.Errorf("wrapped: %w", newInstanceLimitReachedError("instance limit reached"))},
				codes.Unavailable,
			),
			doNotLogExpected: true,
		},
		"a tsdbUnavailableError gets translated into an errorWithHTTPStatus 503 error": {
			err: newTSDBUnavailableError("tsdb stopping"),
			expectedTranslation: newErrorWithHTTPStatus(
				newTSDBUnavailableError("tsdb stopping"),
				http.StatusServiceUnavailable,
			),
		},
		"a wrapped tsdbUnavailableError gets translated into an errorWithHTTPStatus 503 error": {
			err: fmt.Errorf("wrapped: %w", newTSDBUnavailableError("tsdb stopping")),
			expectedTranslation: newErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", newTSDBUnavailableError("tsdb stopping")),
				http.StatusServiceUnavailable,
			),
		},
		"a sampleError gets translated into an errorWithHTTPStatus 400 error": {
			err: newSampleError("id", "sample error", timestamp, labelAdapters),
			expectedTranslation: newErrorWithHTTPStatus(
				newSampleError("id", "sample error", timestamp, labelAdapters),
				http.StatusBadRequest,
			),
		},
		"a wrapped sample gets translated into an errorWithHTTPStatus 400 error": {
			err: fmt.Errorf("wrapped: %w", newSampleError("id", "sample error", timestamp, labelAdapters)),
			expectedTranslation: newErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", newSampleError("id", "sample error", timestamp, labelAdapters)),
				http.StatusBadRequest,
			),
		},
		"a exemplarError gets translated into an errorWithHTTPStatus 400 error": {
			err: newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters),
			expectedTranslation: newErrorWithHTTPStatus(
				newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters),
				http.StatusBadRequest,
			),
		},
		"a wrapped exemplarError gets translated into an errorWithHTTPStatus 400 error": {
			err: fmt.Errorf("wrapped: %w", newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters)),
			expectedTranslation: newErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters)),
				http.StatusBadRequest,
			),
		},
		"a perUserSeriesLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: newPerUserSeriesLimitReachedError(10),
			expectedTranslation: newErrorWithHTTPStatus(
				newPerUserSeriesLimitReachedError(10),
				http.StatusBadRequest,
			),
		},
		"a wrapped perUserSeriesLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: fmt.Errorf("wrapped: %w", newPerUserSeriesLimitReachedError(10)),
			expectedTranslation: newErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", newPerUserSeriesLimitReachedError(10)),
				http.StatusBadRequest,
			),
		},
		"a perMetricSeriesLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: newPerMetricSeriesLimitReachedError(10, labelAdapters),
			expectedTranslation: newErrorWithHTTPStatus(
				newPerMetricSeriesLimitReachedError(10, labelAdapters),
				http.StatusBadRequest,
			),
		},
		"a wrapped perMetricSeriesLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: fmt.Errorf("wrapped: %w", newPerMetricSeriesLimitReachedError(10, labelAdapters)),
			expectedTranslation: newErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", newPerMetricSeriesLimitReachedError(10, labelAdapters)),
				http.StatusBadRequest,
			),
		},
		"a perUserMetadataLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: newPerUserMetadataLimitReachedError(10),
			expectedTranslation: newErrorWithHTTPStatus(
				newPerUserMetadataLimitReachedError(10),
				http.StatusBadRequest,
			),
		},
		"a wrapped perUserMetadataLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: fmt.Errorf("wrapped: %w", newPerUserMetadataLimitReachedError(10)),
			expectedTranslation: newErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", newPerUserMetadataLimitReachedError(10)),
				http.StatusBadRequest,
			),
		},
		"a perMetricMetadataLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: newPerMetricMetadataLimitReachedError(10, family),
			expectedTranslation: newErrorWithHTTPStatus(
				newPerMetricMetadataLimitReachedError(10, family),
				http.StatusBadRequest,
			),
		},
		"a wrapped perMetricMetadataLimitReachedError gets translated into an errorWithHTTPStatus 400 error": {
			err: fmt.Errorf("wrapped: %w", newPerMetricMetadataLimitReachedError(10, family)),
			expectedTranslation: newErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", newPerMetricMetadataLimitReachedError(10, family)),
				http.StatusBadRequest,
			),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			handledErr := mapPushErrorToErrorWithHTTPOrGRPCStatus(tc.err)
			require.Equal(t, tc.expectedTranslation, handledErr)
			if tc.doNotLogExpected {
				var doNotLogError middleware.DoNotLogError
				require.ErrorAs(t, handledErr, &doNotLogError)
				require.False(t, doNotLogError.ShouldLog(context.Background(), 0))
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
		"an unavailableError gets translated into an errorWithStatus Unavailable error with details": {
			err:             newUnavailableError(services.Stopping),
			expectedCode:    codes.Unavailable,
			expectedMessage: newUnavailableError(services.Stopping).Error(),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"a wrapped unavailableError gets translated into an errorWithStatus Unavailable error": {
			err:             fmt.Errorf("wrapped: %w", newUnavailableError(services.Stopping)),
			expectedCode:    codes.Unavailable,
			expectedMessage: fmt.Sprintf("wrapped: %s", newUnavailableError(services.Stopping).Error()),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE},
		},
		"tooBusyError gets translated into an errorWithStatus ResourceExhausted error with details": {
			err:             tooBusyError,
			expectedCode:    codes.ResourceExhausted,
			expectedMessage: tooBusyErrorMsg,
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TOO_BUSY},
		},
		"a wrapped tooBusyError gets translated into an errorWithStatus ResourceExhausted error with details": {
			err:             fmt.Errorf("wrapped: %w", tooBusyError),
			expectedCode:    codes.ResourceExhausted,
			expectedMessage: fmt.Sprintf("wrapped: %s", tooBusyErrorMsg),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.TOO_BUSY},
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
		"an unavailableError gets translated into an errorWithStatus Unavailable error with details": {
			err:                 newUnavailableError(services.Stopping),
			expectedTranslation: newErrorWithStatus(newUnavailableError(services.Stopping), codes.Unavailable),
		},
		"a wrapped unavailableError gets translated into an errorWithStatus Unavailable error": {
			err:                 fmt.Errorf("wrapped: %w", newUnavailableError(services.Stopping)),
			expectedTranslation: newErrorWithStatus(fmt.Errorf("wrapped: %w", newUnavailableError(services.Stopping)), codes.Unavailable),
		},
		"tooBusyError gets translated into an errorWithHTTPStatus with status code 503": {
			err:                 tooBusyError,
			expectedTranslation: newErrorWithHTTPStatus(tooBusyError, http.StatusServiceUnavailable),
		},
		"a wrapped tooBusyError gets translated into an errorWithStatus with status code 503": {
			err:                 fmt.Errorf("wrapped: %w", tooBusyError),
			expectedTranslation: newErrorWithHTTPStatus(fmt.Errorf("wrapped: %w", tooBusyError), http.StatusServiceUnavailable),
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			handledErr := mapReadErrorToErrorWithHTTPOrGRPCStatus(tc.err)
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
