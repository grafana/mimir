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
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/log"
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
	checkIngesterError(t, err, unavailable, false)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &unavailableError{})
	checkIngesterError(t, wrappedErr, unavailable, false)
}

func TestInstanceLimitReachedError(t *testing.T) {
	limitErrorMessage := "this is a limit error message"
	err := newInstanceLimitReachedError(limitErrorMessage)
	require.Error(t, err)
	require.EqualError(t, err, limitErrorMessage)
	checkIngesterError(t, err, instanceLimitReached, false)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &instanceLimitReachedError{})
	checkIngesterError(t, wrappedErr, instanceLimitReached, false)
}

func TestNewTSDBUnavailableError(t *testing.T) {
	tsdbErrMsg := "TSDB Head forced compaction in progress and no write request is currently allowed"
	err := newTSDBUnavailableError(tsdbErrMsg)
	require.Error(t, err)
	require.EqualError(t, err, tsdbErrMsg)
	checkIngesterError(t, err, tsdbUnavailable, false)

	wrappedErr := fmt.Errorf("wrapped: %w", err)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &tsdbUnavailableError{})

	wrappedWithUserErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedWithUserErr, err)
	require.ErrorAs(t, wrappedWithUserErr, &tsdbUnavailableError{})
	checkIngesterError(t, wrappedErr, tsdbUnavailable, false)
}

func TestNewPerUserSeriesLimitError(t *testing.T) {
	limit := 100
	err := newPerUserSeriesLimitReachedError(limit)
	expectedErrMsg := globalerror.MaxSeriesPerUser.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("per-user series limit of %d exceeded", limit),
		validation.MaxSeriesPerUserFlag,
	)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, badData, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perUserSeriesLimitReachedError{})
	checkIngesterError(t, wrappedErr, badData, true)
}

func TestNewPerUserMetadataLimitError(t *testing.T) {
	limit := 100
	err := newPerUserMetadataLimitReachedError(limit)
	expectedErrMsg := globalerror.MaxMetadataPerUser.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("per-user metric metadata limit of %d exceeded", limit),
		validation.MaxMetadataPerUserFlag,
	)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, badData, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perUserMetadataLimitReachedError{})
	checkIngesterError(t, wrappedErr, badData, true)
}

func TestNewPerMetricSeriesLimitError(t *testing.T) {
	limit := 100
	labels := mimirpb.FromLabelAdaptersToLabels(
		[]mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}},
	)
	err := newPerMetricSeriesLimitReachedError(limit, labels)
	expectedErrMsg := fmt.Sprintf("%s This is for series %s",
		globalerror.MaxSeriesPerMetric.MessageWithPerTenantLimitConfig(
			fmt.Sprintf("per-metric series limit of %d exceeded", limit),
			validation.MaxSeriesPerMetricFlag,
		),
		labels.String(),
	)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, badData, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perMetricSeriesLimitReachedError{})
	checkIngesterError(t, wrappedErr, badData, true)
}

func TestNewPerMetricMetadataLimitError(t *testing.T) {
	limit := 100
	labels := mimirpb.FromLabelAdaptersToLabels(
		[]mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}},
	)
	err := newPerMetricMetadataLimitReachedError(limit, labels)
	expectedErrMsg := fmt.Sprintf("%s This is for series %s",
		globalerror.MaxMetadataPerMetric.MessageWithPerTenantLimitConfig(
			fmt.Sprintf("per-metric metadata limit of %d exceeded", limit),
			validation.MaxMetadataPerMetricFlag,
		),
		labels.String(),
	)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, badData, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &perMetricMetadataLimitReachedError{})
	checkIngesterError(t, wrappedErr, badData, true)
}

func TestNewSampleError(t *testing.T) {
	seriesLabels := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	tests := map[string]struct {
		err         error
		expectedMsg string
	}{
		"newSampleTimestampTooOldError": {
			err:         newSampleTimestampTooOldError(timestamp, seriesLabels),
			expectedMsg: `the sample has been rejected because its timestamp is too old (err-mimir-sample-timestamp-too-old). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series {__name__="test"}`,
		},
		"newSampleTimestampTooOldOOOEnabledError": {
			err:         newSampleTimestampTooOldOOOEnabledError(timestamp, seriesLabels, 2*time.Hour),
			expectedMsg: `the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window of 2h (err-mimir-sample-timestamp-too-old). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series {__name__="test"}`,
		},
		"newSampleTimestampTooFarInFutureError": {
			err:         newSampleTimestampTooFarInFutureError(timestamp, seriesLabels),
			expectedMsg: `received a sample whose timestamp is too far in the future (err-mimir-too-far-in-future). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series {__name__="test"}`,
		},
		"newSampleOutOfOrderError": {
			err:         newSampleOutOfOrderError(timestamp, seriesLabels),
			expectedMsg: `the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed (err-mimir-sample-out-of-order). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series {__name__="test"}`,
		},
		"newSampleDuplicateTimestampError": {
			err:         newSampleDuplicateTimestampError(timestamp, seriesLabels),
			expectedMsg: `the sample has been rejected because another sample with the same timestamp, but a different value, has already been ingested (err-mimir-sample-duplicate-timestamp). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series {__name__="test"}`,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, tc.expectedMsg, tc.err.Error())
			checkIngesterError(t, tc.err, badData, true)

			wrappedErr := wrapOrAnnotateWithUser(tc.err, userID)
			require.ErrorIs(t, wrappedErr, tc.err)
			var sampleErr sampleError
			require.ErrorAs(t, wrappedErr, &sampleErr)
			checkIngesterError(t, wrappedErr, badData, true)
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
			expectedMsg: `the exemplar has been rejected because the related series has not been ingested yet (err-mimir-exemplar-series-missing). The affected exemplar is {traceID="123"} with timestamp 1970-01-19T05:30:43.969Z for series {__name__="test"}`,
		},
		"newExemplarTimestampTooFarInFutureError": {
			err:         newExemplarTimestampTooFarInFutureError(timestamp, seriesLabels, exemplarsLabels),
			expectedMsg: `received an exemplar whose timestamp is too far in the future (err-mimir-exemplar-too-far-in-future). The affected exemplar is {traceID="123"} with timestamp 1970-01-19T05:30:43.969Z for series {__name__="test"}`,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, tc.expectedMsg, tc.err.Error())
			checkIngesterError(t, tc.err, badData, true)

			wrappedErr := wrapOrAnnotateWithUser(tc.err, userID)
			require.ErrorIs(t, wrappedErr, tc.err)
			var exemplarErr exemplarError
			require.ErrorAs(t, wrappedErr, &exemplarErr)
			checkIngesterError(t, wrappedErr, badData, true)
		})
	}
}

func TestNewTSDBIngestExemplarErr(t *testing.T) {
	seriesLabels := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	exemplarsLabels := []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}}
	anotherErr := errors.New("another error")
	err := newTSDBIngestExemplarErr(anotherErr, timestamp, seriesLabels, exemplarsLabels)
	expectedErrMsg := fmt.Sprintf("err: %v. timestamp=1970-01-19T05:30:43.969Z, series={__name__=\"test\"}, exemplar={traceID=\"123\"}", anotherErr)
	require.Equal(t, expectedErrMsg, err.Error())
	checkIngesterError(t, err, badData, true)

	wrappedErr := wrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
	require.ErrorAs(t, wrappedErr, &tsdbIngestExemplarErr{})
	checkIngesterError(t, wrappedErr, badData, true)
}

func TestErrorWithStatus(t *testing.T) {
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	err := newSampleTimestampTooOldError(timestamp, metricLabelAdapters)
	errWithStatus := newErrorWithStatus(err, codes.Unavailable)
	require.Error(t, errWithStatus)
	// Ensure gogo's status.FromError recognizes errWithStatus.
	stat, ok := status.FromError(errWithStatus)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, stat.Code())
	require.Errorf(t, err, stat.Message())
	require.Empty(t, stat.Details())

	// Ensure grpc's status.FromError recognizes errWithStatus.
	st, ok := grpcstatus.FromError(errWithStatus)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())
	require.Errorf(t, err, st.Message())

	// Ensure httpgrpc's HTTPResponseFromError does not recognize errWithStatuserrWithStatus.
	resp, ok := httpgrpc.HTTPResponseFromError(errWithStatus)
	require.False(t, ok)
	require.Nil(t, resp)
}

func TestErrorWithHTTPStatus(t *testing.T) {
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	err := newSampleTimestampTooOldError(timestamp, metricLabelAdapters)
	errWithHTTPStatus := newErrorWithHTTPStatus(err, http.StatusBadRequest)
	require.Error(t, errWithHTTPStatus)
	// Ensure gogo's status.FromError recognizes errWithStatuserrWithHTTPStatus.
	stat, ok := status.FromError(errWithHTTPStatus)
	require.True(t, ok)
	require.Equal(t, http.StatusBadRequest, int(stat.Code()))
	require.Errorf(t, err, stat.Message())
	require.NotEmpty(t, stat.Details())

	// Ensure grpc's status.FromError recognizes errWithHTTPStatus.
	st, ok := grpcstatus.FromError(errWithHTTPStatus)
	require.True(t, ok)
	require.Equal(t, http.StatusBadRequest, int(st.Code()))
	require.Errorf(t, err, st.Message())

	// Ensure httpgrpc's HTTPResponseFromError recognizes errWithStatuserrWithHTTPStatus.
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

func checkIngesterError(t *testing.T, err error, expectedType ingesterErrorType, isSoft bool) {
	var ingesterErr ingesterError
	require.ErrorAs(t, err, &ingesterErr)
	require.Equal(t, expectedType, ingesterErr.errorType())

	if isSoft {
		var softErr softError
		require.ErrorAs(t, err, &softErr)
	}
}

func TestHandlePushError(t *testing.T) {
	originalMsg := "this is an error"
	originalErr := errors.New(originalMsg)
	labelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
	labels := mimirpb.FromLabelAdaptersToLabels(labelAdapters)

	timestamp := model.Time(1)
	testCases := []struct {
		name                string
		err                 error
		doNotLogExpected    bool
		expectedTranslation error
	}{
		{
			name:                "a generic error is not translated",
			err:                 originalErr,
			expectedTranslation: originalErr,
		},
		{
			name:                "a DoNotLog error of a generic error is not translated",
			err:                 log.DoNotLogError{Err: originalErr},
			expectedTranslation: log.DoNotLogError{Err: originalErr},
			doNotLogExpected:    true,
		},
		{
			name:                "an unavailableError gets translated into an errorWithStatus Unavailable error",
			err:                 newUnavailableError(services.Stopping),
			expectedTranslation: newErrorWithStatus(newUnavailableError(services.Stopping), codes.Unavailable),
		},
		{
			name: "a wrapped unavailableError gets translated into a non-loggable errorWithStatus Unavailable error",
			err:  fmt.Errorf("wrapped: %w", newUnavailableError(services.Stopping)),
			expectedTranslation: newErrorWithStatus(
				fmt.Errorf("wrapped: %w", newUnavailableError(services.Stopping)),
				codes.Unavailable,
			),
		},
		{
			name: "an instanceLimitReachedError gets translated into a non-loggable errorWithStatus Unavailable error",
			err:  newInstanceLimitReachedError("instance limit reached"),
			expectedTranslation: newErrorWithStatus(
				log.DoNotLogError{Err: newInstanceLimitReachedError("instance limit reached")},
				codes.Unavailable,
			),
			doNotLogExpected: true,
		},
		{
			name: "a wrapped instanceLimitReachedError gets translated into a non-loggable errorWithStatus Unavailable error",
			err:  fmt.Errorf("wrapped: %w", newInstanceLimitReachedError("instance limit reached")),
			expectedTranslation: newErrorWithStatus(
				log.DoNotLogError{Err: fmt.Errorf("wrapped: %w", newInstanceLimitReachedError("instance limit reached"))},
				codes.Unavailable,
			),
			doNotLogExpected: true,
		},
		{
			name: "a tsdbUnavailableError gets translated into an errorWithHTTPStatus 503 error",
			err:  newTSDBUnavailableError("tsdb stopping"),
			expectedTranslation: newErrorWithHTTPStatus(
				newTSDBUnavailableError("tsdb stopping"),
				http.StatusServiceUnavailable,
			),
		},
		{
			name: "a wrapped tsdbUnavailableError gets translated into an errorWithHTTPStatus 503 error",
			err:  fmt.Errorf("wrapped: %w", newTSDBUnavailableError("tsdb stopping")),
			expectedTranslation: newErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", newTSDBUnavailableError("tsdb stopping")),
				http.StatusServiceUnavailable,
			),
		},
		{
			name: "a sampleError gets translated into an errorWithHTTPStatus 400 error",
			err:  newSampleError("id", "sample error", timestamp, labelAdapters),
			expectedTranslation: newErrorWithHTTPStatus(
				newSampleError("id", "sample error", timestamp, labelAdapters),
				http.StatusBadRequest,
			),
		},
		{
			name: "a wrapped exemplarError gets translated into an errorWithHTTPStatus 400 error",
			err:  fmt.Errorf("wrapped: %w", newSampleError("id", "sample error", timestamp, labelAdapters)),
			expectedTranslation: newErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", newSampleError("id", "sample error", timestamp, labelAdapters)),
				http.StatusBadRequest,
			),
		},
		{
			name: "a exemplarError gets translated into an errorWithHTTPStatus 400 error",
			err:  newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters),
			expectedTranslation: newErrorWithHTTPStatus(
				newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters),
				http.StatusBadRequest,
			),
		},
		{
			name: "a wrapped exemplarError gets translated into an errorWithHTTPStatus 400 error",
			err:  fmt.Errorf("wrapped: %w", newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters)),
			expectedTranslation: newErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", newExemplarError("id", "exemplar error", timestamp, labelAdapters, labelAdapters)),
				http.StatusBadRequest,
			),
		},
		{
			name: "a perUserMetadataLimitReachedError gets translated into an errorWithHTTPStatus 400 error",
			err:  newPerUserSeriesLimitReachedError(10),
			expectedTranslation: newErrorWithHTTPStatus(
				newPerUserSeriesLimitReachedError(10),
				http.StatusBadRequest,
			),
		},
		{
			name: "a wrapped perUserMetadataLimitReachedError gets translated into an errorWithHTTPStatus 400 error",
			err:  fmt.Errorf("wrapped: %w", newPerUserSeriesLimitReachedError(10)),
			expectedTranslation: newErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", newPerUserSeriesLimitReachedError(10)),
				http.StatusBadRequest,
			),
		},
		{
			name: "a perMetricMetadataLimitReachedError gets translated into an errorWithHTTPStatus 400 error",
			err:  newPerMetricSeriesLimitReachedError(10, labels),
			expectedTranslation: newErrorWithHTTPStatus(
				newPerMetricSeriesLimitReachedError(10, labels),
				http.StatusBadRequest,
			),
		},
		{
			name: "a wrapped perMetricMetadataLimitReachedError gets translated into an errorWithHTTPStatus 400 error",
			err:  fmt.Errorf("wrapped: %w", newPerMetricSeriesLimitReachedError(10, labels)),
			expectedTranslation: newErrorWithHTTPStatus(
				fmt.Errorf("wrapped: %w", newPerMetricSeriesLimitReachedError(10, labels)),
				http.StatusBadRequest,
			),
		},
	}

	for _, tc := range testCases {
		handledErr := handlePushError(tc.err)
		require.Equal(t, tc.expectedTranslation, handledErr)
		if tc.doNotLogExpected {
			var doNotLogError log.DoNotLogError
			require.ErrorAs(t, handledErr, &doNotLogError)
			require.False(t, doNotLogError.ShouldLog(context.Background(), 0))
		}
	}
}
