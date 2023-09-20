// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	timestamp = model.Time(1575043969)
)

func TestNewIngestErrMsgs(t *testing.T) {
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	tests := map[string]struct {
		err error
		msg string
	}{
		"newIngestErrSampleTimestampTooOld": {
			err: newIngestErrSampleTimestampTooOld(timestamp, metricLabelAdapters),
			msg: `the sample has been rejected because its timestamp is too old (err-mimir-sample-timestamp-too-old). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series {__name__="test"}`,
		},
		"newIngestErrSampleTimestampTooOld_out_of_order_enabled": {
			err: newIngestErrSampleTimestampTooOldOOOEnabled(timestamp, metricLabelAdapters, 2*time.Hour),
			msg: `the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window of 2h (err-mimir-sample-timestamp-too-old). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series {__name__="test"}`,
		},
		"newIngestErrSampleOutOfOrder": {
			err: newIngestErrSampleOutOfOrder(timestamp, metricLabelAdapters),
			msg: `the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed (err-mimir-sample-out-of-order). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series {__name__="test"}`,
		},
		"newIngestErrSampleDuplicateTimestamp": {
			err: newIngestErrSampleDuplicateTimestamp(timestamp, metricLabelAdapters),
			msg: `the sample has been rejected because another sample with the same timestamp, but a different value, has already been ingested (err-mimir-sample-duplicate-timestamp). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series {__name__="test"}`,
		},
		"newIngestErrExemplarMissingSeries": {
			err: newIngestErrExemplarMissingSeries(timestamp, metricLabelAdapters, []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}}),
			msg: `the exemplar has been rejected because the related series has not been ingested yet (err-mimir-exemplar-series-missing). The affected exemplar is {traceID="123"} with timestamp 1970-01-19T05:30:43.969Z for series {__name__="test"}`,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, tc.msg, tc.err.Error())
			var safe safeToWrap
			assert.ErrorAs(t, tc.err, &safe)
		})
	}
}

func TestSafeToWrapError(t *testing.T) {
	err := safeToWrapError("this is a safe to wrap error")
	require.Error(t, err)
	var safe safeToWrap
	require.ErrorAs(t, err, &safe)
}

func TestErrorWithStatus(t *testing.T) {
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	err := newIngestErrSampleTimestampTooOld(timestamp, metricLabelAdapters)
	errWithStatus := newErrorWithStatus(err, codes.Unavailable)
	require.Error(t, errWithStatus)
	stat, ok := status.FromError(errWithStatus)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, stat.Code())
	require.Errorf(t, err, stat.Message())
	require.Empty(t, stat.Details())
}

func TestErrorWithHTTPStatus(t *testing.T) {
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	err := newIngestErrSampleTimestampTooOld(timestamp, metricLabelAdapters)
	errWithHTTPStatus := newErrorWithHTTPStatus(err, http.StatusBadRequest)
	require.Error(t, errWithHTTPStatus)
	stat, ok := status.FromError(errWithHTTPStatus)
	require.True(t, ok)
	require.Equal(t, http.StatusBadRequest, int(stat.Code()))
	require.Errorf(t, err, stat.Message())
	require.NotEmpty(t, stat.Details())
	resp, ok := httpgrpc.HTTPResponseFromError(errWithHTTPStatus)
	require.True(t, ok)
	require.Equal(t, int32(http.StatusBadRequest), resp.Code)
	require.Errorf(t, err, errWithHTTPStatus.Error())
}

func TestWrapOrAnnotateWithUser(t *testing.T) {
	userID := "1"
	unsafeErr := errors.New("this is an unsafe error")
	safeErr := safeToWrapError("this is a safe error")

	annotatedUnsafeErr := wrapOrAnnotateWithUser(unsafeErr, userID)
	require.Error(t, annotatedUnsafeErr)
	require.NotErrorIs(t, annotatedUnsafeErr, unsafeErr)
	require.Nil(t, errors.Unwrap(annotatedUnsafeErr))

	wrappedSafeErr := wrapOrAnnotateWithUser(safeErr, userID)
	require.Error(t, wrappedSafeErr)
	require.ErrorIs(t, wrappedSafeErr, safeErr)
}
