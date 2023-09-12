// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/errors.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"fmt"
	"net/http"
	"time"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	// This is the closest fitting Prometheus API error code for requests rejected due to limiting.
	tooBusyError = httpgrpc.Errorf(http.StatusServiceUnavailable,
		"the ingester is currently too busy to process queries, try again later")
)

type safeToWrap interface {
	safeToWrap()
}

type safeToWrapError string

func (s safeToWrapError) safeToWrap() {}

func (s safeToWrapError) Error() string {
	return string(s)
}

// errorWithStatus is used for wrapping errors returned by ingester.
type errorWithStatus struct {
	err    error // underlying error
	status *status.Status
}

func newErrorWithStatus(err error, code int) errorWithStatus {
	return errorWithStatus{
		err:    err,
		status: status.New(codes.Code(code), err.Error()),
	}
}

func (e errorWithStatus) Error() string {
	return e.status.String()
}

func (e errorWithStatus) Unwrap() error {
	return e.err
}

func (e errorWithStatus) GRPCStatus() *status.Status {
	return e.status
}

// annotateWithUser prepends the user to the error. It does not retain a reference to err.
func annotateWithUser(err error, userID string) error {
	return fmt.Errorf("user=%s: %s", userID, err)
}

// wrapWithUser prepends the user to the error. It retains a reference to err.
func wrapWithUser(err error, userID string) error {
	return fmt.Errorf("user=%s: %w", userID, err)
}

func newIngestErrSample(errID globalerror.ID, errMsg string, timestamp model.Time, labels []mimirpb.LabelAdapter) error {
	return safeToWrapError(
		fmt.Sprintf("%v. The affected sample has timestamp %s and is from series %s",
			errID.Message(errMsg),
			timestamp.Time().UTC().Format(time.RFC3339Nano),
			mimirpb.FromLabelAdaptersToLabels(labels).String(),
		),
	)
}

func newIngestErrSampleTimestampTooOld(timestamp model.Time, labels []mimirpb.LabelAdapter) error {
	return newIngestErrSample(globalerror.SampleTimestampTooOld, "the sample has been rejected because its timestamp is too old", timestamp, labels)
}

func newIngestErrSampleTimestampTooOldOOOEnabled(timestamp model.Time, labels []mimirpb.LabelAdapter, oooTimeWindow time.Duration) error {
	return newIngestErrSample(globalerror.SampleTimestampTooOld, fmt.Sprintf("the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window of %s", model.Duration(oooTimeWindow).String()), timestamp, labels)
}

func newIngestErrSampleTimestampTooFarInFuture(timestamp model.Time, labels []mimirpb.LabelAdapter) error {
	return newIngestErrSample(globalerror.SampleTooFarInFuture, "received a sample whose timestamp is too far in the future", timestamp, labels)
}

func newIngestErrSampleOutOfOrder(timestamp model.Time, labels []mimirpb.LabelAdapter) error {
	return newIngestErrSample(globalerror.SampleOutOfOrder, "the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed", timestamp, labels)
}

func newIngestErrSampleDuplicateTimestamp(timestamp model.Time, labels []mimirpb.LabelAdapter) error {
	return newIngestErrSample(globalerror.SampleDuplicateTimestamp, "the sample has been rejected because another sample with the same timestamp, but a different value, has already been ingested", timestamp, labels)
}

func newIngestErrExemplar(errID globalerror.ID, errMsg string, timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) error {
	return safeToWrapError(
		fmt.Sprintf("%v. The affected exemplar is %s with timestamp %s for series %s",
			errID.Message(errMsg),
			mimirpb.FromLabelAdaptersToLabels(exemplarLabels).String(),
			timestamp.Time().UTC().Format(time.RFC3339Nano),
			mimirpb.FromLabelAdaptersToLabels(seriesLabels).String(),
		),
	)
}

func newIngestErrExemplarMissingSeries(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) error {
	return newIngestErrExemplar(globalerror.ExemplarSeriesMissing, "the exemplar has been rejected because the related series has not been ingested yet", timestamp, seriesLabels, exemplarLabels)
}

func newIngestErrExemplarTimestampTooFarInFuture(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) error {
	return newIngestErrExemplar(globalerror.ExemplarTooFarInFuture, "received an exemplar whose timestamp is too far in the future", timestamp, seriesLabels, exemplarLabels)
}

func formatMaxSeriesPerUserError(limits *validation.Overrides, userID string) error {
	globalLimit := limits.MaxGlobalSeriesPerUser(userID)
	return safeToWrapError(
		globalerror.MaxSeriesPerUser.MessageWithPerTenantLimitConfig(
			fmt.Sprintf("per-user series limit of %d exceeded", globalLimit),
			validation.MaxSeriesPerUserFlag,
		),
	)
}

func formatMaxSeriesPerMetricError(limits *validation.Overrides, labels labels.Labels, userID string) error {
	globalLimit := limits.MaxGlobalSeriesPerMetric(userID)
	return safeToWrapError(
		fmt.Sprintf("%s This is for series %s",
			globalerror.MaxSeriesPerMetric.MessageWithPerTenantLimitConfig(
				fmt.Sprintf("per-metric series limit of %d exceeded", globalLimit),
				validation.MaxSeriesPerMetricFlag,
			),
			labels.String(),
		),
	)
}

func formatMaxMetadataPerUserError(limits *validation.Overrides, userID string) error {
	globalLimit := limits.MaxGlobalMetricsWithMetadataPerUser(userID)
	return safeToWrapError(
		globalerror.MaxMetadataPerUser.MessageWithPerTenantLimitConfig(
			fmt.Sprintf("per-user metric metadata limit of %d exceeded", globalLimit),
			validation.MaxMetadataPerUserFlag,
		),
	)
}

func formatMaxMetadataPerMetricError(limits *validation.Overrides, labels labels.Labels, userID string) error {
	globalLimit := limits.MaxGlobalMetadataPerMetric(userID)
	return safeToWrapError(
		fmt.Sprintf("%s This is for series %s",
			globalerror.MaxMetadataPerMetric.MessageWithPerTenantLimitConfig(
				fmt.Sprintf("per-metric metadata limit of %d exceeded", globalLimit),
				validation.MaxMetadataPerMetricFlag,
			),
			labels.String(),
		),
	)
}

type ingesterErrSamplers struct {
	sampleTimestampTooOld             *log.Sampler
	sampleTimestampTooOldOOOEnabled   *log.Sampler
	sampleTimestampTooFarInFuture     *log.Sampler
	sampleOutOfOrder                  *log.Sampler
	sampleDuplicateTimestamp          *log.Sampler
	maxSeriesPerMetricLimitExceeded   *log.Sampler
	maxMetadataPerMetricLimitExceeded *log.Sampler
	maxSeriesPerUserLimitExceeded     *log.Sampler
	maxMetadataPerUserLimitExceeded   *log.Sampler
}

func newIngesterErrSamplers(freq int64) ingesterErrSamplers {
	return ingesterErrSamplers{
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
		log.NewSampler(freq),
	}
}
