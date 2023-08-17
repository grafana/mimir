// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/errors.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

// ValidationError is an error returned by series validation.
//
//nolint:revive // ignore stutter warning
type ValidationError error

// genericValidationError is a basic implementation of ValidationError which can be used when the
// error format only contains the cause and the series.
type genericValidationError struct {
	message string
	cause   string
	series  []mimirpb.LabelAdapter
}

func (e genericValidationError) Error() string {
	return fmt.Sprintf(e.message, e.cause, formatLabelSet(e.series))
}

var labelNameTooLongMsgFormat = globalerror.SeriesLabelNameTooLong.MessageWithPerTenantLimitConfig(
	"received a series whose label name length exceeds the limit, label: '%.200s' series: '%.200s'",
	maxLabelNameLengthFlag)

func newLabelNameTooLongError(series []mimirpb.LabelAdapter, labelName string) ValidationError {
	return genericValidationError{
		message: labelNameTooLongMsgFormat,
		cause:   labelName,
		series:  series,
	}
}

// labelValueTooLongError is a customized ValidationError, in that the cause and the series are
// are formatted in different order in Error.
type labelValueTooLongError struct {
	labelValue string
	series     []mimirpb.LabelAdapter
}

func (e labelValueTooLongError) Error() string {
	return globalerror.SeriesLabelValueTooLong.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("received a series whose label value length exceeds the limit, value: '%.200s' (truncated) series: '%.200s'", e.labelValue, formatLabelSet(e.series)),
		maxLabelValueLengthFlag)
}

func newLabelValueTooLongError(series []mimirpb.LabelAdapter, labelValue string) ValidationError {
	return labelValueTooLongError{
		labelValue: labelValue,
		series:     series,
	}
}

var invalidLabelMsgFormat = globalerror.SeriesInvalidLabel.Message(
	"received a series with an invalid label: '%.200s' series: '%.200s'")

func newInvalidLabelError(series []mimirpb.LabelAdapter, labelName string) ValidationError {
	return genericValidationError{
		message: invalidLabelMsgFormat,
		cause:   labelName,
		series:  series,
	}
}

var duplicateLabelMsgFormat = globalerror.SeriesWithDuplicateLabelNames.Message(
	"received a series with duplicate label name, label: '%.200s' series: '%.200s'")

func newDuplicatedLabelError(series []mimirpb.LabelAdapter, labelName string) ValidationError {
	return genericValidationError{
		message: duplicateLabelMsgFormat,
		cause:   labelName,
		series:  series,
	}
}

type tooManyLabelsError struct {
	series []mimirpb.LabelAdapter
	limit  int
}

func newTooManyLabelsError(series []mimirpb.LabelAdapter, limit int) ValidationError {
	return tooManyLabelsError{
		series: series,
		limit:  limit,
	}
}

func (e tooManyLabelsError) Error() string {
	metric := mimirpb.FromLabelAdaptersToMetric(e.series).String()
	ellipsis := ""

	if utf8.RuneCountInString(metric) > 200 {
		ellipsis = "\u2026"
	}

	return globalerror.MaxLabelNamesPerSeries.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("received a series whose number of labels exceeds the limit (actual: %d, limit: %d) series: '%.200s%s'", len(e.series), e.limit, metric, ellipsis),
		maxLabelNamesPerSeriesFlag)
}

type noMetricNameError struct{}

func newNoMetricNameError() ValidationError {
	return noMetricNameError{}
}

func (e noMetricNameError) Error() string {
	return globalerror.MissingMetricName.Message("received series has no metric name")
}

type invalidMetricNameError struct {
	metricName string
}

func newInvalidMetricNameError(metricName string) ValidationError {
	return invalidMetricNameError{
		metricName: metricName,
	}
}

func (e invalidMetricNameError) Error() string {
	return globalerror.InvalidMetricName.Message(fmt.Sprintf("received a series with invalid metric name: '%.200s'", e.metricName))
}

// sampleValidationError is a ValidationError implementation suitable for sample validation errors.
type sampleValidationError struct {
	message    string
	metricName string
	timestamp  int64
}

func (e sampleValidationError) Error() string {
	return fmt.Sprintf(e.message, e.timestamp, e.metricName)
}

type maxNativeHistogramBucketsError struct {
	seriesLabels []mimirpb.LabelAdapter
	timestamp    int64
	bucketCount  int
	bucketLimit  int
}

func newMaxNativeHistogramBucketsError(seriesLabels []mimirpb.LabelAdapter, timestamp int64, bucketCount, bucketLimit int) maxNativeHistogramBucketsError {
	return maxNativeHistogramBucketsError{
		seriesLabels: seriesLabels,
		timestamp:    timestamp,
		bucketCount:  bucketCount,
		bucketLimit:  bucketLimit,
	}
}

func (e maxNativeHistogramBucketsError) Error() string {
	return fmt.Sprintf("received a native histogram sample with too many buckets, timestamp: %d series: %s, buckets: %d, limit: %d (%s)",
		e.timestamp, mimirpb.FromLabelAdaptersToLabels(e.seriesLabels).String(), e.bucketCount, e.bucketLimit, globalerror.MaxNativeHistogramBuckets)
}

var sampleTimestampTooNewMsgFormat = globalerror.SampleTooFarInFuture.MessageWithPerTenantLimitConfig(
	"received a sample whose timestamp is too far in the future, timestamp: %d series: '%.200s'",
	creationGracePeriodFlag)

func newSampleTimestampTooNewError(metricName string, timestamp int64) ValidationError {
	return sampleValidationError{
		message:    sampleTimestampTooNewMsgFormat,
		metricName: metricName,
		timestamp:  timestamp,
	}
}

// exemplarValidationError is a ValidationError implementation suitable for exemplar validation errors.
type exemplarValidationError struct {
	message        string
	seriesLabels   []mimirpb.LabelAdapter
	exemplarLabels []mimirpb.LabelAdapter
	timestamp      int64
}

func (e exemplarValidationError) Error() string {
	return fmt.Sprintf(e.message, e.timestamp, mimirpb.FromLabelAdaptersToLabels(e.seriesLabels).String(), mimirpb.FromLabelAdaptersToLabels(e.exemplarLabels).String())
}

var exemplarEmptyLabelsMsgFormat = globalerror.ExemplarLabelsMissing.Message(
	"received an exemplar with no valid labels, timestamp: %d series: %s labels: %s")

func newExemplarEmptyLabelsError(seriesLabels []mimirpb.LabelAdapter, exemplarLabels []mimirpb.LabelAdapter, timestamp int64) ValidationError {
	return exemplarValidationError{
		message:        exemplarEmptyLabelsMsgFormat,
		seriesLabels:   seriesLabels,
		exemplarLabels: exemplarLabels,
		timestamp:      timestamp,
	}
}

var exemplarMissingTimestampMsgFormat = globalerror.ExemplarTimestampInvalid.Message(
	"received an exemplar with no timestamp, timestamp: %d series: %s labels: %s")

func newExemplarMissingTimestampError(seriesLabels []mimirpb.LabelAdapter, exemplarLabels []mimirpb.LabelAdapter, timestamp int64) ValidationError {
	return exemplarValidationError{
		message:        exemplarMissingTimestampMsgFormat,
		seriesLabels:   seriesLabels,
		exemplarLabels: exemplarLabels,
		timestamp:      timestamp,
	}
}

var exemplarMaxLabelLengthMsgFormat = globalerror.ExemplarLabelsTooLong.Message(
	fmt.Sprintf("received an exemplar where the size of its combined labels exceeds the limit of %d characters, timestamp: %%d series: %%s labels: %%s", ExemplarMaxLabelSetLength))

func newExemplarMaxLabelLengthError(seriesLabels []mimirpb.LabelAdapter, exemplarLabels []mimirpb.LabelAdapter, timestamp int64) ValidationError {
	return exemplarValidationError{
		message:        exemplarMaxLabelLengthMsgFormat,
		seriesLabels:   seriesLabels,
		exemplarLabels: exemplarLabels,
		timestamp:      timestamp,
	}
}

type metadataMetricNameMissingError struct{}

func newMetadataMetricNameMissingError() ValidationError {
	return metadataMetricNameMissingError{}
}

func (e metadataMetricNameMissingError) Error() string {
	return globalerror.MetricMetadataMissingMetricName.Message("received a metric metadata with no metric name")
}

// metadataValidationError is a ValidationError implementation suitable for metadata validation errors.
type metadataValidationError struct {
	message    string
	cause      string
	metricName string
}

func (e metadataValidationError) Error() string {
	return fmt.Sprintf(e.message, e.cause, e.metricName)
}

var metadataMetricNameTooLongMsgFormat = globalerror.MetricMetadataMetricNameTooLong.MessageWithPerTenantLimitConfig(
	// When formatting this error the "cause" will always be an empty string.
	"received a metric metadata whose metric name length exceeds the limit, metric name: '%.200[2]s'",
	maxMetadataLengthFlag)

func newMetadataMetricNameTooLongError(metadata *mimirpb.MetricMetadata) ValidationError {
	return metadataValidationError{
		message:    metadataMetricNameTooLongMsgFormat,
		cause:      "",
		metricName: metadata.GetMetricFamilyName(),
	}
}

var metadataUnitTooLongMsgFormat = globalerror.MetricMetadataUnitTooLong.MessageWithPerTenantLimitConfig(
	"received a metric metadata whose unit name length exceeds the limit, unit: '%.200s' metric name: '%.200s'",
	maxMetadataLengthFlag)

func newMetadataUnitTooLongError(metadata *mimirpb.MetricMetadata) ValidationError {
	return metadataValidationError{
		message:    metadataUnitTooLongMsgFormat,
		cause:      metadata.GetUnit(),
		metricName: metadata.GetMetricFamilyName(),
	}
}

func NewMaxQueryLengthError(actualQueryLen, maxQueryLength time.Duration) LimitError {
	return LimitError(globalerror.MaxQueryLength.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the query time range exceeds the limit (query length: %s, limit: %s)", actualQueryLen, maxQueryLength),
		maxPartialQueryLengthFlag))
}

func NewMaxTotalQueryLengthError(actualQueryLen, maxTotalQueryLength time.Duration) LimitError {
	return LimitError(globalerror.MaxTotalQueryLength.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the total query time range exceeds the limit (query length: %s, limit: %s)", actualQueryLen, maxTotalQueryLength),
		maxTotalQueryLengthFlag))
}

func NewMaxQueryExpressionSizeBytesError(actualSizeBytes, maxQuerySizeBytes int) LimitError {
	return LimitError(globalerror.MaxQueryExpressionSizeBytes.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the raw query size in bytes exceeds the limit (query size: %d, limit: %d)", actualSizeBytes, maxQuerySizeBytes),
		maxQueryExpressionSizeBytesFlag))
}

func NewRequestRateLimitedError(limit float64, burst int) LimitError {
	return LimitError(globalerror.RequestRateLimited.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the request has been rejected because the tenant exceeded the request rate limit, set to %v requests/s across all distributors with a maximum allowed burst of %d", limit, burst),
		requestRateFlag, requestBurstSizeFlag))
}

func NewIngestionRateLimitedError(limit float64, burst int) LimitError {
	return LimitError(globalerror.IngestionRateLimited.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the request has been rejected because the tenant exceeded the ingestion rate limit, set to %v items/s with a maximum allowed burst of %d. This limit is applied on the total number of samples, exemplars and metadata received across all distributors", limit, burst),
		ingestionRateFlag, ingestionBurstSizeFlag))
}

// formatLabelSet formats label adapters as a metric name with labels, while preserving
// label order, and keeping duplicates. If there are multiple "__name__" labels, only
// first one is used as metric name, other ones will be included as regular labels.
func formatLabelSet(ls []mimirpb.LabelAdapter) string {
	metricName, hasMetricName := "", false

	labelStrings := make([]string, 0, len(ls))
	for _, l := range ls {
		if l.Name == model.MetricNameLabel && !hasMetricName && l.Value != "" {
			metricName = l.Value
			hasMetricName = true
		} else {
			labelStrings = append(labelStrings, fmt.Sprintf("%s=%q", l.Name, l.Value))
		}
	}

	if len(labelStrings) == 0 {
		if hasMetricName {
			return metricName
		}
		return "{}"
	}

	return fmt.Sprintf("%s{%s}", metricName, strings.Join(labelStrings, ", "))
}
