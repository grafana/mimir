// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/errors.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/distributor/distributorerror"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

func genericValidationError(format string, cause string, series []mimirpb.LabelAdapter) error {
	return fmt.Errorf(format, cause, formatLabelSet(series))
}

func newLabelNameTooLongError(series []mimirpb.LabelAdapter, labelName string) distributorerror.Validation {
	labelNameTooLongMsgFormat := globalerror.SeriesLabelNameTooLong.MessageWithPerTenantLimitConfig(
		"received a series whose label name length exceeds the limit, label: '%.200s' series: '%.200s'",
		maxLabelNameLengthFlag,
	)
	return distributorerror.NewValidationError(genericValidationError(labelNameTooLongMsgFormat, labelName, series))
}

func newLabelValueTooLongError(series []mimirpb.LabelAdapter, labelValue string) distributorerror.Validation {
	return distributorerror.NewValidationError(
		errors.New(
			globalerror.SeriesLabelValueTooLong.MessageWithPerTenantLimitConfig(
				fmt.Sprintf("received a series whose label value length exceeds the limit, value: '%.200s' (truncated) series: '%.200s'", labelValue, formatLabelSet(series)),
				maxLabelValueLengthFlag,
			),
		),
	)
}

func newInvalidLabelError(series []mimirpb.LabelAdapter, labelName string) distributorerror.Validation {
	invalidLabelMsgFormat := globalerror.SeriesInvalidLabel.Message("received a series with an invalid label: '%.200s' series: '%.200s'")
	return distributorerror.NewValidationError(genericValidationError(invalidLabelMsgFormat, labelName, series))
}

func newDuplicatedLabelError(series []mimirpb.LabelAdapter, labelName string) distributorerror.Validation {
	duplicateLabelMsgFormat := globalerror.SeriesWithDuplicateLabelNames.Message("received a series with duplicate label name, label: '%.200s' series: '%.200s'")
	return distributorerror.NewValidationError(genericValidationError(duplicateLabelMsgFormat, labelName, series))
}

func newTooManyLabelsError(series []mimirpb.LabelAdapter, limit int) distributorerror.Validation {
	metric := mimirpb.FromLabelAdaptersToMetric(series).String()
	ellipsis := ""

	if utf8.RuneCountInString(metric) > 200 {
		ellipsis = "\u2026"
	}

	return distributorerror.NewValidationError(
		errors.New(
			globalerror.MaxLabelNamesPerSeries.MessageWithPerTenantLimitConfig(
				fmt.Sprintf("received a series whose number of labels exceeds the limit (actual: %d, limit: %d) series: '%.200s%s'", len(series), limit, metric, ellipsis),
				maxLabelNamesPerSeriesFlag,
			),
		),
	)
}

func newNoMetricNameError() distributorerror.Validation {
	return distributorerror.NewValidationError(
		errors.New(
			globalerror.MissingMetricName.Message("received series has no metric name"),
		),
	)
}

func newInvalidMetricNameError(metricName string) distributorerror.Validation {
	return distributorerror.NewValidationError(
		errors.New(
			globalerror.InvalidMetricName.Message(fmt.Sprintf("received a series with invalid metric name: '%.200s'", metricName)),
		),
	)
}

func newMaxNativeHistogramBucketsError(seriesLabels []mimirpb.LabelAdapter, timestamp int64, bucketCount, bucketLimit int) distributorerror.Validation {
	return distributorerror.NewValidationError(
		fmt.Errorf("received a native histogram sample with too many buckets, timestamp: %d series: %s, buckets: %d, limit: %d (%s)",
			timestamp,
			mimirpb.FromLabelAdaptersToLabels(seriesLabels).String(),
			bucketCount,
			bucketLimit,
			globalerror.MaxNativeHistogramBuckets,
		),
	)
}

func newSampleTimestampTooNewError(metricName string, timestamp int64) distributorerror.Validation {
	sampleTimestampTooNewMsgFormat := globalerror.SampleTooFarInFuture.MessageWithPerTenantLimitConfig(
		"received a sample whose timestamp is too far in the future, timestamp: %d series: '%.200s'",
		creationGracePeriodFlag,
	)
	return distributorerror.NewValidationError(fmt.Errorf(sampleTimestampTooNewMsgFormat, timestamp, metricName))
}

func exemplarValidationError(format string, timestamp int64, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) error {
	return fmt.Errorf(format, timestamp, mimirpb.FromLabelAdaptersToLabels(seriesLabels).String(), mimirpb.FromLabelAdaptersToLabels(exemplarLabels).String())
}

func newExemplarEmptyLabelsError(seriesLabels []mimirpb.LabelAdapter, exemplarLabels []mimirpb.LabelAdapter, timestamp int64) distributorerror.Validation {
	exemplarEmptyLabelsMsgFormat := globalerror.ExemplarLabelsMissing.Message(
		"received an exemplar with no valid labels, timestamp: %d series: %s labels: %s",
	)
	return distributorerror.NewValidationError(exemplarValidationError(exemplarEmptyLabelsMsgFormat, timestamp, seriesLabels, exemplarLabels))
}

func newExemplarMissingTimestampError(seriesLabels []mimirpb.LabelAdapter, exemplarLabels []mimirpb.LabelAdapter, timestamp int64) distributorerror.Validation {
	exemplarMissingTimestampMsgFormat := globalerror.ExemplarTimestampInvalid.Message(
		"received an exemplar with no timestamp, timestamp: %d series: %s labels: %s",
	)
	return distributorerror.NewValidationError(exemplarValidationError(exemplarMissingTimestampMsgFormat, timestamp, seriesLabels, exemplarLabels))
}

func newExemplarMaxLabelLengthError(seriesLabels []mimirpb.LabelAdapter, exemplarLabels []mimirpb.LabelAdapter, timestamp int64) distributorerror.Validation {
	exemplarMaxLabelLengthMsgFormat := globalerror.ExemplarLabelsTooLong.Message(
		fmt.Sprintf("received an exemplar where the size of its combined labels exceeds the limit of %d characters, timestamp: %%d series: %%s labels: %%s", ExemplarMaxLabelSetLength),
	)
	return distributorerror.NewValidationError(exemplarValidationError(exemplarMaxLabelLengthMsgFormat, timestamp, seriesLabels, exemplarLabels))
}

func newMetadataMetricNameMissingError() distributorerror.Validation {
	return distributorerror.NewValidationError(
		errors.New(
			globalerror.MetricMetadataMissingMetricName.Message("received a metric metadata with no metric name"),
		),
	)
}

func metadataValidationError(format, cause, metricName string) error {
	return fmt.Errorf(format, cause, metricName)
}

func newMetadataMetricNameTooLongError(metadata *mimirpb.MetricMetadata) distributorerror.Validation {
	metadataMetricNameTooLongMsgFormat := globalerror.MetricMetadataMetricNameTooLong.MessageWithPerTenantLimitConfig(
		// When formatting this error the "cause" will always be an empty string.
		"received a metric metadata whose metric name length exceeds the limit, metric name: '%.200[2]s'",
		maxMetadataLengthFlag,
	)
	return distributorerror.NewValidationError(metadataValidationError(metadataMetricNameTooLongMsgFormat, "", metadata.GetMetricFamilyName()))
}

func newMetadataUnitTooLongError(metadata *mimirpb.MetricMetadata) distributorerror.Validation {
	metadataUnitTooLongMsgFormat := globalerror.MetricMetadataUnitTooLong.MessageWithPerTenantLimitConfig(
		"received a metric metadata whose unit name length exceeds the limit, unit: '%.200s' metric name: '%.200s'",
		maxMetadataLengthFlag,
	)
	return distributorerror.NewValidationError(metadataValidationError(metadataUnitTooLongMsgFormat, metadata.GetUnit(), metadata.GetMetricFamilyName()))
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

func NewRequestRateLimitedError(limit float64, burst int) distributorerror.RequestRateLimited {
	format := globalerror.RequestRateLimited.MessageWithPerTenantLimitConfig(
		"the request has been rejected because the tenant exceeded the request rate limit, set to %v requests/s across all distributors with a maximum allowed burst of %d",
		requestRateFlag,
		requestBurstSizeFlag,
	)
	return distributorerror.NewRequestRateLimitedError(format, limit, burst)
}

func NewIngestionRateLimitedError(limit float64, burst int) distributorerror.IngestionRateLimited {
	format := globalerror.IngestionRateLimited.MessageWithPerTenantLimitConfig(
		"the request has been rejected because the tenant exceeded the ingestion rate limit, set to %v items/s with a maximum allowed burst of %d. This limit is applied on the total number of samples, exemplars and metadata received across all distributors",
		ingestionRateFlag,
		ingestionBurstSizeFlag,
	)
	return distributorerror.NewIngestionRateLimitedError(format, limit, burst)
}

func NewQueryBlockedError() LimitError {
	return LimitError(globalerror.QueryBlocked.Message("the request has been blocked by the cluster administrator"))
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
