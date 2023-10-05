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

var (
	labelNameTooLongMsgFormat = globalerror.SeriesLabelNameTooLong.MessageWithPerTenantLimitConfig(
		"received a series whose label name length exceeds the limit, label: '%.200s' series: '%.200s'",
		maxLabelNameLengthFlag,
	)
	labelValueTooLongMsgFormat = globalerror.SeriesLabelValueTooLong.MessageWithPerTenantLimitConfig(
		"received a series whose label value length exceeds the limit, value: '%.200s' (truncated) series: '%.200s'",
		maxLabelValueLengthFlag,
	)
	invalidLabelMsgFormat   = globalerror.SeriesInvalidLabel.Message("received a series with an invalid label: '%.200s' series: '%.200s'")
	duplicateLabelMsgFormat = globalerror.SeriesWithDuplicateLabelNames.Message("received a series with duplicate label name, label: '%.200s' series: '%.200s'")
	tooManyLabelsMsgFormat  = globalerror.MaxLabelNamesPerSeries.MessageWithPerTenantLimitConfig(
		"received a series whose number of labels exceeds the limit (actual: %d, limit: %d) series: '%.200s%s'",
		maxLabelNamesPerSeriesFlag,
	)
	noMetricNameMsgFormat              = globalerror.MissingMetricName.Message("received series has no metric name")
	invalidMetricNameMsgFormat         = globalerror.InvalidMetricName.Message("received a series with invalid metric name: '%.200s'")
	maxNativeHistogramBucketsMsgFormat = fmt.Sprintf(
		"received a native histogram sample with too many buckets, timestamp: %%d series: %%s, buckets: %%d, limit: %%d (%s)",
		globalerror.MaxNativeHistogramBuckets,
	)
	sampleTimestampTooNewMsgFormat = globalerror.SampleTooFarInFuture.MessageWithPerTenantLimitConfig(
		"received a sample whose timestamp is too far in the future, timestamp: %d series: '%.200s'",
		creationGracePeriodFlag,
	)
	exemplarEmptyLabelsMsgFormat = globalerror.ExemplarLabelsMissing.Message(
		"received an exemplar with no valid labels, timestamp: %d series: %s labels: %s",
	)
	exemplarMissingTimestampMsgFormat = globalerror.ExemplarTimestampInvalid.Message(
		"received an exemplar with no timestamp, timestamp: %d series: %s labels: %s",
	)
	exemplarMaxLabelLengthMsgFormat = globalerror.ExemplarLabelsTooLong.Message(
		fmt.Sprintf("received an exemplar where the size of its combined labels exceeds the limit of %d characters, timestamp: %%d series: %%s labels: %%s", ExemplarMaxLabelSetLength),
	)
	metadataMetricNameMissingMsgFormat = globalerror.MetricMetadataMissingMetricName.Message("received a metric metadata with no metric name")
	metadataMetricNameTooLongMsgFormat = globalerror.MetricMetadataMetricNameTooLong.MessageWithPerTenantLimitConfig(
		// When formatting this error the "cause" will always be an empty string.
		"received a metric metadata whose metric name length exceeds the limit, metric name: '%.200[2]s'",
		maxMetadataLengthFlag,
	)
	metadataUnitTooLongMsgFormat = globalerror.MetricMetadataUnitTooLong.MessageWithPerTenantLimitConfig(
		"received a metric metadata whose unit name length exceeds the limit, unit: '%.200s' metric name: '%.200s'",
		maxMetadataLengthFlag,
	)
	IngestionRateLimitedMsgFormat = globalerror.IngestionRateLimited.MessageWithPerTenantLimitConfig(
		"the request has been rejected because the tenant exceeded the ingestion rate limit, set to %v items/s with a maximum allowed burst of %d. This limit is applied on the total number of samples, exemplars and metadata received across all distributors",
		ingestionRateFlag,
		ingestionBurstSizeFlag,
	)
	RequestRateLimitedMsgFormat = globalerror.RequestRateLimited.MessageWithPerTenantLimitConfig(
		"the request has been rejected because the tenant exceeded the request rate limit, set to %v requests/s across all distributors with a maximum allowed burst of %d",
		requestRateFlag,
		requestBurstSizeFlag,
	)
)

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

func NewQueryBlockedError() LimitError {
	return LimitError(globalerror.QueryBlocked.Message("the request has been blocked by the cluster administrator"))
}

func FormatRequestRateLimitedMessage(limit float64, burst int) string {
	return globalerror.RequestRateLimited.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the request has been rejected because the tenant exceeded the request rate limit, set to %v requests/s across all distributors with a maximum allowed burst of %d", limit, burst),
		requestRateFlag,
		requestBurstSizeFlag,
	)
}

func FormatIngestionRateLimitedMessage(limit float64, burst int) string {
	return globalerror.IngestionRateLimited.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the request has been rejected because the tenant exceeded the ingestion rate limit, set to %v items/s with a maximum allowed burst of %d. This limit is applied on the total number of samples, exemplars and metadata received across all distributors", limit, burst),
		ingestionRateFlag,
		ingestionBurstSizeFlag,
	)
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

func getMetricAndEllipsis(ls []mimirpb.LabelAdapter) (string, string) {
	metric := mimirpb.FromLabelAdaptersToMetric(ls).String()
	ellipsis := ""

	if utf8.RuneCountInString(metric) > 200 {
		ellipsis = "\u2026"
	}
	return metric, ellipsis
}
