// SPDX-License-Identifier: AGPL-3.0-only

package globalerror

import (
	"fmt"
	"strings"
)

type ID string

// This block defines error IDs exposed to the final user. These IDs are expected to be
// *immutable*, so don't rename them over time.
const (
	errPrefix = "err-mimir-"

	MissingMetricName                     ID = "missing-metric-name"
	InvalidMetricName                     ID = "metric-name-invalid"
	MaxLabelNamesPerSeries                ID = "max-label-names-per-series"
	MaxNativeHistogramBuckets             ID = "max-native-histogram-buckets"
	NotReducibleNativeHistogram           ID = "not-reducible-native-histogram"
	InvalidSchemaNativeHistogram          ID = "invalid-native-histogram-schema"
	SeriesInvalidLabel                    ID = "label-invalid"
	SeriesInvalidLabelValue               ID = "label-value-invalid"
	SeriesLabelNameTooLong                ID = "label-name-too-long"
	SeriesLabelValueTooLong               ID = "label-value-too-long"
	SeriesWithDuplicateLabelNames         ID = "duplicate-label-names"
	SeriesLabelsNotSorted                 ID = "labels-not-sorted"
	SampleTooFarInFuture                  ID = "too-far-in-future"
	SampleTooFarInPast                    ID = "too-far-in-past"
	MaxSeriesPerMetric                    ID = "max-series-per-metric"
	MaxMetadataPerMetric                  ID = "max-metadata-per-metric"
	MaxSeriesPerUser                      ID = "max-series-per-user"
	MaxMetadataPerUser                    ID = "max-metadata-per-user"
	MaxChunksPerQuery                     ID = "max-chunks-per-query"
	MaxSeriesPerQuery                     ID = "max-series-per-query"
	MaxChunkBytesPerQuery                 ID = "max-chunks-bytes-per-query"
	MaxEstimatedChunksPerQuery            ID = "max-estimated-chunks-per-query"
	MaxEstimatedMemoryConsumptionPerQuery ID = "max-estimated-memory-consumption-per-query"

	DistributorMaxIngestionRate             ID = "distributor-max-ingestion-rate"
	DistributorMaxInflightPushRequests      ID = "distributor-max-inflight-push-requests"
	DistributorMaxInflightPushRequestsBytes ID = "distributor-max-inflight-push-requests-bytes"

	IngesterMaxIngestionRate             ID = "ingester-max-ingestion-rate"
	IngesterMaxTenants                   ID = "ingester-max-tenants"
	IngesterMaxInMemorySeries            ID = "ingester-max-series"
	IngesterMaxInflightPushRequests      ID = "ingester-max-inflight-push-requests"
	IngesterMaxInflightPushRequestsBytes ID = "ingester-max-inflight-push-requests-bytes"

	ExemplarLabelsMissing    ID = "exemplar-labels-missing"
	ExemplarLabelsTooLong    ID = "exemplar-labels-too-long"
	ExemplarTimestampInvalid ID = "exemplar-timestamp-invalid"

	MetricMetadataMissingMetricName ID = "metadata-missing-metric-name"
	MetricMetadataMetricNameTooLong ID = "metric-name-too-long"
	MetricMetadataHelpTooLong       ID = "help-too-long" // unused, left here to prevent reuse for different purpose
	MetricMetadataUnitTooLong       ID = "unit-too-long"

	MaxQueryLength              ID = "max-query-length"
	MaxTotalQueryLength         ID = "max-total-query-length"
	MaxQueryExpressionSizeBytes ID = "max-query-expression-size-bytes"
	RequestRateLimited          ID = "tenant-max-request-rate"
	IngestionRateLimited        ID = "tenant-max-ingestion-rate"
	TooManyHAClusters           ID = "tenant-too-many-ha-clusters"
	QueryBlocked                ID = "query-blocked"

	SampleTimestampTooOld    ID = "sample-timestamp-too-old"
	SampleOutOfOrder         ID = "sample-out-of-order"
	SampleDuplicateTimestamp ID = "sample-duplicate-timestamp"
	ExemplarSeriesMissing    ID = "exemplar-series-missing"
	ExemplarTooFarInFuture   ID = "exemplar-too-far-in-future"
	ExemplarTooFarInPast     ID = "exemplar-too-far-in-past"

	StoreConsistencyCheckFailed ID = "store-consistency-check-failed"
	BucketIndexTooOld           ID = "bucket-index-too-old"

	DistributorMaxWriteMessageSize         ID = "distributor-max-write-message-size"
	DistributorMaxOTLPRequestSize          ID = "distributor-max-otlp-request-size"
	DistributorMaxWriteRequestDataItemSize ID = "distributor-max-write-request-data-item-size"

	// Map Prometheus TSDB native histogram validation errors to Mimir errors.
	// E.g. histogram.ErrHistogramCountNotBigEnough -> NativeHistogramCountNotBigEnough
	NativeHistogramCountMismatch        ID = "native-histogram-count-mismatch"
	NativeHistogramCountNotBigEnough    ID = "native-histogram-count-not-big-enough"
	NativeHistogramNegativeBucketCount  ID = "native-histogram-negative-bucket-count"
	NativeHistogramSpanNegativeOffset   ID = "native-histogram-span-negative-offset"
	NativeHistogramSpansBucketsMismatch ID = "native-histogram-spans-buckets-mismatch"

	// Alertmanager errors
	AlertmanagerMaxGrafanaConfigSize ID = "alertmanager-max-grafana-config-size"
	AlertmanagerMaxGrafanaStateSize  ID = "alertmanager-max-grafana-state-size"
)

// Message returns the provided msg, appending the error id.
func (id ID) Message(msg string) string {
	return fmt.Sprintf("%s (%s%s)", msg, errPrefix, id)
}

// MessageWithPerInstanceLimitConfig returns the provided msg, appending the error id and a suggestion on
// which configuration flag(s) to use to change the per-instance limit.
func (id ID) MessageWithPerInstanceLimitConfig(msg, flag string, addFlags ...string) string {
	flagsList, plural := buildFlagsList(flag, addFlags...)
	return fmt.Sprintf("%s (%s%s). To adjust the related limit%s, configure %s, or contact your service administrator.", msg, errPrefix, id, plural, flagsList)
}

// MessageWithPerTenantLimitConfig returns the provided msg, appending the error id and a suggestion on
// which configuration flag(s) to use to change the per-tenant limit.
func (id ID) MessageWithPerTenantLimitConfig(msg, flag string, addFlags ...string) string {
	flagsList, plural := buildFlagsList(flag, addFlags...)
	return fmt.Sprintf("%s (%s%s). To adjust the related per-tenant limit%s, configure %s, or contact your service administrator.", msg, errPrefix, id, plural, flagsList)
}

// MessageWithStrategyAndPerTenantLimitConfig returns the provided msg, appending the error id and a
// suggestion on which strategy to follow to try not hitting the limit, plus which configuration
// flag(s) to otherwise change the per-tenant limit.
func (id ID) MessageWithStrategyAndPerTenantLimitConfig(msg, strategy, flag string, addFlags ...string) string {
	flagsList, plural := buildFlagsList(flag, addFlags...)
	return fmt.Sprintf("%s (%s%s). %s. Otherwise, to adjust the related per-tenant limit%s, configure %s, or contact your service administrator.",
		msg, errPrefix, id, strategy, plural, flagsList)
}

// LabelValue returns the error ID converted to a form suitable for use as a Prometheus label value.
func (id ID) LabelValue() string {
	return strings.ReplaceAll(string(id), "-", "_")
}

// Error implements error.
func (id ID) Error() string {
	return string(id)
}

func buildFlagsList(flag string, addFlags ...string) (string, string) {
	var sb strings.Builder
	sb.WriteString("-")
	sb.WriteString(flag)
	plural := ""
	if len(addFlags) > 0 {
		plural = "s"
		for _, addFlag := range addFlags[:len(addFlags)-1] {
			sb.WriteString(", -")
			sb.WriteString(addFlag)
		}
		sb.WriteString(" and -")
		sb.WriteString(addFlags[len(addFlags)-1])
	}

	return sb.String(), plural
}
