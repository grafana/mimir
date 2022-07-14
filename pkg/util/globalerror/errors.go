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

	MissingMetricName             ID = "missing-metric-name"
	InvalidMetricName             ID = "metric-name-invalid"
	MaxLabelNamesPerSeries        ID = "max-label-names-per-series"
	SeriesInvalidLabel            ID = "label-invalid"
	SeriesLabelNameTooLong        ID = "label-name-too-long"
	SeriesLabelValueTooLong       ID = "label-value-too-long"
	SeriesWithDuplicateLabelNames ID = "duplicate-label-names"
	SeriesLabelsNotSorted         ID = "labels-not-sorted"
	SampleTooFarInFuture          ID = "too-far-in-future"
	MaxSeriesPerMetric            ID = "max-series-per-metric"
	MaxMetadataPerMetric          ID = "max-metadata-per-metric"
	MaxSeriesPerUser              ID = "max-series-per-user"
	MaxMetadataPerUser            ID = "max-metadata-per-user"
	MaxChunksPerQuery             ID = "max-chunks-per-query"
	MaxSeriesPerQuery             ID = "max-series-per-query"
	MaxChunkBytesPerQuery         ID = "max-chunks-bytes-per-query"

	DistributorMaxIngestionRate                 ID = "distributor-max-ingestion-rate"
	DistributorMaxInflightPushRequests          ID = "distributor-max-inflight-push-requests"
	DistributorMaxInflightPushRequestsTotalSize ID = "distributor-max-inflight-push-requests-total-size"

	IngesterMaxIngestionRate        ID = "ingester-max-ingestion-rate"
	IngesterMaxTenants              ID = "ingester-max-tenants"
	IngesterMaxInMemorySeries       ID = "ingester-max-series"
	IngesterMaxInflightPushRequests ID = "ingester-max-inflight-push-requests"

	ExemplarLabelsMissing    ID = "exemplar-labels-missing"
	ExemplarLabelsTooLong    ID = "exemplar-labels-too-long"
	ExemplarTimestampInvalid ID = "exemplar-timestamp-invalid"

	MetricMetadataMissingMetricName ID = "metadata-missing-metric-name"
	MetricMetadataMetricNameTooLong ID = "metric-name-too-long"
	MetricMetadataHelpTooLong       ID = "help-too-long"
	MetricMetadataUnitTooLong       ID = "unit-too-long"

	MaxQueryLength       ID = "max-query-length"
	RequestRateLimited   ID = "tenant-max-request-rate"
	IngestionRateLimited ID = "tenant-max-ingestion-rate"
	TooManyHAClusters    ID = "tenant-too-many-ha-clusters"

	SampleTimestampTooOld    ID = "sample-timestamp-too-old"
	SampleOutOfOrder         ID = "sample-out-of-order"
	SampleDuplicateTimestamp ID = "sample-duplicate-timestamp"
	ExemplarSeriesMissing    ID = "exemplar-series-missing"

	StoreConsistencyCheckFailed ID = "store-consistency-check-failed"
	BucketIndexTooOld           ID = "bucket-index-too-old"
)

// Message returns the provided msg, appending the error id.
func (id ID) Message(msg string) string {
	return fmt.Sprintf("%s (%s%s)", msg, errPrefix, id)
}

// MessageWithLimitConfig returns the provided msg, appending the error id and a suggestion on
// which configuration flag(s) to use to change the limit.
func (id ID) MessageWithLimitConfig(msg, flag string, addFlags ...string) string {
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
	return fmt.Sprintf("%s (%s%s). To adjust the related per-tenant limit%s, configure %s, or contact your service administrator.", msg, errPrefix, id, plural, sb.String())
}
