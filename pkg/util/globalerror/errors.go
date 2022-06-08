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
	SeriesInvalidLabel            ID = "label-invalid"
	SeriesWithDuplicateLabelNames ID = "duplicate-label-names"
	SeriesLabelsNotSorted         ID = "labels-not-sorted"

	MaxLabelNamesPerSeries  ID = "tenant-max-label-names-per-series"
	SeriesLabelNameTooLong  ID = "tenant-label-name-too-long"
	SeriesLabelValueTooLong ID = "tenant-label-value-too-long"
	SampleTooFarInFuture    ID = "tenant-too-far-in-future"
	MaxSeriesPerMetric      ID = "tenant-max-series-per-metric"
	MaxMetadataPerMetric    ID = "tenant-max-metadata-per-metric"
	MaxSeriesPerUser        ID = "tenant-max-series-per-user"
	MaxMetadataPerUser      ID = "tenant-max-metadata-per-user"
	MaxChunksPerQuery       ID = "tenant-max-chunks-per-query"
	MaxSeriesPerQuery       ID = "tenant-max-series-per-query"
	MaxChunkBytesPerQuery   ID = "tenant-max-chunks-bytes-per-query"

	DistributorMaxIngestionRate        ID = "distributor-max-ingestion-rate"
	DistributorMaxInflightPushRequests ID = "distributor-max-inflight-push-requests"

	IngesterMaxIngestionRate        ID = "ingester-max-ingestion-rate"
	IngesterMaxTenants              ID = "ingester-max-tenants"
	IngesterMaxInMemorySeries       ID = "ingester-max-series"
	IngesterMaxInflightPushRequests ID = "ingester-max-inflight-push-requests"

	ExemplarLabelsMissing    ID = "exemplar-labels-missing"
	ExemplarLabelsTooLong    ID = "exemplar-labels-too-long"
	ExemplarTimestampInvalid ID = "exemplar-timestamp-invalid"

	MetricMetadataMissingMetricName ID = "metadata-missing-metric-name"

	MetricMetadataMetricNameTooLong ID = "tenant-metric-name-too-long"
	MetricMetadataHelpTooLong       ID = "tenant-help-too-long"
	MetricMetadataUnitTooLong       ID = "tenant-unit-too-long"

	MaxQueryLength       ID = "tenant-max-query-length"
	RequestRateLimited   ID = "tenant-max-request-rate"
	IngestionRateLimited ID = "tenant-max-ingestion-rate"
	TooManyHAClusters    ID = "tenant-too-many-ha-clusters"
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
	return fmt.Sprintf("%s (%s%s). You can adjust the related per-tenant limit%s by configuring %s, or by contacting your service administrator.", msg, errPrefix, id, plural, sb.String())
}
