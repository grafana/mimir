// SPDX-License-Identifier: AGPL-3.0-only

package globalerror

import (
	"fmt"
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

	MaxQueryLength ID = "max-query-length"
)

// Message returns the provided msg, appending the error id.
func (id ID) Message(msg string) string {
	return fmt.Sprintf("%s (%s%s)", msg, errPrefix, id)
}

// MessageWithLimitConfig return the provided msg, appending the error id and a suggestion on
// which configuration flag to use to change the limit.
func (id ID) MessageWithLimitConfig(flag, msg string) string {
	return fmt.Sprintf("%s (%s%s). You can adjust the related per-tenant limit by configuring -%s, or by contacting your service administrator.", msg, errPrefix, id, flag)
}
