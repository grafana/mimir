// SPDX-License-Identifier: AGPL-3.0-only

package globalerror

import (
	"fmt"
)

type ErrID string

// This block defines error IDs exposed to the final user. These IDs are expected to be
// *immutable*, so don't rename them over time.
const (
	errPrefix = "err-mimir-"

	ErrIDMissingMetricName             ErrID = "missing-metric-name"
	ErrIDInvalidMetricName             ErrID = "metric-name-invalid"
	ErrIDMaxLabelNamesPerSeries        ErrID = "max-label-names-per-series"
	ErrIDSeriesInvalidLabel            ErrID = "label-invalid"
	ErrIDSeriesLabelNameTooLong        ErrID = "label-name-too-long"
	ErrIDSeriesLabelValueTooLong       ErrID = "label-value-too-long"
	ErrIDSeriesWithDuplicateLabelNames ErrID = "duplicate-label-names"
	ErrIDSeriesLabelsNotSorted         ErrID = "labels-not-sorted"
	ErrIDSampleTooFarInFuture          ErrID = "too-far-in-future"

	ErrIDExemplarLabelsMissing    ErrID = "exemplar-labels-missing"
	ErrIDExemplarLabelsTooLong    ErrID = "exemplar-labels-too-long"
	ErrIDExemplarTimestampInvalid ErrID = "exemplar-timestamp-invalid"
)

// Format the provided message, appending the error id.
// The provided message and arguments are formatted with fmt.Sprintf().
func Format(id ErrID, format string, args ...interface{}) string {
	return fmt.Sprintf("%s (%s%s)", fmt.Sprintf(format, args...), errPrefix, id)
}

// FormatWithLimitConfig the provided message, appending the error id and a suggestion on
// which configuration flag to use to change the limit.
// The provided message and arguments are formatted with fmt.Sprintf().
func FormatWithLimitConfig(id ErrID, flag, format string, args ...interface{}) string {
	return fmt.Sprintf("%s (%s%s). You can adjust the related per-tenant limit by configuring -%s, or by contacting your service administrator.", fmt.Sprintf(format, args...), errPrefix, id, flag)
}
