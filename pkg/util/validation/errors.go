// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/errors.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"fmt"
	"strings"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerrors"
)

// ValidationError is an error returned by series validation.
//
// nolint:golint ignore stutter warning
type ValidationError error

// genericValidationError is a basic implementation of ValidationError which can be used when the
// error format only contains the cause and the series.
type genericValidationError struct {
	message string
	cause   string
	series  []mimirpb.LabelAdapter
}

func (e *genericValidationError) Error() string {
	return fmt.Sprintf(e.message, e.cause, formatLabelSet(e.series))
}

var labelNameTooLongMsgFormat = globalerrors.FormatWithLimitConfig(
	globalerrors.ErrIDSeriesLabelNameTooLong,
	maxLabelNameLengthFlag,
	"received series with label name length exceeding the limit, label: '%%.200s' series: '%%.200s'")

func newLabelNameTooLongError(series []mimirpb.LabelAdapter, labelName string) ValidationError {
	return &genericValidationError{
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

func (e *labelValueTooLongError) Error() string {
	return globalerrors.FormatWithLimitConfig(
		globalerrors.ErrIDSeriesLabelValueTooLong,
		maxLabelValueLengthFlag,
		"received series with label value length exceeding the limit, value: '%.200s' (truncated) series: '%.200s'",
		e.labelValue, formatLabelSet(e.series))
}

func newLabelValueTooLongError(series []mimirpb.LabelAdapter, labelValue string) ValidationError {
	return &labelValueTooLongError{
		labelValue: labelValue,
		series:     series,
	}
}

var invalidLabelMsgFormat = globalerrors.Format(
	globalerrors.ErrIDSeriesInvalidLabel,
	"received series with an invalid label: '%%.200s' series: '%%.200s'")

func newInvalidLabelError(series []mimirpb.LabelAdapter, labelName string) ValidationError {
	return &genericValidationError{
		message: invalidLabelMsgFormat,
		cause:   labelName,
		series:  series,
	}
}

var duplicateLabelMsgFormat = globalerrors.Format(
	globalerrors.ErrIDSeriesWithDuplicateLabelNames,
	"received series with duplicate label name, label: '%%.200s' series: '%%.200s'")

func newDuplicatedLabelError(series []mimirpb.LabelAdapter, labelName string) ValidationError {
	return &genericValidationError{
		message: duplicateLabelMsgFormat,
		cause:   labelName,
		series:  series,
	}
}

var labelsNotSortedMsgFormat = globalerrors.Format(
	globalerrors.ErrIDSeriesLabelsNotSorted,
	"received series with label name not alphabetically sorted, label: '%%.200s' series: '%%.200s'")

func newLabelsNotSortedError(series []mimirpb.LabelAdapter, labelName string) ValidationError {
	return &genericValidationError{
		message: labelsNotSortedMsgFormat,
		cause:   labelName,
		series:  series,
	}
}

type tooManyLabelsError struct {
	series []mimirpb.LabelAdapter
	limit  int
}

func newTooManyLabelsError(series []mimirpb.LabelAdapter, limit int) ValidationError {
	return &tooManyLabelsError{
		series: series,
		limit:  limit,
	}
}

func (e *tooManyLabelsError) Error() string {
	return globalerrors.FormatWithLimitConfig(
		globalerrors.ErrIDMaxLabelNamesPerSeries,
		maxLabelNamesPerSeriesFlag,
		"received series with a number of labels exceeding the limit (actual: %d, limit: %d) series: '%.200s'",
		len(e.series), e.limit, mimirpb.FromLabelAdaptersToMetric(e.series).String())
}

type noMetricNameError struct{}

func newNoMetricNameError() ValidationError {
	return &noMetricNameError{}
}

func (e *noMetricNameError) Error() string {
	return globalerrors.Format(globalerrors.ErrIDMissingMetricName, "received series has no metric name")
}

type invalidMetricNameError struct {
	metricName string
}

func newInvalidMetricNameError(metricName string) ValidationError {
	return &invalidMetricNameError{
		metricName: metricName,
	}
}

func (e *invalidMetricNameError) Error() string {
	return globalerrors.Format(globalerrors.ErrIDInvalidMetricName, "received series with invalid metric name: '%.200s'", e.metricName)
}

// sampleValidationError is a ValidationError implementation suitable for sample validation errors.
type sampleValidationError struct {
	message    string
	metricName string
	timestamp  int64
}

func (e *sampleValidationError) Error() string {
	return fmt.Sprintf(e.message, e.timestamp, e.metricName)
}

var sampleTimestampTooNewMsgFormat = globalerrors.FormatWithLimitConfig(
	globalerrors.ErrIDSampleTooFarInFuture,
	creationGracePeriodFlag,
	"received sample with a timestamp too far in the future, timestamp: %%d series: '%%.200s'")

func newSampleTimestampTooNewError(metricName string, timestamp int64) ValidationError {
	return &sampleValidationError{
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

func (e *exemplarValidationError) Error() string {
	return fmt.Sprintf(e.message, e.timestamp, mimirpb.FromLabelAdaptersToLabels(e.seriesLabels).String(), mimirpb.FromLabelAdaptersToLabels(e.exemplarLabels).String())
}

var exemplarEmptyLabelsMsgFormat = globalerrors.Format(
	globalerrors.ErrIDExemplarLabelsMissing,
	"received exemplar with no valid labels, timestamp: %%d series: %%s labels: %%s")

func newExemplarEmptyLabelsError(seriesLabels []mimirpb.LabelAdapter, exemplarLabels []mimirpb.LabelAdapter, timestamp int64) ValidationError {
	return &exemplarValidationError{
		message:        exemplarEmptyLabelsMsgFormat,
		seriesLabels:   seriesLabels,
		exemplarLabels: exemplarLabels,
		timestamp:      timestamp,
	}
}

var exemplarMissingTimestampMsgFormat = globalerrors.Format(
	globalerrors.ErrIDExemplarTimestampInvalid,
	"received exemplar with no timestamp, timestamp: %%d series: %%s labels: %%s")

func newExemplarMissingTimestampError(seriesLabels []mimirpb.LabelAdapter, exemplarLabels []mimirpb.LabelAdapter, timestamp int64) ValidationError {
	return &exemplarValidationError{
		message:        exemplarMissingTimestampMsgFormat,
		seriesLabels:   seriesLabels,
		exemplarLabels: exemplarLabels,
		timestamp:      timestamp,
	}
}

var exemplarMaxLabelLengthMsgFormat = globalerrors.Format(
	globalerrors.ErrIDExemplarLabelsTooLong,
	"received exemplar whose combined labels set size exceeds the limit of %d characters, timestamp: %%d series: %%s labels: %%s",
	ExemplarMaxLabelSetLength)

func newExemplarMaxLabelLengthError(seriesLabels []mimirpb.LabelAdapter, exemplarLabels []mimirpb.LabelAdapter, timestamp int64) ValidationError {
	return &exemplarValidationError{
		message:        exemplarMaxLabelLengthMsgFormat,
		seriesLabels:   seriesLabels,
		exemplarLabels: exemplarLabels,
		timestamp:      timestamp,
	}
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
