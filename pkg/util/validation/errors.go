// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/errors.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirpb"
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

func newLabelNameTooLongError(series []mimirpb.LabelAdapter, labelName string) ValidationError {
	return &genericValidationError{
		message: "label name too long: %.200q metric %.200q",
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
	return fmt.Sprintf("label value too long for metric: %.200q label value: %.200q", formatLabelSet(e.series), e.labelValue)
}

func newLabelValueTooLongError(series []mimirpb.LabelAdapter, labelValue string) ValidationError {
	return &labelValueTooLongError{
		labelValue: labelValue,
		series:     series,
	}
}

func newInvalidLabelError(series []mimirpb.LabelAdapter, labelName string) ValidationError {
	return &genericValidationError{
		message: "sample invalid label: %.200q metric %.200q",
		cause:   labelName,
		series:  series,
	}
}

func newDuplicatedLabelError(series []mimirpb.LabelAdapter, labelName string) ValidationError {
	return &genericValidationError{
		message: "duplicate label name: %.200q metric %.200q",
		cause:   labelName,
		series:  series,
	}
}

func newLabelsNotSortedError(series []mimirpb.LabelAdapter, labelName string) ValidationError {
	return &genericValidationError{
		message: "labels not sorted: %.200q metric %.200q",
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
	return fmt.Sprintf(
		"series has too many labels (actual: %d, limit: %d) series: '%s'",
		len(e.series), e.limit, mimirpb.FromLabelAdaptersToMetric(e.series).String())
}

type noMetricNameError struct{}

func newNoMetricNameError() ValidationError {
	return &noMetricNameError{}
}

func (e *noMetricNameError) Error() string {
	return "sample missing metric name"
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
	return fmt.Sprintf("sample invalid metric name: %.200q", e.metricName)
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

func newSampleTimestampTooOldError(metricName string, timestamp int64) ValidationError {
	return &sampleValidationError{
		message:    "timestamp too old: %d metric: %.200q",
		metricName: metricName,
		timestamp:  timestamp,
	}
}

func newSampleTimestampTooNewError(metricName string, timestamp int64) ValidationError {
	return &sampleValidationError{
		message:    "timestamp too new: %d metric: %.200q",
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

func newExemplarEmtpyLabelsError(seriesLabels []mimirpb.LabelAdapter, exemplarLabels []mimirpb.LabelAdapter, timestamp int64) ValidationError {
	return &exemplarValidationError{
		message:        "exemplar missing labels, timestamp: %d series: %s labels: %s",
		seriesLabels:   seriesLabels,
		exemplarLabels: exemplarLabels,
		timestamp:      timestamp,
	}
}

func newExemplarMissingTimestampError(seriesLabels []mimirpb.LabelAdapter, exemplarLabels []mimirpb.LabelAdapter, timestamp int64) ValidationError {
	return &exemplarValidationError{
		message:        "exemplar missing timestamp, timestamp: %d series: %s labels: %s",
		seriesLabels:   seriesLabels,
		exemplarLabels: exemplarLabels,
		timestamp:      timestamp,
	}
}

var labelLenMsg = "exemplar combined labelset exceeds " + strconv.Itoa(ExemplarMaxLabelSetLength) + " characters, timestamp: %d series: %s labels: %s"

func newExemplarLabelLengthError(seriesLabels []mimirpb.LabelAdapter, exemplarLabels []mimirpb.LabelAdapter, timestamp int64) ValidationError {
	return &exemplarValidationError{
		message:        labelLenMsg,
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
