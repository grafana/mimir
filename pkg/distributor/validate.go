// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/validate.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	// The combined length of the label names and values of an Exemplar's LabelSet MUST NOT exceed 128 UTF-8 characters
	// https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#exemplars
	ExemplarMaxLabelSetLength = 128
)

var (
	// Discarded series / samples reasons.
	reasonMissingMetricName            = globalerror.MissingMetricName.LabelValue()
	reasonInvalidMetricName            = globalerror.InvalidMetricName.LabelValue()
	reasonMaxLabelNamesPerSeries       = globalerror.MaxLabelNamesPerSeries.LabelValue()
	reasonMaxLabelNamesPerInfoSeries   = globalerror.MaxLabelNamesPerInfoSeries.LabelValue()
	reasonInvalidLabel                 = globalerror.SeriesInvalidLabel.LabelValue()
	reasonInvalidLabelValue            = globalerror.SeriesInvalidLabelValue.LabelValue()
	reasonLabelNameTooLong             = globalerror.SeriesLabelNameTooLong.LabelValue()
	reasonLabelValueTooLong            = globalerror.SeriesLabelValueTooLong.LabelValue()
	reasonMaxNativeHistogramBuckets    = globalerror.MaxNativeHistogramBuckets.LabelValue()
	reasonInvalidNativeHistogramSchema = globalerror.InvalidSchemaNativeHistogram.LabelValue()
	reasonDuplicateLabelNames          = globalerror.SeriesWithDuplicateLabelNames.LabelValue()
	reasonTooFarInFuture               = globalerror.SampleTooFarInFuture.LabelValue()
	reasonTooFarInPast                 = globalerror.SampleTooFarInPast.LabelValue()

	// Discarded exemplars reasons.
	reasonExemplarLabelsMissing               = globalerror.ExemplarLabelsMissing.LabelValue()
	reasonExemplarLabelsTooLong               = globalerror.ExemplarLabelsTooLong.LabelValue()
	reasonExemplarTimestampInvalid            = globalerror.ExemplarTimestampInvalid.LabelValue()
	reasonExemplarLabelsBlank                 = "exemplar_labels_blank"
	reasonExemplarTooOld                      = "exemplar_too_old"
	reasonExemplarTooFarInFuture              = "exemplar_too_far_in_future"
	reasonTooManyExemplarsPerSeriesPerRequest = "too_many_exemplars_per_series_per_request"

	// Discarded metadata reasons.
	reasonMetadataMetricNameTooLong = globalerror.MetricMetadataMetricNameTooLong.LabelValue()
	reasonMetadataUnitTooLong       = globalerror.MetricMetadataUnitTooLong.LabelValue()

	// reasonRateLimited is one of the values for the reason to discard samples.
	// Declared here to avoid duplication in ingester and distributor.
	reasonRateLimited = "rate_limited" // same for request and ingestion which are separate errors, so not using metricReasonFromErrorID with global error

	// reasonTooManyHAClusters is one of the reasons for discarding samples.
	reasonTooManyHAClusters = "too_many_ha_clusters"

	labelNameTooLongMsgFormat = globalerror.SeriesLabelNameTooLong.MessageWithPerTenantLimitConfig(
		"received a series whose label name length exceeds the limit, label: '%.200s' series: '%.200s'",
		validation.MaxLabelNameLengthFlag,
	)
	labelValueTooLongMsgFormat = globalerror.SeriesLabelValueTooLong.MessageWithPerTenantLimitConfig(
		"received a series whose label value length exceeds the limit, label: '%s', value: '%.200s' (truncated) series: '%.200s'",
		validation.MaxLabelValueLengthFlag,
	)
	invalidLabelMsgFormat      = globalerror.SeriesInvalidLabel.Message("received a series with an invalid label: '%.200s' series: '%.200s'")
	invalidLabelValueMsgFormat = globalerror.SeriesInvalidLabelValue.Message("received a series with invalid value in label '%.200s': '%.200s' metric: '%.200s'")
	duplicateLabelMsgFormat    = globalerror.SeriesWithDuplicateLabelNames.Message("received a series with duplicate label name, label: '%.200s' series: '%.200s'")

	tooManyLabelsMsgFormat = globalerror.MaxLabelNamesPerSeries.MessageWithPerTenantLimitConfig(
		"received a series whose number of labels exceeds the limit (actual: %d, limit: %d) series: '%.200s%s'",
		validation.MaxLabelNamesPerSeriesFlag,
	)
	tooManyInfoLabelsMsgFormat = globalerror.MaxLabelNamesPerSeries.MessageWithPerTenantLimitConfig(
		"received an info series whose number of labels exceeds the limit (actual: %d, limit: %d) series: '%.200s%s'",
		validation.MaxLabelNamesPerSeriesFlag,
	)

	noMetricNameMsgFormat                 = globalerror.MissingMetricName.Message("received series has no metric name")
	invalidMetricNameMsgFormat            = globalerror.InvalidMetricName.Message("received a series with invalid metric name: '%.200s'")
	maxNativeHistogramBucketsMsgFormat    = globalerror.MaxNativeHistogramBuckets.Message("received a native histogram sample with too many buckets, timestamp: %d series: %s, buckets: %d, limit: %d")
	notReducibleNativeHistogramMsgFormat  = globalerror.NotReducibleNativeHistogram.Message("received a native histogram sample with too many buckets and cannot reduce, timestamp: %d series: %s, buckets: %d, limit: %d")
	invalidSchemaNativeHistogramMsgFormat = globalerror.InvalidSchemaNativeHistogram.Message("received a native histogram sample with an invalid schema: %d")
	sampleTimestampTooNewMsgFormat        = globalerror.SampleTooFarInFuture.MessageWithPerTenantLimitConfig(
		"received a sample whose timestamp is too far in the future, timestamp: %d series: '%.200s'",
		validation.CreationGracePeriodFlag,
	)
	sampleTimestampTooOldMsgFormat = globalerror.SampleTooFarInPast.MessageWithPerTenantLimitConfig(
		"received a sample whose timestamp is too far in the past, timestamp: %d series: '%.200s'",
		validation.PastGracePeriodFlag,
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
		validation.MaxMetadataLengthFlag,
	)
	metadataUnitTooLongMsgFormat = globalerror.MetricMetadataUnitTooLong.MessageWithPerTenantLimitConfig(
		"received a metric metadata whose unit name length exceeds the limit, unit: '%.200s' metric name: '%.200s'",
		validation.MaxMetadataLengthFlag,
	)
)

// sampleValidationConfig helps with getting required config to validate sample.
type sampleValidationConfig interface {
	CreationGracePeriod(userID string) time.Duration
	PastGracePeriod(userID string) time.Duration
	MaxNativeHistogramBuckets(userID string) int
	ReduceNativeHistogramOverMaxBuckets(userID string) bool
	OutOfOrderTimeWindow(userID string) time.Duration
}

// sampleValidationMetrics is a collection of metrics used during sample validation.
type sampleValidationMetrics struct {
	missingMetricName            *prometheus.CounterVec
	invalidMetricName            *prometheus.CounterVec
	maxLabelNamesPerSeries       *prometheus.CounterVec
	maxLabelNamesPerInfoSeries   *prometheus.CounterVec
	invalidLabel                 *prometheus.CounterVec
	invalidLabelValue            *prometheus.CounterVec
	labelNameTooLong             *prometheus.CounterVec
	labelValueTooLong            *prometheus.CounterVec
	maxNativeHistogramBuckets    *prometheus.CounterVec
	invalidNativeHistogramSchema *prometheus.CounterVec
	duplicateLabelNames          *prometheus.CounterVec
	tooFarInFuture               *prometheus.CounterVec
	tooFarInPast                 *prometheus.CounterVec
}

func (m *sampleValidationMetrics) deleteUserMetrics(userID string) {
	filter := prometheus.Labels{"user": userID}
	m.missingMetricName.DeletePartialMatch(filter)
	m.invalidMetricName.DeletePartialMatch(filter)
	m.maxLabelNamesPerSeries.DeletePartialMatch(filter)
	m.maxLabelNamesPerInfoSeries.DeletePartialMatch(filter)
	m.invalidLabel.DeletePartialMatch(filter)
	m.invalidLabelValue.DeletePartialMatch(filter)
	m.labelNameTooLong.DeletePartialMatch(filter)
	m.labelValueTooLong.DeletePartialMatch(filter)
	m.maxNativeHistogramBuckets.DeletePartialMatch(filter)
	m.invalidNativeHistogramSchema.DeletePartialMatch(filter)
	m.duplicateLabelNames.DeletePartialMatch(filter)
	m.tooFarInFuture.DeletePartialMatch(filter)
	m.tooFarInPast.DeletePartialMatch(filter)
}

func (m *sampleValidationMetrics) deleteUserMetricsForGroup(userID, group string) {
	m.missingMetricName.DeleteLabelValues(userID, group)
	m.invalidMetricName.DeleteLabelValues(userID, group)
	m.maxLabelNamesPerSeries.DeleteLabelValues(userID, group)
	m.maxLabelNamesPerInfoSeries.DeleteLabelValues(userID, group)
	m.invalidLabel.DeleteLabelValues(userID, group)
	m.invalidLabelValue.DeleteLabelValues(userID, group)
	m.labelNameTooLong.DeleteLabelValues(userID, group)
	m.labelValueTooLong.DeleteLabelValues(userID, group)
	m.maxNativeHistogramBuckets.DeleteLabelValues(userID, group)
	m.invalidNativeHistogramSchema.DeleteLabelValues(userID, group)
	m.duplicateLabelNames.DeleteLabelValues(userID, group)
	m.tooFarInFuture.DeleteLabelValues(userID, group)
	m.tooFarInPast.DeleteLabelValues(userID, group)
}

func newSampleValidationMetrics(r prometheus.Registerer) *sampleValidationMetrics {
	return &sampleValidationMetrics{
		missingMetricName:            validation.DiscardedSamplesCounter(r, reasonMissingMetricName),
		invalidMetricName:            validation.DiscardedSamplesCounter(r, reasonInvalidMetricName),
		maxLabelNamesPerSeries:       validation.DiscardedSamplesCounter(r, reasonMaxLabelNamesPerSeries),
		maxLabelNamesPerInfoSeries:   validation.DiscardedSamplesCounter(r, reasonMaxLabelNamesPerInfoSeries),
		invalidLabel:                 validation.DiscardedSamplesCounter(r, reasonInvalidLabel),
		invalidLabelValue:            validation.DiscardedSamplesCounter(r, reasonInvalidLabelValue),
		labelNameTooLong:             validation.DiscardedSamplesCounter(r, reasonLabelNameTooLong),
		labelValueTooLong:            validation.DiscardedSamplesCounter(r, reasonLabelValueTooLong),
		maxNativeHistogramBuckets:    validation.DiscardedSamplesCounter(r, reasonMaxNativeHistogramBuckets),
		invalidNativeHistogramSchema: validation.DiscardedSamplesCounter(r, reasonInvalidNativeHistogramSchema),
		duplicateLabelNames:          validation.DiscardedSamplesCounter(r, reasonDuplicateLabelNames),
		tooFarInFuture:               validation.DiscardedSamplesCounter(r, reasonTooFarInFuture),
		tooFarInPast:                 validation.DiscardedSamplesCounter(r, reasonTooFarInPast),
	}
}

// exemplarValidationMetrics is a collection of metrics used by exemplar validation.
type exemplarValidationMetrics struct {
	labelsMissing    *prometheus.CounterVec
	timestampInvalid *prometheus.CounterVec
	labelsTooLong    *prometheus.CounterVec
	labelsBlank      *prometheus.CounterVec
	tooOld           *prometheus.CounterVec
	tooFarInFuture   *prometheus.CounterVec
	tooManyExemplars *prometheus.CounterVec
}

func (m *exemplarValidationMetrics) deleteUserMetrics(userID string) {
	m.labelsMissing.DeleteLabelValues(userID)
	m.timestampInvalid.DeleteLabelValues(userID)
	m.labelsTooLong.DeleteLabelValues(userID)
	m.labelsBlank.DeleteLabelValues(userID)
	m.tooOld.DeleteLabelValues(userID)
	m.tooFarInFuture.DeleteLabelValues(userID)
	m.tooManyExemplars.DeleteLabelValues(userID)
}

func newExemplarValidationMetrics(r prometheus.Registerer) *exemplarValidationMetrics {
	return &exemplarValidationMetrics{
		labelsMissing:    validation.DiscardedExemplarsCounter(r, reasonExemplarLabelsMissing),
		timestampInvalid: validation.DiscardedExemplarsCounter(r, reasonExemplarTimestampInvalid),
		labelsTooLong:    validation.DiscardedExemplarsCounter(r, reasonExemplarLabelsTooLong),
		labelsBlank:      validation.DiscardedExemplarsCounter(r, reasonExemplarLabelsBlank),
		tooOld:           validation.DiscardedExemplarsCounter(r, reasonExemplarTooOld),
		tooFarInFuture:   validation.DiscardedExemplarsCounter(r, reasonExemplarTooFarInFuture),
		tooManyExemplars: validation.DiscardedExemplarsCounter(r, reasonTooManyExemplarsPerSeriesPerRequest),
	}
}

// validateSample returns an err if the sample is invalid.
// The returned error may retain the provided series labels.
// It uses the passed 'now' time to measure the relative time of the sample.
func validateSample(m *sampleValidationMetrics, now model.Time, cfg sampleValidationConfig, userID, group string, ls []mimirpb.LabelAdapter, s mimirpb.Sample) error {
	if model.Time(s.TimestampMs) > now.Add(cfg.CreationGracePeriod(userID)) {
		m.tooFarInFuture.WithLabelValues(userID, group).Inc()
		unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)
		return fmt.Errorf(sampleTimestampTooNewMsgFormat, s.TimestampMs, unsafeMetricName)
	}

	if cfg.PastGracePeriod(userID) > 0 && model.Time(s.TimestampMs) < now.Add(-cfg.PastGracePeriod(userID)).Add(-cfg.OutOfOrderTimeWindow(userID)) {
		m.tooFarInPast.WithLabelValues(userID, group).Inc()
		unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)
		return fmt.Errorf(sampleTimestampTooOldMsgFormat, s.TimestampMs, unsafeMetricName)
	}

	return nil
}

// validateSampleHistogram returns an err if the sample is invalid.
// The returned error may retain the provided series labels.
// It uses the passed 'now' time to measure the relative time of the sample.
func validateSampleHistogram(m *sampleValidationMetrics, now model.Time, cfg sampleValidationConfig, userID, group string, ls []mimirpb.LabelAdapter, s *mimirpb.Histogram) (bool, error) {
	if model.Time(s.Timestamp) > now.Add(cfg.CreationGracePeriod(userID)) {
		m.tooFarInFuture.WithLabelValues(userID, group).Inc()
		unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)
		return false, fmt.Errorf(sampleTimestampTooNewMsgFormat, s.Timestamp, unsafeMetricName)
	}

	if cfg.PastGracePeriod(userID) > 0 && model.Time(s.Timestamp) < now.Add(-cfg.PastGracePeriod(userID)).Add(-cfg.OutOfOrderTimeWindow(userID)) {
		m.tooFarInPast.WithLabelValues(userID, group).Inc()
		unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)
		return false, fmt.Errorf(sampleTimestampTooOldMsgFormat, s.Timestamp, unsafeMetricName)
	}

	if s.Schema < mimirpb.MinimumHistogramSchema || s.Schema > mimirpb.MaximumHistogramSchema {
		m.invalidNativeHistogramSchema.WithLabelValues(userID, group).Inc()
		return false, fmt.Errorf(invalidSchemaNativeHistogramMsgFormat, s.Schema)
	}

	if bucketLimit := cfg.MaxNativeHistogramBuckets(userID); bucketLimit > 0 {
		var bucketCount int
		if s.IsFloatHistogram() {
			bucketCount = len(s.GetNegativeCounts()) + len(s.GetPositiveCounts())
		} else {
			bucketCount = len(s.GetNegativeDeltas()) + len(s.GetPositiveDeltas())
		}
		if bucketCount > bucketLimit {
			if !cfg.ReduceNativeHistogramOverMaxBuckets(userID) {
				m.maxNativeHistogramBuckets.WithLabelValues(userID, group).Inc()
				return false, fmt.Errorf(maxNativeHistogramBucketsMsgFormat, s.Timestamp, mimirpb.FromLabelAdaptersToString(ls), bucketCount, bucketLimit)
			}

			for {
				bc, err := s.ReduceResolution()
				if err != nil {
					m.maxNativeHistogramBuckets.WithLabelValues(userID, group).Inc()
					return false, fmt.Errorf(notReducibleNativeHistogramMsgFormat, s.Timestamp, mimirpb.FromLabelAdaptersToString(ls), bucketCount, bucketLimit)
				}
				if bc < bucketLimit {
					break
				}
			}

			return true, nil
		}
	}

	return false, nil
}

// validateExemplar returns an error if the exemplar is invalid.
// The returned error may retain the provided series labels.
func validateExemplar(m *exemplarValidationMetrics, userID string, ls []mimirpb.LabelAdapter, e mimirpb.Exemplar) error {
	if len(e.Labels) <= 0 {
		m.labelsMissing.WithLabelValues(userID).Inc()
		return fmt.Errorf(exemplarEmptyLabelsMsgFormat, e.TimestampMs, mimirpb.FromLabelAdaptersToString(ls), mimirpb.FromLabelAdaptersToString([]mimirpb.LabelAdapter{}))
	}

	if e.TimestampMs == 0 {
		m.timestampInvalid.WithLabelValues(userID).Inc()
		return fmt.Errorf(exemplarMissingTimestampMsgFormat, e.TimestampMs, mimirpb.FromLabelAdaptersToString(ls), mimirpb.FromLabelAdaptersToString(e.Labels))
	}

	// Exemplar label length does not include chars involved in text
	// rendering such as quotes, commas, etc.  See spec and const definition.
	labelSetLen := 0
	// We require at least one label to have a non-empty name and value. Otherwise,
	// return an error. Labels with no value are ignored by the Prometheus TSDB
	// exemplar code and will be dropped when stored. We explicitly return an error
	// when there are no valid labels to make bad data easier to spot.
	foundValidLabel := false
	for _, l := range e.Labels {
		if l.Name != "" && l.Value != "" {
			foundValidLabel = true
		}

		labelSetLen += utf8.RuneCountInString(l.Name)
		labelSetLen += utf8.RuneCountInString(l.Value)
	}

	if labelSetLen > ExemplarMaxLabelSetLength {
		m.labelsTooLong.WithLabelValues(userID).Inc()
		return fmt.Errorf(exemplarMaxLabelLengthMsgFormat, e.TimestampMs, mimirpb.FromLabelAdaptersToString(ls), mimirpb.FromLabelAdaptersToString(e.Labels))
	}

	if !foundValidLabel {
		m.labelsBlank.WithLabelValues(userID).Inc()
		return fmt.Errorf(exemplarEmptyLabelsMsgFormat, e.TimestampMs, mimirpb.FromLabelAdaptersToString(ls), mimirpb.FromLabelAdaptersToString(e.Labels))
	}

	return nil
}

// validateExemplarTimestamp returns true if the exemplar timestamp is between minTS and maxTS.
// This is separate from validateExemplar() so we can silently drop old ones, not log an error.
func validateExemplarTimestamp(m *exemplarValidationMetrics, userID string, minTS, maxTS int64, e mimirpb.Exemplar) bool {
	if e.TimestampMs < minTS {
		m.tooOld.WithLabelValues(userID).Inc()
		return false
	}
	if e.TimestampMs > maxTS {
		m.tooFarInFuture.WithLabelValues(userID).Inc()
		return false
	}
	return true
}

// labelValidationConfig helps with getting required config to validate labels.
type labelValidationConfig interface {
	MaxLabelNamesPerSeries(userID string) int
	MaxLabelNamesPerInfoSeries(userID string) int
	MaxLabelNameLength(userID string) int
	MaxLabelValueLength(userID string) int
}

func removeNonASCIIChars(in string) (out string) {
	foundNonASCII := false

	out = strings.Map(func(r rune) rune {
		if r <= unicode.MaxASCII {
			return r
		}

		foundNonASCII = true
		return -1
	}, in)

	if foundNonASCII {
		out = out + " (non-ascii characters removed)"
	}

	return out
}

// validateLabels returns an err if the labels are invalid.
// The returned error may retain the provided series labels.
func validateLabels(m *sampleValidationMetrics, cfg labelValidationConfig, userID, group string, ls []mimirpb.LabelAdapter, skipLabelValidation, skipLabelCountValidation bool) error {
	unsafeMetricName, err := extract.UnsafeMetricNameFromLabelAdapters(ls)
	if err != nil {
		m.missingMetricName.WithLabelValues(userID, group).Inc()
		return errors.New(noMetricNameMsgFormat)
	}

	if !model.IsValidMetricName(model.LabelValue(unsafeMetricName)) {
		m.invalidMetricName.WithLabelValues(userID, group).Inc()
		return fmt.Errorf(invalidMetricNameMsgFormat, removeNonASCIIChars(unsafeMetricName))
	}

	if !skipLabelCountValidation && len(ls) > cfg.MaxLabelNamesPerSeries(userID) {
		if strings.HasSuffix(unsafeMetricName, "_info") {
			if len(ls) > cfg.MaxLabelNamesPerInfoSeries(userID) {
				m.maxLabelNamesPerInfoSeries.WithLabelValues(userID, group).Inc()
				metric, ellipsis := getMetricAndEllipsis(ls)
				return fmt.Errorf(tooManyInfoLabelsMsgFormat, len(ls), cfg.MaxLabelNamesPerInfoSeries(userID), metric, ellipsis)
			}
		} else {
			m.maxLabelNamesPerSeries.WithLabelValues(userID, group).Inc()
			metric, ellipsis := getMetricAndEllipsis(ls)
			return fmt.Errorf(tooManyLabelsMsgFormat, len(ls), cfg.MaxLabelNamesPerSeries(userID), metric, ellipsis)
		}
	}

	maxLabelNameLength := cfg.MaxLabelNameLength(userID)
	maxLabelValueLength := cfg.MaxLabelValueLength(userID)
	lastLabelName := ""
	for _, l := range ls {
		if !skipLabelValidation && !model.LabelName(l.Name).IsValid() {
			m.invalidLabel.WithLabelValues(userID, group).Inc()
			return fmt.Errorf(invalidLabelMsgFormat, l.Name, mimirpb.FromLabelAdaptersToString(ls))
		} else if len(l.Name) > maxLabelNameLength {
			m.labelNameTooLong.WithLabelValues(userID, group).Inc()
			return fmt.Errorf(labelNameTooLongMsgFormat, l.Name, mimirpb.FromLabelAdaptersToString(ls))
		} else if !skipLabelValidation && !model.LabelValue(l.Value).IsValid() {
			m.invalidLabelValue.WithLabelValues(userID, group).Inc()
			return fmt.Errorf(invalidLabelValueMsgFormat, l.Name, strings.ToValidUTF8(l.Value, ""), unsafeMetricName)
		} else if len(l.Value) > maxLabelValueLength {
			m.labelValueTooLong.WithLabelValues(userID, group).Inc()
			return fmt.Errorf(labelValueTooLongMsgFormat, l.Name, l.Value, mimirpb.FromLabelAdaptersToString(ls))
		} else if lastLabelName == l.Name {
			m.duplicateLabelNames.WithLabelValues(userID, group).Inc()
			return fmt.Errorf(duplicateLabelMsgFormat, l.Name, mimirpb.FromLabelAdaptersToString(ls))
		}

		lastLabelName = l.Name
	}
	return nil
}

// metadataValidationMetrics is a collection of metrics used by metadata validation.
type metadataValidationMetrics struct {
	missingMetricName *prometheus.CounterVec
	metricNameTooLong *prometheus.CounterVec
	unitTooLong       *prometheus.CounterVec
}

func (m *metadataValidationMetrics) deleteUserMetrics(userID string) {
	m.missingMetricName.DeleteLabelValues(userID)
	m.metricNameTooLong.DeleteLabelValues(userID)
	m.unitTooLong.DeleteLabelValues(userID)
}

func newMetadataValidationMetrics(r prometheus.Registerer) *metadataValidationMetrics {
	return &metadataValidationMetrics{
		missingMetricName: validation.DiscardedMetadataCounter(r, reasonMissingMetricName),
		metricNameTooLong: validation.DiscardedMetadataCounter(r, reasonMetadataMetricNameTooLong),
		unitTooLong:       validation.DiscardedMetadataCounter(r, reasonMetadataUnitTooLong),
	}
}

// metadataValidationConfig helps with getting required config to validate metadata.
type metadataValidationConfig interface {
	EnforceMetadataMetricName(userID string) bool
	MaxMetadataLength(userID string) int
}

// cleanAndValidateMetadata returns an err if a metric metadata is invalid.
func cleanAndValidateMetadata(m *metadataValidationMetrics, cfg metadataValidationConfig, userID string, metadata *mimirpb.MetricMetadata) error {
	if cfg.EnforceMetadataMetricName(userID) && metadata.GetMetricFamilyName() == "" {
		m.missingMetricName.WithLabelValues(userID).Inc()
		return errors.New(metadataMetricNameMissingMsgFormat)
	}

	maxMetadataValueLength := cfg.MaxMetadataLength(userID)

	if len(metadata.Help) > maxMetadataValueLength {
		newlen := 0
		for idx := range metadata.Help {
			if idx > maxMetadataValueLength {
				break
			}
			newlen = idx // idx is the index of the next character, making it the length of what comes before
		}
		metadata.Help = metadata.Help[:newlen]
	}

	var err error
	if len(metadata.GetMetricFamilyName()) > maxMetadataValueLength {
		m.metricNameTooLong.WithLabelValues(userID).Inc()
		err = fmt.Errorf(metadataMetricNameTooLongMsgFormat, "", metadata.GetMetricFamilyName())
	} else if len(metadata.Unit) > maxMetadataValueLength {
		m.unitTooLong.WithLabelValues(userID).Inc()
		err = fmt.Errorf(metadataUnitTooLongMsgFormat, metadata.GetUnit(), metadata.GetMetricFamilyName())
	}

	return err
}

func getMetricAndEllipsis(ls []mimirpb.LabelAdapter) (string, string) {
	metric := mimirpb.FromLabelAdaptersToString(ls)
	ellipsis := ""

	if utf8.RuneCountInString(metric) > 200 {
		ellipsis = "\u2026"
	}
	return metric, ellipsis
}
