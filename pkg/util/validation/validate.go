// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/validate.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

const (
	discardReasonLabel = "reason"

	// The combined length of the label names and values of an Exemplar's LabelSet MUST NOT exceed 128 UTF-8 characters
	// https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#exemplars
	ExemplarMaxLabelSetLength = 128
)

var (
	// Discarded series / samples reasons.
	reasonMissingMetricName         = globalerror.MissingMetricName.LabelValue()
	reasonInvalidMetricName         = globalerror.InvalidMetricName.LabelValue()
	reasonMaxLabelNamesPerSeries    = globalerror.MaxLabelNamesPerSeries.LabelValue()
	reasonInvalidLabel              = globalerror.SeriesInvalidLabel.LabelValue()
	reasonLabelNameTooLong          = globalerror.SeriesLabelNameTooLong.LabelValue()
	reasonLabelValueTooLong         = globalerror.SeriesLabelValueTooLong.LabelValue()
	reasonMaxNativeHistogramBuckets = globalerror.MaxNativeHistogramBuckets.LabelValue()
	reasonDuplicateLabelNames       = globalerror.SeriesWithDuplicateLabelNames.LabelValue()
	reasonTooFarInFuture            = globalerror.SampleTooFarInFuture.LabelValue()

	// Discarded exemplars reasons.
	reasonExemplarLabelsMissing    = globalerror.ExemplarLabelsMissing.LabelValue()
	reasonExemplarLabelsTooLong    = globalerror.ExemplarLabelsTooLong.LabelValue()
	reasonExemplarTimestampInvalid = globalerror.ExemplarTimestampInvalid.LabelValue()
	reasonExemplarLabelsBlank      = "exemplar_labels_blank"
	reasonExemplarTooOld           = "exemplar_too_old"

	// Discarded metadata reasons.
	reasonMetadataMetricNameTooLong = globalerror.MetricMetadataMetricNameTooLong.LabelValue()
	reasonMetadataUnitTooLong       = globalerror.MetricMetadataUnitTooLong.LabelValue()

	// ReasonRateLimited is one of the values for the reason to discard samples.
	// Declared here to avoid duplication in ingester and distributor.
	ReasonRateLimited = "rate_limited" // same for request and ingestion which are separate errors, so not using metricReasonFromErrorID with global error

	// ReasonTooManyHAClusters is one of the reasons for discarding samples.
	ReasonTooManyHAClusters = "too_many_ha_clusters"
)

// DiscardedRequestsCounter creates per-user counter vector for requests discarded for a given reason.
func DiscardedRequestsCounter(reg prometheus.Registerer, reason string) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(
		prometheus.CounterOpts{
			Name: "cortex_discarded_requests_total",
			Help: "The total number of requests that were discarded due to rate limiting.",
			ConstLabels: map[string]string{
				discardReasonLabel: reason,
			},
		}, []string{"user"})
}

// DiscardedSamplesCounter creates per-user counter vector for samples discarded for a given reason.
func DiscardedSamplesCounter(reg prometheus.Registerer, reason string) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_discarded_samples_total",
		Help: "The total number of samples that were discarded.",
		ConstLabels: map[string]string{
			discardReasonLabel: reason,
		},
	}, []string{"user", "group"})
}

// DiscardedExemplarsCounter creates per-user counter vector for exemplars discarded for a given reason.
func DiscardedExemplarsCounter(reg prometheus.Registerer, reason string) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_discarded_exemplars_total",
		Help: "The total number of exemplars that were discarded.",
		ConstLabels: map[string]string{
			discardReasonLabel: reason,
		},
	}, []string{"user"})
}

// DiscardedMetadataCounter creates per-user counter vector for metadata discarded for a given reason.
func DiscardedMetadataCounter(reg prometheus.Registerer, reason string) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_discarded_metadata_total",
		Help: "The total number of metadata that were discarded.",
		ConstLabels: map[string]string{
			discardReasonLabel: reason,
		},
	}, []string{"user"})
}

// SampleValidationConfig helps with getting required config to validate sample.
type SampleValidationConfig interface {
	CreationGracePeriod(userID string) time.Duration
	MaxNativeHistogramBuckets(userID string) int
}

// SampleValidationMetrics is a collection of metrics used during sample validation.
type SampleValidationMetrics struct {
	missingMetricName         *prometheus.CounterVec
	invalidMetricName         *prometheus.CounterVec
	maxLabelNamesPerSeries    *prometheus.CounterVec
	invalidLabel              *prometheus.CounterVec
	labelNameTooLong          *prometheus.CounterVec
	labelValueTooLong         *prometheus.CounterVec
	maxNativeHistogramBuckets *prometheus.CounterVec
	duplicateLabelNames       *prometheus.CounterVec
	tooFarInFuture            *prometheus.CounterVec
}

func (m *SampleValidationMetrics) DeleteUserMetrics(userID string) {
	filter := prometheus.Labels{"user": userID}
	m.missingMetricName.DeletePartialMatch(filter)
	m.invalidMetricName.DeletePartialMatch(filter)
	m.maxLabelNamesPerSeries.DeletePartialMatch(filter)
	m.invalidLabel.DeletePartialMatch(filter)
	m.labelNameTooLong.DeletePartialMatch(filter)
	m.labelValueTooLong.DeletePartialMatch(filter)
	m.maxNativeHistogramBuckets.DeletePartialMatch(filter)
	m.duplicateLabelNames.DeletePartialMatch(filter)
	m.tooFarInFuture.DeletePartialMatch(filter)
}

func (m *SampleValidationMetrics) DeleteUserMetricsForGroup(userID, group string) {
	m.missingMetricName.DeleteLabelValues(userID, group)
	m.invalidMetricName.DeleteLabelValues(userID, group)
	m.maxLabelNamesPerSeries.DeleteLabelValues(userID, group)
	m.invalidLabel.DeleteLabelValues(userID, group)
	m.labelNameTooLong.DeleteLabelValues(userID, group)
	m.labelValueTooLong.DeleteLabelValues(userID, group)
	m.maxNativeHistogramBuckets.DeleteLabelValues(userID, group)
	m.duplicateLabelNames.DeleteLabelValues(userID, group)
	m.tooFarInFuture.DeleteLabelValues(userID, group)
}

func NewSampleValidationMetrics(r prometheus.Registerer) *SampleValidationMetrics {
	return &SampleValidationMetrics{
		missingMetricName:         DiscardedSamplesCounter(r, reasonMissingMetricName),
		invalidMetricName:         DiscardedSamplesCounter(r, reasonInvalidMetricName),
		maxLabelNamesPerSeries:    DiscardedSamplesCounter(r, reasonMaxLabelNamesPerSeries),
		invalidLabel:              DiscardedSamplesCounter(r, reasonInvalidLabel),
		labelNameTooLong:          DiscardedSamplesCounter(r, reasonLabelNameTooLong),
		labelValueTooLong:         DiscardedSamplesCounter(r, reasonLabelValueTooLong),
		maxNativeHistogramBuckets: DiscardedSamplesCounter(r, reasonMaxNativeHistogramBuckets),
		duplicateLabelNames:       DiscardedSamplesCounter(r, reasonDuplicateLabelNames),
		tooFarInFuture:            DiscardedSamplesCounter(r, reasonTooFarInFuture),
	}
}

// ExemplarValidationMetrics is a collection of metrics used by exemplar validation.
type ExemplarValidationMetrics struct {
	labelsMissing    *prometheus.CounterVec
	timestampInvalid *prometheus.CounterVec
	labelsTooLong    *prometheus.CounterVec
	labelsBlank      *prometheus.CounterVec
	tooOld           *prometheus.CounterVec
}

func (m *ExemplarValidationMetrics) DeleteUserMetrics(userID string) {
	m.labelsMissing.DeleteLabelValues(userID)
	m.timestampInvalid.DeleteLabelValues(userID)
	m.labelsTooLong.DeleteLabelValues(userID)
	m.labelsBlank.DeleteLabelValues(userID)
	m.tooOld.DeleteLabelValues(userID)
}

func NewExemplarValidationMetrics(r prometheus.Registerer) *ExemplarValidationMetrics {
	return &ExemplarValidationMetrics{
		labelsMissing:    DiscardedExemplarsCounter(r, reasonExemplarLabelsMissing),
		timestampInvalid: DiscardedExemplarsCounter(r, reasonExemplarTimestampInvalid),
		labelsTooLong:    DiscardedExemplarsCounter(r, reasonExemplarLabelsTooLong),
		labelsBlank:      DiscardedExemplarsCounter(r, reasonExemplarLabelsBlank),
		tooOld:           DiscardedExemplarsCounter(r, reasonExemplarTooOld),
	}
}

// ValidateSample returns an err if the sample is invalid.
// The returned error may retain the provided series labels.
// It uses the passed 'now' time to measure the relative time of the sample.
func ValidateSample(m *SampleValidationMetrics, now model.Time, cfg SampleValidationConfig, userID, group string, ls []mimirpb.LabelAdapter, s mimirpb.Sample) ValidationError {
	if model.Time(s.TimestampMs) > now.Add(cfg.CreationGracePeriod(userID)) {
		m.tooFarInFuture.WithLabelValues(userID, group).Inc()
		unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)
		return newSampleTimestampTooNewError(unsafeMetricName, s.TimestampMs)
	}

	return nil
}

// ValidateSampleHistogram returns an err if the sample is invalid.
// The returned error may retain the provided series labels.
// It uses the passed 'now' time to measure the relative time of the sample.
func ValidateSampleHistogram(m *SampleValidationMetrics, now model.Time, cfg SampleValidationConfig, userID, group string, ls []mimirpb.LabelAdapter, s mimirpb.Histogram) ValidationError {
	if model.Time(s.Timestamp) > now.Add(cfg.CreationGracePeriod(userID)) {
		m.tooFarInFuture.WithLabelValues(userID, group).Inc()
		unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)
		return newSampleTimestampTooNewError(unsafeMetricName, s.Timestamp)
	}

	if bucketLimit := cfg.MaxNativeHistogramBuckets(userID); bucketLimit > 0 {
		var bucketCount int
		if s.IsFloatHistogram() {
			bucketCount = len(s.GetNegativeCounts()) + len(s.GetPositiveCounts())
		} else {
			bucketCount = len(s.GetNegativeDeltas()) + len(s.GetPositiveDeltas())
		}
		if bucketCount > bucketLimit {
			m.maxNativeHistogramBuckets.WithLabelValues(userID, group).Inc()
			return newMaxNativeHistogramBucketsError(ls, s.Timestamp, bucketCount, bucketLimit)
		}
	}

	return nil
}

// ValidateExemplar returns an error if the exemplar is invalid.
// The returned error may retain the provided series labels.
func ValidateExemplar(m *ExemplarValidationMetrics, userID string, ls []mimirpb.LabelAdapter, e mimirpb.Exemplar) ValidationError {
	if len(e.Labels) <= 0 {
		m.labelsMissing.WithLabelValues(userID).Inc()
		return newExemplarEmptyLabelsError(ls, []mimirpb.LabelAdapter{}, e.TimestampMs)
	}

	if e.TimestampMs == 0 {
		m.timestampInvalid.WithLabelValues(userID).Inc()
		return newExemplarMissingTimestampError(
			ls,
			e.Labels,
			e.TimestampMs,
		)
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
		return newExemplarMaxLabelLengthError(
			ls,
			e.Labels,
			e.TimestampMs,
		)
	}

	if !foundValidLabel {
		m.labelsBlank.WithLabelValues(userID).Inc()
		return newExemplarEmptyLabelsError(ls, e.Labels, e.TimestampMs)
	}

	return nil
}

// ExemplarTimestampOK returns true if the timestamp is newer than minTS.
// This is separate from ValidateExemplar() so we can silently drop old ones, not log an error.
func ExemplarTimestampOK(m *ExemplarValidationMetrics, userID string, minTS int64, e mimirpb.Exemplar) bool {
	if e.TimestampMs < minTS {
		m.tooOld.WithLabelValues(userID).Inc()
		return false
	}
	return true
}

// LabelValidationConfig helps with getting required config to validate labels.
type LabelValidationConfig interface {
	MaxLabelNamesPerSeries(userID string) int
	MaxLabelNameLength(userID string) int
	MaxLabelValueLength(userID string) int
}

// ValidateLabels returns an err if the labels are invalid.
// The returned error may retain the provided series labels.
func ValidateLabels(m *SampleValidationMetrics, cfg LabelValidationConfig, userID, group string, ls []mimirpb.LabelAdapter, skipLabelNameValidation bool) ValidationError {
	_, err := extract.UnsafeMetricNameFromLabelAdapters(ls)
	if err != nil {
		m.missingMetricName.WithLabelValues(userID, group).Inc()
		return newNoMetricNameError()
	}

	// if !model.IsValidMetricName(model.LabelValue(unsafeMetricName)) {
	// 	m.invalidMetricName.WithLabelValues(userID, group).Inc()
	// 	return newInvalidMetricNameError(unsafeMetricName)
	// }

	numLabelNames := len(ls)
	if numLabelNames > cfg.MaxLabelNamesPerSeries(userID) {
		m.maxLabelNamesPerSeries.WithLabelValues(userID, group).Inc()
		return newTooManyLabelsError(ls, cfg.MaxLabelNamesPerSeries(userID))
	}

	maxLabelNameLength := cfg.MaxLabelNameLength(userID)
	maxLabelValueLength := cfg.MaxLabelValueLength(userID)
	lastLabelName := ""
	for _, l := range ls {
		if !skipLabelNameValidation && !model.LabelName(l.Name).IsValid() {
			m.invalidLabel.WithLabelValues(userID, group).Inc()
			return newInvalidLabelError(ls, l.Name)
		} else if len(l.Name) > maxLabelNameLength {
			m.labelNameTooLong.WithLabelValues(userID, group).Inc()
			return newLabelNameTooLongError(ls, l.Name)
		} else if len(l.Value) > maxLabelValueLength {
			m.labelValueTooLong.WithLabelValues(userID, group).Inc()
			return newLabelValueTooLongError(ls, l.Value)
		} else if lastLabelName == l.Name {
			m.duplicateLabelNames.WithLabelValues(userID, group).Inc()
			return newDuplicatedLabelError(ls, l.Name)
		}

		lastLabelName = l.Name
	}
	return nil
}

// MetadataValidationMetrics is a collection of metrics used by metadata validation.
type MetadataValidationMetrics struct {
	missingMetricName *prometheus.CounterVec
	metricNameTooLong *prometheus.CounterVec
	unitTooLong       *prometheus.CounterVec
}

func (m *MetadataValidationMetrics) DeleteUserMetrics(userID string) {
	m.missingMetricName.DeleteLabelValues(userID)
	m.metricNameTooLong.DeleteLabelValues(userID)
	m.unitTooLong.DeleteLabelValues(userID)
}

func NewMetadataValidationMetrics(r prometheus.Registerer) *MetadataValidationMetrics {
	return &MetadataValidationMetrics{
		missingMetricName: DiscardedMetadataCounter(r, reasonMissingMetricName),
		metricNameTooLong: DiscardedMetadataCounter(r, reasonMetadataMetricNameTooLong),
		unitTooLong:       DiscardedMetadataCounter(r, reasonMetadataUnitTooLong),
	}
}

// MetadataValidationConfig helps with getting required config to validate metadata.
type MetadataValidationConfig interface {
	EnforceMetadataMetricName(userID string) bool
	MaxMetadataLength(userID string) int
}

// CleanAndValidateMetadata returns an err if a metric metadata is invalid.
func CleanAndValidateMetadata(m *MetadataValidationMetrics, cfg MetadataValidationConfig, userID string, metadata *mimirpb.MetricMetadata) error {
	if cfg.EnforceMetadataMetricName(userID) && metadata.GetMetricFamilyName() == "" {
		m.missingMetricName.WithLabelValues(userID).Inc()
		return newMetadataMetricNameMissingError()
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
		err = newMetadataMetricNameTooLongError(metadata)
	} else if len(metadata.Unit) > maxMetadataValueLength {
		m.unitTooLong.WithLabelValues(userID).Inc()
		err = newMetadataUnitTooLongError(metadata)
	}

	return err
}
