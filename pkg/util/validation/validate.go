// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/validate.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"strings"
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
	reasonMissingMetricName      = metricReasonFromErrorID(globalerror.MissingMetricName)
	reasonInvalidMetricName      = metricReasonFromErrorID(globalerror.InvalidMetricName)
	reasonMaxLabelNamesPerSeries = metricReasonFromErrorID(globalerror.MaxLabelNamesPerSeries)
	reasonInvalidLabel           = metricReasonFromErrorID(globalerror.SeriesInvalidLabel)
	reasonLabelNameTooLong       = metricReasonFromErrorID(globalerror.SeriesLabelNameTooLong)
	reasonLabelValueTooLong      = metricReasonFromErrorID(globalerror.SeriesLabelValueTooLong)
	reasonDuplicateLabelNames    = metricReasonFromErrorID(globalerror.SeriesWithDuplicateLabelNames)
	reasonLabelsNotSorted        = metricReasonFromErrorID(globalerror.SeriesLabelsNotSorted)
	reasonTooFarInFuture         = metricReasonFromErrorID(globalerror.SampleTooFarInFuture)

	// Discarded exemplars reasons.
	reasonExemplarLabelsMissing    = metricReasonFromErrorID(globalerror.ExemplarLabelsMissing)
	reasonExemplarLabelsTooLong    = metricReasonFromErrorID(globalerror.ExemplarLabelsTooLong)
	reasonExemplarTimestampInvalid = metricReasonFromErrorID(globalerror.ExemplarTimestampInvalid)
	reasonExemplarLabelsBlank      = "exemplar_labels_blank"
	reasonExemplarTooOld           = "exemplar_too_old"

	// Discarded metadata reasons.
	reasonMetadataMetricNameTooLong = metricReasonFromErrorID(globalerror.MetricMetadataMetricNameTooLong)
	reasonMetadataUnitTooLong       = metricReasonFromErrorID(globalerror.MetricMetadataUnitTooLong)

	// ReasonRateLimited is one of the values for the reason to discard samples.
	// Declared here to avoid duplication in ingester and distributor.
	ReasonRateLimited = "rate_limited" // same for request and ingestion which are separate errors, so not using metricReasonFromErrorID with global error

	// ReasonTooManyHAClusters is one of the reasons for discarding samples.
	ReasonTooManyHAClusters = "too_many_ha_clusters"
)

func metricReasonFromErrorID(id globalerror.ID) string {
	return strings.ReplaceAll(string(id), "-", "_")
}

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
	}, []string{"user"})
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
}

// SampleValidationMetrics is a collection of metrics used during sample validation.
type SampleValidationMetrics struct {
	missingMetricName      *prometheus.CounterVec
	invalidMetricName      *prometheus.CounterVec
	maxLabelNamesPerSeries *prometheus.CounterVec
	invalidLabel           *prometheus.CounterVec
	labelNameTooLong       *prometheus.CounterVec
	labelValueTooLong      *prometheus.CounterVec
	duplicateLabelNames    *prometheus.CounterVec
	labelsNotSorted        *prometheus.CounterVec
	tooFarInFuture         *prometheus.CounterVec
}

func (m *SampleValidationMetrics) DeleteUserMetrics(userID string) {
	m.missingMetricName.DeleteLabelValues(userID)
	m.invalidMetricName.DeleteLabelValues(userID)
	m.maxLabelNamesPerSeries.DeleteLabelValues(userID)
	m.invalidLabel.DeleteLabelValues(userID)
	m.labelNameTooLong.DeleteLabelValues(userID)
	m.labelValueTooLong.DeleteLabelValues(userID)
	m.duplicateLabelNames.DeleteLabelValues(userID)
	m.labelsNotSorted.DeleteLabelValues(userID)
	m.tooFarInFuture.DeleteLabelValues(userID)
}

func NewSampleValidationMetrics(r prometheus.Registerer) *SampleValidationMetrics {
	return &SampleValidationMetrics{
		missingMetricName:      DiscardedSamplesCounter(r, reasonMissingMetricName),
		invalidMetricName:      DiscardedSamplesCounter(r, reasonInvalidMetricName),
		maxLabelNamesPerSeries: DiscardedSamplesCounter(r, reasonMaxLabelNamesPerSeries),
		invalidLabel:           DiscardedSamplesCounter(r, reasonInvalidLabel),
		labelNameTooLong:       DiscardedSamplesCounter(r, reasonLabelNameTooLong),
		labelValueTooLong:      DiscardedSamplesCounter(r, reasonLabelValueTooLong),
		duplicateLabelNames:    DiscardedSamplesCounter(r, reasonDuplicateLabelNames),
		labelsNotSorted:        DiscardedSamplesCounter(r, reasonLabelsNotSorted),
		tooFarInFuture:         DiscardedSamplesCounter(r, reasonTooFarInFuture),
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
func ValidateSample(m *SampleValidationMetrics, now model.Time, cfg SampleValidationConfig, userID string, ls []mimirpb.LabelAdapter, s mimirpb.Sample) ValidationError {
	unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)

	if model.Time(s.TimestampMs) > now.Add(cfg.CreationGracePeriod(userID)) {
		m.tooFarInFuture.WithLabelValues(userID).Inc()
		return newSampleTimestampTooNewError(unsafeMetricName, s.TimestampMs)
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
func ValidateLabels(m *SampleValidationMetrics, cfg LabelValidationConfig, userID string, ls []mimirpb.LabelAdapter, skipLabelNameValidation bool) ValidationError {
	unsafeMetricName, err := extract.UnsafeMetricNameFromLabelAdapters(ls)
	if err != nil {
		m.missingMetricName.WithLabelValues(userID).Inc()
		return newNoMetricNameError()
	}

	if !model.IsValidMetricName(model.LabelValue(unsafeMetricName)) {
		m.invalidMetricName.WithLabelValues(userID).Inc()
		return newInvalidMetricNameError(unsafeMetricName)
	}

	numLabelNames := len(ls)
	if numLabelNames > cfg.MaxLabelNamesPerSeries(userID) {
		m.maxLabelNamesPerSeries.WithLabelValues(userID).Inc()
		return newTooManyLabelsError(ls, cfg.MaxLabelNamesPerSeries(userID))
	}

	maxLabelNameLength := cfg.MaxLabelNameLength(userID)
	maxLabelValueLength := cfg.MaxLabelValueLength(userID)
	lastLabelName := ""
	for _, l := range ls {
		if !skipLabelNameValidation && !model.LabelName(l.Name).IsValid() {
			m.invalidLabel.WithLabelValues(userID).Inc()
			return newInvalidLabelError(ls, l.Name)
		} else if len(l.Name) > maxLabelNameLength {
			m.labelNameTooLong.WithLabelValues(userID).Inc()
			return newLabelNameTooLongError(ls, l.Name)
		} else if len(l.Value) > maxLabelValueLength {
			m.labelValueTooLong.WithLabelValues(userID).Inc()
			return newLabelValueTooLongError(ls, l.Value)
		} else if lastLabelName == l.Name {
			m.duplicateLabelNames.WithLabelValues(userID).Inc()
			return newDuplicatedLabelError(ls, l.Name)
		} else if lastLabelName > l.Name {
			m.labelsNotSorted.WithLabelValues(userID).Inc()
			return newLabelsNotSortedError(ls, l.Name)
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
