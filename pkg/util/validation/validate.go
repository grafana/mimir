// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/validate.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
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
	reasonMetadataHelpTooLong       = metricReasonFromErrorID(globalerror.MetricMetadataHelpTooLong)
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

// DiscardedRequests is a metric of the number of discarded requests.
var DiscardedRequests = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "cortex_discarded_requests_total",
		Help: "The total number of requests that were discarded due to rate limiting.",
	},
	[]string{discardReasonLabel, "user"},
)

// DiscardedSamples is a metric of the number of discarded samples, by reason.
var DiscardedSamples = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "cortex_discarded_samples_total",
		Help: "The total number of samples that were discarded.",
	},
	[]string{discardReasonLabel, "user"},
)

// DiscardedExemplars is a metric of the number of discarded exemplars, by reason.
var DiscardedExemplars = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "cortex_discarded_exemplars_total",
		Help: "The total number of exemplars that were discarded.",
	},
	[]string{discardReasonLabel, "user"},
)

// DiscardedMetadata is a metric of the number of discarded metadata, by reason.
var DiscardedMetadata = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "cortex_discarded_metadata_total",
		Help: "The total number of metadata that were discarded.",
	},
	[]string{discardReasonLabel, "user"},
)

func init() {
	prometheus.MustRegister(DiscardedSamples)
	prometheus.MustRegister(DiscardedExemplars)
	prometheus.MustRegister(DiscardedMetadata)
}

// SampleValidationConfig helps with getting required config to validate sample.
type SampleValidationConfig interface {
	CreationGracePeriod(userID string) time.Duration
}

// ValidateSample returns an err if the sample is invalid.
// The returned error may retain the provided series labels.
// It uses the passed 'now' time to measure the relative time of the sample.
func ValidateSample(now model.Time, cfg SampleValidationConfig, userID string, ls []mimirpb.LabelAdapter, s mimirpb.Sample) ValidationError {
	unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)

	if model.Time(s.TimestampMs) > now.Add(cfg.CreationGracePeriod(userID)) {
		DiscardedSamples.WithLabelValues(reasonTooFarInFuture, userID).Inc()
		return newSampleTimestampTooNewError(unsafeMetricName, s.TimestampMs)
	}

	return nil
}

// ValidateExemplar returns an error if the exemplar is invalid.
// The returned error may retain the provided series labels.
func ValidateExemplar(userID string, ls []mimirpb.LabelAdapter, e mimirpb.Exemplar) ValidationError {
	if len(e.Labels) <= 0 {
		DiscardedExemplars.WithLabelValues(reasonExemplarLabelsMissing, userID).Inc()
		return newExemplarEmptyLabelsError(ls, []mimirpb.LabelAdapter{}, e.TimestampMs)
	}

	if e.TimestampMs == 0 {
		DiscardedExemplars.WithLabelValues(reasonExemplarTimestampInvalid, userID).Inc()
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
		DiscardedExemplars.WithLabelValues(reasonExemplarLabelsTooLong, userID).Inc()
		return newExemplarMaxLabelLengthError(
			ls,
			e.Labels,
			e.TimestampMs,
		)
	}

	if !foundValidLabel {
		DiscardedExemplars.WithLabelValues(reasonExemplarLabelsBlank, userID).Inc()
		return newExemplarEmptyLabelsError(ls, e.Labels, e.TimestampMs)
	}

	return nil
}

// ExemplarTimestampOK returns true if the timestamp is newer than minTS.
// This is separate from ValidateExemplar() so we can silently drop old ones, not log an error.
func ExemplarTimestampOK(userID string, minTS int64, e mimirpb.Exemplar) bool {
	if e.TimestampMs < minTS {
		DiscardedExemplars.WithLabelValues(reasonExemplarTooOld, userID).Inc()
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
func ValidateLabels(cfg LabelValidationConfig, userID string, ls []mimirpb.LabelAdapter, skipLabelNameValidation bool) ValidationError {
	unsafeMetricName, err := extract.UnsafeMetricNameFromLabelAdapters(ls)
	if err != nil {
		DiscardedSamples.WithLabelValues(reasonMissingMetricName, userID).Inc()
		return newNoMetricNameError()
	}

	if !model.IsValidMetricName(model.LabelValue(unsafeMetricName)) {
		DiscardedSamples.WithLabelValues(reasonInvalidMetricName, userID).Inc()
		return newInvalidMetricNameError(unsafeMetricName)
	}

	numLabelNames := len(ls)
	if numLabelNames > cfg.MaxLabelNamesPerSeries(userID) {
		DiscardedSamples.WithLabelValues(reasonMaxLabelNamesPerSeries, userID).Inc()
		return newTooManyLabelsError(ls, cfg.MaxLabelNamesPerSeries(userID))
	}

	maxLabelNameLength := cfg.MaxLabelNameLength(userID)
	maxLabelValueLength := cfg.MaxLabelValueLength(userID)
	lastLabelName := ""
	for _, l := range ls {
		if !skipLabelNameValidation && !model.LabelName(l.Name).IsValid() {
			DiscardedSamples.WithLabelValues(reasonInvalidLabel, userID).Inc()
			return newInvalidLabelError(ls, l.Name)
		} else if len(l.Name) > maxLabelNameLength {
			DiscardedSamples.WithLabelValues(reasonLabelNameTooLong, userID).Inc()
			return newLabelNameTooLongError(ls, l.Name)
		} else if len(l.Value) > maxLabelValueLength {
			DiscardedSamples.WithLabelValues(reasonLabelValueTooLong, userID).Inc()
			return newLabelValueTooLongError(ls, l.Value)
		} else if lastLabelName == l.Name {
			DiscardedSamples.WithLabelValues(reasonDuplicateLabelNames, userID).Inc()
			return newDuplicatedLabelError(ls, l.Name)
		} else if lastLabelName > l.Name {
			DiscardedSamples.WithLabelValues(reasonLabelsNotSorted, userID).Inc()
			return newLabelsNotSortedError(ls, l.Name)
		}

		lastLabelName = l.Name
	}
	return nil
}

// MetadataValidationConfig helps with getting required config to validate metadata.
type MetadataValidationConfig interface {
	EnforceMetadataMetricName(userID string) bool
	MaxMetadataLength(userID string) int
}

// ValidateMetadata returns an err if a metric metadata is invalid.
func ValidateMetadata(cfg MetadataValidationConfig, userID string, metadata *mimirpb.MetricMetadata) error {
	if cfg.EnforceMetadataMetricName(userID) && metadata.GetMetricFamilyName() == "" {
		DiscardedMetadata.WithLabelValues(reasonMissingMetricName, userID).Inc()
		return newMetadataMetricNameMissingError()
	}

	maxMetadataValueLength := cfg.MaxMetadataLength(userID)
	var reason string
	var err error
	if len(metadata.GetMetricFamilyName()) > maxMetadataValueLength {
		reason = reasonMetadataMetricNameTooLong
		err = newMetadataMetricNameTooLongError(metadata)
	} else if len(metadata.Help) > maxMetadataValueLength {
		reason = reasonMetadataHelpTooLong
		err = newMetadataHelpTooLongError(metadata)
	} else if len(metadata.Unit) > maxMetadataValueLength {
		reason = reasonMetadataUnitTooLong
		err = newMetadataUnitTooLongError(metadata)
	}

	if err != nil {
		DiscardedMetadata.WithLabelValues(reason, userID).Inc()
		return err
	}

	return nil
}

func DeletePerUserValidationMetrics(userID string, log log.Logger) {
	filter := map[string]string{"user": userID}

	if err := util.DeleteMatchingLabels(DiscardedRequests, filter); err != nil {
		level.Warn(log).Log("msg", "failed to remove cortex_discarded_requests_total metric for user", "user", userID, "err", err)
	}
	if err := util.DeleteMatchingLabels(DiscardedSamples, filter); err != nil {
		level.Warn(log).Log("msg", "failed to remove cortex_discarded_samples_total metric for user", "user", userID, "err", err)
	}
	if err := util.DeleteMatchingLabels(DiscardedExemplars, filter); err != nil {
		level.Warn(log).Log("msg", "failed to remove cortex_discarded_exemplars_total metric for user", "user", userID, "err", err)
	}
	if err := util.DeleteMatchingLabels(DiscardedMetadata, filter); err != nil {
		level.Warn(log).Log("msg", "failed to remove cortex_discarded_metadata_total metric for user", "user", userID, "err", err)
	}
}
