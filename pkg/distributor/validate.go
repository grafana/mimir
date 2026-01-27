// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/validate.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/crypto/blake2b"

	"github.com/grafana/mimir/pkg/costattribution"
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
	reasonDuplicateTimestamp           = globalerror.SampleDuplicateTimestamp.LabelValue()

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

	// reasonPerUserActiveSeriesLimit differs from the "per_user_series_limit" that ingesters use.
	reasonPerUserActiveSeriesLimit = "per_user_active_series_limit"

	labelNameTooLongMsgFormat = globalerror.SeriesLabelNameTooLong.MessageWithPerTenantLimitConfig(
		"received a series whose label name length exceeds the limit, label: '%.200s' series: '%.200s'",
		validation.MaxLabelNameLengthFlag,
	)
	labelValueTooLongMsgFormat = globalerror.SeriesLabelValueTooLong.MessageWithPerTenantLimitConfig(
		"received a series whose label value length of %d exceeds the limit of %d, label: '%s', value: '%.200s' (truncated) series: '%.200s'",
		validation.MaxLabelValueLengthFlag,
	)
	truncatedLabelValueMsg = globalerror.SeriesLabelValueTooLong.MessageWithPerTenantLimitConfig(
		"received some series whose label value lengths exceed the limit; label values were truncated and appended their hash value",
		validation.MaxLabelValueLengthFlag,
	)
	droppedLabelValueMsg = globalerror.SeriesLabelValueTooLong.MessageWithPerTenantLimitConfig(
		"received some series whose label value lengths exceed the limit; label values were replaced by their hash value",
		validation.MaxLabelValueLengthFlag,
	)
	invalidLabelMsgFormat      = globalerror.SeriesInvalidLabel.Message("received a series with an invalid label: '%.200s' series: '%.200s'")
	invalidLabelValueMsgFormat = globalerror.SeriesInvalidLabelValue.Message("received a series with invalid value in label '%.200s': '%.200s' metric: '%.200s'")
	duplicateLabelMsgFormat    = globalerror.SeriesWithDuplicateLabelNames.Message("received a series with duplicate label name, label: '%.200s' series: '%.200s'")

	tooManyLabelsMsgFormat = globalerror.MaxLabelNamesPerSeries.MessageWithPerTenantLimitConfig(
		"received a series whose number of labels exceeds the limit (actual: %d, limit: %d) series: '%.200s%s'",
		validation.MaxLabelNamesPerSeriesFlag,
	)
	tooManyInfoLabelsMsgFormat = globalerror.MaxLabelNamesPerInfoSeries.MessageWithPerTenantLimitConfig(
		"received an info series whose number of labels exceeds the limit (actual: %d, limit: %d) series: '%.200s%s'",
		validation.MaxLabelNamesPerInfoSeriesFlag,
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
	nativeHistogramCustomBucketsNotReducibleMsgFormat = globalerror.NativeHistogramCustomBucketsNotReducible.Message("received a native histogram sample with more custom buckets than the limit, timestamp: %d series: %s, buckets: %d, limit: %d")
)

const labelValueTooLongSummariesLimit = 10

type labelValueTooLongError struct {
	Label  labels.Label
	Series string
	Limit  int
}

func (e labelValueTooLongError) Error() string {
	return fmt.Sprintf(labelValueTooLongMsgFormat, len(e.Label.Value), e.Limit, e.Label.Name, e.Label.Value, e.Series)
}

// labelValueTooLongSummaries holds a summary for each metric and label processed
// in a write request that have values exceeding the limit.
type labelValueTooLongSummaries struct {
	globalCount int
	summaries   []labelValueTooLongSummary
}

func (s *labelValueTooLongSummaries) measure(unsafeMetric, unsafeLabel, unsafeValue mimirpb.UnsafeMutableString) {
	if s == nil {
		return
	}
	s.globalCount++
	if s.summaries == nil {
		s.summaries = make([]labelValueTooLongSummary, 0, labelValueTooLongSummariesLimit)
	}

	i := slices.IndexFunc(s.summaries, func(summary labelValueTooLongSummary) bool {
		return summary.metric == unsafeMetric && summary.label == unsafeLabel
	})
	if i != -1 {
		s.summaries[i].count++
		return
	}

	if len(s.summaries) >= labelValueTooLongSummariesLimit {
		return
	}

	s.summaries = append(s.summaries, labelValueTooLongSummary{
		metric:            strings.Clone(unsafeMetric),
		label:             strings.Clone(unsafeLabel),
		sampleValue:       fmt.Sprintf("%.200s (truncated)", unsafeValue),
		sampleValueLength: len(unsafeValue),
		count:             1,
	})
}

type labelValueTooLongSummary struct {
	metric, label     string
	count             int
	sampleValueLength int
	sampleValue       string
}

type validationConfig struct {
	samples  sampleValidationConfig
	labels   labelValidationConfig
	metadata metadataValidationConfig
}

// newValidationConfig builds a validationConfig based on the passed overrides.
// TODO: This could still be more efficient, as each overrides call performs an atomic pointer retrieval and a lookup in a map,
// TODO: but it's already better than the previous implementation, which was doing this per-sample and per-series.
func newValidationConfig(userID string, overrides *validation.Overrides) validationConfig {
	return validationConfig{
		samples: sampleValidationConfig{
			creationGracePeriod:                 overrides.CreationGracePeriod(userID),
			pastGracePeriod:                     overrides.PastGracePeriod(userID),
			maxNativeHistogramBuckets:           overrides.MaxNativeHistogramBuckets(userID),
			reduceNativeHistogramOverMaxBuckets: overrides.ReduceNativeHistogramOverMaxBuckets(userID),
			outOfOrderTimeWindow:                overrides.OutOfOrderTimeWindow(userID),
		},
		labels: labelValidationConfig{
			maxLabelNamesPerSeries:            overrides.MaxLabelNamesPerSeries(userID),
			maxLabelNamesPerInfoSeries:        overrides.MaxLabelNamesPerInfoSeries(userID),
			maxLabelNameLength:                overrides.MaxLabelNameLength(userID),
			maxLabelValueLength:               overrides.MaxLabelValueLength(userID),
			labelValueLengthOverLimitStrategy: overrides.LabelValueLengthOverLimitStrategy(userID),
			nameValidationScheme:              overrides.NameValidationScheme(userID),
		},
		metadata: metadataValidationConfig{
			enforceMetadataMetricName: overrides.EnforceMetadataMetricName(userID),
			maxMetadataLength:         overrides.MaxMetadataLength(userID),
		},
	}
}

// sampleValidationConfig helps with getting required config to validate sample.
type sampleValidationConfig struct {
	creationGracePeriod                 time.Duration
	pastGracePeriod                     time.Duration
	maxNativeHistogramBuckets           int
	reduceNativeHistogramOverMaxBuckets bool
	outOfOrderTimeWindow                time.Duration
}

// labelValidationConfig helps with getting required config to validate labels.
type labelValidationConfig struct {
	maxLabelNamesPerSeries            int
	maxLabelNamesPerInfoSeries        int
	maxLabelNameLength                int
	maxLabelValueLength               int
	labelValueLengthOverLimitStrategy validation.LabelValueLengthOverLimitStrategy
	nameValidationScheme              model.ValidationScheme
}

// metadataValidationConfig helps with getting required config to validate metadata.
type metadataValidationConfig struct {
	enforceMetadataMetricName bool
	maxMetadataLength         int
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
	duplicateTimestamp           *prometheus.CounterVec
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
	m.duplicateTimestamp.DeletePartialMatch(filter)
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
	m.duplicateTimestamp.DeleteLabelValues(userID, group)
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
		duplicateTimestamp:           validation.DiscardedSamplesCounter(r, reasonDuplicateTimestamp),
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
// The returned error MUST NOT retain label strings - they point into a gRPC buffer which is re-used.
// It uses the passed 'now' time to measure the relative time of the sample.
func validateSample(m *sampleValidationMetrics, now model.Time, cfg sampleValidationConfig, userID, group string, ls []mimirpb.UnsafeMutableLabel, s mimirpb.Sample, cat *costattribution.SampleTracker) error {
	if model.Time(s.TimestampMs) > now.Add(cfg.creationGracePeriod) {
		m.tooFarInFuture.WithLabelValues(userID, group).Inc()
		cat.IncrementDiscardedSamples(ls, 1, reasonTooFarInFuture, now.Time())
		unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)
		return fmt.Errorf(sampleTimestampTooNewMsgFormat, s.TimestampMs, unsafeMetricName)
	}

	if cfg.pastGracePeriod > 0 && model.Time(s.TimestampMs) < now.Add(-cfg.pastGracePeriod).Add(-cfg.outOfOrderTimeWindow) {
		m.tooFarInPast.WithLabelValues(userID, group).Inc()
		cat.IncrementDiscardedSamples(ls, 1, reasonTooFarInPast, now.Time())
		unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)
		return fmt.Errorf(sampleTimestampTooOldMsgFormat, s.TimestampMs, unsafeMetricName)
	}

	return nil
}

// validateSampleHistogram returns an err if the sample is invalid.
// The returned error MUST NOT retain label strings - they point into a gRPC buffer which is re-used.
// It uses the passed 'now' time to measure the relative time of the sample.
func validateSampleHistogram(m *sampleValidationMetrics, now model.Time, cfg sampleValidationConfig, userID, group string, ls []mimirpb.UnsafeMutableLabel, s *mimirpb.Histogram, cat *costattribution.SampleTracker) (bool, error) {
	if model.Time(s.Timestamp) > now.Add(cfg.creationGracePeriod) {
		cat.IncrementDiscardedSamples(ls, 1, reasonTooFarInFuture, now.Time())
		m.tooFarInFuture.WithLabelValues(userID, group).Inc()
		unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)
		return false, fmt.Errorf(sampleTimestampTooNewMsgFormat, s.Timestamp, unsafeMetricName)
	}

	if cfg.pastGracePeriod > 0 && model.Time(s.Timestamp) < now.Add(-cfg.pastGracePeriod).Add(-cfg.outOfOrderTimeWindow) {
		cat.IncrementDiscardedSamples(ls, 1, reasonTooFarInPast, now.Time())
		m.tooFarInPast.WithLabelValues(userID, group).Inc()
		unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)
		return false, fmt.Errorf(sampleTimestampTooOldMsgFormat, s.Timestamp, unsafeMetricName)
	}

	// Check if schema is either a valid exponential schema or NHCB.
	if (s.Schema < mimirpb.MinimumHistogramSchema || s.Schema > mimirpb.MaximumHistogramSchema) && s.Schema != mimirpb.NativeHistogramsWithCustomBucketsSchema {
		cat.IncrementDiscardedSamples(ls, 1, reasonInvalidNativeHistogramSchema, now.Time())
		m.invalidNativeHistogramSchema.WithLabelValues(userID, group).Inc()
		return false, fmt.Errorf(invalidSchemaNativeHistogramMsgFormat, s.Schema)
	}

	if bucketLimit := cfg.maxNativeHistogramBuckets; bucketLimit > 0 {
		bucketCount := s.BucketCount()
		if bucketCount > bucketLimit {
			if s.Schema == mimirpb.NativeHistogramsWithCustomBucketsSchema {
				// Custom buckets cannot be scaled down.
				cat.IncrementDiscardedSamples(ls, 1, reasonMaxNativeHistogramBuckets, now.Time())
				m.maxNativeHistogramBuckets.WithLabelValues(userID, group).Inc()
				return false, fmt.Errorf(nativeHistogramCustomBucketsNotReducibleMsgFormat, s.Timestamp, mimirpb.FromLabelAdaptersToString(ls), bucketCount, bucketLimit)
			}
			if !cfg.reduceNativeHistogramOverMaxBuckets {
				cat.IncrementDiscardedSamples(ls, 1, reasonMaxNativeHistogramBuckets, now.Time())
				m.maxNativeHistogramBuckets.WithLabelValues(userID, group).Inc()
				return false, fmt.Errorf(maxNativeHistogramBucketsMsgFormat, s.Timestamp, mimirpb.FromLabelAdaptersToString(ls), bucketCount, bucketLimit)
			}

			for {
				bc, err := s.ReduceResolution()
				if err != nil {
					cat.IncrementDiscardedSamples(ls, 1, reasonMaxNativeHistogramBuckets, now.Time())
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
// The returned error MUST NOT retain label strings - they point into a gRPC buffer which is re-used.
func validateExemplar(m *exemplarValidationMetrics, userID string, ls []mimirpb.UnsafeMutableLabel, e mimirpb.Exemplar) error {
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
// The returned error MUST NOT retain label strings - they point into a gRPC buffer which is re-used.
// It may mutate ls and the underlying UnsafeMutableLabel/Strings.
func validateLabels(m *sampleValidationMetrics, cfg labelValidationConfig, userID, group string, ls []mimirpb.UnsafeMutableLabel, skipLabelValidation, skipLabelCountValidation bool, cat *costattribution.SampleTracker, ts time.Time, valueTooLongSummaries *labelValueTooLongSummaries) error {
	unsafeMetricName, err := extract.UnsafeMetricNameFromLabelAdapters(ls)
	if err != nil {
		cat.IncrementDiscardedSamples(ls, 1, reasonMissingMetricName, ts)
		m.missingMetricName.WithLabelValues(userID, group).Inc()
		return errors.New(noMetricNameMsgFormat)
	}

	validationScheme := cfg.nameValidationScheme

	if !validationScheme.IsValidMetricName(unsafeMetricName) {
		cat.IncrementDiscardedSamples(ls, 1, reasonInvalidMetricName, ts)
		m.invalidMetricName.WithLabelValues(userID, group).Inc()
		return fmt.Errorf(invalidMetricNameMsgFormat, removeNonASCIIChars(unsafeMetricName))
	}

	if !skipLabelCountValidation && len(ls) > cfg.maxLabelNamesPerSeries {
		if strings.HasSuffix(unsafeMetricName, "_info") {
			if len(ls) > cfg.maxLabelNamesPerInfoSeries {
				m.maxLabelNamesPerInfoSeries.WithLabelValues(userID, group).Inc()
				cat.IncrementDiscardedSamples(ls, 1, reasonMaxLabelNamesPerInfoSeries, ts)
				metric, ellipsis := getMetricAndEllipsis(ls)
				return fmt.Errorf(tooManyInfoLabelsMsgFormat, len(ls), cfg.maxLabelNamesPerInfoSeries, metric, ellipsis)
			}
		} else {
			m.maxLabelNamesPerSeries.WithLabelValues(userID, group).Inc()
			cat.IncrementDiscardedSamples(ls, 1, reasonMaxLabelNamesPerSeries, ts)
			metric, ellipsis := getMetricAndEllipsis(ls)
			return fmt.Errorf(tooManyLabelsMsgFormat, len(ls), cfg.maxLabelNamesPerSeries, metric, ellipsis)
		}
	}

	maxLabelNameLength := cfg.maxLabelNameLength
	maxLabelValueLength := cfg.maxLabelValueLength
	labelValueLengthOverLimitStrategy := cfg.labelValueLengthOverLimitStrategy

	lastLabelName := ""
	for i, l := range ls {
		if !skipLabelValidation && !validationScheme.IsValidLabelName(l.Name) {
			m.invalidLabel.WithLabelValues(userID, group).Inc()
			cat.IncrementDiscardedSamples(ls, 1, reasonInvalidLabel, ts)
			return fmt.Errorf(invalidLabelMsgFormat, l.Name, mimirpb.FromLabelAdaptersToString(ls))
		} else if len(l.Name) > maxLabelNameLength {
			cat.IncrementDiscardedSamples(ls, 1, reasonLabelNameTooLong, ts)
			m.labelNameTooLong.WithLabelValues(userID, group).Inc()
			return fmt.Errorf(labelNameTooLongMsgFormat, l.Name, mimirpb.FromLabelAdaptersToString(ls))
		} else if !skipLabelValidation && !model.LabelValue(l.Value).IsValid() {
			cat.IncrementDiscardedSamples(ls, 1, reasonInvalidLabelValue, ts)
			m.invalidLabelValue.WithLabelValues(userID, group).Inc()
			return fmt.Errorf(invalidLabelValueMsgFormat, l.Name, validUTF8Message(l.Value), unsafeMetricName)
		} else if len(l.Value) > maxLabelValueLength {
			switch labelValueLengthOverLimitStrategy {
			case validation.LabelValueLengthOverLimitStrategyError:
				cat.IncrementDiscardedSamples(ls, 1, reasonLabelValueTooLong, ts)
				m.labelValueTooLong.WithLabelValues(userID, group).Inc()
				return labelValueTooLongError{
					Label:  labels.Label{Name: strings.Clone(l.Name), Value: strings.Clone(l.Value)},
					Series: mimirpb.FromLabelAdaptersToString(ls),
					Limit:  maxLabelValueLength,
				}
			case validation.LabelValueLengthOverLimitStrategyTruncate:
				valueTooLongSummaries.measure(unsafeMetricName, l.Name, l.Value)
				_ = hashLabelValueInto(l.Value[maxLabelValueLength-validation.LabelValueHashLen:], l.Value)
				ls[i].Value = ls[i].Value[:maxLabelValueLength]
			case validation.LabelValueLengthOverLimitStrategyDrop:
				valueTooLongSummaries.measure(unsafeMetricName, l.Name, l.Value)
				ls[i].Value = hashLabelValueInto(l.Value[:validation.LabelValueHashLen], l.Value)
			default:
				panic(fmt.Errorf("unexpected value: %v", labelValueLengthOverLimitStrategy))
			}
		}

		// Duplicate check moved outside else-if chain so it always runs,
		// even when value-too-long handling (Truncate/Drop) modified the label.
		if lastLabelName == l.Name {
			cat.IncrementDiscardedSamples(ls, 1, reasonDuplicateLabelNames, ts)
			m.duplicateLabelNames.WithLabelValues(userID, group).Inc()
			return fmt.Errorf(duplicateLabelMsgFormat, l.Name, mimirpb.FromLabelAdaptersToString(ls))
		}
		lastLabelName = l.Name
	}

	return nil
}

func hashLabelValueInto(dst, src mimirpb.UnsafeMutableString) mimirpb.UnsafeMutableString {
	h := blake2b.Sum256(unsafeMutableStringToBytes(src))

	buf := unsafeMutableStringToBytes(dst)
	buf = buf[copy(buf, "(hash:"):]
	buf = buf[hex.Encode(buf, h[:]):]
	buf[0] = ')'
	return dst[:validation.LabelValueHashLen]
}

func unsafeMutableStringToBytes(s mimirpb.UnsafeMutableString) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
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

// cleanAndValidateMetadata returns an err if a metric metadata is invalid.
func cleanAndValidateMetadata(m *metadataValidationMetrics, cfg metadataValidationConfig, userID string, metadata *mimirpb.MetricMetadata) error {
	if cfg.enforceMetadataMetricName && metadata.GetMetricFamilyName() == "" {
		m.missingMetricName.WithLabelValues(userID).Inc()
		return errors.New(metadataMetricNameMissingMsgFormat)
	}

	maxMetadataValueLength := cfg.maxMetadataLength

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

// validUTF8ErrMessage ensures that the given message contains only valid utf8 characters.
// The presence of non-utf8 characters in some errors might break some crucial parts of distributor's logic.
// For example, if httpgrpc.HTTPServer.Handle() returns a httpgprc.Error containing a non-utf8 character,
// this error will not be propagated to httpgrpc.HTTPClient as a htttpgrpc.Error, but as a generic error,
// which might break some of Mimir internal logic.
// This is because golang's proto.Marshal(), which is used by gRPC internally, fails when it marshals the
// httpgrpc.Error containing non-utf8 character produced by httpgrpc.HTTPServer.Handle(), making the resulting
// error lose some important properties.
func validUTF8Message(msg string) string {
	return strings.ToValidUTF8(msg, string(utf8.RuneError))
}
