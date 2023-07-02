// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/validate_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type validateLabelsCfg struct {
	maxLabelNamesPerSeries int
	maxLabelNameLength     int
	maxLabelValueLength    int
}

func (v validateLabelsCfg) MaxLabelNamesPerSeries(_ string) int {
	return v.maxLabelNamesPerSeries
}

func (v validateLabelsCfg) MaxLabelNameLength(_ string) int {
	return v.maxLabelNameLength
}

func (v validateLabelsCfg) MaxLabelValueLength(_ string) int {
	return v.maxLabelValueLength
}

type validateMetadataCfg struct {
	enforceMetadataMetricName bool
	maxMetadataLength         int
}

func (vm validateMetadataCfg) EnforceMetadataMetricName(_ string) bool {
	return vm.enforceMetadataMetricName
}

func (vm validateMetadataCfg) MaxMetadataLength(_ string) int {
	return vm.maxMetadataLength
}

func TestValidateLabels(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	s := NewSampleValidationMetrics(reg)

	var cfg validateLabelsCfg
	userID := "testUser"

	cfg.maxLabelValueLength = 25
	cfg.maxLabelNameLength = 25
	cfg.maxLabelNamesPerSeries = 2

	for _, c := range []struct {
		metric                  []mimirpb.LabelAdapter
		skipLabelNameValidation bool
		err                     error
	}{
		{
			nil,
			false,
			newNoMetricNameError(),
		},
		{
			[]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: " "}},
			false,
			newInvalidMetricNameError(" "),
		},
		{
			[]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "valid"}, {Name: "foo ", Value: "bar"}},
			false,
			newInvalidLabelError([]mimirpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: "valid"},
				{Name: "foo ", Value: "bar"},
			}, "foo "),
		},
		{
			[]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "valid"}},
			false,
			nil,
		},
		{
			[]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "badLabelName"}, {Name: "this_is_a_really_really_long_name_that_should_cause_an_error", Value: "test_value_please_ignore"}},
			false,
			newLabelNameTooLongError([]mimirpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: "badLabelName"},
				{Name: "this_is_a_really_really_long_name_that_should_cause_an_error", Value: "test_value_please_ignore"},
			}, "this_is_a_really_really_long_name_that_should_cause_an_error"),
		},
		{
			[]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "badLabelValue"}, {Name: "much_shorter_name", Value: "test_value_please_ignore_no_really_nothing_to_see_here"}},
			false,
			newLabelValueTooLongError([]mimirpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: "badLabelValue"},
				{Name: "much_shorter_name", Value: "test_value_please_ignore_no_really_nothing_to_see_here"},
			}, "test_value_please_ignore_no_really_nothing_to_see_here"),
		},
		{
			[]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "foo"}, {Name: "bar", Value: "baz"}, {Name: "blip", Value: "blop"}},
			false,
			newTooManyLabelsError([]mimirpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "blip", Value: "blop"},
			}, 2),
		},
		{
			[]mimirpb.LabelAdapter{{Name: "bar", Value: "baz"}, {Name: model.MetricNameLabel, Value: "foo"}},
			false,
			newUnsortedLabelsError([]mimirpb.LabelAdapter{
				{Name: "bar", Value: "baz"},
				{Name: model.MetricNameLabel, Value: "foo"},
			}),
		},
		{
			[]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "foo"}, {Name: "invalid%label&name", Value: "bar"}},
			true,
			nil,
		},
	} {
		err := ValidateLabels(s, cfg, userID, "custom label", c.metric, c.skipLabelNameValidation)
		assert.Equal(t, c.err, err, "wrong error")
	}

	randomReason := DiscardedSamplesCounter(reg, "random reason")
	randomReason.WithLabelValues("different user", "custom label").Inc()

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_discarded_samples_total The total number of samples that were discarded.
			# TYPE cortex_discarded_samples_total counter
			cortex_discarded_samples_total{group="custom label",reason="label_invalid",user="testUser"} 1
			cortex_discarded_samples_total{group="custom label",reason="label_name_too_long",user="testUser"} 1
			cortex_discarded_samples_total{group="custom label",reason="label_value_too_long",user="testUser"} 1
			cortex_discarded_samples_total{group="custom label",reason="labels_not_sorted",user="testUser"} 1
			cortex_discarded_samples_total{group="custom label",reason="max_label_names_per_series",user="testUser"} 1
			cortex_discarded_samples_total{group="custom label",reason="metric_name_invalid",user="testUser"} 1
			cortex_discarded_samples_total{group="custom label",reason="missing_metric_name",user="testUser"} 1

			cortex_discarded_samples_total{group="custom label",reason="random reason",user="different user"} 1
	`), "cortex_discarded_samples_total"))

	s.DeleteUserMetrics(userID)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_discarded_samples_total The total number of samples that were discarded.
			# TYPE cortex_discarded_samples_total counter
			cortex_discarded_samples_total{group="custom label",reason="random reason",user="different user"} 1
	`), "cortex_discarded_samples_total"))
}

func TestValidateExemplars(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := NewExemplarValidationMetrics(reg)

	userID := "testUser"

	invalidExemplars := []mimirpb.Exemplar{
		{
			// Missing labels
			Labels: nil,
		},
		{
			// Labels all blank
			Labels:      []mimirpb.LabelAdapter{{Name: "", Value: ""}},
			TimestampMs: 1000,
		},
		{
			// Labels value blank
			Labels:      []mimirpb.LabelAdapter{{Name: "foo", Value: ""}},
			TimestampMs: 1000,
		},
		{
			// Invalid timestamp
			Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
		},
		{
			// Combined labelset too long
			Labels:      []mimirpb.LabelAdapter{{Name: "foo", Value: strings.Repeat("0", 126)}},
			TimestampMs: 1000,
		},
	}

	for _, ie := range invalidExemplars {
		assert.Error(t, ValidateExemplar(m, userID, []mimirpb.LabelAdapter{}, ie))
	}

	validExemplars := []mimirpb.Exemplar{
		{
			// Valid labels
			Labels:      []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
			TimestampMs: 1000,
		},
		{
			// Single label blank value with one valid value
			Labels:      []mimirpb.LabelAdapter{{Name: "foo", Value: ""}, {Name: "traceID", Value: "123abc"}},
			TimestampMs: 1000,
		},
	}

	for _, ve := range validExemplars {
		assert.NoError(t, ValidateExemplar(m, userID, []mimirpb.LabelAdapter{}, ve))
	}

	DiscardedExemplarsCounter(reg, "random reason").WithLabelValues("different user").Inc()

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_discarded_exemplars_total The total number of exemplars that were discarded.
			# TYPE cortex_discarded_exemplars_total counter
			cortex_discarded_exemplars_total{reason="exemplar_labels_blank",user="testUser"} 2
			cortex_discarded_exemplars_total{reason="exemplar_labels_missing",user="testUser"} 1
			cortex_discarded_exemplars_total{reason="exemplar_labels_too_long",user="testUser"} 1
			cortex_discarded_exemplars_total{reason="exemplar_timestamp_invalid",user="testUser"} 1

			cortex_discarded_exemplars_total{reason="random reason",user="different user"} 1
		`), "cortex_discarded_exemplars_total"))

	// Delete test user and verify only different remaining
	m.DeleteUserMetrics(userID)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_discarded_exemplars_total The total number of exemplars that were discarded.
			# TYPE cortex_discarded_exemplars_total counter
			cortex_discarded_exemplars_total{reason="random reason",user="different user"} 1
	`), "cortex_discarded_exemplars_total"))
}

func TestValidateMetadata(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := NewMetadataValidationMetrics(reg)

	userID := "testUser"
	var cfg validateMetadataCfg
	cfg.enforceMetadataMetricName = true
	cfg.maxMetadataLength = 22

	for _, c := range []struct {
		desc        string
		metadata    *mimirpb.MetricMetadata
		err         error
		metadataOut *mimirpb.MetricMetadata
	}{
		{
			"with a valid config",
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: mimirpb.COUNTER, Help: "Number of goroutines.", Unit: ""},
			nil,
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: mimirpb.COUNTER, Help: "Number of goroutines.", Unit: ""},
		},
		{
			"with no metric name",
			&mimirpb.MetricMetadata{MetricFamilyName: "", Type: mimirpb.COUNTER, Help: "Number of goroutines.", Unit: ""},
			newMetadataMetricNameMissingError(),
			nil,
		},
		{
			"with a long metric name",
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines_and_routines_and_routines", Type: mimirpb.COUNTER, Help: "Number of goroutines.", Unit: ""},
			newMetadataMetricNameTooLongError(&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines_and_routines_and_routines"}),
			nil,
		},
		{
			"with a long help",
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: mimirpb.COUNTER, Help: "Number of goroutines that currently exist.", Unit: ""},
			nil,
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: mimirpb.COUNTER, Help: "Number of goroutines t", Unit: ""},
		},
		{
			"with a long UTF-8 help",
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: mimirpb.COUNTER, Help: "This help has wchar:日日日", Unit: ""},
			nil,
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: mimirpb.COUNTER, Help: "This help has wchar:", Unit: ""},
		},
		{
			"with invalid long UTF-8 help",
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: mimirpb.COUNTER, Help: "This help has \xe6char:日日日", Unit: ""},
			nil,
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: mimirpb.COUNTER, Help: "This help has \xe6char:", Unit: ""},
		},
		{
			"with a long unit",
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: mimirpb.COUNTER, Help: "Number of goroutines.", Unit: "a_made_up_unit_that_is_really_long"},
			newMetadataUnitTooLongError(&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Unit: "a_made_up_unit_that_is_really_long"}),
			nil,
		},
	} {
		t.Run(c.desc, func(t *testing.T) {
			err := CleanAndValidateMetadata(m, cfg, userID, c.metadata)
			assert.Equal(t, c.err, err, "wrong error")
			if err == nil {
				assert.Equal(t, c.metadataOut, c.metadata)
			}
		})
	}

	DiscardedMetadataCounter(reg, "random reason").WithLabelValues("different user").Inc()

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_discarded_metadata_total The total number of metadata that were discarded.
			# TYPE cortex_discarded_metadata_total counter
			cortex_discarded_metadata_total{reason="metric_name_too_long",user="testUser"} 1
			cortex_discarded_metadata_total{reason="missing_metric_name",user="testUser"} 1
			cortex_discarded_metadata_total{reason="unit_too_long",user="testUser"} 1

			cortex_discarded_metadata_total{reason="random reason",user="different user"} 1
	`), "cortex_discarded_metadata_total"))

	m.DeleteUserMetrics(userID)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_discarded_metadata_total The total number of metadata that were discarded.
			# TYPE cortex_discarded_metadata_total counter
			cortex_discarded_metadata_total{reason="random reason",user="different user"} 1
	`), "cortex_discarded_metadata_total"))
}

func TestValidateLabelDuplication(t *testing.T) {
	var cfg validateLabelsCfg
	cfg.maxLabelNameLength = 10
	cfg.maxLabelNamesPerSeries = 10
	cfg.maxLabelValueLength = 10

	userID := "testUser"

	actual := ValidateLabels(NewSampleValidationMetrics(nil), cfg, userID, "", []mimirpb.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "a"},
		{Name: model.MetricNameLabel, Value: "b"},
	}, false)
	expected := newDuplicatedLabelError([]mimirpb.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "a"},
		{Name: model.MetricNameLabel, Value: "b"},
	}, model.MetricNameLabel)
	assert.Equal(t, expected, actual)

	actual = ValidateLabels(NewSampleValidationMetrics(nil), cfg, userID, "", []mimirpb.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "a"},
		{Name: "a", Value: "a"},
		{Name: "a", Value: "a"},
	}, false)
	expected = newDuplicatedLabelError([]mimirpb.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "a"},
		{Name: "a", Value: "a"},
		{Name: "a", Value: "a"},
	}, "a")
	assert.Equal(t, expected, actual)
}

type sampleValidationConfig struct {
	maxNativeHistogramBuckets int
}

func (c sampleValidationConfig) CreationGracePeriod(_ string) time.Duration {
	return 0
}

func (c sampleValidationConfig) MaxNativeHistogramBuckets(_ string) int {
	return c.maxNativeHistogramBuckets
}

func TestMaxNativeHistorgramBuckets(t *testing.T) {
	// All will have 2 buckets, one negative and one positive
	testCases := map[string]mimirpb.Histogram{
		"integer counter": {
			Count:          &mimirpb.Histogram_CountInt{CountInt: 2},
			Sum:            10,
			Schema:         1,
			ZeroThreshold:  0.001,
			ZeroCount:      &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 0},
			NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
			NegativeDeltas: []int64{1},
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
			PositiveDeltas: []int64{1},
			ResetHint:      mimirpb.Histogram_UNKNOWN,
			Timestamp:      0,
		},
		"integer gauge": {
			Count:          &mimirpb.Histogram_CountInt{CountInt: 2},
			Sum:            10,
			Schema:         1,
			ZeroThreshold:  0.001,
			ZeroCount:      &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 0},
			NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
			NegativeDeltas: []int64{1},
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
			PositiveDeltas: []int64{1},
			ResetHint:      mimirpb.Histogram_GAUGE,
			Timestamp:      0,
		},
		"float counter": {
			Count:          &mimirpb.Histogram_CountFloat{CountFloat: 2},
			Sum:            10,
			Schema:         1,
			ZeroThreshold:  0.001,
			ZeroCount:      &mimirpb.Histogram_ZeroCountFloat{ZeroCountFloat: 0},
			NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
			NegativeCounts: []float64{1},
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
			PositiveCounts: []float64{1},
			ResetHint:      mimirpb.Histogram_UNKNOWN,
			Timestamp:      0,
		},
		"float gauge": {
			Count:          &mimirpb.Histogram_CountFloat{CountFloat: 2},
			Sum:            10,
			Schema:         1,
			ZeroThreshold:  0.001,
			ZeroCount:      &mimirpb.Histogram_ZeroCountFloat{ZeroCountFloat: 0},
			NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
			NegativeCounts: []float64{1},
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
			PositiveCounts: []float64{1},
			ResetHint:      mimirpb.Histogram_GAUGE,
			Timestamp:      0,
		},
		"integer counter positive buckets": {
			Count:          &mimirpb.Histogram_CountInt{CountInt: 2},
			Sum:            10,
			Schema:         1,
			ZeroThreshold:  0.001,
			ZeroCount:      &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 0},
			NegativeSpans:  []mimirpb.BucketSpan{},
			NegativeDeltas: []int64{},
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}, {Offset: 2, Length: 1}},
			PositiveDeltas: []int64{1, 0},
			ResetHint:      mimirpb.Histogram_UNKNOWN,
			Timestamp:      0,
		},
		"integer counter negative buckets": {
			Count:          &mimirpb.Histogram_CountInt{CountInt: 2},
			Sum:            10,
			Schema:         1,
			ZeroThreshold:  0.001,
			ZeroCount:      &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 0},
			NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 2}},
			NegativeDeltas: []int64{1, 0},
			PositiveSpans:  []mimirpb.BucketSpan{},
			PositiveDeltas: []int64{},
			ResetHint:      mimirpb.Histogram_UNKNOWN,
			Timestamp:      0,
		},
		"float counter positive buckets": {
			Count:          &mimirpb.Histogram_CountFloat{CountFloat: 2},
			Sum:            10,
			Schema:         1,
			ZeroThreshold:  0.001,
			ZeroCount:      &mimirpb.Histogram_ZeroCountFloat{ZeroCountFloat: 0},
			NegativeSpans:  []mimirpb.BucketSpan{},
			NegativeCounts: []float64{},
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 2}},
			PositiveCounts: []float64{1, 1},
			ResetHint:      mimirpb.Histogram_UNKNOWN,
			Timestamp:      0,
		},
		"float counter negative buckets": {
			Count:          &mimirpb.Histogram_CountFloat{CountFloat: 2},
			Sum:            10,
			Schema:         1,
			ZeroThreshold:  0.001,
			ZeroCount:      &mimirpb.Histogram_ZeroCountFloat{ZeroCountFloat: 0},
			NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 2}},
			NegativeCounts: []float64{1, 1},
			PositiveSpans:  []mimirpb.BucketSpan{},
			PositiveCounts: []float64{},
			ResetHint:      mimirpb.Histogram_UNKNOWN,
			Timestamp:      0,
		},
	}

	registry := prometheus.NewRegistry()
	metrics := NewSampleValidationMetrics(registry)

	for _, limit := range []int{0, 1, 2} {
		for name, h := range testCases {
			t.Run(fmt.Sprintf("limit-%d-%s", limit, name), func(t *testing.T) {
				var cfg sampleValidationConfig
				cfg.maxNativeHistogramBuckets = limit

				err := ValidateSampleHistogram(metrics, model.Now(), cfg, "user-1", "group-1", []mimirpb.LabelAdapter{
					{Name: model.MetricNameLabel, Value: "a"},
					{Name: "a", Value: "a"}}, h)

				if limit == 1 {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}

	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
			# HELP cortex_discarded_samples_total The total number of samples that were discarded.
			# TYPE cortex_discarded_samples_total counter
			cortex_discarded_samples_total{group="group-1",reason="max_native_histogram_buckets",user="user-1"} 8
	`), "cortex_discarded_samples_total"))
}
