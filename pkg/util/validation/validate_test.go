// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/validate_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

type validateLabelsCfg struct {
	maxLabelNamesPerSeries int
	maxLabelNameLength     int
	maxLabelValueLength    int
}

func (v validateLabelsCfg) MaxLabelNamesPerSeries(userID string) int {
	return v.maxLabelNamesPerSeries
}

func (v validateLabelsCfg) MaxLabelNameLength(userID string) int {
	return v.maxLabelNameLength
}

func (v validateLabelsCfg) MaxLabelValueLength(userID string) int {
	return v.maxLabelValueLength
}

type validateMetadataCfg struct {
	enforceMetadataMetricName bool
	maxMetadataLength         int
}

func (vm validateMetadataCfg) EnforceMetadataMetricName(userID string) bool {
	return vm.enforceMetadataMetricName
}

func (vm validateMetadataCfg) MaxMetadataLength(userID string) int {
	return vm.maxMetadataLength
}

func TestValidateLabels(t *testing.T) {
	var cfg validateLabelsCfg
	userID := "testUser"

	cfg.maxLabelValueLength = 25
	cfg.maxLabelNameLength = 25
	cfg.maxLabelNamesPerSeries = 2

	for _, c := range []struct {
		metric                  model.Metric
		skipLabelNameValidation bool
		err                     error
	}{
		{
			map[model.LabelName]model.LabelValue{},
			false,
			newNoMetricNameError(),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: " "},
			false,
			newInvalidMetricNameError(" "),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "valid", "foo ": "bar"},
			false,
			newInvalidLabelError([]mimirpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: "valid"},
				{Name: "foo ", Value: "bar"},
			}, "foo "),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "valid"},
			false,
			nil,
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelName", "this_is_a_really_really_long_name_that_should_cause_an_error": "test_value_please_ignore"},
			false,
			newLabelNameTooLongError([]mimirpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: "badLabelName"},
				{Name: "this_is_a_really_really_long_name_that_should_cause_an_error", Value: "test_value_please_ignore"},
			}, "this_is_a_really_really_long_name_that_should_cause_an_error"),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelValue", "much_shorter_name": "test_value_please_ignore_no_really_nothing_to_see_here"},
			false,
			newLabelValueTooLongError([]mimirpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: "badLabelValue"},
				{Name: "much_shorter_name", Value: "test_value_please_ignore_no_really_nothing_to_see_here"},
			}, "test_value_please_ignore_no_really_nothing_to_see_here"),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo", "bar": "baz", "blip": "blop"},
			false,
			newTooManyLabelsError([]mimirpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "blip", Value: "blop"},
			}, 2),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo", "invalid%label&name": "bar"},
			true,
			nil,
		},
	} {
		err := ValidateLabels(cfg, userID, mimirpb.FromMetricsToLabelAdapters(c.metric), c.skipLabelNameValidation)
		assert.Equal(t, c.err, err, "wrong error")
	}

	DiscardedSamples.WithLabelValues("random reason", "different user").Inc()

	require.NoError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(`
			# HELP cortex_discarded_samples_total The total number of samples that were discarded.
			# TYPE cortex_discarded_samples_total counter
			cortex_discarded_samples_total{reason="label_invalid",user="testUser"} 1
			cortex_discarded_samples_total{reason="label_name_too_long",user="testUser"} 1
			cortex_discarded_samples_total{reason="label_value_too_long",user="testUser"} 1
			cortex_discarded_samples_total{reason="max_label_names_per_series",user="testUser"} 1
			cortex_discarded_samples_total{reason="metric_name_invalid",user="testUser"} 1
			cortex_discarded_samples_total{reason="missing_metric_name",user="testUser"} 1

			cortex_discarded_samples_total{reason="random reason",user="different user"} 1
	`), "cortex_discarded_samples_total"))

	DeletePerUserValidationMetrics(userID, util_log.Logger)

	require.NoError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(`
			# HELP cortex_discarded_samples_total The total number of samples that were discarded.
			# TYPE cortex_discarded_samples_total counter
			cortex_discarded_samples_total{reason="random reason",user="different user"} 1
	`), "cortex_discarded_samples_total"))
}

func TestValidateExemplars(t *testing.T) {
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
		assert.Error(t, ValidateExemplar(userID, []mimirpb.LabelAdapter{}, ie))
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
		assert.NoError(t, ValidateExemplar(userID, []mimirpb.LabelAdapter{}, ve))
	}

	DiscardedExemplars.WithLabelValues("random reason", "different user").Inc()

	require.NoError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(`
			# HELP cortex_discarded_exemplars_total The total number of exemplars that were discarded.
			# TYPE cortex_discarded_exemplars_total counter
			cortex_discarded_exemplars_total{reason="exemplar_labels_blank",user="testUser"} 2
			cortex_discarded_exemplars_total{reason="exemplar_labels_missing",user="testUser"} 1
			cortex_discarded_exemplars_total{reason="exemplar_labels_too_long",user="testUser"} 1
			cortex_discarded_exemplars_total{reason="exemplar_timestamp_invalid",user="testUser"} 1

			cortex_discarded_exemplars_total{reason="random reason",user="different user"} 1
		`), "cortex_discarded_exemplars_total"))

	// Delete test user and verify only different remaining
	DeletePerUserValidationMetrics(userID, util_log.Logger)
	require.NoError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(`
			# HELP cortex_discarded_exemplars_total The total number of exemplars that were discarded.
			# TYPE cortex_discarded_exemplars_total counter
			cortex_discarded_exemplars_total{reason="random reason",user="different user"} 1
	`), "cortex_discarded_exemplars_total"))
}

func TestValidateMetadata(t *testing.T) {
	userID := "testUser"
	var cfg validateMetadataCfg
	cfg.enforceMetadataMetricName = true
	cfg.maxMetadataLength = 22

	for _, c := range []struct {
		desc     string
		metadata *mimirpb.MetricMetadata
		err      error
	}{
		{
			"with a valid config",
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: mimirpb.COUNTER, Help: "Number of goroutines.", Unit: ""},
			nil,
		},
		{
			"with no metric name",
			&mimirpb.MetricMetadata{MetricFamilyName: "", Type: mimirpb.COUNTER, Help: "Number of goroutines.", Unit: ""},
			newMetadataMetricNameMissingError(),
		},
		{
			"with a long metric name",
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines_and_routines_and_routines", Type: mimirpb.COUNTER, Help: "Number of goroutines.", Unit: ""},
			newMetadataMetricNameTooLongError(&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines_and_routines_and_routines"}),
		},
		{
			"with a long help",
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: mimirpb.COUNTER, Help: "Number of goroutines that currently exist.", Unit: ""},
			newMetadataHelpTooLongError(&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Help: "Number of goroutines that currently exist."}),
		},
		{
			"with a long unit",
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Type: mimirpb.COUNTER, Help: "Number of goroutines.", Unit: "a_made_up_unit_that_is_really_long"},
			newMetadataUnitTooLongError(&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines", Unit: "a_made_up_unit_that_is_really_long"}),
		},
	} {
		t.Run(c.desc, func(t *testing.T) {
			err := ValidateMetadata(cfg, userID, c.metadata)
			assert.Equal(t, c.err, err, "wrong error")
		})
	}

	DiscardedMetadata.WithLabelValues("random reason", "different user").Inc()

	require.NoError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(`
			# HELP cortex_discarded_metadata_total The total number of metadata that were discarded.
			# TYPE cortex_discarded_metadata_total counter
			cortex_discarded_metadata_total{reason="help_too_long",user="testUser"} 1
			cortex_discarded_metadata_total{reason="metric_name_too_long",user="testUser"} 1
			cortex_discarded_metadata_total{reason="missing_metric_name",user="testUser"} 1
			cortex_discarded_metadata_total{reason="unit_too_long",user="testUser"} 1

			cortex_discarded_metadata_total{reason="random reason",user="different user"} 1
	`), "cortex_discarded_metadata_total"))

	DeletePerUserValidationMetrics(userID, util_log.Logger)

	require.NoError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(`
			# HELP cortex_discarded_metadata_total The total number of metadata that were discarded.
			# TYPE cortex_discarded_metadata_total counter
			cortex_discarded_metadata_total{reason="random reason",user="different user"} 1
	`), "cortex_discarded_metadata_total"))
}

func TestValidateLabelOrder(t *testing.T) {
	var cfg validateLabelsCfg
	cfg.maxLabelNameLength = 10
	cfg.maxLabelNamesPerSeries = 10
	cfg.maxLabelValueLength = 10

	userID := "testUser"

	actual := ValidateLabels(cfg, userID, []mimirpb.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "m"},
		{Name: "b", Value: "b"},
		{Name: "a", Value: "a"},
	}, false)
	expected := newLabelsNotSortedError([]mimirpb.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "m"},
		{Name: "b", Value: "b"},
		{Name: "a", Value: "a"},
	}, "a")
	assert.Equal(t, expected, actual)
}

func TestValidateLabelDuplication(t *testing.T) {
	var cfg validateLabelsCfg
	cfg.maxLabelNameLength = 10
	cfg.maxLabelNamesPerSeries = 10
	cfg.maxLabelValueLength = 10

	userID := "testUser"

	actual := ValidateLabels(cfg, userID, []mimirpb.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "a"},
		{Name: model.MetricNameLabel, Value: "b"},
	}, false)
	expected := newDuplicatedLabelError([]mimirpb.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "a"},
		{Name: model.MetricNameLabel, Value: "b"},
	}, model.MetricNameLabel)
	assert.Equal(t, expected, actual)

	actual = ValidateLabels(cfg, userID, []mimirpb.LabelAdapter{
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
