// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/validate_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	grpcstatus "google.golang.org/grpc/status"
	golangproto "google.golang.org/protobuf/proto"

	"github.com/grafana/mimir/pkg/costattribution"
	"github.com/grafana/mimir/pkg/costattribution/testutils"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

type validateLabelsCfg struct {
	maxLabelNamesPerSeries     int
	maxLabelNamesPerInfoSeries int
	maxLabelNameLength         int
	maxLabelValueLength        int
	validationScheme           model.ValidationScheme
}

func (v validateLabelsCfg) MaxLabelNamesPerSeries(_ string) int {
	return v.maxLabelNamesPerSeries
}

func (v validateLabelsCfg) MaxLabelNamesPerInfoSeries(_ string) int {
	return v.maxLabelNamesPerInfoSeries
}

func (v validateLabelsCfg) MaxLabelNameLength(_ string) int {
	return v.maxLabelNameLength
}

func (v validateLabelsCfg) MaxLabelValueLength(_ string) int {
	return v.maxLabelValueLength
}

func (v validateLabelsCfg) NameValidationScheme(_ string) model.ValidationScheme {
	return v.validationScheme
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
	t.Parallel()

	const repeatsPerCase = 100

	const defaultUserID = "testUserDefault"
	const utf8UserID = "testUserUTF8"

	limits := prepareDefaultLimits()
	limits.MaxLabelValueLength = 25
	limits.MaxLabelNameLength = 25
	limits.MaxLabelNamesPerSeries = 4
	limits.MaxLabelNamesPerInfoSeries = 5
	limits.SeparateMetricsGroupLabel = "group"

	perTenant := map[string]*validation.Limits{}
	for _, userID := range []string{defaultUserID, utf8UserID} {
		limits := *limits
		perTenant[userID] = &limits
	}
	perTenant[defaultUserID].NameValidationScheme = validation.ValidationSchemeValue(model.LegacyValidation)
	perTenant[utf8UserID].NameValidationScheme = validation.ValidationSchemeValue(model.UTF8Validation)

	overrides := func(limits *validation.Limits) *validation.Overrides {
		return testutils.NewMockCostAttributionOverrides(*limits, perTenant, 0,
			[]string{defaultUserID, "team"},
			[]string{utf8UserID, "team"},
		)
	}

	reg := prometheus.NewPedanticRegistry()
	careg := prometheus.NewRegistry()
	manager, err := costattribution.NewManager(5*time.Second, 10*time.Second, log.NewNopLogger(), overrides(limits), reg, careg)
	require.NoError(t, err)

	ds, _, _, _ := prepare(t, prepConfig{
		numIngesters:       2,
		happyIngesters:     2,
		numDistributors:    1,
		limits:             limits,
		overrides:          overrides,
		reg:                reg,
		costAttributionMgr: manager,
	})
	d := ds[0]

	newRequestBuffers := func() *util.RequestBuffers {
		return util.NewRequestBuffers(d.RequestBufferPool, util.TaintBuffersOnCleanUp([]byte("The beef is dead.")))
	}

	validationSchemes := []model.ValidationScheme{
		model.LegacyValidation,
		model.UTF8Validation,
	}

	// alwaysErr ensures this error is returned for legacy and utf8 validation.
	alwaysErr := func(err error) func(model.ValidationScheme) error {
		return func(model.ValidationScheme) error {
			return err
		}
	}

	// legacyErr ensures err is only returned when legacy validation scheme is used.
	legacyErr := func(err error) func(model.ValidationScheme) error {
		return func(scheme model.ValidationScheme) error {
			if scheme == model.LegacyValidation {
				return err
			}
			return nil
		}
	}

	testCases := []struct {
		name                     string
		metric                   model.Metric
		skipLabelNameValidation  bool
		skipLabelCountValidation bool
		wantErr                  func(model.ValidationScheme) error
	}{
		{
			name:                     "missing metric name",
			metric:                   map[model.LabelName]model.LabelValue{"team": "a"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
			wantErr:                  alwaysErr(errors.New(noMetricNameMsgFormat)),
		},
		{
			name:                     "blank metric name",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: " ", "team": "a"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
			wantErr:                  legacyErr(fmt.Errorf(invalidMetricNameMsgFormat, " ")),
		},
		{
			name:                     "metric name with invalid utf8",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "metric_name_with_\xb0_invalid_utf8_\xb0", "team": "a"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
			wantErr: alwaysErr(
				fmt.Errorf(invalidMetricNameMsgFormat, "metric_name_with__invalid_utf8_ (non-ascii characters removed)"),
			),
		},
		{
			name:                     "invalid label name with space",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "valid", "foo ": "bar", "team": "a"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
			wantErr: legacyErr(fmt.Errorf(
				invalidLabelMsgFormat,
				"foo ",
				mimirpb.FromLabelAdaptersToString(
					[]mimirpb.LabelAdapter{
						{Name: model.MetricNameLabel, Value: "valid"},
						{Name: "foo ", Value: "bar"},
						{Name: "group", Value: "custom label"},
						{Name: "team", Value: "a"},
					},
				),
			)),
		},
		{
			name:                     "valid metric",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "valid", "team": "c"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
		},
		{
			name:                     "label name too long",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelName", "this_is_a_really_really_long_name_that_should_cause_an_error": "test_value_please_ignore", "team": "biz"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
			wantErr: alwaysErr(fmt.Errorf(
				labelNameTooLongMsgFormat,
				"this_is_a_really_really_long_name_that_should_cause_an_error",
				mimirpb.FromLabelAdaptersToString(
					[]mimirpb.LabelAdapter{
						{Name: "group", Value: "custom label"},
						{Name: "team", Value: "biz"},
						{Name: model.MetricNameLabel, Value: "badLabelName"},
						{Name: "this_is_a_really_really_long_name_that_should_cause_an_error", Value: "test_value_please_ignore"},
					},
				),
			)),
		},
		{
			name:                     "label value too long",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelValue", "much_shorter_name": "test_value_please_ignore_no_really_nothing_to_see_here", "team": "biz"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
			wantErr: alwaysErr(fmt.Errorf(
				labelValueTooLongMsgFormat,
				"much_shorter_name",
				"test_value_please_ignore_no_really_nothing_to_see_here",
				mimirpb.FromLabelAdaptersToString(
					[]mimirpb.LabelAdapter{
						{Name: model.MetricNameLabel, Value: "badLabelValue"},
						{Name: "group", Value: "custom label"},
						{Name: "much_shorter_name", Value: "test_value_please_ignore_no_really_nothing_to_see_here"},
						{Name: "team", Value: "biz"},
					},
				),
			)),
		},
		{
			name:                     "too many labels",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo", "bar": "baz", "blip": "blop", "team": "plof"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
			wantErr: alwaysErr(fmt.Errorf(
				tooManyLabelsMsgFormat,
				tooManyLabelsArgs(
					[]mimirpb.LabelAdapter{
						{Name: model.MetricNameLabel, Value: "foo"},
						{Name: "group", Value: "custom label"},
						{Name: "bar", Value: "baz"},
						{Name: "blip", Value: "blop"},
						{Name: "team", Value: "plof"},
					},
					limits.MaxLabelNamesPerSeries,
				)...,
			)),
		},
		{
			name: "valid info metric within limits",
			// *_info metrics have higher label limits.
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo_info", "bar": "baz", "blip": "blop", "team": "a"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
		},
		{
			name: "info metric too many labels",
			// *_info metrics have higher label limits.
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo_info", "bar": "baz", "blip": "blop", "blap": "blup", "team": "a"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
			wantErr: alwaysErr(fmt.Errorf(
				tooManyInfoLabelsMsgFormat,
				tooManyLabelsArgs(
					[]mimirpb.LabelAdapter{
						{Name: model.MetricNameLabel, Value: "foo_info"},
						{Name: "group", Value: "custom label"},
						{Name: "bar", Value: "baz"},
						{Name: "blip", Value: "blop"},
						{Name: "blap", Value: "blup"},
						{Name: "team", Value: "a"},
					},
					limits.MaxLabelNamesPerInfoSeries,
				)...,
			)),
		},
		{
			name:                     "skip label count validation",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo", "bar": "baz", "blip": "blop", "team": "a"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: true,
		},
		{
			name:                     "skip label name validation",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo", "invalid%label&name": "bar", "team": "biz"},
			skipLabelNameValidation:  true,
			skipLabelCountValidation: false,
		},
		{
			name:                     "valid unicode label value",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo", "label1": "你好", "team": "plof"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
		},
		{
			name:                     "invalid utf8 label value",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo", "label1": "abc\xfe\xfddef", "team": "plof"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
			wantErr: alwaysErr(fmt.Errorf(
				invalidLabelValueMsgFormat,
				"label1", "abc\ufffddef", "foo",
			)),
		},
		{
			name:                     "invalid utf8 label value with name validation skipped",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo", "label1": "abc\xfe\xfddef"},
			skipLabelNameValidation:  true,
			skipLabelCountValidation: false,
		},
		{
			name:                     "emoji in label name",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo", "name😀": "value", "team": "b"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
			wantErr: legacyErr(fmt.Errorf(
				invalidLabelMsgFormat,
				"name😀",
				mimirpb.FromLabelAdaptersToString(
					[]mimirpb.LabelAdapter{
						{Name: model.MetricNameLabel, Value: "foo"},
						{Name: "group", Value: "custom label"},
						{Name: "name😀", Value: "value"},
						{Name: "team", Value: "b"},
					},
				),
			)),
		},
		{
			name:                     "emoji in metric name",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "name😀", "team": "b"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
			wantErr: legacyErr(fmt.Errorf(
				invalidMetricNameMsgFormat, "name (non-ascii characters removed)"),
			),
		},
	}

	// We want to check those after all subtests are done, but parent tests
	// cannot wait for subtests. So we're going to run this in the last subtest
	// that finishes instead.
	finalChecks := func(t *testing.T) {
		// [labelValue][userID][team] -> expected discarded samples
		discardedSamplesValues := map[string]map[string]map[string]int{}
		for _, c := range testCases {
			if c.wantErr == nil {
				continue
			}
			for _, scheme := range validationSchemes {
				if err := c.wantErr(scheme); err != nil {
					for _, id := range []globalerror.ID{
						globalerror.SeriesInvalidLabel,
						globalerror.SeriesInvalidLabelValue,
						globalerror.SeriesLabelNameTooLong,
						globalerror.SeriesLabelValueTooLong,
						globalerror.MaxLabelNamesPerSeries,
						globalerror.MaxLabelNamesPerInfoSeries,
						globalerror.InvalidMetricName,
						globalerror.MissingMetricName,
					} {
						if strings.Contains(err.Error(), string(id)) {
							if discardedSamplesValues[id.LabelValue()] == nil {
								discardedSamplesValues[id.LabelValue()] = map[string]map[string]int{}
							}
							var userID string
							switch scheme {
							case model.LegacyValidation:
								userID = defaultUserID
							case model.UTF8Validation:
								userID = utf8UserID
							default:
								panic(fmt.Errorf("unhandled name validation scheme: %s", scheme))
							}
							if discardedSamplesValues[id.LabelValue()][userID] == nil {
								discardedSamplesValues[id.LabelValue()][userID] = map[string]int{}
							}
							team := string(c.metric["team"])
							discardedSamplesValues[id.LabelValue()][userID][team] += repeatsPerCase
						}
					}
				}
			}
		}

		// Get metrics from the distributor's registry
		randomReason := validation.DiscardedSamplesCounter(reg, "random reason")
		randomReason.WithLabelValues("different user", "custom label").Inc()

		wantDiscardedSamples := `
			# HELP cortex_discarded_samples_total The total number of samples that were discarded.
			# TYPE cortex_discarded_samples_total counter
			cortex_discarded_samples_total{group="custom label",reason="random reason",user="different user"} 1
		`
		wantDiscardedAttrSamples := `
			# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
			# TYPE cortex_discarded_attributed_samples_total counter
		`

		sumSamples := func(m map[string]int) (sum int) {
			for _, v := range m {
				sum += v
			}
			return
		}

		for reason, byUser := range discardedSamplesValues {
			for userID, countByTeam := range byUser {
				wantDiscardedSamples += fmt.Sprintf(
					`cortex_discarded_samples_total{group="custom label",reason="%s",user="%s"} %d`+"\n",
					reason,
					userID,
					sumSamples(countByTeam),
				)
				for team, count := range countByTeam {
					wantDiscardedAttrSamples += fmt.Sprintf(
						`cortex_discarded_attributed_samples_total{reason="%s",team="%s",tenant="%s",tracker="cost-attribution"} %d`+"\n",
						reason,
						team,
						userID,
						count,
					)
				}
			}
		}

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(wantDiscardedSamples), "cortex_discarded_samples_total"))
		require.NoError(t, testutil.GatherAndCompare(careg, strings.NewReader(wantDiscardedAttrSamples), "cortex_discarded_attributed_samples_total"))

		d.sampleValidationMetrics.deleteUserMetrics(defaultUserID)
		d.sampleValidationMetrics.deleteUserMetrics(utf8UserID)
	}

	var inFlightSubsets atomic.Int64
	inFlightSubsets.Store(int64(len(testCases) * len(validationSchemes)))

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			for _, scheme := range validationSchemes {
				t.Run(scheme.String(), func(t *testing.T) {
					t.Parallel()

					defer func() {
						if inFlightSubsets.Add(-1) == 0 {
							finalChecks(t)
						}
					}()

					var userID string
					switch scheme {
					case model.LegacyValidation:
						userID = defaultUserID
					case model.UTF8Validation:
						userID = utf8UserID
					default:
						panic(fmt.Errorf("unhandled name validation scheme: %s", scheme))
					}

					handler := Handler(100000, newRequestBuffers, nil, true, true, d.limits, RetryConfig{},
						d.PushWithMiddlewares,
						nil, log.NewNopLogger(),
					)

					var wantErr error
					if c.wantErr != nil {
						wantErr = c.wantErr(scheme)
					}

					ts := mimirpb.PreallocTimeseries{
						TimeSeries: &mimirpb.TimeSeries{
							Labels: []mimirpb.LabelAdapter{{Name: "group", Value: "custom label"}},
							Samples: []mimirpb.Sample{
								{Value: 1, TimestampMs: time.Now().UnixMilli()},
							},
						},
					}
					for name, value := range c.metric {
						ts.TimeSeries.Labels = append(ts.TimeSeries.Labels, mimirpb.LabelAdapter{Name: string(name), Value: string(value)})
					}

					var wg sync.WaitGroup
					wg.Add(repeatsPerCase)

					for range repeatsPerCase {
						go func() {
							defer wg.Done()

							req := createRequest(t, createMimirWriteRequestProtobuf(t, c.skipLabelNameValidation, c.skipLabelCountValidation, ts))
							if c.skipLabelNameValidation {
								req.Header.Set(SkipLabelNameValidationHeader, "true")
							}
							if c.skipLabelCountValidation {
								req.Header.Set(SkipLabelCountValidationHeader, "true")
							}
							req.Header.Set("X-Scope-OrgID", userID)
							ctx := user.InjectOrgID(context.Background(), userID)
							req = req.WithContext(ctx)

							resp := httptest.NewRecorder()
							handler.ServeHTTP(resp, req)

							if wantErr != nil {
								assert.Equal(t, 400, resp.Code, resp.Body.String())
								assert.Contains(t, resp.Body.String(), wantErr.Error())
							} else {
								assert.Equal(t, 200, resp.Code, resp.Body.String())
							}
						}()
					}

					wg.Wait()
				})
			}
		})
	}
}

func TestValidateExemplars(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newExemplarValidationMetrics(reg)

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
		assert.Error(t, validateExemplar(m, userID, []mimirpb.LabelAdapter{}, ie))
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
		assert.NoError(t, validateExemplar(m, userID, []mimirpb.LabelAdapter{}, ve))
	}

	validation.DiscardedExemplarsCounter(reg, "random reason").WithLabelValues("different user").Inc()

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
	m.deleteUserMetrics(userID)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_discarded_exemplars_total The total number of exemplars that were discarded.
			# TYPE cortex_discarded_exemplars_total counter
			cortex_discarded_exemplars_total{reason="random reason",user="different user"} 1
	`), "cortex_discarded_exemplars_total"))
}

func TestValidateMetadata(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newMetadataValidationMetrics(reg)

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
			errors.New(metadataMetricNameMissingMsgFormat),
			nil,
		},
		{
			"with a long metric name",
			&mimirpb.MetricMetadata{MetricFamilyName: "go_goroutines_and_routines_and_routines", Type: mimirpb.COUNTER, Help: "Number of goroutines.", Unit: ""},
			fmt.Errorf(metadataMetricNameTooLongMsgFormat, "", "go_goroutines_and_routines_and_routines"),
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
			fmt.Errorf(metadataUnitTooLongMsgFormat, "a_made_up_unit_that_is_really_long", "go_goroutines"),
			nil,
		},
	} {
		t.Run(c.desc, func(t *testing.T) {
			err := cleanAndValidateMetadata(m, cfg, userID, c.metadata)
			assert.Equal(t, c.err, err, "wrong error")
			if err == nil {
				assert.Equal(t, c.metadataOut, c.metadata)
			}
		})
	}

	validation.DiscardedMetadataCounter(reg, "random reason").WithLabelValues("different user").Inc()

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_discarded_metadata_total The total number of metadata that were discarded.
			# TYPE cortex_discarded_metadata_total counter
			cortex_discarded_metadata_total{reason="metric_name_too_long",user="testUser"} 1
			cortex_discarded_metadata_total{reason="missing_metric_name",user="testUser"} 1
			cortex_discarded_metadata_total{reason="unit_too_long",user="testUser"} 1

			cortex_discarded_metadata_total{reason="random reason",user="different user"} 1
	`), "cortex_discarded_metadata_total"))

	m.deleteUserMetrics(userID)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_discarded_metadata_total The total number of metadata that were discarded.
			# TYPE cortex_discarded_metadata_total counter
			cortex_discarded_metadata_total{reason="random reason",user="different user"} 1
	`), "cortex_discarded_metadata_total"))
}

func TestValidateLabelDuplication(t *testing.T) {
	ts := time.Now()
	var cfg validateLabelsCfg
	cfg.maxLabelNameLength = 10
	cfg.maxLabelNamesPerSeries = 10
	cfg.maxLabelValueLength = 10
	cfg.validationScheme = model.LegacyValidation

	userID := "testUser"
	actual := validateLabels(newSampleValidationMetrics(nil), cfg, userID, "", []mimirpb.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "a"},
		{Name: model.MetricNameLabel, Value: "b"},
	}, false, false, nil, ts)
	expected := fmt.Errorf(
		duplicateLabelMsgFormat,
		model.MetricNameLabel,
		mimirpb.FromLabelAdaptersToString(
			[]mimirpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: "a"},
				{Name: model.MetricNameLabel, Value: "b"},
			},
		),
	)
	assert.Equal(t, expected, actual)

	actual = validateLabels(newSampleValidationMetrics(nil), cfg, userID, "", []mimirpb.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "a"},
		{Name: "a", Value: "a"},
		{Name: "a", Value: "a"},
	}, false, false, nil, ts)
	expected = fmt.Errorf(
		duplicateLabelMsgFormat,
		"a",
		mimirpb.FromLabelAdaptersToString(
			[]mimirpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: "a"},
				{Name: "a", Value: "a"},
				{Name: "a", Value: "a"},
			},
		),
	)
	assert.Equal(t, expected, actual)
}

type sampleValidationCfg struct {
	maxNativeHistogramBuckets           int
	reduceNativeHistogramOverMaxBuckets bool
}

func (c sampleValidationCfg) CreationGracePeriod(_ string) time.Duration {
	return 0
}

func (c sampleValidationCfg) PastGracePeriod(_ string) time.Duration {
	return 0
}

func (c sampleValidationCfg) OutOfOrderTimeWindow(_ string) time.Duration {
	return 0
}

func (c sampleValidationCfg) MaxNativeHistogramBuckets(_ string) int {
	return c.maxNativeHistogramBuckets
}

func (c sampleValidationCfg) ReduceNativeHistogramOverMaxBuckets(_ string) bool {
	return c.reduceNativeHistogramOverMaxBuckets
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
	metrics := newSampleValidationMetrics(registry)
	for _, limit := range []int{0, 1, 2} {
		for name, h := range testCases {
			t.Run(fmt.Sprintf("limit-%d-%s", limit, name), func(t *testing.T) {
				var cfg sampleValidationCfg
				cfg.maxNativeHistogramBuckets = limit
				ls := []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "a"}, {Name: "a", Value: "a"}}

				_, err := validateSampleHistogram(metrics, model.Now(), cfg, "user-1", "group-1", ls, &h, nil)

				if limit == 1 {
					require.Error(t, err)
					expectedErr := fmt.Errorf("received a native histogram sample with too many buckets, timestamp: %d series: a{a=\"a\"}, buckets: 2, limit: %d (err-mimir-max-native-histogram-buckets)", h.Timestamp, limit)
					require.Equal(t, expectedErr, err)
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

func TestInvalidNativeHistogramSchema(t *testing.T) {
	testCases := map[string]struct {
		schema        int32
		expectedError error
	}{
		"a valid schema causes no error": {
			schema:        3,
			expectedError: nil,
		},
		"custom bucket schema causes no error": {
			schema:        -53,
			expectedError: nil,
		},
		"a schema lower than the minimum causes an error": {
			schema:        -5,
			expectedError: fmt.Errorf("received a native histogram sample with an invalid schema: -5 (err-mimir-invalid-native-histogram-schema)"),
		},
		"a schema higher than the maximum causes an error": {
			schema:        10,
			expectedError: fmt.Errorf("received a native histogram sample with an invalid schema: 10 (err-mimir-invalid-native-histogram-schema)"),
		},
	}

	registry := prometheus.NewRegistry()
	metrics := newSampleValidationMetrics(registry)
	cfg := sampleValidationCfg{}
	hist := &mimirpb.Histogram{}
	labels := []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "a"}, {Name: "a", Value: "a"}}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			hist.Schema = testCase.schema
			_, err := validateSampleHistogram(metrics, model.Now(), cfg, "user-1", "group-1", labels, hist, nil)
			require.Equal(t, testCase.expectedError, err)
		})
	}

	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
			# HELP cortex_discarded_samples_total The total number of samples that were discarded.
			# TYPE cortex_discarded_samples_total counter
			cortex_discarded_samples_total{group="group-1",reason="invalid_native_histogram_schema",user="user-1"} 2
	`), "cortex_discarded_samples_total"))
}

func TestNativeHistogramDownScaling(t *testing.T) {
	testCases := map[string]struct {
		cfg            sampleValidationCfg
		schema         int32
		offset         int32
		deltas         []int64 // We're just using consecutive positive deltas.
		expectedError  error
		expectedDeltas []int64
		expectedUpdate bool
	}{
		"valid exponential schema": {
			schema:         3,
			deltas:         []int64{1, 2, 3},
			expectedError:  nil,
			expectedDeltas: []int64{1, 2, 3},
		},
		"downscaling not allowed": {
			cfg:           sampleValidationCfg{maxNativeHistogramBuckets: 2},
			schema:        3,
			deltas:        []int64{1, 2, 3},
			expectedError: fmt.Errorf("received a native histogram sample with too many buckets"),
		},
		"downscaling allowed": {
			cfg:            sampleValidationCfg{maxNativeHistogramBuckets: 2, reduceNativeHistogramOverMaxBuckets: true},
			schema:         3,
			offset:         1,
			deltas:         []int64{1, 2, 10},
			expectedError:  nil,
			expectedDeltas: []int64{17},
			expectedUpdate: true,
		},
		"downscaling allowed but impossible": {
			cfg:           sampleValidationCfg{maxNativeHistogramBuckets: 2, reduceNativeHistogramOverMaxBuckets: true},
			schema:        3,
			offset:        0, // This means we would have to join bucket around the boundary of 1.0, but that will never happen.
			deltas:        []int64{1, 2, 10},
			expectedError: fmt.Errorf("received a native histogram sample with too many buckets and cannot reduce"),
		},
		"valid nhcb": {
			schema:         -53,
			deltas:         []int64{1, 2, 3},
			expectedError:  nil,
			expectedDeltas: []int64{1, 2, 3},
		},
		"downscaling not possible for nhcb": {
			cfg:           sampleValidationCfg{maxNativeHistogramBuckets: 2, reduceNativeHistogramOverMaxBuckets: true},
			schema:        -53,
			offset:        1,
			deltas:        []int64{1, 2, 3},
			expectedError: fmt.Errorf("received a native histogram sample with more custom buckets than the limit"),
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			metrics := newSampleValidationMetrics(registry)
			labels := []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "h"}, {Name: "a", Value: "a"}}
			hist := &mimirpb.Histogram{
				Schema:         tc.schema,
				PositiveSpans:  []mimirpb.BucketSpan{{Offset: tc.offset, Length: uint32(len(tc.deltas))}},
				PositiveDeltas: tc.deltas,
			}
			updated, err := validateSampleHistogram(metrics, model.Now(), tc.cfg, "user-1", "group-1", labels, hist, nil)
			if tc.expectedError != nil {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError.Error())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedUpdate, updated)
			require.Equal(t, tc.expectedDeltas, hist.PositiveDeltas)
		})
	}
}

func tooManyLabelsArgs(series []mimirpb.LabelAdapter, limit int) []any {
	metric := mimirpb.FromLabelAdaptersToMetric(series).String()
	ellipsis := ""

	if utf8.RuneCountInString(metric) > 200 {
		ellipsis = "\u2026"
	}

	return []any{len(series), limit, metric, ellipsis}
}

func TestValidUTF8Message(t *testing.T) {
	testCases := map[string]struct {
		body                      []byte
		containsNonUTF8Characters bool
	}{
		"valid message returns no error": {
			body:                      []byte("valid message"),
			containsNonUTF8Characters: false,
		},
		"message containing only UTF8 characters returns no error": {
			body:                      []byte("\n\ufffd\u0016\n\ufffd\u0002\n\u001D\n\u0011container.runtime\u0012\b\n\u0006docker\n'\n\u0012container.h"),
			containsNonUTF8Characters: false,
		},
		"message containing non-UTF8 character returns an error": {
			// \xf6 and \xd3 are not valid UTF8 characters.
			body:                      []byte("\n\xf6\x1a\n\xd3\x02\n\x1d\n\x11container.runtime\x12\x08\n\x06docker\n'\n\x12container.h"),
			containsNonUTF8Characters: true,
		},
	}

	for name, tc := range testCases {
		for _, withValidation := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s withValidation: %v", name, withValidation), func(t *testing.T) {
				msg := string(tc.body)
				if withValidation {
					msg = validUTF8Message(msg)
				}
				httpgrpcErr := httpgrpc.Error(http.StatusBadRequest, msg)

				// gogo's proto.Marshal() correctly processes both httpgrpc errors with and without non-utf8 characters.
				st, ok := grpcutil.ErrorToStatus(httpgrpcErr)
				require.True(t, ok)
				stBytes, err := proto.Marshal(st.Proto())
				require.NoError(t, err)
				require.NotNil(t, stBytes)

				//lint:ignore faillint We want to explicitly use on grpcstatus.FromError()
				grpcSt, ok := grpcstatus.FromError(httpgrpcErr)
				require.True(t, ok)
				stBytes, err = golangproto.Marshal(grpcSt.Proto())
				if withValidation {
					// Ensure that errors with validated messages can always be correctly marshaled.
					require.NoError(t, err)
					require.NotNil(t, stBytes)
				} else {
					if tc.containsNonUTF8Characters {
						// Ensure that errors with non-validated non-utf8 messages cannot be correctly marshaled.
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						require.NotNil(t, stBytes)
					}
				}
			})
		}
	}
}
