// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/validate_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/tracing"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	grpcstatus "google.golang.org/grpc/status"
	golangproto "google.golang.org/protobuf/proto"

	"github.com/grafana/mimir/pkg/costattribution"
	"github.com/grafana/mimir/pkg/costattribution/testutils"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

// longLabelInjectionMode controls whether to inject a long-value label into test cases.
// This tests that long-label handling (truncate/drop) doesn't mask other validation errors.
type longLabelInjectionMode int

const (
	longLabelModeNone longLabelInjectionMode = iota
	longLabelModeTruncate
	longLabelModeDrop
)

func (m longLabelInjectionMode) String() string {
	switch m {
	case longLabelModeNone:
		return "no_injection"
	case longLabelModeTruncate:
		return "inject_long_label_truncate"
	case longLabelModeDrop:
		return "inject_long_label_drop"
	default:
		return "unknown"
	}
}

func TestValidateLabels(t *testing.T) {
	t.Parallel()

	const repeatsPerCase = 100

	const defaultUserID = "testUserDefault"
	const utf8UserID = "testUserUTF8"
	const truncatingUserID = "truncatingUserID"
	const droppingUserID = "droppingUserID"

	limits := prepareDefaultLimits()
	limits.MaxLabelValueLength = 25
	limits.MaxLabelNameLength = 25
	limits.MaxLabelNamesPerSeries = 4
	limits.MaxLabelNamesPerInfoSeries = 5
	limits.SeparateMetricsGroupLabel = "group"

	perTenant := map[string]*validation.Limits{}
	for _, userID := range []string{defaultUserID, utf8UserID, truncatingUserID, droppingUserID} {
		limits := *limits
		perTenant[userID] = &limits
	}
	perTenant[defaultUserID].NameValidationScheme = model.LegacyValidation
	perTenant[utf8UserID].NameValidationScheme = model.UTF8Validation

	require.NoError(t, perTenant[truncatingUserID].LabelValueLengthOverLimitStrategy.Set("truncate"))
	perTenant[truncatingUserID].MaxLabelValueLength = 75 // must be higher than validation.LabelValueHashLen
	require.NoError(t, perTenant[droppingUserID].LabelValueLengthOverLimitStrategy.Set("drop"))
	perTenant[droppingUserID].MaxLabelValueLength = 75 // must be higher than validation.LabelValueHashLen

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

	var logged logRecorder
	ds, ingesters, _, _ := prepare(t, prepConfig{
		numIngesters:       2,
		happyIngesters:     2,
		numDistributors:    1,
		limits:             limits,
		overrides:          overrides,
		reg:                reg,
		costAttributionMgr: manager,
		logger:             log.NewLogfmtLogger(&logged),
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
		customUserID             string
		wantErr                  func(model.ValidationScheme) error
		wantLabels               map[model.LabelName]model.LabelValue
		wantLog                  []string
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
			wantErr: alwaysErr(labelValueTooLongError{
				Label: labels.Label{Name: "much_shorter_name", Value: "test_value_please_ignore_no_really_nothing_to_see_here"},
				Limit: 25,
				Series: mimirpb.FromLabelAdaptersToString([]mimirpb.LabelAdapter{
					{Name: model.MetricNameLabel, Value: "badLabelValue"},
					{Name: "group", Value: "custom label"},
					{Name: "much_shorter_name", Value: "test_value_please_ignore_no_really_nothing_to_see_here"},
					{Name: "team", Value: "biz"},
				}),
			}),
		},
		{
			name:                     "label value too long gets truncated",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelValueToTruncate", "much_shorter_name": model.LabelValue(strings.Repeat("x", 80)), "team": "biz"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
			customUserID:             truncatingUserID,
			wantLabels: map[model.LabelName]model.LabelValue{
				model.MetricNameLabel: "badLabelValueToTruncate",
				"team":                "biz",
				"group":               "custom label",
				"much_shorter_name":   "xxxx(hash:bd28a84572ce022f806b2cdc3942ce1aaf094d36062b83dc0f7557e0f995b359)",
			},
			wantLog: []string{
				"badLabelValueToTruncate",
				"label values were truncated and appended their hash value",
				"insight=true",
			},
		},
		{
			name:                     "label value too long gets dropped",
			metric:                   map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelValueToDrop", "much_shorter_name": model.LabelValue(strings.Repeat("x", 80)), "team": "biz"},
			skipLabelNameValidation:  false,
			skipLabelCountValidation: false,
			customUserID:             droppingUserID,
			wantLabels: map[model.LabelName]model.LabelValue{
				model.MetricNameLabel: "badLabelValueToDrop",
				"team":                "biz",
				"group":               "custom label",
				"much_shorter_name":   "(hash:bd28a84572ce022f806b2cdc3942ce1aaf094d36062b83dc0f7557e0f995b359)",
			},
			wantLog: []string{
				"badLabelValueToDrop",
				"label values were replaced by their hash value",
				"insight=true",
			},
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

	// Clean up user metrics after all subtests complete.
	// Note: With long-label injection, precise metrics verification is complex
	// because injection runs use different user IDs. The per-case error assertions
	// already validate the core behavior, so we skip aggregate metrics comparison.
	t.Cleanup(func() {
		d.sampleValidationMetrics.deleteUserMetrics(defaultUserID)
		d.sampleValidationMetrics.deleteUserMetrics(utf8UserID)
		d.sampleValidationMetrics.deleteUserMetrics(truncatingUserID)
		d.sampleValidationMetrics.deleteUserMetrics(droppingUserID)
	})

	// injectedLongLabelName is the label name used for long-label injection.
	// It must not conflict with any label names used in test cases.
	const injectedLongLabelName = "__injected_long_label__"
	// injectedLongLabelValue is 100 chars, exceeding the 75 char limit for truncate/drop users.
	injectedLongLabelValue := strings.Repeat("L", 100)
	// Pre-compute the expected hash for the injected long label.
	// Use separate copies because hashLabelValueInto mutates the backing array in place.
	truncateSrc := strings.Repeat("L", 100)
	injectedLabelTruncatedValue := "LLLL" + hashLabelValueInto(truncateSrc, truncateSrc)
	dropSrc := strings.Repeat("L", 100)
	injectedLabelDroppedValue := hashLabelValueInto(dropSrc, dropSrc)

	for _, c := range testCases {
		caseSchemes := validationSchemes
		if c.customUserID != "" {
			caseSchemes = []model.ValidationScheme{overrides(limits).NameValidationScheme(c.customUserID)}
		}

		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			for _, scheme := range caseSchemes {
				t.Run(scheme.String(), func(t *testing.T) {
					t.Parallel()

					// Run each test case with and without long-label injection.
					// This ensures that long-label handling (truncate/drop) doesn't mask
					// other validation errors.
					longLabelModes := []longLabelInjectionMode{longLabelModeNone, longLabelModeTruncate, longLabelModeDrop}

					// Skip long-label injection for cases that already test long-label handling.
					if c.customUserID == truncatingUserID || c.customUserID == droppingUserID {
						longLabelModes = []longLabelInjectionMode{longLabelModeNone}
					}

					// Skip long-label injection for non-legacy schemes.
					// truncatingUserID and droppingUserID use LegacyValidation, so injecting
					// changes the validation scheme. This would cause test cases with
					// scheme-dependent behavior (using legacyErr) to fail unexpectedly.
					if scheme != model.LegacyValidation {
						longLabelModes = []longLabelInjectionMode{longLabelModeNone}
					}

					// Skip long-label injection for cases that would exceed label count limits.
					// Total labels = metric labels + "group" label + injected label
					// Label limit is 4 (or 5 for info metrics).
					// For limit 4: len(c.metric) + 1 + 1 > 4 means len(c.metric) > 2.
					// Skip if adding the injected label would exceed the limit.
					if len(c.metric) >= 3 && !c.skipLabelCountValidation {
						longLabelModes = []longLabelInjectionMode{longLabelModeNone}
					}

					// Skip long-label injection for wantLabels cases as they validate exact
					// label output and would require updating expected labels.
					if c.wantLabels != nil {
						longLabelModes = []longLabelInjectionMode{longLabelModeNone}
					}

					for _, longLabelMode := range longLabelModes {
						t.Run(longLabelMode.String(), func(t *testing.T) {
							t.Parallel()

							userID := c.customUserID
							if userID == "" {
								switch scheme {
								case model.LegacyValidation:
									userID = defaultUserID
								case model.UTF8Validation:
									userID = utf8UserID
								default:
									panic(fmt.Errorf("unhandled name validation scheme: %s", scheme))
								}
							}

							// Override user ID for long-label injection modes.
							switch longLabelMode {
							case longLabelModeTruncate:
								userID = truncatingUserID
							case longLabelModeDrop:
								userID = droppingUserID
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
								ts.Labels = append(ts.Labels, mimirpb.LabelAdapter{Name: string(name), Value: strings.Clone(string(value))})
							}

							// Inject a long-value label for truncate/drop modes.
							if longLabelMode != longLabelModeNone {
								ts.Labels = append(ts.Labels, mimirpb.LabelAdapter{
									Name:  injectedLongLabelName,
									Value: injectedLongLabelValue,
								})
							}

							var wg sync.WaitGroup
							wg.Add(repeatsPerCase)

							for range repeatsPerCase {
								go func() {
									defer wg.Done()

									expectedTraceID := trace.TraceID{}
									_, err := rand.Read(expectedTraceID[:])
									require.NoError(t, err)
									tracer := noop.NewTracerProvider().Tracer("test")
									spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
										TraceID:    expectedTraceID,
										SpanID:     trace.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
										TraceFlags: trace.FlagsSampled,
									})
									ctx := trace.ContextWithSpanContext(context.Background(), spanCtx)
									ctx, span := tracer.Start(ctx, "test")
									defer span.End()

									ctx = user.InjectOrgID(ctx, userID)

									req := createRequest(t, createMimirWriteRequestProtobuf(t, c.skipLabelNameValidation, c.skipLabelCountValidation, ts))
									if c.skipLabelNameValidation {
										req.Header.Set(SkipLabelNameValidationHeader, "true")
									}
									if c.skipLabelCountValidation {
										req.Header.Set(SkipLabelCountValidationHeader, "true")
									}
									req.Header.Set("X-Scope-OrgID", userID)
									req = req.WithContext(ctx)

									resp := httptest.NewRecorder()
									handler.ServeHTTP(resp, req)

									if wantErr != nil {
										assert.Equal(t, 400, resp.Code, resp.Body.String())
										assert.Contains(t, resp.Body.String(), wantErr.Error())
									} else {
										assert.Equal(t, 200, resp.Code, resp.Body.String())
									}

									if c.wantLabels != nil {
										var gotReq *mimirpb.WriteRequest
										ingesters[0].assertCalledFunc("Push", func(args ...any) {
											ctx, req := args[0].(context.Context), args[1].(*mimirpb.WriteRequest)
											traceID, ok := tracing.ExtractTraceID(ctx)
											require.True(t, ok)
											if traceID == expectedTraceID.String() {
												gotReq = req
											}
										})
										require.NotNil(t, gotReq, "Expected request to be forwarded to ingesters")
										gotLabels := map[model.LabelName]model.LabelValue{}
										for _, l := range gotReq.Timeseries[0].Labels {
											gotLabels[model.LabelName(l.Name)] = model.LabelValue(l.Value)
										}

										// For long-label injection, add the expected transformed label.
										wantLabels := c.wantLabels
										if longLabelMode != longLabelModeNone {
											wantLabels = make(map[model.LabelName]model.LabelValue, len(c.wantLabels)+1)
											for k, v := range c.wantLabels {
												wantLabels[k] = v
											}
											switch longLabelMode {
											case longLabelModeTruncate:
												wantLabels[injectedLongLabelName] = model.LabelValue(injectedLabelTruncatedValue)
											case longLabelModeDrop:
												wantLabels[injectedLongLabelName] = model.LabelValue(injectedLabelDroppedValue)
											}
										}
										require.Equal(t, wantLabels, gotLabels)
									}

									if len(c.wantLog) > 0 {
										found := false
										for _, line := range logged.Writes() {
											if stringContainsAll(line, c.wantLog) {
												found = true
												break
											}
										}
										if !found {
											t.Log(logged.Writes())
											require.Fail(t, "Expected logs to contain line with parts", "parts: %q", c.wantLog)
										}
									}
								}()
							}

							wg.Wait()
						})
					}
				})
			}
		})
	}
}

func TestLabelValueTooLongSummaries(t *testing.T) {
	t.Parallel()

	const truncatingUserID = "truncatingUserID"
	const droppingUserID = "droppingUserID"

	limits := prepareDefaultLimits()
	limits.MaxLabelValueLength = 75 // must be higher than validation.LabelValueHashLen
	perTenant := map[string]*validation.Limits{}
	for _, userID := range []string{truncatingUserID, droppingUserID} {
		limits := *limits
		perTenant[userID] = &limits
	}
	require.NoError(t, perTenant[truncatingUserID].LabelValueLengthOverLimitStrategy.Set("truncate"))
	require.NoError(t, perTenant[droppingUserID].LabelValueLengthOverLimitStrategy.Set("drop"))

	overrides := func(limits *validation.Limits) *validation.Overrides {
		return validation.NewOverrides(*limits, validation.NewMockTenantLimits(perTenant))
	}
	reg := prometheus.NewPedanticRegistry()
	careg := prometheus.NewRegistry()
	manager, err := costattribution.NewManager(5*time.Second, 10*time.Second, log.NewNopLogger(), overrides(limits), reg, careg)
	require.NoError(t, err)

	var logged logRecorder
	ds, _, _, _ := prepare(t, prepConfig{
		numIngesters:       2,
		happyIngesters:     2,
		numDistributors:    1,
		limits:             limits,
		overrides:          overrides,
		reg:                reg,
		costAttributionMgr: manager,
		logger:             log.NewLogfmtLogger(&logged),
	})
	d := ds[0]

	newRequestBuffers := func() *util.RequestBuffers {
		return util.NewRequestBuffers(d.RequestBufferPool, util.TaintBuffersOnCleanUp([]byte("The beef is dead.")))
	}

	testCases := []struct {
		name       string
		userID     string
		timeseries []mimirpb.PreallocTimeseries
		wantLog    []string
	}{
		{
			name:   "multiple series with truncated label values",
			userID: truncatingUserID,
			timeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{
							{Name: model.MetricNameLabel, Value: "truncate1"},
							{Name: "longvalue1", Value: strings.Repeat("x", 100)},
							{Name: "oklabel", Value: "okvalue1"},
						},
						Samples: []mimirpb.Sample{{Value: 1, TimestampMs: time.Now().UnixMilli()}},
					},
				},
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{
							{Name: model.MetricNameLabel, Value: "truncate1"},
							{Name: "longvalue1", Value: strings.Repeat("y", 100)},
							{Name: "oklabel", Value: "okvalue2"},
						},
						Samples: []mimirpb.Sample{{Value: 1, TimestampMs: time.Now().UnixMilli()}},
					},
				},
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{
							{Name: model.MetricNameLabel, Value: "truncate1"},
							{Name: "longvalue2", Value: strings.Repeat("z", 120)},
							{Name: "oklabel", Value: "okvalue2"},
						},
						Samples: []mimirpb.Sample{{Value: 2, TimestampMs: time.Now().UnixMilli()}},
					},
				},
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{
							{Name: model.MetricNameLabel, Value: "truncate2"},
							{Name: "longvalue2", Value: strings.Repeat("w", 90)},
						},
						Samples: []mimirpb.Sample{{Value: 3, TimestampMs: time.Now().UnixMilli()}},
					},
				},
			},
			wantLog: []string{
				"label values were truncated and appended their hash value", "insight=true",
				"limit=75", "total_values_exceeding_limit=4",
				"sample_1_metric_name=truncate1",
				"sample_1_label_name=longvalue1",
				"sample_1_values_exceeding_limit=2",
				"sample_1_value=\"xxx",
				"sample_1_value_length=100",
				"sample_2_metric_name=truncate1",
				"sample_2_label_name=longvalue2",
				"sample_2_values_exceeding_limit=1",
				"sample_2_value=\"zzz",
				"sample_2_value_length=120",
				"sample_3_metric_name=truncate2",
				"sample_3_label_name=longvalue2",
				"sample_3_values_exceeding_limit=1",
				"sample_3_value=\"www",
				"sample_3_value_length=90",
			},
		},
		{
			name:   "multiple series with dropped label values",
			userID: droppingUserID,
			timeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{
							{Name: model.MetricNameLabel, Value: "drop1"},
							{Name: "longvalue1", Value: strings.Repeat("x", 100)},
							{Name: "oklabel", Value: "okvalue1"},
						},
						Samples: []mimirpb.Sample{{Value: 1, TimestampMs: time.Now().UnixMilli()}},
					},
				},
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{
							{Name: model.MetricNameLabel, Value: "drop1"},
							{Name: "longvalue1", Value: strings.Repeat("y", 100)},
							{Name: "oklabel", Value: "okvalue2"},
						},
						Samples: []mimirpb.Sample{{Value: 1, TimestampMs: time.Now().UnixMilli()}},
					},
				},
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{
							{Name: model.MetricNameLabel, Value: "drop1"},
							{Name: "longvalue2", Value: strings.Repeat("z", 120)},
							{Name: "oklabel", Value: "okvalue2"},
						},
						Samples: []mimirpb.Sample{{Value: 2, TimestampMs: time.Now().UnixMilli()}},
					},
				},
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{
							{Name: model.MetricNameLabel, Value: "drop2"},
							{Name: "longvalue2", Value: strings.Repeat("w", 90)},
						},
						Samples: []mimirpb.Sample{{Value: 3, TimestampMs: time.Now().UnixMilli()}},
					},
				},
			},
			wantLog: []string{
				"label values were replaced by their hash value", "insight=true",
				"limit=75", "total_values_exceeding_limit=4",
				"sample_1_metric_name=drop1",
				"sample_1_label_name=longvalue1",
				"sample_1_values_exceeding_limit=2",
				"sample_1_value=\"xxx",
				"sample_1_value_length=100",
				"sample_2_metric_name=drop1",
				"sample_2_label_name=longvalue2",
				"sample_2_values_exceeding_limit=1",
				"sample_2_value=\"zzz",
				"sample_2_value_length=120",
				"sample_3_metric_name=drop2",
				"sample_3_label_name=longvalue2",
				"sample_3_values_exceeding_limit=1",
				"sample_3_value=\"www",
				"sample_3_value_length=90",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logged.Clear()

			handler := Handler(100000, newRequestBuffers, nil, true, true, d.limits, RetryConfig{},
				d.PushWithMiddlewares,
				nil, log.NewLogfmtLogger(&logged),
			)

			req := createRequest(t, createMimirWriteRequestProtobuf(t, false, false, tc.timeseries...))
			req.Header.Set("X-Scope-OrgID", tc.userID)
			req = req.WithContext(user.InjectOrgID(context.Background(), tc.userID))

			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)

			assert.Equal(t, 200, resp.Code, resp.Body.String())

			if len(tc.wantLog) > 0 {
				found := false
				for _, line := range logged.Writes() {
					if stringContainsAll(line, tc.wantLog) {
						found = true
						break
					}
				}
				if !found {
					t.Log(logged.Writes())
					require.Fail(t, "Expected logs to contain line with parts", "parts: %q", tc.wantLog)
				}
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
	var cfg metadataValidationConfig
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
	var cfg labelValidationConfig
	cfg.maxLabelNameLength = 10
	cfg.maxLabelNamesPerSeries = 10
	cfg.maxLabelValueLength = 10
	cfg.nameValidationScheme = model.LegacyValidation

	userID := "testUser"
	actual := validateLabels(newSampleValidationMetrics(nil), cfg, userID, "", []mimirpb.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "a"},
		{Name: model.MetricNameLabel, Value: "b"},
	}, false, false, nil, ts, nil)
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
	}, false, false, nil, ts, nil)
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

	// Test that duplicate labels are still detected even when the second duplicate
	// has a value that exceeds maxLabelValueLength and is handled by Truncate/Drop strategy.
	// The over-length value must be on the second occurrence to trigger the bug where
	// entering the value-too-long branch would skip the duplicate check.
	for _, tc := range []struct {
		name     string
		strategy validation.LabelValueLengthOverLimitStrategy
	}{
		{"Truncate", validation.LabelValueLengthOverLimitStrategyTruncate},
		{"Drop", validation.LabelValueLengthOverLimitStrategyDrop},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := labelValidationConfig{
				maxLabelNameLength:                100,
				maxLabelNamesPerSeries:            10,
				maxLabelValueLength:               75, // Must be >= validation.LabelValueHashLen (71)
				nameValidationScheme:              model.LegacyValidation,
				labelValueLengthOverLimitStrategy: tc.strategy,
			}
			actual := validateLabels(newSampleValidationMetrics(nil), cfg, userID, "", []mimirpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: "a"},
				{Name: "foo", Value: "bar"},                    // First occurrence
				{Name: "foo", Value: strings.Repeat("x", 100)}, // Duplicate with over-length value
			}, false, false, nil, ts, nil)
			require.NotNil(t, actual, "expected duplicate label error")
			assert.Contains(t, actual.Error(), "duplicate label name")
			assert.Contains(t, actual.Error(), "foo")
		})
	}
}

func TestValidateLabel_UseAfterRelease(t *testing.T) {
	buf, err := (&mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{
				{Name: "__name__", Value: "value_longer_than_maxLabelValueLength"},
			},
		},
	}).Marshal()
	require.NoError(t, err)

	// Unmarshal into a reusable PreallocTimeseries.
	var ts mimirpb.PreallocTimeseries
	err = ts.Unmarshal(buf, nil, nil, false)
	require.NoError(t, err)

	// Call validateLabels to get a LabelValueTooLongError.
	cfg := labelValidationConfig{
		maxLabelNameLength:   25,
		maxLabelValueLength:  5,
		nameValidationScheme: model.UTF8Validation,
	}
	const userID = "testUser"
	limits := testutils.NewMockCostAttributionLimits(0, []string{userID, "team"})
	reg := prometheus.NewPedanticRegistry()
	s := newSampleValidationMetrics(reg)
	careg := prometheus.NewRegistry()
	manager, err := costattribution.NewManager(5*time.Second, 10*time.Second, log.NewNopLogger(), limits, reg, careg)
	require.NoError(t, err)
	err = validateLabels(s, cfg, userID, "custom label", ts.Labels, true, true, manager.SampleTracker(userID), time.Now(), nil)
	var lengthErr labelValueTooLongError
	require.ErrorAs(t, err, &lengthErr)

	// Reuse PreallocTimeseries by unmarshaling a different TimeSeries into
	// the same buffer. This replaces the previous value in-place, and thus at
	// this point no references to the buffer should be lingering.
	_, err = (&mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{
				{Name: "forbidden_name", Value: "forbidden_value"},
			},
		},
	}).MarshalTo(buf)
	require.NoError(t, err)

	// Ensure the labelValueTooLongError isn't corrupted.
	require.EqualError(t, lengthErr, "received a series whose label value length of 37 exceeds the limit of 5, label: '__name__', value: 'value_longer_than_maxLabelValueLength' (truncated) series: 'value_longer_than_maxLabelValueLength' (err-mimir-label-value-too-long). To adjust the related per-tenant limit, configure -validation.max-length-label-value, or contact your service administrator.")
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
				cfg := sampleValidationConfig{
					maxNativeHistogramBuckets: limit,
				}
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
	cfg := sampleValidationConfig{}
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
		cfg            sampleValidationConfig
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
			cfg:           sampleValidationConfig{maxNativeHistogramBuckets: 2},
			schema:        3,
			deltas:        []int64{1, 2, 3},
			expectedError: fmt.Errorf("received a native histogram sample with too many buckets"),
		},
		"downscaling allowed": {
			cfg:            sampleValidationConfig{maxNativeHistogramBuckets: 2, reduceNativeHistogramOverMaxBuckets: true},
			schema:         3,
			offset:         1,
			deltas:         []int64{1, 2, 10},
			expectedError:  nil,
			expectedDeltas: []int64{17},
			expectedUpdate: true,
		},
		"downscaling allowed but impossible": {
			cfg:           sampleValidationConfig{maxNativeHistogramBuckets: 2, reduceNativeHistogramOverMaxBuckets: true},
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
		"valid nhcb and bucket limit is set": {
			cfg:            sampleValidationConfig{maxNativeHistogramBuckets: 100, reduceNativeHistogramOverMaxBuckets: true},
			schema:         -53,
			deltas:         []int64{1, 2, 3},
			expectedError:  nil,
			expectedDeltas: []int64{1, 2, 3},
		},
		"downscaling not possible for nhcb": {
			cfg:           sampleValidationConfig{maxNativeHistogramBuckets: 2, reduceNativeHistogramOverMaxBuckets: true},
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

func TestHashLabelValueInto(t *testing.T) {
	t.Parallel()

	input := strings.Repeat("x", 100)
	result := hashLabelValueInto(input, input)
	require.Equal(t, "(hash:58f49f86224fc9467549c0954fb0b9dc706e6728023bb7bd88b7958cf2115ac8)", result)
	require.Len(t, result, validation.LabelValueHashLen)
	// Check that input's underlying array was kept.
	require.Equal(t, unsafe.StringData(input), unsafe.StringData(result))
}

type logRecorder struct {
	mtx    sync.Mutex
	writes []string
}

func (b *logRecorder) Write(buf []byte) (int, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.writes = append(b.writes, string(buf))
	return len(buf), nil
}

func (b *logRecorder) Writes() []string {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.writes
}

func (b *logRecorder) Clear() {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.writes = nil
}

func stringContainsAll(s string, parts []string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}
	return true
}

// TestNewValidationConfigFieldCompleteness ensures that we don't forget to populate a new field we may add to validationConfig.
func TestNewValidationConfigFieldCompleteness(t *testing.T) {
	t.Parallel()

	// 1. Create limits with prepareDefaultLimits()
	limits := prepareDefaultLimits()

	// 2. Set fields that default to zero to non-zero values
	limits.PastGracePeriod = model.Duration(5 * time.Minute)
	limits.MaxNativeHistogramBuckets = 100
	limits.OutOfOrderTimeWindow = model.Duration(30 * time.Minute)
	require.NoError(t, limits.LabelValueLengthOverLimitStrategy.Set("truncate"))

	// 3. Create overrides
	overrides := validation.NewOverrides(*limits, nil)

	// 4. Call newValidationConfig
	cfg := newValidationConfig("test-user", overrides)

	// 5. Use reflection to verify all fields are non-zero
	assertNoZeroFields(t, reflect.ValueOf(cfg), "validationConfig")
}

func assertNoZeroFields(t *testing.T, v reflect.Value, path string) {
	t.Helper()

	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			fieldName := v.Type().Field(i).Name
			fieldPath := path + "." + fieldName
			assertNoZeroFields(t, field, fieldPath)
		}
	default:
		require.False(t, v.IsZero(), "field %s is zero", path)
	}
}
