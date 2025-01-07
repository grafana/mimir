// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/compat_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/storage/series"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/test"
)

type fakePusher struct {
	request  *mimirpb.WriteRequest
	response *mimirpb.WriteResponse
	err      error
}

func (p *fakePusher) Push(_ context.Context, r *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	p.request = r
	return p.response, p.err
}

func TestPusherAppendable(t *testing.T) {
	pusher := &fakePusher{}
	pa := NewPusherAppendable(pusher, "user-1", promauto.With(nil).NewCounter(prometheus.CounterOpts{}), promauto.With(nil).NewCounter(prometheus.CounterOpts{}))

	type sample struct {
		series         string
		value          float64
		histogram      *histogram.Histogram
		floatHistogram *histogram.FloatHistogram
		ts             int64
	}

	for _, tc := range []struct {
		name         string
		hasNanSample bool // If true, it will be a single float sample with NaN.
		samples      []sample
	}{
		{
			name: "tenant without delay, normal value",
			samples: []sample{
				{
					series: "foo_bar",
					value:  1.234,
					ts:     120_000,
				},
			},
		},
		{
			name:         "tenant without delay, stale nan value",
			hasNanSample: true,
			samples: []sample{
				{
					series: "foo_bar",
					value:  math.Float64frombits(value.StaleNaN),
					ts:     120_000,
				},
			},
		},
		{
			name: "ALERTS, normal value",
			samples: []sample{
				{
					series: `ALERTS{alertname="boop"}`,
					value:  1.234,
					ts:     120_000,
				},
			},
		},
		{
			name:         "ALERTS, stale nan value",
			hasNanSample: true,
			samples: []sample{
				{
					series: `ALERTS{alertname="boop"}`,
					value:  math.Float64frombits(value.StaleNaN),
					ts:     120_000,
				},
			},
		},
		{
			name: "tenant without delay, histogram value",
			samples: []sample{
				{
					series:    "foo_bar",
					histogram: test.GenerateTestHistogram(10),
					ts:        200_000,
				},
			},
		},
		{
			name: "tenant without delay, float histogram value",
			samples: []sample{
				{
					series:         "foo_bar",
					floatHistogram: test.GenerateTestFloatHistogram(10),
					ts:             230_000,
				},
			},
		},
		{
			name: "mix of float and float histogram",
			samples: []sample{
				{
					series: "foo_bar1",
					value:  999,
					ts:     230_000,
				},
				{
					series: "foo_bar3",
					value:  888,
					ts:     230_000,
				},
				{
					series:         "foo_bar2",
					floatHistogram: test.GenerateTestFloatHistogram(10),
					ts:             230_000,
				},
				{
					series:         "foo_bar4",
					floatHistogram: test.GenerateTestFloatHistogram(99),
					ts:             230_000,
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			var expReq []mimirpb.PreallocTimeseries

			pusher.response = &mimirpb.WriteResponse{}
			a := pa.Appender(ctx)
			for _, sm := range tc.samples {
				lbls, err := parser.ParseMetric(sm.series)
				require.NoError(t, err)
				timeseries := mimirpb.PreallocTimeseries{
					TimeSeries: &mimirpb.TimeSeries{
						Labels:    mimirpb.FromLabelsToLabelAdapters(lbls),
						Exemplars: []mimirpb.Exemplar{},
						Samples:   []mimirpb.Sample{},
					},
				}
				expReq = append(expReq, timeseries)

				if sm.histogram != nil || sm.floatHistogram != nil {
					_, err = a.AppendHistogram(0, lbls, sm.ts, sm.histogram, sm.floatHistogram)
					if sm.histogram != nil {
						timeseries.Histograms = append(timeseries.Histograms, mimirpb.FromHistogramToHistogramProto(sm.ts, sm.histogram))
					} else {
						timeseries.Histograms = append(timeseries.Histograms, mimirpb.FromFloatHistogramToHistogramProto(sm.ts, sm.floatHistogram))
					}
				} else {
					_, err = a.Append(0, lbls, sm.ts, sm.value)
					timeseries.Samples = append(timeseries.Samples, mimirpb.Sample{
						TimestampMs: sm.ts,
						Value:       sm.value,
					})
				}
				require.NoError(t, err)
			}
			require.NoError(t, a.Commit())

			if !tc.hasNanSample {
				require.Equal(t, expReq, pusher.request.Timeseries)
				return
			}

			// For NaN, we cannot use require.Equal.
			require.Len(t, pusher.request.Timeseries, 1)
			require.Len(t, pusher.request.Timeseries[0].Samples, 1)
			lbls, err := parser.ParseMetric(tc.samples[0].series)
			require.NoError(t, err)
			require.Equal(t, 0, labels.Compare(mimirpb.FromLabelAdaptersToLabels(pusher.request.Timeseries[0].Labels), lbls))
			require.Equal(t, tc.samples[0].ts, pusher.request.Timeseries[0].Samples[0].TimestampMs)
			require.True(t, math.IsNaN(pusher.request.Timeseries[0].Samples[0].Value))
		})
	}
}

func TestPusherErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		returnedError    error
		expectedWrites   int
		expectedFailures int
	}{
		"no error": {
			expectedWrites:   1,
			expectedFailures: 0,
		},
		"a 400 HTTPgRPC error is not reported as failure": {
			returnedError:    httpgrpc.Errorf(http.StatusBadRequest, "test error"),
			expectedWrites:   1,
			expectedFailures: 0,
		},
		"a 500 HTTPgRPC error is reported as failure": {
			returnedError:    httpgrpc.Errorf(http.StatusInternalServerError, "test error"),
			expectedWrites:   1,
			expectedFailures: 1,
		},
		"a BAD_DATA push error is not reported as failure": {
			returnedError:    mustStatusWithDetails(codes.FailedPrecondition, mimirpb.BAD_DATA).Err(),
			expectedWrites:   1,
			expectedFailures: 0,
		},
		"a METHOD_NOT_ALLOWED push error is reported as failure": {
			returnedError:    mustStatusWithDetails(codes.Unimplemented, mimirpb.METHOD_NOT_ALLOWED).Err(),
			expectedWrites:   1,
			expectedFailures: 1,
		},
		"a TSDB_UNAVAILABLE push error is reported as failure": {
			returnedError:    mustStatusWithDetails(codes.FailedPrecondition, mimirpb.TSDB_UNAVAILABLE).Err(),
			expectedWrites:   1,
			expectedFailures: 1,
		},
		"an unknown error is reported as failure": {
			returnedError:    errors.New("test error"),
			expectedWrites:   1,
			expectedFailures: 1, // unknown errors are not 400, so they are reported.
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			pusher := &fakePusher{err: tc.returnedError, response: &mimirpb.WriteResponse{}}

			writes := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			failures := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			pa := NewPusherAppendable(pusher, "user-1", writes, failures)

			lbls, err := parser.ParseMetric("foo_bar")
			require.NoError(t, err)

			a := pa.Appender(ctx)
			_, err = a.Append(0, lbls, int64(model.Now()), 123456)
			require.NoError(t, err)

			require.Equal(t, tc.returnedError, a.Commit())

			require.Equal(t, tc.expectedWrites, int(testutil.ToFloat64(writes)))
			require.Equal(t, tc.expectedFailures, int(testutil.ToFloat64(failures)))
		})
	}
}

func TestMetricsQueryFuncErrors(t *testing.T) {
	// Cases handled the same on remote and local
	commonCases := map[string]struct {
		returnedError         error
		expectedError         error
		expectedQueries       int
		expectedFailedQueries int
	}{
		"no error": {
			expectedQueries:       1,
			expectedFailedQueries: 0,
		},
		"promql.ErrStorage": {
			returnedError:         WrapQueryableErrors(promql.ErrStorage{Err: errors.New("test error")}),
			expectedError:         promql.ErrStorage{Err: errors.New("test error")},
			expectedQueries:       1,
			expectedFailedQueries: 1,
		},
		"promql.ErrQueryCanceled": {
			returnedError:         WrapQueryableErrors(promql.ErrQueryCanceled("test error")),
			expectedError:         promql.ErrQueryCanceled("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // Not interesting.
		},

		"promql.ErrQueryTimeout": {
			returnedError:         WrapQueryableErrors(promql.ErrQueryTimeout("test error")),
			expectedError:         promql.ErrQueryTimeout("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // Not interesting.
		},

		"promql.ErrTooManySamples": {
			returnedError:         WrapQueryableErrors(promql.ErrTooManySamples("test error")),
			expectedError:         promql.ErrTooManySamples("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // Not interesting.
		},
		"unknown error": {
			returnedError:         WrapQueryableErrors(errors.New("test error")),
			expectedError:         errors.New("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 1, // unknown errors are not 400, so they are reported.
		},
	}
	type testCase struct {
		returnedError         error
		expectedError         error
		expectedQueries       int
		expectedFailedQueries int
		remoteQuerier         bool
	}
	// Add special cases to test first.
	allCases := map[string]testCase{
		"httpgrpc 400 error": {
			returnedError:         httpgrpc.Errorf(http.StatusBadRequest, "test error"),
			expectedError:         httpgrpc.Errorf(http.StatusBadRequest, "test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // 400 errors not reported as failures.
			remoteQuerier:         true,
		},

		"httpgrpc 500 error": {
			returnedError:         httpgrpc.Errorf(http.StatusInternalServerError, "test error"),
			expectedError:         httpgrpc.Errorf(http.StatusInternalServerError, "test error"),
			expectedQueries:       1,
			expectedFailedQueries: 1, // 500 errors are failures
			remoteQuerier:         true,
		},

		"unknown but non-queryable error from remote": {
			returnedError:         errors.New("test error"),
			expectedError:         errors.New("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 1, // Any other error should always be reported.
			remoteQuerier:         true,
		},
		"unknown but non-queryable error from local": {
			returnedError:         errors.New("test error"),
			expectedError:         errors.New("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // Assume that simple local errors are coming from user errors.
		},
	}
	// Add the common cases for both remote and local.
	for _, rc := range []bool{true, false} {
		for name, tc := range commonCases {
			allCases[fmt.Sprintf("%s remote=%v", name, rc)] = testCase{
				returnedError:         tc.returnedError,
				expectedError:         tc.expectedError,
				expectedQueries:       tc.expectedQueries,
				expectedFailedQueries: tc.expectedFailedQueries,
				remoteQuerier:         rc,
			}
		}
	}
	for name, tc := range allCases {
		t.Run(name, func(t *testing.T) {
			queries := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			failures := promauto.With(nil).NewCounter(prometheus.CounterOpts{})

			mockFunc := func(context.Context, string, time.Time) (promql.Vector, error) {
				return promql.Vector{}, tc.returnedError
			}
			qf := MetricsQueryFunc(mockFunc, queries, failures, tc.remoteQuerier)

			_, err := qf(context.Background(), "test", time.Now())
			require.Equal(t, tc.expectedError, err)

			require.Equal(t, tc.expectedQueries, int(testutil.ToFloat64(queries)))
			require.Equal(t, tc.expectedFailedQueries, int(testutil.ToFloat64(failures)))
		})
	}
}

func TestRecordAndReportRuleQueryMetrics(t *testing.T) {
	queryTime := promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"})
	zeroFetchedSeriesCount := promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"})

	mockFunc := func(context.Context, string, time.Time) (promql.Vector, error) {
		time.Sleep(1 * time.Second)
		return promql.Vector{}, nil
	}
	qf := RecordAndReportRuleQueryMetrics(mockFunc, queryTime.WithLabelValues("userID"), zeroFetchedSeriesCount.WithLabelValues("userID"), log.NewNopLogger())

	// Ensure we start with counters at 0.
	require.LessOrEqual(t, float64(0), testutil.ToFloat64(queryTime.WithLabelValues("userID")))
	require.Equal(t, float64(0), testutil.ToFloat64(zeroFetchedSeriesCount.WithLabelValues("userID")))

	// Increment zeroFetchedSeriesCount for non-existent series.
	_, _ = qf(context.Background(), "test", time.Now())
	require.LessOrEqual(t, float64(1), testutil.ToFloat64(queryTime.WithLabelValues("userID")))
	require.Equal(t, float64(1), testutil.ToFloat64(zeroFetchedSeriesCount.WithLabelValues("userID")))

	// Increment zeroFetchedSeriesCount for another non-existent series.
	_, _ = qf(context.Background(), "test2", time.Now())
	require.LessOrEqual(t, float64(2), testutil.ToFloat64(queryTime.WithLabelValues("userID")))
	require.Equal(t, float64(2), testutil.ToFloat64(zeroFetchedSeriesCount.WithLabelValues("userID")))

	// Don't increment zeroFetchedSeriesCount for query without series selectors.
	_, _ = qf(context.Background(), "vector(0.995)", time.Now())
	require.LessOrEqual(t, float64(3), testutil.ToFloat64(queryTime.WithLabelValues("userID")))
	require.Equal(t, float64(2), testutil.ToFloat64(zeroFetchedSeriesCount.WithLabelValues("userID")))

	// Don't increment zeroFetchedSeriesCount for another query without series selectors.
	_, _ = qf(context.Background(), "vector(2.4192e+15 / 1e+09)", time.Now())
	require.LessOrEqual(t, float64(4), testutil.ToFloat64(queryTime.WithLabelValues("userID")))
	require.Equal(t, float64(2), testutil.ToFloat64(zeroFetchedSeriesCount.WithLabelValues("userID")))

	// Increment zeroFetchedSeriesCount for non-existent series even when combined with a non-series selector.
	_, _ = qf(context.Background(), "test + vector(0.995)", time.Now())
	require.LessOrEqual(t, float64(5), testutil.ToFloat64(queryTime.WithLabelValues("userID")))
	require.Equal(t, float64(3), testutil.ToFloat64(zeroFetchedSeriesCount.WithLabelValues("userID")))

	// Don't increment zeroFetchedSeriesCount for queries with errors.
	_, _ = qf(context.Background(), "rate(test)", time.Now())
	require.LessOrEqual(t, float64(6), testutil.ToFloat64(queryTime.WithLabelValues("userID")))
	require.Equal(t, float64(3), testutil.ToFloat64(zeroFetchedSeriesCount.WithLabelValues("userID")))
}

// TestDefaultManagerFactory_CorrectQueryableUsed ensures that when evaluating a group with non-empty SourceTenants
// the federated queryable is called. If SourceTenants are empty, then the regular queryable should be used.
// This is to ensure that the `__tenant_id__` label is present for all rules evaluating within a federated rule group.
func TestDefaultManagerFactory_CorrectQueryableUsed(t *testing.T) {
	const userID = "tenant-1"

	dummyRules := []*rulespb.RuleDesc{createRecordingRule("sum:up", "sum(up)")}

	testCases := map[string]struct {
		ruleGroup rulespb.RuleGroupDesc

		federatedQueryableCalled bool
		regularQueryableCalled   bool
	}{
		"regular rule group (without source tenants) uses regular querier": {
			ruleGroup: rulespb.RuleGroupDesc{
				Name:  "non-federated",
				Rules: dummyRules,
			},
			regularQueryableCalled: true,
		},
		"federated rule group with single source tenant uses federated querier": {
			ruleGroup: rulespb.RuleGroupDesc{
				Name:          "federated-1",
				SourceTenants: []string{"tenant-2"},
				Rules:         dummyRules,
			},
			federatedQueryableCalled: true,
		},
		"federated rule group with multiple source tenants uses federated querier": {
			ruleGroup: rulespb.RuleGroupDesc{
				Name:          "federated-2",
				SourceTenants: []string{"tenant-2", "tenant-3"},
				Rules:         dummyRules,
			},
			federatedQueryableCalled: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// assumptions
			require.True(t, tc.regularQueryableCalled != tc.federatedQueryableCalled, "choose only one of regularQueryableCalled or federatedQueryableCalled")
			for _, r := range tc.ruleGroup.Rules {
				const msg = "this test only works with recording rules: the regular queryable will be " +
					"called an additional one time to restore the state of an alerting rule"
				require.NotEmpty(t, r.Record, msg)
				require.Empty(t, r.Alert, msg)
			}

			// setup
			cfg := defaultRulerConfig(t)
			options := applyPrepareOptions(t, cfg.Ring.Common.InstanceID)
			notifierManager := notifier.NewManager(&notifier.Options{Do: func(_ context.Context, _ *http.Client, _ *http.Request) (*http.Response, error) { return nil, nil }}, util_log.SlogFromGoKit(options.logger))
			ruleFiles := writeRuleGroupToFiles(t, cfg.RulePath, options.logger, userID, tc.ruleGroup)
			regularQueryable, federatedQueryable := newMockQueryable(), newMockQueryable()

			tracker := promql.NewActiveQueryTracker(t.TempDir(), 20, promslog.NewNopLogger())
			eng := promql.NewEngine(promql.EngineOpts{
				MaxSamples:         1e6,
				ActiveQueryTracker: tracker,
				Timeout:            2 * time.Minute,
			})
			regularQueryFunc := rules.EngineQueryFunc(eng, regularQueryable)
			federatedQueryFunc := rules.EngineQueryFunc(eng, federatedQueryable)

			queryFunc := TenantFederationQueryFunc(regularQueryFunc, federatedQueryFunc)

			// create and use manager factory
			pusher := newPusherMock()
			pusher.MockPush(&mimirpb.WriteResponse{}, nil)
			managerFactory := DefaultTenantManagerFactory(cfg, pusher, federatedQueryable, queryFunc, &NoopMultiTenantConcurrencyController{}, options.limits, nil)

			manager := managerFactory(context.Background(), userID, notifierManager, options.logger, nil)

			// load rules into manager and start
			require.NoError(t, manager.Update(time.Millisecond, ruleFiles, labels.EmptyLabels(), "", nil))
			go manager.Run()

			select {
			case <-regularQueryable.called:
				require.True(t, tc.regularQueryableCalled, "unexpected call to regular queryable")
			case <-federatedQueryable.called:
				require.True(t, tc.federatedQueryableCalled, "unexpected call to federated queryable")
			case <-time.NewTimer(time.Second).C:
				require.Fail(t, "neither of the queryables was called within the timeout")
			}

			// Ensure the result has been written.
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				pusher.AssertCalled(test.NewCollectWithLogf(collect), "Push", mock.Anything, mock.Anything)
			}, 5*time.Second, 100*time.Millisecond)

			manager.Stop()
		})
	}
}

func TestDefaultManagerFactory_ShouldNotWriteRecordingRuleResultsWhenDisabled(t *testing.T) {
	const userID = "tenant-1"

	for _, writeEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("write enabled: %t", writeEnabled), func(t *testing.T) {
			t.Parallel()

			// Create a test recording rule.
			ruleGroup := rulespb.RuleGroupDesc{
				Name:     "test",
				Interval: time.Second,
				Rules: []*rulespb.RuleDesc{{
					Record: "test",
					Expr:   "1",
				}},
			}

			// Setup ruler with writes disabled.
			cfg := defaultRulerConfig(t)
			cfg.RuleEvaluationWriteEnabled = writeEnabled

			var (
				options         = applyPrepareOptions(t, cfg.Ring.Common.InstanceID)
				notifierManager = notifier.NewManager(&notifier.Options{Do: func(_ context.Context, _ *http.Client, _ *http.Request) (*http.Response, error) { return nil, nil }}, util_log.SlogFromGoKit(options.logger))
				ruleFiles       = writeRuleGroupToFiles(t, cfg.RulePath, options.logger, userID, ruleGroup)
				queryable       = newMockQueryable()
				tracker         = promql.NewActiveQueryTracker(t.TempDir(), 20, util_log.SlogFromGoKit(log.NewNopLogger()))
				eng             = promql.NewEngine(promql.EngineOpts{
					MaxSamples:         1e6,
					ActiveQueryTracker: tracker,
					Timeout:            2 * time.Minute,
				})
				queryFunc = rules.EngineQueryFunc(eng, queryable)
			)

			pusher := newPusherMock()
			pusher.MockPush(&mimirpb.WriteResponse{}, nil)

			factory := DefaultTenantManagerFactory(cfg, pusher, queryable, queryFunc, &NoopMultiTenantConcurrencyController{}, options.limits, nil)
			manager := factory(context.Background(), userID, notifierManager, options.logger, nil)

			// Load rules into manager and start it.
			require.NoError(t, manager.Update(time.Millisecond, ruleFiles, labels.EmptyLabels(), "", nil))
			go manager.Run()

			// Wait until the query has been executed.
			select {
			case <-queryable.called:
				t.Log("query executed")
			case <-time.NewTimer(time.Second).C:
				require.Fail(t, "no query executed")
			}

			if writeEnabled {
				// Ensure the result has been written.
				require.EventuallyWithT(t, func(collect *assert.CollectT) {
					pusher.AssertCalled(test.NewCollectWithLogf(collect), "Push", mock.Anything, mock.Anything)
				}, 5*time.Second, 100*time.Millisecond)
			} else {
				// Ensure no write occurred within a reasonable amount of time.
				time.Sleep(time.Second)
				pusher.AssertNumberOfCalls(t, "Push", 0)
			}

			manager.Stop()

		})
	}

}

func TestDefaultManagerFactory_ShouldInjectReadConsistencyToContextBasedOnRuleDetail(t *testing.T) {
	const userID = "tenant-1"

	tests := map[string]struct {
		ruleGroup               rulespb.RuleGroupDesc
		expectedReadConsistency map[string]string
	}{
		"should inject strong read consistency if the rule is not independent": {
			ruleGroup: rulespb.RuleGroupDesc{
				Name: "dependent-rules",
				Rules: []*rulespb.RuleDesc{
					createRecordingRule("sum:up:1", "sum(up)"),
					createRecordingRule("sum:up:2", "sum:up:1"),
				},
			},
			expectedReadConsistency: map[string]string{
				`__name__="up"`:       "", // Does not depend on any other rule in the group.
				`__name__="sum:up:1"`: api.ReadConsistencyStrong,
			},
		},
		"should not inject read consistency level if the rule is independent, to let run with the per-tenant default": {
			ruleGroup: rulespb.RuleGroupDesc{
				Name: "independent-rules",
				Rules: []*rulespb.RuleDesc{
					createRecordingRule("sum:up:1", "sum(up_1)"),
					createRecordingRule("sum:up:2", "sum(up_2)"),
				},
			},
			expectedReadConsistency: map[string]string{
				`__name__="up_1"`: "",
				`__name__="up_2"`: "",
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			var (
				cfg             = defaultRulerConfig(t)
				options         = applyPrepareOptions(t, cfg.Ring.Common.InstanceID)
				notifierManager = notifier.NewManager(&notifier.Options{Do: func(_ context.Context, _ *http.Client, _ *http.Request) (*http.Response, error) { return nil, nil }}, util_log.SlogFromGoKit(options.logger))
				tracker         = promql.NewActiveQueryTracker(t.TempDir(), 20, util_log.SlogFromGoKit(options.logger))
				eng             = promql.NewEngine(promql.EngineOpts{
					MaxSamples:         1e6,
					ActiveQueryTracker: tracker,
					Timeout:            2 * time.Minute,
				})

				// Count the number of rules evaluated.
				ruleEvaluationsCount = atomic.NewInt64(0)

				// Channel that gets closed once the expected number of rules have been evaluated.
				ruleEvaluationsDone = make(chan struct{})
			)

			// Mock the querier.
			querier := newQuerierMock()
			querier.selectFunc = func(ctx context.Context, _ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
				// Stringify the matchers.
				matchersStrings := make([]string, 0, len(matchers))
				for _, m := range matchers {
					matchersStrings = append(matchersStrings, m.String())
				}
				matchersString := strings.Join(matchersStrings, ",")

				// Ensure the read consistency injected in the context is the expected one.
				actual, _ := api.ReadConsistencyLevelFromContext(ctx)
				expected, hasExpected := testData.expectedReadConsistency[matchersString]
				assert.Truef(t, hasExpected, "missing expected read consistency for matchers: %s", matchersString)
				assert.Equal(t, expected, actual)

				if ruleEvaluationsCount.Inc() == int64(len(testData.ruleGroup.Rules)) {
					close(ruleEvaluationsDone)
				}

				return storage.EmptySeriesSet()
			}

			// Mock the pusher.
			pusher := newPusherMock()
			pusher.MockPush(&mimirpb.WriteResponse{}, nil)

			// Create the manager from the factory.
			queryable := &storage.MockQueryable{MockQuerier: querier}
			managerFactory := DefaultTenantManagerFactory(cfg, pusher, queryable, rules.EngineQueryFunc(eng, queryable), &NoopMultiTenantConcurrencyController{}, options.limits, nil)
			manager := managerFactory(context.Background(), userID, notifierManager, options.logger, nil)

			// Load rules into manager.
			ruleFiles := writeRuleGroupToFiles(t, cfg.RulePath, options.logger, userID, testData.ruleGroup)
			require.NoError(t, manager.Update(time.Millisecond, ruleFiles, labels.EmptyLabels(), "", nil))

			// Start the manager.
			go manager.Run()
			t.Cleanup(manager.Stop)

			// Wait until the expected rule evaluations have run.
			select {
			case <-ruleEvaluationsDone:
			case <-time.After(5 * time.Second):
				t.Fatalf("the ruler manager has not evaluated the expected number of rules (evaluated: %d)", ruleEvaluationsCount.Load())
			}
		})
	}
}

func TestDefaultManagerFactory_ShouldInjectStrongReadConsistencyToContextWhenQueryingAlertsForStateMetric(t *testing.T) {
	const (
		userID     = "tenant-1"
		metricName = "test_metric"
	)

	// Configure the ruler so that rules are evaluated very frequently and any alert state restored
	// (active alerts with a "for" duration > the "grace period" are restored).
	cfg := defaultRulerConfig(t)
	cfg.ForGracePeriod = 0
	cfg.EvaluationInterval = time.Second

	var (
		options         = applyPrepareOptions(t, cfg.Ring.Common.InstanceID)
		notifierManager = notifier.NewManager(&notifier.Options{Do: func(_ context.Context, _ *http.Client, _ *http.Request) (*http.Response, error) { return nil, nil }}, util_log.SlogFromGoKit(options.logger))
		tracker         = promql.NewActiveQueryTracker(t.TempDir(), 20, util_log.SlogFromGoKit(options.logger))
		eng             = promql.NewEngine(promql.EngineOpts{
			MaxSamples:         1e6,
			ActiveQueryTracker: tracker,
			Timeout:            2 * time.Minute,
		})

		// Create a test alerting rule with a "for" duration greater than the "grace period".
		ruleGroup = rulespb.RuleGroupDesc{
			Name:     "test",
			Interval: cfg.EvaluationInterval,
			Rules: []*rulespb.RuleDesc{{
				Expr:  fmt.Sprintf("%s > 0", metricName),
				Alert: "test",
				For:   100 * time.Millisecond,
			}},
		}

		// Channel that gets closed once the ALERTS_FOR_STATE metric is queried.
		alertsForStateQueried = make(chan struct{})
	)

	// Mock the querier.
	querier := newQuerierMock()
	querier.selectFunc = func(ctx context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
		t.Logf("Select() called with matchers: %v", matchers)

		if isQueryingAlertsForStateMetric("", matchers...) {
			// Ensure it's queried with strong read consistency.
			actual, ok := api.ReadConsistencyLevelFromContext(ctx)
			assert.True(t, ok)
			assert.Equal(t, api.ReadConsistencyStrong, actual)

			// Close alertsForStateQueried channel (only once).
			select {
			case <-alertsForStateQueried:
			default:
				close(alertsForStateQueried)
			}

			return storage.EmptySeriesSet()
		}

		// If it's not the ALERTS_FOR_STATE query, then it should be the alerting query. We want the alert
		// to fire, so we return a non-empty series set.
		return series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
			series.NewConcreteSeries(
				labels.FromStrings(labels.MetricName, metricName),
				[]model.SamplePair{{Timestamp: model.Time(hints.End - 1), Value: 1.0}},
				nil,
			),
		})
	}

	// Mock the pusher.
	pusher := newPusherMock()
	pusher.MockPush(&mimirpb.WriteResponse{}, nil)

	// Create the manager from the factory.
	queryable := &storage.MockQueryable{MockQuerier: querier}
	managerFactory := DefaultTenantManagerFactory(cfg, pusher, queryable, rules.EngineQueryFunc(eng, queryable), &NoopMultiTenantConcurrencyController{}, options.limits, nil)
	manager := managerFactory(context.Background(), userID, notifierManager, options.logger, nil)

	// Load rules into manager.
	ruleFiles := writeRuleGroupToFiles(t, cfg.RulePath, options.logger, userID, ruleGroup)
	require.NoError(t, manager.Update(time.Millisecond, ruleFiles, labels.EmptyLabels(), "", nil))

	// Start the manager.
	go manager.Run()
	t.Cleanup(manager.Stop)

	// Wait until the ALERTS_FOR_STATE metric has been queried.
	select {
	case <-alertsForStateQueried:
	case <-time.After(5 * time.Second):
		t.Fatal("the ruler manager has not queried ALERTS_FOR_STATE metric")
	}
}

func writeRuleGroupToFiles(t *testing.T, path string, logger log.Logger, userID string, ruleGroup rulespb.RuleGroupDesc) []string {
	_, files, err := newMapper(path, logger).MapRules(userID, map[string][]rulefmt.RuleGroup{
		"namespace": {rulespb.FromProto(&ruleGroup)},
	})
	require.NoError(t, err)
	require.Len(t, files, 1, "writing a single namespace, expecting a single file")

	return files
}

// mockQueryable closes called when it's Querier method gets called.
// You can use newMockQueryable to instantiate one.
type mockQueryable struct {
	called chan struct{}
}

func newMockQueryable() *mockQueryable {
	return &mockQueryable{
		called: make(chan struct{}),
	}
}

func (m *mockQueryable) Querier(_, _ int64) (storage.Querier, error) {
	select {
	case <-m.called:
		// already closed
	default:
		close(m.called)
	}
	return storage.NoopQuerier(), nil
}

func mustStatusWithDetails(code codes.Code, cause mimirpb.ErrorCause) *status.Status {
	s, err := status.New(code, "").WithDetails(&mimirpb.ErrorDetails{Cause: cause})
	if err != nil {
		panic(err)
	}
	return s
}
