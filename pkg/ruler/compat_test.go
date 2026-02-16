// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/compat_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"bytes"
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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/spf13/afero"
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
	"github.com/grafana/mimir/pkg/util/promqlext"
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
	pa := NewPusherAppendable(
		pusher,
		"user-1",
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user", "reason"}),
	)

	for _, tc := range []struct {
		name         string
		hasNanSample bool // If true, it will be a single float sample with NaN.
		series       []test.Series
	}{
		{
			name: "tenant without delay, normal value",
			series: []test.Series{
				{
					Labels:  labels.FromStrings(model.MetricNameLabel, "foo_bar"),
					Samples: []test.Sample{{TS: 120_000, Val: 1.234}},
				},
			},
		},

		{
			name:         "tenant without delay, stale nan value",
			hasNanSample: true,
			series: []test.Series{
				{
					Labels:  labels.FromStrings(model.MetricNameLabel, "foo_bar"),
					Samples: []test.Sample{{TS: 120_000, Val: math.Float64frombits(value.StaleNaN)}},
				},
			},
		},
		{
			name: "ALERTS, normal value",
			series: []test.Series{
				{
					Labels:  labels.FromStrings(model.MetricNameLabel, "ALERT", labels.AlertName, "boop"),
					Samples: []test.Sample{{TS: 120_000, Val: 1.234}},
				},
			},
		},
		{
			name:         "ALERTS, stale nan value",
			hasNanSample: true,
			series: []test.Series{
				{
					Labels:  labels.FromStrings(model.MetricNameLabel, "ALERT", labels.AlertName, "boop"),
					Samples: []test.Sample{{TS: 120_000, Val: math.Float64frombits(value.StaleNaN)}},
				},
			},
		},
		{
			name: "tenant without delay, histogram value",
			series: []test.Series{
				{
					Labels:  labels.FromStrings(model.MetricNameLabel, "foo_bar"),
					Samples: []test.Sample{{TS: 200_000, Hist: test.GenerateTestHistogram(10)}},
				},
			},
		},
		{
			name: "tenant without delay, float histogram value",
			series: []test.Series{
				{
					Labels:  labels.FromStrings(model.MetricNameLabel, "foo_bar"),
					Samples: []test.Sample{{TS: 230_000, FloatHist: test.GenerateTestFloatHistogram(10)}},
				},
			},
		},
		{
			name: "mix of float and float histogram",
			series: []test.Series{
				{
					Labels:  labels.FromStrings(model.MetricNameLabel, "foo_bar1"),
					Samples: []test.Sample{{TS: 230_000, Val: 999}},
				},
				{
					Labels:  labels.FromStrings(model.MetricNameLabel, "foo_bar3"),
					Samples: []test.Sample{{TS: 230_000, Val: 888}},
				},
				{
					Labels:  labels.FromStrings(model.MetricNameLabel, "foo_bar2"),
					Samples: []test.Sample{{TS: 230_000, FloatHist: test.GenerateTestFloatHistogram(10)}},
				},
				{
					Labels:  labels.FromStrings(model.MetricNameLabel, "foo_bar4"),
					Samples: []test.Sample{{TS: 230_000, FloatHist: test.GenerateTestFloatHistogram(99)}},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			var expReq []mimirpb.PreallocTimeseries

			pusher.response = &mimirpb.WriteResponse{}
			a := pa.Appender(ctx)
			for _, tcSeries := range tc.series {
				var err error
				lbls := tcSeries.Labels
				sample := tcSeries.Samples[0] // each input tcSeries only has one sample
				timeseries := mimirpb.PreallocTimeseries{
					TimeSeries: &mimirpb.TimeSeries{
						Labels:    mimirpb.FromLabelsToLabelAdapters(lbls),
						Exemplars: []mimirpb.Exemplar{},
						Samples:   []mimirpb.Sample{},
					},
				}
				expReq = append(expReq, timeseries)

				if sample.H() != nil || sample.FH() != nil {
					_, err = a.AppendHistogram(0, lbls, sample.T(), sample.H(), sample.FH())
					if sample.H() != nil {
						timeseries.Histograms = append(timeseries.Histograms, mimirpb.FromHistogramToHistogramProto(sample.T(), sample.H()))
					} else {
						timeseries.Histograms = append(timeseries.Histograms, mimirpb.FromFloatHistogramToHistogramProto(sample.T(), sample.FH()))
					}
				} else {
					_, err = a.Append(0, lbls, sample.T(), sample.F())
					timeseries.Samples = append(timeseries.Samples, mimirpb.Sample{
						TimestampMs: sample.T(),
						Value:       sample.F(),
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
			lbls := tc.series[0].Labels
			require.Equal(t, 0, labels.Compare(mimirpb.FromLabelAdaptersToLabels(pusher.request.Timeseries[0].Labels), lbls))
			require.Equal(t, tc.series[0].Samples[0].T(), pusher.request.Timeseries[0].Samples[0].TimestampMs)
			require.True(t, math.IsNaN(pusher.request.Timeseries[0].Samples[0].Value))
		})
	}
}

func TestPusherErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		returnedError         error
		expectedWrites        int
		expectedFailures      int
		expectedFailureReason string // default to "error"
	}{
		"no error": {
			expectedWrites:   1,
			expectedFailures: 0,
		},
		"a 400 HTTPgRPC error is reported as client failure": {
			returnedError:         httpgrpc.Errorf(http.StatusBadRequest, "test error"),
			expectedWrites:        1,
			expectedFailures:      1,
			expectedFailureReason: failureReasonClientError,
		},
		"a 500 HTTPgRPC error is reported as failure": {
			returnedError:    httpgrpc.Errorf(http.StatusInternalServerError, "test error"),
			expectedWrites:   1,
			expectedFailures: 1,
		},
		"a BAD_DATA push error is reported as client failure": {
			returnedError:         mustStatusWithDetails(codes.FailedPrecondition, mimirpb.ERROR_CAUSE_BAD_DATA).Err(),
			expectedWrites:        1,
			expectedFailures:      1,
			expectedFailureReason: failureReasonClientError,
		},
		"a METHOD_NOT_ALLOWED push error is reported as failure": {
			returnedError:    mustStatusWithDetails(codes.Unimplemented, mimirpb.ERROR_CAUSE_METHOD_NOT_ALLOWED).Err(),
			expectedWrites:   1,
			expectedFailures: 1,
		},
		"a TSDB_UNAVAILABLE push error is reported as failure": {
			returnedError:    mustStatusWithDetails(codes.FailedPrecondition, mimirpb.ERROR_CAUSE_TSDB_UNAVAILABLE).Err(),
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

			writes := promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"})
			failures := promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user", "reason"})
			pa := NewPusherAppendable(pusher, "user-1", writes, failures)

			lbls, err := promqlext.NewExperimentalParser().ParseMetric("foo_bar")
			require.NoError(t, err)

			a := pa.Appender(ctx)
			_, err = a.Append(0, lbls, int64(model.Now()), 123456)
			require.NoError(t, err)

			require.Equal(t, tc.returnedError, a.Commit())

			require.Equal(t, tc.expectedWrites, int(testutil.ToFloat64(writes.WithLabelValues("user-1"))))

			expectedFailureReason := tc.expectedFailureReason
			if expectedFailureReason == "" {
				expectedFailureReason = failureReasonServerError
			}
			require.Equal(t, tc.expectedFailures, int(testutil.ToFloat64(failures.WithLabelValues("user-1", expectedFailureReason))))
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
		expectedFailedReason  string // default "error"
		remoteQuerier         bool
	}
	// Add special cases to test first.
	allCases := map[string]testCase{
		"httpgrpc 400 error": {
			returnedError:         httpgrpc.Errorf(http.StatusBadRequest, "test error"),
			expectedError:         httpgrpc.Errorf(http.StatusBadRequest, "test error"),
			expectedQueries:       1,
			expectedFailedQueries: 1,
			expectedFailedReason:  failureReasonClientError, // 400 errors coming from remote querier are reported as their own reason.
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
			queries := promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"})
			failures := promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user", "reason"})

			mockFunc := func(context.Context, string, time.Time) (promql.Vector, error) {
				return promql.Vector{}, tc.returnedError
			}
			qf := MetricsQueryFunc(mockFunc, "user-1", queries, failures, tc.remoteQuerier)

			_, err := qf(context.Background(), "test", time.Now())
			require.Equal(t, tc.expectedError, err)

			require.Equal(t, tc.expectedQueries, int(testutil.ToFloat64(queries.WithLabelValues("user-1"))))

			expectedFailedReason := tc.expectedFailedReason
			if expectedFailedReason == "" {
				expectedFailedReason = failureReasonServerError
			}
			require.Equal(t, tc.expectedFailedQueries, int(testutil.ToFloat64(failures.WithLabelValues("user-1", expectedFailedReason))))
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
	qf := RecordAndReportRuleQueryMetrics(mockFunc, queryTime.WithLabelValues("userID"), zeroFetchedSeriesCount.WithLabelValues("userID"), false, log.NewNopLogger())

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

func TestRecordAndReportRuleQueryMetrics_Logging(t *testing.T) {
	for _, remoteQuerier := range []bool{false, true} {
		t.Run(fmt.Sprintf("remoteQuerier=%v", remoteQuerier), func(t *testing.T) {
			queryTime := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			zeroFetchedSeriesCount := promauto.With(nil).NewCounter(prometheus.CounterOpts{})

			innerQueryFunc := func(context.Context, string, time.Time) (promql.Vector, error) {
				v := promql.Vector{
					{T: 1, F: 10, Metric: labels.FromStrings("env", "prod")},
					{T: 1, F: 20, Metric: labels.FromStrings("env", "test")},
				}

				return v, nil
			}

			buffer := &bytes.Buffer{}
			logger := log.NewLogfmtLogger(buffer)
			queryFunc := RecordAndReportRuleQueryMetrics(innerQueryFunc, queryTime, zeroFetchedSeriesCount, remoteQuerier, logger)

			_, err := queryFunc(context.Background(), "test", time.Now())
			require.NoError(t, err)

			logMessages := strings.Split(strings.TrimSuffix(buffer.String(), "\n"), "\n")
			require.Len(t, logMessages, 1, "expected exactly one log message")

			logMessage := logMessages[0]
			require.Contains(t, logMessage, `msg="query stats"`)
			require.Contains(t, logMessage, `query=test`)
			require.Contains(t, logMessage, `result_series_count=2`)

			if remoteQuerier {
				require.NotContains(t, logMessage, "query_wall_time_seconds")
				require.NotContains(t, logMessage, "fetched_series_count")
				require.NotContains(t, logMessage, "fetched_chunk_bytes")
				require.NotContains(t, logMessage, "fetched_chunks_count")
				require.NotContains(t, logMessage, "sharded_queries")
			} else {
				require.Contains(t, logMessage, "query_wall_time_seconds")
				require.Contains(t, logMessage, "fetched_series_count")
				require.Contains(t, logMessage, "fetched_chunk_bytes")
				require.Contains(t, logMessage, "fetched_chunks_count")
				require.Contains(t, logMessage, "sharded_queries")
			}
		})
	}
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
			notifierManager := notifier.NewManager(&notifier.Options{
				Do: func(_ context.Context, _ *http.Client, _ *http.Request) (*http.Response, error) { return nil, nil },
			}, model.UTF8Validation, util_log.SlogFromGoKit(options.logger))
			fs := afero.NewMemMapFs()
			ruleFiles := writeRuleGroupToFiles(t, fs, cfg.RulePath, options.logger, userID, tc.ruleGroup)
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
			managerFactory := DefaultTenantManagerFactory(cfg, pusher, federatedQueryable, queryFunc, fs, &NoopMultiTenantConcurrencyController{}, options.limits, nil)

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
				notifierManager = notifier.NewManager(&notifier.Options{
					Do: func(_ context.Context, _ *http.Client, _ *http.Request) (*http.Response, error) { return nil, nil },
				}, model.UTF8Validation, util_log.SlogFromGoKit(options.logger))
				fs        = afero.NewMemMapFs()
				ruleFiles = writeRuleGroupToFiles(t, fs, cfg.RulePath, options.logger, userID, ruleGroup)
				queryable = newMockQueryable()
				tracker   = promql.NewActiveQueryTracker(t.TempDir(), 20, util_log.SlogFromGoKit(log.NewNopLogger()))
				eng       = promql.NewEngine(promql.EngineOpts{
					MaxSamples:         1e6,
					ActiveQueryTracker: tracker,
					Timeout:            2 * time.Minute,
				})
				queryFunc = rules.EngineQueryFunc(eng, queryable)
			)

			pusher := newPusherMock()
			pusher.MockPush(&mimirpb.WriteResponse{}, nil)

			factory := DefaultTenantManagerFactory(cfg, pusher, queryable, queryFunc, fs, &NoopMultiTenantConcurrencyController{}, options.limits, nil)
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
				notifierManager = notifier.NewManager(&notifier.Options{
					Do: func(_ context.Context, _ *http.Client, _ *http.Request) (*http.Response, error) { return nil, nil },
				}, model.UTF8Validation, util_log.SlogFromGoKit(options.logger))
				tracker = promql.NewActiveQueryTracker(t.TempDir(), 20, util_log.SlogFromGoKit(options.logger))
				eng     = promql.NewEngine(promql.EngineOpts{
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
			fs := afero.NewMemMapFs()
			managerFactory := DefaultTenantManagerFactory(cfg, pusher, queryable, rules.EngineQueryFunc(eng, queryable), fs, &NoopMultiTenantConcurrencyController{}, options.limits, nil)
			manager := managerFactory(context.Background(), userID, notifierManager, options.logger, nil)

			// Load rules into manager.
			ruleFiles := writeRuleGroupToFiles(t, fs, cfg.RulePath, options.logger, userID, testData.ruleGroup)
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
		notifierManager = notifier.NewManager(&notifier.Options{
			Do: func(_ context.Context, _ *http.Client, _ *http.Request) (*http.Response, error) { return nil, nil },
		}, model.UTF8Validation, util_log.SlogFromGoKit(options.logger))
		tracker = promql.NewActiveQueryTracker(t.TempDir(), 20, util_log.SlogFromGoKit(options.logger))
		eng     = promql.NewEngine(promql.EngineOpts{
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
				labels.FromStrings(model.MetricNameLabel, metricName),
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
	fs := afero.NewMemMapFs()
	managerFactory := DefaultTenantManagerFactory(cfg, pusher, queryable, rules.EngineQueryFunc(eng, queryable), fs, &NoopMultiTenantConcurrencyController{}, options.limits, nil)
	manager := managerFactory(context.Background(), userID, notifierManager, options.logger, nil)

	// Load rules into manager.
	ruleFiles := writeRuleGroupToFiles(t, fs, cfg.RulePath, options.logger, userID, ruleGroup)
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

func writeRuleGroupToFiles(t *testing.T, fs afero.Fs, path string, logger log.Logger, userID string, ruleGroup rulespb.RuleGroupDesc) []string {
	_, files, err := newMapper(path, fs, logger).MapRules(userID, map[string][]rulefmt.RuleGroup{
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

func TestRulerErrorClassifier_IsOperatorControllable(t *testing.T) {
	tests := []struct {
		name                   string
		err                    error
		expectedUserFailed     bool
		expectedOperatorFailed bool
		remoteQuerier          bool
	}{
		// PromQL errors
		{
			name:                   "promql error - timeout (user, local querier)",
			err:                    promql.ErrQueryTimeout("query timeout"),
			expectedUserFailed:     true,
			expectedOperatorFailed: false,
			remoteQuerier:          false,
		},
		{
			name:                   "promql error - cancelled (user, local querier)",
			err:                    promql.ErrQueryCanceled("query cancelled"),
			expectedUserFailed:     true,
			expectedOperatorFailed: false,
			remoteQuerier:          false,
		},
		{
			name:                   "promql.ErrStorage (operator, local querier)",
			err:                    promql.ErrStorage{Err: errors.New("storage unavailable")},
			expectedUserFailed:     false,
			expectedOperatorFailed: true,
			remoteQuerier:          false,
		},

		// HTTP status codes - same for both remote and local
		{
			name:                   "500 internal server error (operator, local querier)",
			err:                    httpgrpc.Errorf(http.StatusInternalServerError, "internal error"),
			expectedUserFailed:     false,
			expectedOperatorFailed: true,
			remoteQuerier:          false,
		},
		{
			name:                   "500 internal server error (operator, remote querier)",
			err:                    httpgrpc.Errorf(http.StatusInternalServerError, "internal error"),
			expectedUserFailed:     false,
			expectedOperatorFailed: true,
			remoteQuerier:          true,
		},
		{
			name:                   "429 rate limited (operator, local querier)",
			err:                    httpgrpc.Errorf(http.StatusTooManyRequests, "rate limited"),
			expectedUserFailed:     false,
			expectedOperatorFailed: true,
			remoteQuerier:          false,
		},
		{
			name:                   "429 rate limited (operator, remote querier)",
			err:                    httpgrpc.Errorf(http.StatusTooManyRequests, "rate limited"),
			expectedUserFailed:     false,
			expectedOperatorFailed: true,
			remoteQuerier:          true,
		},
		{
			name:                   "400 bad request (user, local querier)",
			err:                    httpgrpc.Errorf(http.StatusBadRequest, "bad request"),
			expectedUserFailed:     true,
			expectedOperatorFailed: false,
			remoteQuerier:          false,
		},
		{
			name:                   "400 bad request (user, remote querier)",
			err:                    httpgrpc.Errorf(http.StatusBadRequest, "bad request"),
			expectedUserFailed:     true,
			expectedOperatorFailed: false,
			remoteQuerier:          true,
		},

		// Context errors - same for both remote and local
		{
			name:                   "context cancelled (user, local querier)",
			err:                    context.Canceled,
			expectedUserFailed:     true,
			expectedOperatorFailed: false,
			remoteQuerier:          false,
		},
		{
			name:                   "context cancelled (user, remote querier)",
			err:                    context.Canceled,
			expectedUserFailed:     true,
			expectedOperatorFailed: false,
			remoteQuerier:          true,
		},

		// Generic errors - behavior differs based on remoteQuerier
		{
			name:                   "generic error without status code (user, local querier)",
			err:                    errors.New("some generic error"),
			expectedUserFailed:     true,
			expectedOperatorFailed: false,
			remoteQuerier:          false,
		},
		{
			name:                   "generic error without status code (operator, remote querier)",
			err:                    errors.New("some generic error"),
			expectedUserFailed:     false,
			expectedOperatorFailed: true,
			remoteQuerier:          true,
		},

		// Push errors with error details - same for both remote and local
		{
			name:                   "BAD_DATA push error (user, local querier)",
			err:                    mustStatusWithDetails(codes.FailedPrecondition, mimirpb.ERROR_CAUSE_BAD_DATA).Err(),
			expectedUserFailed:     true,
			expectedOperatorFailed: false,
			remoteQuerier:          false,
		},
		{
			name:                   "BAD_DATA push error (user, remote querier)",
			err:                    mustStatusWithDetails(codes.FailedPrecondition, mimirpb.ERROR_CAUSE_BAD_DATA).Err(),
			expectedUserFailed:     true,
			expectedOperatorFailed: false,
			remoteQuerier:          true,
		},
		{
			name:                   "METHOD_NOT_ALLOWED push error (operator, local querier)",
			err:                    mustStatusWithDetails(codes.Unimplemented, mimirpb.ERROR_CAUSE_METHOD_NOT_ALLOWED).Err(),
			expectedUserFailed:     false,
			expectedOperatorFailed: true,
			remoteQuerier:          false,
		},
		{
			name:                   "METHOD_NOT_ALLOWED push error (operator, remote querier)",
			err:                    mustStatusWithDetails(codes.Unimplemented, mimirpb.ERROR_CAUSE_METHOD_NOT_ALLOWED).Err(),
			expectedUserFailed:     false,
			expectedOperatorFailed: true,
			remoteQuerier:          true,
		},

		// gRPC errors with no details
		{
			name:                   "gRPC Internal error (operator, remote querier)",
			err:                    status.New(codes.Internal, "codes.Internal").Err(),
			expectedUserFailed:     false,
			expectedOperatorFailed: true,
			remoteQuerier:          true,
		},
		{
			name:                   "gRPC Internal error (operator, local querier)",
			err:                    status.New(codes.Internal, "codes.Internal").Err(),
			expectedUserFailed:     false,
			expectedOperatorFailed: true,
			remoteQuerier:          false,
		},

		// Rule evaluation failures - duplicate labelsets
		{
			name:                   "duplicate labelset after applying alert labels (user, remote querier)",
			err:                    rules.ErrDuplicateAlertLabelSet,
			expectedUserFailed:     true,
			expectedOperatorFailed: false,
			remoteQuerier:          true,
		},
		{
			name:                   "duplicate labelset after applying alert labels (user, local querier)",
			err:                    rules.ErrDuplicateAlertLabelSet,
			expectedUserFailed:     true,
			expectedOperatorFailed: false,
			remoteQuerier:          false,
		},
		{
			name:                   "duplicate labelset after applying rule labels (user, remote querier)",
			err:                    rules.ErrDuplicateRecordingLabelSet,
			expectedUserFailed:     true,
			expectedOperatorFailed: false,
			remoteQuerier:          true,
		},
		{
			name:                   "duplicate labelset after applying rule labels (user, local querier)",
			err:                    rules.ErrDuplicateRecordingLabelSet,
			expectedUserFailed:     true,
			expectedOperatorFailed: false,
			remoteQuerier:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classifier := NewRulerErrorClassifier(tt.remoteQuerier)
			result := classifier.IsOperatorControllable(tt.err)

			require.Equal(t, tt.expectedOperatorFailed, result)
			require.Equal(t, tt.expectedUserFailed, !result)
		})
	}
}

func TestRulerErrorClassifier_ErrorClassificationDuringRuleEvaluation(t *testing.T) {
	const userID = "tenant-1"
	const interval100ms = 100 * time.Millisecond

	tests := map[string]struct {
		queryError             error
		writeError             error
		expectedOperatorFailed bool
		expectedUserFailed     bool
	}{
		// Query path errors
		"storage error during query (operator)": {
			queryError:             promql.ErrStorage{Err: errors.New("storage unavailable")},
			expectedOperatorFailed: true,
			expectedUserFailed:     false,
		},
		"500 server error during query (operator)": {
			queryError:             httpgrpc.Errorf(http.StatusInternalServerError, "internal server error"),
			expectedOperatorFailed: true,
			expectedUserFailed:     false,
		},
		"503 service unavailable during query (operator)": {
			queryError:             httpgrpc.Errorf(http.StatusServiceUnavailable, "service unavailable"),
			expectedOperatorFailed: true,
			expectedUserFailed:     false,
		},
		"429 rate limit during query (operator)": {
			queryError:             httpgrpc.Errorf(http.StatusTooManyRequests, "rate limited"),
			expectedOperatorFailed: true,
			expectedUserFailed:     false,
		},
		"400 bad request during query (user)": {
			queryError:             httpgrpc.Errorf(http.StatusBadRequest, "bad request"),
			expectedOperatorFailed: false,
			expectedUserFailed:     true,
		},
		"unknown error during query (user)": {
			queryError:             errors.New("test error"),
			expectedOperatorFailed: false,
			expectedUserFailed:     true,
		},

		// Write path errors - matching TestPusherErrors patterns
		"500 HTTPgRPC error during write (operator)": {
			writeError:             httpgrpc.Errorf(http.StatusInternalServerError, "test error"),
			expectedOperatorFailed: true,
			expectedUserFailed:     false,
		},
		"BAD_DATA push error during write (user)": {
			writeError:             mustStatusWithDetails(codes.FailedPrecondition, mimirpb.ERROR_CAUSE_BAD_DATA).Err(),
			expectedOperatorFailed: false,
			expectedUserFailed:     true,
		},
		"METHOD_NOT_ALLOWED push error during write (operator)": {
			writeError:             mustStatusWithDetails(codes.Unimplemented, mimirpb.ERROR_CAUSE_METHOD_NOT_ALLOWED).Err(),
			expectedOperatorFailed: true,
			expectedUserFailed:     false,
		},
		"TSDB_UNAVAILABLE push error during write (operator)": {
			writeError:             mustStatusWithDetails(codes.FailedPrecondition, mimirpb.ERROR_CAUSE_TSDB_UNAVAILABLE).Err(),
			expectedOperatorFailed: true,
			expectedUserFailed:     false,
		},
		"GRPC Internal error without details(operator)": {
			writeError:             status.New(codes.Internal, "codes.Internal").Err(),
			expectedOperatorFailed: true,
			expectedUserFailed:     false,
		},
		"unknown error during write (user)": {
			writeError:             errors.New("test error"),
			expectedOperatorFailed: false,
			expectedUserFailed:     true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Create a simple recording rule
			ruleGroup := rulespb.RuleGroupDesc{
				Name:     "test",
				Interval: interval100ms,
				Rules: []*rulespb.RuleDesc{{
					Record: "test_metric",
					Expr:   "up",
				}},
			}

			// Setup ruler
			cfg := defaultRulerConfig(t)
			cfg.EvaluationInterval = interval100ms

			var (
				options         = applyPrepareOptions(t, cfg.Ring.Common.InstanceID)
				notifierManager = notifier.NewManager(&notifier.Options{
					Do: func(_ context.Context, _ *http.Client, _ *http.Request) (*http.Response, error) { return nil, nil },
				}, model.UTF8Validation, util_log.SlogFromGoKit(options.logger))
				fs        = afero.NewMemMapFs()
				ruleFiles = writeRuleGroupToFiles(t, fs, cfg.RulePath, options.logger, userID, ruleGroup)
				tracker   = promql.NewActiveQueryTracker(t.TempDir(), 20, util_log.SlogFromGoKit(log.NewNopLogger()))
				eng       = promql.NewEngine(promql.EngineOpts{
					MaxSamples:         1e6,
					ActiveQueryTracker: tracker,
					Timeout:            2 * time.Minute,
				})
				prometheusReg = prometheus.NewRegistry()
			)

			// Mock querier to return the test error
			querier := newQuerierMock()
			querier.selectFunc = func(ctx context.Context, _ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
				if tc.queryError != nil {
					return storage.ErrSeriesSet(tc.queryError)
				}
				// Return empty series set on success
				return storage.EmptySeriesSet()
			}

			// Use fakePusher for write path errors (consistent with TestPusherErrors)
			pusher := &fakePusher{
				err:      tc.writeError,
				response: &mimirpb.WriteResponse{},
			}

			// Create manager from factory
			queryable := &storage.MockQueryable{MockQuerier: querier}
			queryFunc := rules.EngineQueryFunc(eng, queryable)

			factory := DefaultTenantManagerFactory(cfg, pusher, queryable, queryFunc, fs, &NoopMultiTenantConcurrencyController{}, options.limits, nil)
			manager := factory(context.Background(), userID, notifierManager, options.logger, prometheusReg)

			// Load rules into manager and start
			require.NoError(t, manager.Update(interval100ms, ruleFiles, labels.EmptyLabels(), "", nil))
			go manager.Run()
			t.Cleanup(manager.Stop)

			// Wait for rule evaluation to complete and metrics to be recorded
			if tc.expectedOperatorFailed {
				require.Eventually(t, func() bool {
					operatorFailed := getMetricValue(t, prometheusReg, "prometheus_rule_evaluation_failures_total", "reason", "operator")
					userFailed := getMetricValue(t, prometheusReg, "prometheus_rule_evaluation_failures_total", "reason", "user")
					return operatorFailed > 0 && userFailed == 0
				}, 5*time.Second, 100*time.Millisecond, "expected at least one operator failure and no user failures")
			} else if tc.expectedUserFailed {
				require.Eventually(t, func() bool {
					operatorFailed := getMetricValue(t, prometheusReg, "prometheus_rule_evaluation_failures_total", "reason", "operator")
					userFailed := getMetricValue(t, prometheusReg, "prometheus_rule_evaluation_failures_total", "reason", "user")
					return userFailed > 0 && operatorFailed == 0
				}, 5*time.Second, 100*time.Millisecond, "expected at least one user failure and no operator failures")
			}
		})
	}
}

func getMetricValue(t *testing.T, reg prometheus.Gatherer, metricName, labelName, labelValue string) float64 {
	metricFamilies, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range metricFamilies {
		if mf.GetName() == metricName {
			for _, metric := range mf.GetMetric() {
				for _, label := range metric.GetLabel() {
					if label.GetName() == labelName && label.GetValue() == labelValue {
						return metric.GetCounter().GetValue()
					}
				}
			}
		}
	}
	return 0
}

func TestPrometheusErrorStringsForDuplicateLabelsets(t *testing.T) {
	logger := promslog.New(&promslog.Config{})
	expr, err := promqlext.NewExperimentalParser().ParseExpr("test_metric")
	require.NoError(t, err)

	// Mock query function that returns duplicate metrics
	queryFunc := func(_ context.Context, _ string, _ time.Time) (promql.Vector, error) {
		return promql.Vector{
			promql.Sample{Metric: labels.FromStrings("__name__", "test_metric", "job", "test"), T: 0, F: 1.0},
			promql.Sample{Metric: labels.FromStrings("__name__", "test_metric", "job", "test"), T: 0, F: 2.0},
		}, nil
	}

	testCases := []struct {
		name          string
		rule          rules.Rule
		expectedError error
	}{
		{
			name:          "alerting rule",
			rule:          rules.NewAlertingRule("test_alert", expr, time.Minute, 0, labels.EmptyLabels(), labels.EmptyLabels(), labels.EmptyLabels(), "", false, logger),
			expectedError: rules.ErrDuplicateAlertLabelSet,
		},
		{
			name:          "recording rule",
			rule:          rules.NewRecordingRule("test_record", expr, labels.EmptyLabels()),
			expectedError: rules.ErrDuplicateRecordingLabelSet,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.rule.SetDependencyRules([]rules.Rule{})

			_, err := tc.rule.Eval(context.Background(), 0, time.Now(), queryFunc, nil, 0)

			require.Error(t, err)
			require.ErrorIs(t, err, tc.expectedError,
				"Prometheus sentinel error changed! Update the inline check in IsOperatorControllable() in compat.go")
		})
	}
}
