// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/compat_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"errors"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/util/validation"
)

type fakePusher struct {
	request  *mimirpb.WriteRequest
	response *mimirpb.WriteResponse
	err      error
}

func (p *fakePusher) Push(ctx context.Context, r *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	p.request = r
	return p.response, p.err
}

func TestPusherAppendable(t *testing.T) {
	pusher := &fakePusher{}
	pa := NewPusherAppendable(pusher, "user-1", nil, promauto.With(nil).NewCounter(prometheus.CounterOpts{}), promauto.With(nil).NewCounter(prometheus.CounterOpts{}))

	for _, tc := range []struct {
		name       string
		series     string
		value      float64
		expectedTS int64
	}{
		{
			name:       "tenant without delay, normal value",
			series:     "foo_bar",
			value:      1.234,
			expectedTS: 120_000,
		},
		{
			name:       "tenant without delay, stale nan value",
			series:     "foo_bar",
			value:      math.Float64frombits(value.StaleNaN),
			expectedTS: 120_000,
		},
		{
			name:       "ALERTS, normal value",
			series:     `ALERTS{alertname="boop"}`,
			value:      1.234,
			expectedTS: 120_000,
		},
		{
			name:       "ALERTS, stale nan value",
			series:     `ALERTS{alertname="boop"}`,
			value:      math.Float64frombits(value.StaleNaN),
			expectedTS: 120_000,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			lbls, err := parser.ParseMetric(tc.series)
			require.NoError(t, err)

			pusher.response = &mimirpb.WriteResponse{}
			a := pa.Appender(ctx)
			_, err = a.Append(0, lbls, 120_000, tc.value)
			require.NoError(t, err)

			require.NoError(t, a.Commit())

			require.Equal(t, tc.expectedTS, pusher.request.Timeseries[0].Samples[0].TimestampMs)

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

		"400 error": {
			returnedError:    httpgrpc.Errorf(http.StatusBadRequest, "test error"),
			expectedWrites:   1,
			expectedFailures: 0, // 400 errors not reported as failures.
		},

		"500 error": {
			returnedError:    httpgrpc.Errorf(http.StatusInternalServerError, "test error"),
			expectedWrites:   1,
			expectedFailures: 1, // 500 errors are failures
		},

		"unknown error": {
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
			limits := validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
				defaults.RulerEvaluationDelay = 0
			})

			pa := NewPusherAppendable(pusher, "user-1", limits, writes, failures)

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
	for name, tc := range map[string]struct {
		returnedError         error
		expectedError         error
		expectedQueries       int
		expectedFailedQueries int
	}{
		"no error": {
			expectedQueries:       1,
			expectedFailedQueries: 0,
		},

		"httpgrpc 400 error": {
			returnedError:         httpgrpc.Errorf(http.StatusBadRequest, "test error"),
			expectedError:         httpgrpc.Errorf(http.StatusBadRequest, "test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // 400 errors not reported as failures.
		},

		"httpgrpc 500 error": {
			returnedError:         httpgrpc.Errorf(http.StatusInternalServerError, "test error"),
			expectedError:         httpgrpc.Errorf(http.StatusInternalServerError, "test error"),
			expectedQueries:       1,
			expectedFailedQueries: 1, // 500 errors are failures
		},

		"unknown but non-queryable error": {
			returnedError:         errors.New("test error"),
			expectedError:         errors.New("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 1, // Any other error should always be reported.
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
	} {
		t.Run(name, func(t *testing.T) {
			queries := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			failures := promauto.With(nil).NewCounter(prometheus.CounterOpts{})

			mockFunc := func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
				return promql.Vector{}, tc.returnedError
			}
			qf := MetricsQueryFunc(mockFunc, queries, failures)

			_, err := qf(context.Background(), "test", time.Now())
			require.Equal(t, tc.expectedError, err)

			require.Equal(t, tc.expectedQueries, int(testutil.ToFloat64(queries)))
			require.Equal(t, tc.expectedFailedQueries, int(testutil.ToFloat64(failures)))
		})
	}
}

func TestRecordAndReportRuleQueryMetrics(t *testing.T) {
	queryTime := promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"})

	mockFunc := func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
		time.Sleep(1 * time.Second)
		return promql.Vector{}, nil
	}
	qf := RecordAndReportRuleQueryMetrics(mockFunc, queryTime.WithLabelValues("userID"), log.NewNopLogger())
	_, _ = qf(context.Background(), "test", time.Now())

	require.GreaterOrEqual(t, testutil.ToFloat64(queryTime.WithLabelValues("userID")), float64(1))
}

// TestManagerFactory_CorrectQueryableUsed ensures that when evaluating a group with non-empty SourceTenants
// the federated queryable is called. If SourceTenants are empty, then the regular queryable should be used.
// This is to ensure that the `__tenant_id__` label is present for all rules evaluating within a federated rule group.
func TestManagerFactory_CorrectQueryableUsed(t *testing.T) {
	const userID = "tenant-1"

	dummyRules := []*rulespb.RuleDesc{mockRecordingRuleDesc("sum:up", "sum(up)")}

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
			options := applyPrepareOptions()
			notifierManager := notifier.NewManager(&notifier.Options{Do: func(_ context.Context, _ *http.Client, _ *http.Request) (*http.Response, error) { return nil, nil }}, options.logger)
			ruleFiles := writeRuleGroupToFiles(t, cfg.RulePath, options.logger, userID, tc.ruleGroup)
			regularQueryable, federatedQueryable := newMockQueryable(), newMockQueryable()

			tracker := promql.NewActiveQueryTracker(t.TempDir(), 20, log.NewNopLogger())
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
			managerFactory := DefaultTenantManagerFactory(cfg, pusher, federatedQueryable, queryFunc, options.limits, nil)

			manager := managerFactory(context.Background(), userID, notifierManager, options.logger, nil)

			// load rules into manager and start
			require.NoError(t, manager.Update(time.Millisecond, ruleFiles, nil, "", nil))
			go manager.Run()

			select {
			case <-regularQueryable.called:
				require.True(t, tc.regularQueryableCalled, "unexpected call to regular queryable")
			case <-federatedQueryable.called:
				require.True(t, tc.federatedQueryableCalled, "unexpected call to federated queryable")
			case <-time.NewTimer(time.Second).C:
				require.Fail(t, "neither of the queryables was called within the timeout")
			}
			manager.Stop()
		})
	}
}

func writeRuleGroupToFiles(t *testing.T, path string, logger log.Logger, userID string, ruleGroup rulespb.RuleGroupDesc) []string {
	_, files, err := newMapper(path, logger).MapRules(userID, map[string][]rulefmt.RuleGroup{
		"namespace": {rulespb.FromProto(&ruleGroup)},
	}, 0)
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

func (m *mockQueryable) Querier(_ context.Context, _, _ int64) (storage.Querier, error) {
	select {
	case <-m.called:
		// already closed
	default:
		close(m.called)
	}
	return storage.NoopQuerier(), nil
}
