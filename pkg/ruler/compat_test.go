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
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
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
	pa := NewPusherAppendable(pusher, "user-1", nil, prometheus.NewCounter(prometheus.CounterOpts{}), prometheus.NewCounter(prometheus.CounterOpts{}))

	for _, tc := range []struct {
		name       string
		series     string
		evalDelay  time.Duration
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
			name:       "tenant with delay, normal value",
			series:     "foo_bar",
			value:      1.234,
			expectedTS: 120_000,
			evalDelay:  time.Minute,
		},
		{
			name:       "tenant with delay, stale nan value",
			value:      math.Float64frombits(value.StaleNaN),
			expectedTS: 60_000,
			evalDelay:  time.Minute,
		},
		{
			name:       "ALERTS without delay, normal value",
			series:     `ALERTS{alertname="boop"}`,
			value:      1.234,
			expectedTS: 120_000,
		},
		{
			name:       "ALERTS without delay, stale nan value",
			series:     `ALERTS{alertname="boop"}`,
			value:      math.Float64frombits(value.StaleNaN),
			expectedTS: 120_000,
		},
		{
			name:       "ALERTS with delay, normal value",
			series:     `ALERTS{alertname="boop"}`,
			value:      1.234,
			expectedTS: 60_000,
			evalDelay:  time.Minute,
		},
		{
			name:       "ALERTS with delay, stale nan value",
			series:     `ALERTS_FOR_STATE{alertname="boop"}`,
			value:      math.Float64frombits(value.StaleNaN),
			expectedTS: 60_000,
			evalDelay:  time.Minute,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			pa.rulesLimits = &ruleLimits{
				evalDelay: tc.evalDelay,
			}

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

			writes := prometheus.NewCounter(prometheus.CounterOpts{})
			failures := prometheus.NewCounter(prometheus.CounterOpts{})

			pa := NewPusherAppendable(pusher, "user-1", ruleLimits{evalDelay: 10 * time.Second}, writes, failures)

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
		expectedQueries       int
		expectedFailedQueries int
	}{
		"no error": {
			expectedQueries:       1,
			expectedFailedQueries: 0,
		},

		"400 error": {
			returnedError:         httpgrpc.Errorf(http.StatusBadRequest, "test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // 400 errors not reported as failures.
		},

		"500 error": {
			returnedError:         httpgrpc.Errorf(http.StatusInternalServerError, "test error"),
			expectedQueries:       1,
			expectedFailedQueries: 1, // 500 errors are failures
		},

		"promql.ErrStorage": {
			returnedError:         promql.ErrStorage{Err: errors.New("test error")},
			expectedQueries:       1,
			expectedFailedQueries: 1,
		},

		"promql.ErrQueryCanceled": {
			returnedError:         promql.ErrQueryCanceled("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // Not interesting.
		},

		"promql.ErrQueryTimeout": {
			returnedError:         promql.ErrQueryTimeout("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // Not interesting.
		},

		"promql.ErrTooManySamples": {
			returnedError:         promql.ErrTooManySamples("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // Not interesting.
		},

		"unknown error": {
			returnedError:         errors.New("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 1, // unknown errors are not 400, so they are reported.
		},
	} {
		t.Run(name, func(t *testing.T) {
			queries := prometheus.NewCounter(prometheus.CounterOpts{})
			failures := prometheus.NewCounter(prometheus.CounterOpts{})

			mockFunc := func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
				return promql.Vector{}, WrapQueryableErrors(tc.returnedError)
			}
			qf := MetricsQueryFunc(mockFunc, queries, failures)

			_, err := qf(context.Background(), "test", time.Now())
			require.Equal(t, tc.returnedError, err)

			require.Equal(t, tc.expectedQueries, int(testutil.ToFloat64(queries)))
			require.Equal(t, tc.expectedFailedQueries, int(testutil.ToFloat64(failures)))
		})
	}
}

func TestRecordAndReportRuleQueryMetrics(t *testing.T) {
	queryTime := prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user"})

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

	dummyRules := []*rulespb.RuleDesc{{
		Expr:   "sum(up)",
		Record: "sum:up",
	}}

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
			engine, _, pusher, logger, overrides := testSetup(t)
			notifierManager := notifier.NewManager(&notifier.Options{Do: func(_ context.Context, _ *http.Client, _ *http.Request) (*http.Response, error) { return nil, nil }}, logger)
			ruleFiles := writeRuleGroupToFiles(t, cfg.RulePath, logger, userID, tc.ruleGroup)
			regularQueryable, federatedQueryable := newMockQueryable(), newMockQueryable()

			// create and use manager factory
			managerFactory := DefaultTenantManagerFactory(cfg, pusher, regularQueryable, federatedQueryable, engine, overrides, nil)
			manager := managerFactory(context.Background(), userID, notifierManager, logger, nil)

			// load rules into manager and start
			require.NoError(t, manager.Update(time.Millisecond, ruleFiles, nil, ""))
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

func (m *mockQueryable) Querier(_ context.Context, _, _ int64) (storage.Querier, error) {
	close(m.called)
	return storage.NoopQuerier(), nil
}
