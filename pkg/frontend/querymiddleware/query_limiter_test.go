// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestQueryLimiterMiddleware_RangeAndInstantQuery(t *testing.T) {
	// All tests run with same query
	query := "rate(metric_counter[5m])"

	tests := []struct {
		name                     string
		limits                   mockLimits
		expectSecondQueryBlocked bool
	}{
		{
			name:   "empty limits should not block",
			limits: mockLimits{},
		},
		{
			name: "non-empty limit should block",
			limits: mockLimits{limitedQueries: []validation.LimitedQuery{
				{Query: query, AllowedFrequency: time.Minute},
			}},
			expectSecondQueryBlocked: true,
		},
		{
			name: "short max frequency should not block",
			limits: mockLimits{limitedQueries: []validation.LimitedQuery{
				{Query: query, AllowedFrequency: time.Second},
			}},
		},
		{
			name: "non-matching pattern should not block",
			limits: mockLimits{limitedQueries: []validation.LimitedQuery{
				{Query: "increase(metric_counter[5m])", AllowedFrequency: time.Minute},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqs := map[string]MetricsQueryRequest{
				"range queries": &PrometheusRangeQueryRequest{
					queryExpr: parseQuery(t, query),
				},
				"instant queries": &PrometheusInstantQueryRequest{
					queryExpr: parseQuery(t, query),
				},
			}

			for reqType, req := range reqs {
				t.Run(reqType, func(t *testing.T) {
					c := cache.NewInstrumentedMockCache()
					keyGen := NewDefaultCacheKeyGenerator(newTestCodec(), time.Second)
					reg := prometheus.NewPedanticRegistry()
					blockedQueriesCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
						Name: "cortex_query_frontend_rejected_queries_total",
						Help: "Number of queries that were rejected by the cluster administrator.",
					}, []string{"user", "reason"})
					logger := log.NewNopLogger()
					mw := newQueryLimiterMiddleware(c, keyGen, tt.limits, logger, blockedQueriesCounter)
					for i := 0; i < 2; i++ {
						_, err := mw.Wrap(&mockNextHandler{t: t, shouldContinue: !tt.expectSecondQueryBlocked || i < 1}).Do(user.InjectOrgID(context.Background(), "test"), req)

						if i == 1 && tt.expectSecondQueryBlocked {
							require.Error(t, err)
							require.Contains(t, err.Error(), globalerror.QueryLimited)
							require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
								# HELP cortex_query_frontend_rejected_queries_total Number of queries that were rejected by the cluster administrator.
								# TYPE cortex_query_frontend_rejected_queries_total counter
								cortex_query_frontend_rejected_queries_total{reason="limited", user="test"} 1
							`)))
						} else {
							require.NoError(t, err)
							require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(``)))
						}
						// Advance cache time by enough to allow very short frequency queries to be run again
						c.Advance(time.Second)
					}

				})
			}
		})
	}
}

func TestQueryLimiterMiddleware_MultipleUsers_RangeAndInstantQuery(t *testing.T) {
	// All tests run with same query
	query := "rate(metric_counter[5m])"
	// We query against both tenants test1 and test2; if blocking happens, it happens against test2
	userID := "test1|test2"

	tests := []struct {
		name                     string
		limits                   *multiTenantMockLimits
		expectSecondQueryBlocked bool
	}{
		{
			name:   "no limits should not block",
			limits: &multiTenantMockLimits{},
		},
		{
			name: "limit matching for one tenant but not other should block",
			limits: &multiTenantMockLimits{byTenant: map[string]mockLimits{
				"test2": {limitedQueries: []validation.LimitedQuery{
					{
						Query: query, AllowedFrequency: time.Minute,
					}},
				},
			}},
			expectSecondQueryBlocked: true,
		},
		{
			name: "limit does not exist for queried tenants should not block",
			limits: &multiTenantMockLimits{byTenant: map[string]mockLimits{
				"test3": {limitedQueries: []validation.LimitedQuery{
					{
						Query: query, AllowedFrequency: time.Minute,
					}},
				},
			}},
		},
		{
			name: "tenants with different limit frequencies (one long) should block",
			limits: &multiTenantMockLimits{byTenant: map[string]mockLimits{
				"test1": {limitedQueries: []validation.LimitedQuery{
					{
						Query: query, AllowedFrequency: time.Second,
					}},
				},
				"test2": {limitedQueries: []validation.LimitedQuery{
					{
						Query: query, AllowedFrequency: time.Minute,
					}},
				},
			}},
			expectSecondQueryBlocked: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqs := map[string]MetricsQueryRequest{
				"range queries": &PrometheusRangeQueryRequest{
					queryExpr: parseQuery(t, query),
				},
				"instant queries": &PrometheusInstantQueryRequest{
					queryExpr: parseQuery(t, query),
				},
			}

			for reqType, req := range reqs {
				t.Run(reqType, func(t *testing.T) {
					c := cache.NewInstrumentedMockCache()
					keyGen := NewDefaultCacheKeyGenerator(newTestCodec(), time.Second)
					reg := prometheus.NewPedanticRegistry()
					blockedQueriesCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
						Name: "cortex_query_frontend_rejected_queries_total",
						Help: "Number of queries that were rejected by the cluster administrator.",
					}, []string{"user", "reason"})
					logger := log.NewNopLogger()
					mw := newQueryLimiterMiddleware(c, keyGen, tt.limits, logger, blockedQueriesCounter)
					for i := 0; i < 2; i++ {
						_, err := mw.Wrap(&mockNextHandler{t: t, shouldContinue: !tt.expectSecondQueryBlocked || i < 1}).Do(user.InjectOrgID(context.Background(), userID), req)

						if i == 1 && tt.expectSecondQueryBlocked {
							require.Error(t, err)
							require.Contains(t, err.Error(), globalerror.QueryLimited)
							require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
								# HELP cortex_query_frontend_rejected_queries_total Number of queries that were rejected by the cluster administrator.
								# TYPE cortex_query_frontend_rejected_queries_total counter
								cortex_query_frontend_rejected_queries_total{reason="limited", user="test2"} 1
							`)))
						} else {
							require.NoError(t, err)
							require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(``)))
						}
						// Advance cache time by enough to allow very short frequency queries to be run again
						c.Advance(time.Second)
					}

				})
			}
		})
	}
}

func TestQueryLimiterMiddleware_RemoteRead(t *testing.T) {
	query := &prompb.Query{
		Matchers: []*prompb.LabelMatcher{
			{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "metric_counter"},
			{Type: prompb.LabelMatcher_RE, Name: "pod", Value: "app-.*"},
		},
	}
	queryString := `{__name__="metric_counter",pod=~"app-.*"}`

	tests := []struct {
		name                     string
		limits                   mockLimits
		expectSecondQueryBlocked bool
	}{
		{
			name:   "empty limits should not block",
			limits: mockLimits{},
		},
		{
			name: "matching query should block",
			limits: mockLimits{limitedQueries: []validation.LimitedQuery{
				{Query: queryString, AllowedFrequency: time.Minute},
			}},
			expectSecondQueryBlocked: true,
		},
		{
			name: "non-matching query should not block",
			limits: mockLimits{limitedQueries: []validation.LimitedQuery{
				{Query: `{__name__="sample_counter",pod=~"app-.*"}`, AllowedFrequency: time.Minute},
			}},
		},
		{
			name: "short max frequency should not block",
			limits: mockLimits{limitedQueries: []validation.LimitedQuery{
				{Query: queryString, AllowedFrequency: time.Second},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := cache.NewInstrumentedMockCache()
			keyGen := NewDefaultCacheKeyGenerator(newTestCodec(), time.Second)
			req, err := remoteReadToMetricsQueryRequest(remoteReadPathSuffix, query)
			require.NoError(t, err)
			reg := prometheus.NewPedanticRegistry()
			blockedQueriesCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "cortex_query_frontend_rejected_queries_total",
				Help: "Number of queries that were rejected by the cluster administrator.",
			}, []string{"user", "reason"})
			logger := log.NewNopLogger()
			mw := newQueryLimiterMiddleware(c, keyGen, tt.limits, logger, blockedQueriesCounter)
			for i := 0; i < 2; i++ {
				_, err = mw.Wrap(&mockNextHandler{t: t, shouldContinue: !tt.expectSecondQueryBlocked || i < 1}).Do(user.InjectOrgID(context.Background(), "test"), req)

				if tt.expectSecondQueryBlocked && i == 1 {
					require.Error(t, err)
					require.Contains(t, err.Error(), globalerror.QueryLimited)
					require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
							# HELP cortex_query_frontend_rejected_queries_total Number of queries that were rejected by the cluster administrator.
							# TYPE cortex_query_frontend_rejected_queries_total counter
							cortex_query_frontend_rejected_queries_total{reason="limited", user="test"} 1
						`)))
				} else {
					require.NoError(t, err)
					require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(``)))
				}
				c.Advance(time.Second)
			}

		})
	}
}

func TestQueryLimiterMiddleware_MultipleUsers_RemoteRead(t *testing.T) {
	query := &prompb.Query{
		Matchers: []*prompb.LabelMatcher{
			{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "metric_counter"},
			{Type: prompb.LabelMatcher_RE, Name: "pod", Value: "app-.*"},
		},
	}
	queryString := `{__name__="metric_counter",pod=~"app-.*"}`
	userID := "test1|test2"

	tests := []struct {
		name                     string
		limits                   *multiTenantMockLimits
		expectSecondQueryBlocked bool
	}{
		{
			name:   "no limits should not block",
			limits: &multiTenantMockLimits{},
		},
		{
			name: "limit matching for one tenant but not other should block",
			limits: &multiTenantMockLimits{byTenant: map[string]mockLimits{
				"test2": {limitedQueries: []validation.LimitedQuery{
					{
						Query:            queryString,
						AllowedFrequency: time.Minute,
					}},
				},
			}},
			expectSecondQueryBlocked: true,
		},
		{
			name: "limit does not exist for queried tenants should not block",
			limits: &multiTenantMockLimits{byTenant: map[string]mockLimits{
				"test3": {limitedQueries: []validation.LimitedQuery{
					{
						Query:            queryString,
						AllowedFrequency: time.Minute,
					}},
				},
			}},
		},
		{
			name: "tenants with different limit frequencies (one long) should block",
			limits: &multiTenantMockLimits{byTenant: map[string]mockLimits{
				"test1": {limitedQueries: []validation.LimitedQuery{
					{
						Query:            queryString,
						AllowedFrequency: time.Second,
					}},
				},
				"test2": {limitedQueries: []validation.LimitedQuery{
					{
						Query:            queryString,
						AllowedFrequency: time.Minute,
					}},
				},
			}},
			expectSecondQueryBlocked: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := cache.NewInstrumentedMockCache()
			keyGen := NewDefaultCacheKeyGenerator(newTestCodec(), time.Second)
			req, err := remoteReadToMetricsQueryRequest(remoteReadPathSuffix, query)
			require.NoError(t, err)
			reg := prometheus.NewPedanticRegistry()
			blockedQueriesCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "cortex_query_frontend_rejected_queries_total",
				Help: "Number of queries that were rejected by the cluster administrator.",
			}, []string{"user", "reason"})
			logger := log.NewNopLogger()
			mw := newQueryLimiterMiddleware(c, keyGen, tt.limits, logger, blockedQueriesCounter)
			for i := 0; i < 2; i++ {
				_, err = mw.Wrap(&mockNextHandler{t: t, shouldContinue: !tt.expectSecondQueryBlocked || i < 1}).Do(user.InjectOrgID(context.Background(), userID), req)

				if tt.expectSecondQueryBlocked && i == 1 {
					require.Error(t, err)
					require.Contains(t, err.Error(), globalerror.QueryLimited)
					require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
							# HELP cortex_query_frontend_rejected_queries_total Number of queries that were rejected by the cluster administrator.
							# TYPE cortex_query_frontend_rejected_queries_total counter
							cortex_query_frontend_rejected_queries_total{reason="limited", user="test2"} 1
						`)))
				} else {
					require.NoError(t, err)
					require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(``)))
				}
				c.Advance(time.Second)
			}

		})
	}
}
