package querymiddleware

import (
	"context"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
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
			limits: mockLimits{limitedQueries: []*validation.LimitedQuery{
				{Query: "rate(metric_counter[5m])", AllowedFrequency: time.Minute},
			}},
			expectSecondQueryBlocked: true,
		},
		{
			name: "short max frequency should not block",
			limits: mockLimits{limitedQueries: []*validation.LimitedQuery{
				{Query: "rate(metric_counter[5m])", AllowedFrequency: time.Nanosecond},
			}},
		},
		{
			name: "non-matching pattern should not block",
			limits: mockLimits{limitedQueries: []*validation.LimitedQuery{
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
					keyGen := NewDefaultCacheKeyGenerator(newTestPrometheusCodec(), time.Second)
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
					}

				})
			}
		})
	}
}

func TestQueryLimiterMiddleware_RemoteRead(t *testing.T) {
	query := &prompb.Query{
		Matchers: []*prompb.LabelMatcher{
			{Type: prompb.LabelMatcher_EQ, Name: labels.MetricName, Value: "metric_counter"},
			{Type: prompb.LabelMatcher_RE, Name: "pod", Value: "app-.*"},
		},
	}

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
			limits: mockLimits{limitedQueries: []*validation.LimitedQuery{
				{Query: `{__name__="metric_counter",pod=~"app-.*"}`, AllowedFrequency: time.Minute},
			}},
			expectSecondQueryBlocked: true,
		},
		{
			name: "non-matching query should not block",
			limits: mockLimits{limitedQueries: []*validation.LimitedQuery{
				{Query: `{__name__="sample_counter",pod=~"app-.*"}`, AllowedFrequency: time.Minute},
			}},
		},
		{
			name: "short max frequency should not block",
			limits: mockLimits{limitedQueries: []*validation.LimitedQuery{
				{Query: `{__name__="metric_counter",pod=~"app-.*"}`, AllowedFrequency: time.Nanosecond},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := cache.NewInstrumentedMockCache()
			keyGen := NewDefaultCacheKeyGenerator(newTestPrometheusCodec(), time.Second)
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
			}

		})
	}
}
