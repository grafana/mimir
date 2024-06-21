// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestQueryBlockerMiddleware_RangeAndInstantQuery(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		limits          mockLimits
		expectedBlocked bool
	}{
		{
			name:   "doesn't block queries due to empty limits",
			limits: mockLimits{},
			query:  "rate(metric_counter[5m])",
		},
		{
			name: "blocks single line query non regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: "rate(metric_counter[5m])", Regex: false},
				},
			},
			query:           "rate(metric_counter[5m])",
			expectedBlocked: true,
		},
		{
			name: "not blocks single line query non regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: "rate(metric_counter[5m])", Regex: false},
				},
			},
			query: "rate(metric_counter[15m])",
		},
		{
			name: "blocks multiple line query non regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: `rate(metric_counter[5m]) / rate(other_counter[5m])`, Regex: false},
				},
			},
			query: `
				rate(metric_counter[5m])
				/
				rate(other_counter[5m])
			`,
			expectedBlocked: true,
		},
		{
			name: "not blocks multiple line query non regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: "rate(metric_counter[5m])", Regex: false},
				},
			},
			query: `
				rate(metric_counter[15m])
				/
				rate(other_counter[15m])
			`,
		},
		{
			name: "blocks single line query regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: ".*metric_counter.*", Regex: true},
				},
			},
			query:           "rate(metric_counter[5m])",
			expectedBlocked: true,
		},
		{
			name: "blocks multiple line query regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					// We need to turn on the s flag to allow dot matches newlines.
					{Pattern: "(?s).*metric_counter.*", Regex: true},
				},
			},
			query: `
				rate(other_counter[15m])
				/
				rate(metric_counter[15m])
			`,
			expectedBlocked: true,
		},
		{
			name: "invalid regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: "[a-9}", Regex: true},
				},
			},
			query: "rate(metric_counter[5m])",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqs := map[string]MetricsQueryRequest{
				"range query": &PrometheusRangeQueryRequest{
					queryExpr: parseQuery(t, tt.query),
				},
				"instant query": &PrometheusRangeQueryRequest{
					queryExpr: parseQuery(t, tt.query),
				},
			}

			for reqType, req := range reqs {
				t.Run(reqType, func(t *testing.T) {
					reg := prometheus.NewPedanticRegistry()
					logger := log.NewNopLogger()
					mw := newQueryBlockerMiddleware(tt.limits, logger, reg)
					_, err := mw.Wrap(&mockNextHandler{t: t, shouldContinue: !tt.expectedBlocked}).Do(user.InjectOrgID(context.Background(), "test"), req)

					if tt.expectedBlocked {
						require.Error(t, err)
						require.Contains(t, err.Error(), globalerror.QueryBlocked)
						require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
							# HELP cortex_query_frontend_rejected_queries_total Number of queries that were rejected by the cluster administrator.
							# TYPE cortex_query_frontend_rejected_queries_total counter
							cortex_query_frontend_rejected_queries_total{reason="blocked", user="test"} 1
						`)))
					} else {
						require.NoError(t, err)
						require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(``)))
					}
				})
			}
		})
	}
}

func TestQueryBlockerMiddleware_RemoteRead(t *testing.T) {
	// All tests run on the same query.
	query := &prompb.Query{
		Matchers: []*prompb.LabelMatcher{
			{Type: prompb.LabelMatcher_EQ, Name: labels.MetricName, Value: "metric_counter"},
			{Type: prompb.LabelMatcher_RE, Name: "pod", Value: "app-.*"},
		},
	}

	tests := []struct {
		name            string
		limits          mockLimits
		expectedBlocked bool
	}{
		{
			name:   "doesn't block queries due to empty limits",
			limits: mockLimits{},
		},
		{
			name: "blocks query via non regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: `{__name__="metric_counter",pod=~"app-.*"}`, Regex: false},
				},
			},
			expectedBlocked: true,
		},
		{
			name: "not blocks query via non regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: `{__name__="another_metric",pod=~"app-.*"}`, Regex: false},
				},
			},
		},
		{
			name: "blocks query via regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: ".*metric_counter.*", Regex: true},
				},
			},
			expectedBlocked: true,
		},
		{
			name: "blocks query via regex pattern, with begin/end curly brackets used as a trick to match only remote read requests",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: "\\{.*metric_counter.*\\}", Regex: true},
				},
			},
			expectedBlocked: true,
		},
		{
			name: "not blocks query via regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: ".*another_metric.*", Regex: true},
				},
			},
		},
		{
			name: "invalid regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: "[a-9}", Regex: true},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := remoteReadToMetricsQueryRequest(remoteReadPathSuffix, query)
			require.NoError(t, err)

			reg := prometheus.NewPedanticRegistry()
			logger := log.NewNopLogger()
			mw := newQueryBlockerMiddleware(tt.limits, logger, reg)
			_, err = mw.Wrap(&mockNextHandler{t: t, shouldContinue: !tt.expectedBlocked}).Do(user.InjectOrgID(context.Background(), "test"), req)

			if tt.expectedBlocked {
				require.Error(t, err)
				require.Contains(t, err.Error(), globalerror.QueryBlocked)
				require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
							# HELP cortex_query_frontend_rejected_queries_total Number of queries that were rejected by the cluster administrator.
							# TYPE cortex_query_frontend_rejected_queries_total counter
							cortex_query_frontend_rejected_queries_total{reason="blocked", user="test"} 1
						`)))
			} else {
				require.NoError(t, err)
				require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(``)))
			}
		})
	}
}

type mockNextHandler struct {
	t              *testing.T
	shouldContinue bool
}

func (h *mockNextHandler) Do(_ context.Context, _ MetricsQueryRequest) (Response, error) {
	if !h.shouldContinue {
		h.t.Error("The next middleware should not be called.")
	}
	return nil, nil
}
