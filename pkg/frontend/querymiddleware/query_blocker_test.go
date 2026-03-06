// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

// unmarshalBlockedQueries unmarshals blocked queries from YAML, matching the production
// code path. Non-regex patterns are canonicalized (labels sorted, whitespace normalized).
// Regex patterns must also use YAML parsing to validate escaping (use single quotes '\d+'
// or double backslashes "\\d+" for regex special characters).
func unmarshalBlockedQueries(t *testing.T, yamlStr string) []validation.BlockedQuery {
	t.Helper()

	// Initialize with minimal required defaults to pass validation
	limits := validation.Limits{
		IngestStorageReadConsistency: "eventual", // Required for validation
	}

	err := yaml.Unmarshal([]byte(yamlStr), &limits)
	require.NoError(t, err)

	return limits.BlockedQueries
}

func TestQueryBlockerMiddleware_RangeAndInstantQuery(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		limitsYAML      string
		expectedBlocked bool
	}{
		{
			name:  "doesn't block queries due to empty limits",
			query: "rate(metric_counter[5m])",
		},
		{
			name: "blocks single line query non regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: false
`,
			query:           "rate(metric_counter[5m])",
			expectedBlocked: true,
		},
		{
			name: "blocks query with non-canonical pattern (label order differs) - validates canonicalization",
			limitsYAML: `
blocked_queries:
  - pattern: 'up{pod="test", job="test"}'
    regex: false
`,
			query:           `up{job="test",pod="test"}`, // Query has labels in different order
			expectedBlocked: true,
		},
		{
			name: "not blocks single line query non regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: false
`,
			query: "rate(metric_counter[15m])",
		},
		{
			name: "blocks multiple line query non regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m]) / rate(other_counter[5m])"
    regex: false
`,
			query: `
				rate(metric_counter[5m])
				/
				rate(other_counter[5m])
			`,
			expectedBlocked: true,
		},
		{
			name: "not blocks multiple line query non regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: false
`,
			query: `
				rate(metric_counter[15m])
				/
				rate(other_counter[15m])
			`,
		},
		{
			name: "blocks single line query regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: ".*metric_counter.*"
    regex: true
`,
			query:           "rate(metric_counter[5m])",
			expectedBlocked: true,
		},
		{
			name: "blocks multiple line query regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "(?s).*metric_counter.*"
    regex: true
`,
			query: `
				rate(other_counter[15m])
				/
				rate(metric_counter[15m])
			`,
			expectedBlocked: true,
		},
		{
			name: "invalid regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "[a-9}"
    regex: true
`,
			query: "rate(metric_counter[5m])",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var limits mockLimits
			if tt.limitsYAML != "" {
				limits.blockedQueries = unmarshalBlockedQueries(t, tt.limitsYAML)
			}

			reqs := map[string]MetricsQueryRequest{
				"range query": &PrometheusRangeQueryRequest{
					queryExpr: parseQuery(t, tt.query),
				},
				"instant query": &PrometheusInstantQueryRequest{
					queryExpr: parseQuery(t, tt.query),
				},
			}

			for reqType, req := range reqs {
				t.Run(reqType, func(t *testing.T) {
					reg := prometheus.NewPedanticRegistry()
					blockedQueriesCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
						Name: "cortex_query_frontend_rejected_queries_total",
						Help: "Number of queries that were rejected by the cluster administrator.",
					}, []string{"user", "reason"})
					logger := log.NewNopLogger()
					mw := newQueryBlockerMiddleware(limits, logger, blockedQueriesCounter)
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
			{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "metric_counter"},
			{Type: prompb.LabelMatcher_RE, Name: "pod", Value: "app-.*"},
		},
	}

	tests := []struct {
		name            string
		limitsYAML      string
		expectedBlocked bool
	}{
		{
			name: "doesn't block queries due to empty limits",
		},
		{
			name: "blocks query via non regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: '{__name__="metric_counter",pod=~"app-.*"}'
    regex: false
`,
			expectedBlocked: true,
		},
		{
			name: "not blocks query via non regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: '{__name__="another_metric",pod=~"app-.*"}'
    regex: false
`,
		},
		{
			name: "blocks query via regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: ".*metric_counter.*"
    regex: true
`,
			expectedBlocked: true,
		},
		{
			name: "blocks query via regex pattern, with begin/end curly brackets used as a trick to match only remote read requests",
			limitsYAML: `
blocked_queries:
  - pattern: '\{.*metric_counter.*\}'
    regex: true
`,
			expectedBlocked: true,
		},
		{
			name: "blocks query via regex pattern with double-quote escaping (alternative to single quotes)",
			limitsYAML: `
blocked_queries:
  - pattern: "\\{.*metric_counter.*\\}"
    regex: true
`,
			expectedBlocked: true,
		},
		{
			name: "not blocks query via regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: ".*another_metric.*"
    regex: true
`,
		},
		{
			name: "invalid regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "[a-9}"
    regex: true
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var limits mockLimits
			if tt.limitsYAML != "" {
				limits.blockedQueries = unmarshalBlockedQueries(t, tt.limitsYAML)
			}

			req, err := remoteReadToMetricsQueryRequest(remoteReadPathSuffix, query)
			require.NoError(t, err)

			reg := prometheus.NewPedanticRegistry()
			blockedQueriesCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "cortex_query_frontend_rejected_queries_total",
				Help: "Number of queries that were rejected by the cluster administrator.",
			}, []string{"user", "reason"})
			logger := log.NewNopLogger()
			mw := newQueryBlockerMiddleware(limits, logger, blockedQueriesCounter)
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
