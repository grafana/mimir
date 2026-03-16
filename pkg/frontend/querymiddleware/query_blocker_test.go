// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

// parseBlockedQueriesYAML processes blocked queries through the production code path: string -> YAML -> limits.
// All queries must be valid YAML (strings quoted, regexes escaped).
// Non-regex queries are further canonicalized (label sorting, etc) via the PromQL parser.
func parseBlockedQueriesYAML(t *testing.T, yamlStr string) []validation.BlockedQuery {
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
			name:            "empty limits",
			query:           "rate(metric_counter[5m])",
			expectedBlocked: false,
		},
		{
			name: "single line non-regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: false
`,
			query:           "rate(metric_counter[5m])",
			expectedBlocked: true,
		},
		{
			name: "non-canonical pattern - label order differs",
			limitsYAML: `
blocked_queries:
  - pattern: 'up{pod="test", job="test"}'
    regex: false
`,
			query:           `up{job="test",pod="test"}`, // Query has labels in different order
			expectedBlocked: true,
		},
		{
			name: "non-canonical pattern - extra whitespace and trailing comma",
			limitsYAML: `
blocked_queries:
  - pattern: 'up{ job="test" , pod="test" , }'
    regex: false
`,
			query:           `up{job="test",pod="test"}`, // Query is canonical (no extra whitespace/comma)
			expectedBlocked: true,
		},
		{
			name: "non-canonical pattern - function with extra whitespace",
			limitsYAML: `
blocked_queries:
  - pattern: 'rate( metric_counter[ 5m ] )'
    regex: false
`,
			query:           `rate(metric_counter[5m])`, // Query is canonical (no extra whitespace)
			expectedBlocked: true,
		},
		{
			name: "non-canonical pattern - aggregation with extra whitespace",
			limitsYAML: `
blocked_queries:
  - pattern: 'sum( rate(metric_counter[5m]) )'
    regex: false
`,
			query:           `sum(rate(metric_counter[5m]))`, // Query is canonical (no extra whitespace)
			expectedBlocked: true,
		},
		{
			name: "non-canonical pattern - aggregation with by() and extra whitespace",
			limitsYAML: `
blocked_queries:
  - pattern: 'sum( rate(metric_counter[5m]) ) by ( job , pod )'
    regex: false
`,
			query:           `sum(rate(metric_counter[5m])) by(job,pod)`, // Query is canonical (no extra whitespace)
			expectedBlocked: true,
		},
		{
			name: "by() labels not sorted - different order doesn't match",
			limitsYAML: `
blocked_queries:
  - pattern: 'sum(rate(metric_counter[5m])) by(job,pod)'
    regex: false
`,
			query:           `sum(rate(metric_counter[5m])) by(pod,job)`, // Different label order in by()
			expectedBlocked: false,
		},
		{
			name: "different pattern - no match",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: false
`,
			query:           "rate(metric_counter[15m])",
			expectedBlocked: false,
		},
		{
			name: "multiple line non-regex pattern",
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
			name: "multiple line different pattern - no match",
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
			expectedBlocked: false,
		},
		{
			name: "single line regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: ".*metric_counter.*"
    regex: true
`,
			query:           "rate(metric_counter[5m])",
			expectedBlocked: true,
		},
		{
			name: "multiple line regex pattern",
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
			name: "regex not canonicalized - out-of-order labels don't match",
			limitsYAML: `
blocked_queries:
  - pattern: 'up\{pod="test",job="test"\}'
    regex: true
`,
			query:           `up{job="test",pod="test"}`, // Canonical query has different label order
			expectedBlocked: false,
		},
		{
			name: "regex not canonicalized - extra whitespace doesn't match",
			limitsYAML: `
blocked_queries:
  - pattern: 'rate\( metric_counter\[ 5m \] \)'
    regex: true
`,
			query:           `rate(metric_counter[5m])`, // Canonical query has no extra whitespace
			expectedBlocked: false,
		},
		{
			name: "invalid regex pattern - no match",
			limitsYAML: `
blocked_queries:
  - pattern: "[a-9}"
    regex: true
`,
			query:           "rate(metric_counter[5m])",
			expectedBlocked: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var limits mockLimits
			if tt.limitsYAML != "" {
				limits.blockedQueries = parseBlockedQueriesYAML(t, tt.limitsYAML)
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

func TestQueryBlockerMiddleware_UnalignedRangeQueries(t *testing.T) {
	var (
		step           = time.Minute
		alignedStart   = timestamp.Time(0).Add(100 * step)
		alignedEnd     = alignedStart.Add(time.Hour)
		unalignedStart = alignedStart.Add(2 * time.Second) // not a multiple of step
		unalignedEnd   = unalignedStart.Add(time.Hour)
	)

	tests := []struct {
		name            string
		query           string
		limitsYAML      string
		start, end      time.Time
		expectedBlocked bool
	}{
		{
			name: "unaligned range query is blocked when unaligned_range_queries is true",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    unaligned_range_queries: true
`,
			query:           "rate(metric_counter[5m])",
			start:           unalignedStart,
			end:             unalignedEnd,
			expectedBlocked: true,
		},
		{
			name: "aligned range query is not blocked when unaligned_range_queries is true",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    unaligned_range_queries: true
`,
			query:           "rate(metric_counter[5m])",
			start:           alignedStart,
			end:             alignedEnd,
			expectedBlocked: false,
		},
		{
			name: "unaligned range query is blocked regardless of unaligned_range_queries when it is false",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    unaligned_range_queries: false
`,
			query:           "rate(metric_counter[5m])",
			start:           unalignedStart,
			end:             unalignedEnd,
			expectedBlocked: true,
		},
		{
			name: "aligned range query is blocked when unaligned_range_queries is false",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    unaligned_range_queries: false
`,
			query:           "rate(metric_counter[5m])",
			start:           alignedStart,
			end:             alignedEnd,
			expectedBlocked: true,
		},
		{
			name: "regex pattern: unaligned range query is blocked when unaligned_range_queries is true",
			limitsYAML: `
blocked_queries:
  - pattern: ".*metric_counter.*"
    regex: true
    unaligned_range_queries: true
`,
			query:           "rate(metric_counter[5m])",
			start:           unalignedStart,
			end:             unalignedEnd,
			expectedBlocked: true,
		},
		{
			name: "regex pattern: aligned range query is not blocked when unaligned_range_queries is true",
			limitsYAML: `
blocked_queries:
  - pattern: ".*metric_counter.*"
    regex: true
    unaligned_range_queries: true
`,
			query:           "rate(metric_counter[5m])",
			start:           alignedStart,
			end:             alignedEnd,
			expectedBlocked: false,
		},
		{
			name: "second rule blocks after first rule skipped due to alignment",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    unaligned_range_queries: true
  - pattern: "rate(metric_counter[5m])"
    reason: "blocked by second rule"
`,
			query:           "rate(metric_counter[5m])",
			start:           alignedStart,
			end:             alignedEnd,
			expectedBlocked: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limits := mockLimits{blockedQueries: parseBlockedQueriesYAML(t, tt.limitsYAML)}

			req := &PrometheusRangeQueryRequest{
				queryExpr: parseQuery(t, tt.query),
				start:     timestamp.FromTime(tt.start),
				end:       timestamp.FromTime(tt.end),
				step:      step.Milliseconds(),
			}

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
}

func TestQueryBlockerMiddleware_UnalignedRangeQueries_InstantAndRemoteRead(t *testing.T) {
	tests := []struct {
		name       string
		limitsYAML string
		makeReq    func(t *testing.T) MetricsQueryRequest
	}{
		{
			name: "instant query",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    unaligned_range_queries: true
`,
			makeReq: func(t *testing.T) MetricsQueryRequest {
				return &PrometheusInstantQueryRequest{queryExpr: parseQuery(t, "rate(metric_counter[5m])")}
			},
		},
		{
			name: "remote read",
			limitsYAML: `
blocked_queries:
  - pattern: '{__name__="metric_counter"}'
    unaligned_range_queries: true
`,
			makeReq: func(t *testing.T) MetricsQueryRequest {
				req, err := remoteReadToMetricsQueryRequest(remoteReadPathSuffix, &prompb.Query{
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "metric_counter"},
					},
				})
				require.NoError(t, err)
				return req
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limits := mockLimits{blockedQueries: parseBlockedQueriesYAML(t, tt.limitsYAML)}
			reg := prometheus.NewPedanticRegistry()
			blockedQueriesCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "cortex_query_frontend_rejected_queries_total",
				Help: "Number of queries that were rejected by the cluster administrator.",
			}, []string{"user", "reason"})
			mw := newQueryBlockerMiddleware(limits, log.NewNopLogger(), blockedQueriesCounter)
			_, err := mw.Wrap(&mockNextHandler{t: t, shouldContinue: true}).Do(user.InjectOrgID(context.Background(), "test"), tt.makeReq(t))

			require.NoError(t, err)
			require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader("")))
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
			name:            "empty limits",
			expectedBlocked: false,
		},
		{
			name: "non-regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: '{__name__="metric_counter",pod=~"app-.*"}'
    regex: false
`,
			expectedBlocked: true,
		},
		{
			name: "different non-regex pattern - no match",
			limitsYAML: `
blocked_queries:
  - pattern: '{__name__="another_metric",pod=~"app-.*"}'
    regex: false
`,
			expectedBlocked: false,
		},
		{
			name: "regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: ".*metric_counter.*"
    regex: true
`,
			expectedBlocked: true,
		},
		{
			name: "regex with escaped braces for remote read",
			limitsYAML: `
blocked_queries:
  - pattern: '\{.*metric_counter.*\}'
    regex: true
`,
			expectedBlocked: true,
		},
		{
			name: "regex with double-quote escaping",
			limitsYAML: `
blocked_queries:
  - pattern: "\\{.*metric_counter.*\\}"
    regex: true
`,
			expectedBlocked: true,
		},
		{
			name: "different regex pattern - no match",
			limitsYAML: `
blocked_queries:
  - pattern: ".*another_metric.*"
    regex: true
`,
			expectedBlocked: false,
		},
		{
			name: "invalid regex pattern - no match",
			limitsYAML: `
blocked_queries:
  - pattern: "[a-9}"
    regex: true
`,
			expectedBlocked: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var limits mockLimits
			if tt.limitsYAML != "" {
				limits.blockedQueries = parseBlockedQueriesYAML(t, tt.limitsYAML)
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
