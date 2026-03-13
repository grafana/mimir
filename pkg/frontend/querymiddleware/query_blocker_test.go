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
	now := time.Now()
	tests := []struct {
		name                   string
		query                  string
		queryStart             time.Time
		queryEnd               time.Time
		limitsYAML             string
		expectedBlockedRange   bool
		expectedBlockedInstant bool
	}{
		{
			name:                   "empty limits",
			query:                  "rate(metric_counter[5m])",
			expectedBlockedRange:   false,
			expectedBlockedInstant: false,
		},
		{
			name: "single line non-regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: false
`,
			query:                  "rate(metric_counter[5m])",
			expectedBlockedRange:   true,
			expectedBlockedInstant: true,
		},
		{
			name: "non-canonical pattern - label order differs",
			limitsYAML: `
blocked_queries:
  - pattern: 'up{pod="test", job="test"}'
    regex: false
`,
			query:                  `up{job="test",pod="test"}`, // Query has labels in different order
			expectedBlockedRange:   true,
			expectedBlockedInstant: true,
		},
		{
			name: "non-canonical pattern - extra whitespace and trailing comma",
			limitsYAML: `
blocked_queries:
  - pattern: 'up{ job="test" , pod="test" , }'
    regex: false
`,
			query:                  `up{job="test",pod="test"}`, // Query is canonical (no extra whitespace/comma)
			expectedBlockedRange:   true,
			expectedBlockedInstant: true,
		},
		{
			name: "non-canonical pattern - function with extra whitespace",
			limitsYAML: `
blocked_queries:
  - pattern: 'rate( metric_counter[ 5m ] )'
    regex: false
`,
			query:                  `rate(metric_counter[5m])`, // Query is canonical (no extra whitespace)
			expectedBlockedRange:   true,
			expectedBlockedInstant: true,
		},
		{
			name: "non-canonical pattern - aggregation with extra whitespace",
			limitsYAML: `
blocked_queries:
  - pattern: 'sum( rate(metric_counter[5m]) )'
    regex: false
`,
			query:                  `sum(rate(metric_counter[5m]))`, // Query is canonical (no extra whitespace)
			expectedBlockedRange:   true,
			expectedBlockedInstant: true,
		},
		{
			name: "non-canonical pattern - aggregation with by() and extra whitespace",
			limitsYAML: `
blocked_queries:
  - pattern: 'sum( rate(metric_counter[5m]) ) by ( job , pod )'
    regex: false
`,
			query:                  `sum(rate(metric_counter[5m])) by(job,pod)`, // Query is canonical (no extra whitespace)
			expectedBlockedRange:   true,
			expectedBlockedInstant: true,
		},
		{
			name: "by() labels not sorted - different order",
			limitsYAML: `
blocked_queries:
  - pattern: 'sum(rate(metric_counter[5m])) by(job,pod)'
    regex: false
`,
			query:                  `sum(rate(metric_counter[5m])) by(pod,job)`, // Different label order in by()
			expectedBlockedRange:   false,
			expectedBlockedInstant: false,
		},
		{
			name: "different pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: false
`,
			query:                  "rate(metric_counter[15m])",
			expectedBlockedRange:   false,
			expectedBlockedInstant: false,
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
			expectedBlockedRange:   true,
			expectedBlockedInstant: true,
		},
		{
			name: "multiple line different pattern",
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
			expectedBlockedRange:   false,
			expectedBlockedInstant: false,
		},
		{
			name: "single line regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: ".*metric_counter.*"
    regex: true
`,
			query:                  "rate(metric_counter[5m])",
			expectedBlockedRange:   true,
			expectedBlockedInstant: true,
		},
		{
			name: "block all queries with .* regex",
			limitsYAML: `
blocked_queries:
  - pattern: ".*"
    regex: true
    reason: "all queries are blocked"
`,
			query:                  "up",
			expectedBlockedRange:   true,
			expectedBlockedInstant: true,
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
			expectedBlockedRange:   true,
			expectedBlockedInstant: true,
		},
		{
			name: "regex not canonicalized - out-of-order labels don't match",
			limitsYAML: `
blocked_queries:
  - pattern: 'up\{pod="test",job="test"\}'
    regex: true
`,
			query:                  `up{job="test",pod="test"}`, // Canonical query has different label order
			expectedBlockedRange:   false,
			expectedBlockedInstant: false,
		},
		{
			name: "regex not canonicalized - extra whitespace",
			limitsYAML: `
blocked_queries:
  - pattern: 'rate\( metric_counter\[ 5m \] \)'
    regex: true
`,
			query:                  `rate(metric_counter[5m])`, // Canonical query has no extra whitespace
			expectedBlockedRange:   false,
			expectedBlockedInstant: false,
		},
		{
			name: "invalid regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "[a-9}"
    regex: true
`,
			query:                  "rate(metric_counter[5m])",
			expectedBlockedRange:   false,
			expectedBlockedInstant: false,
		},
		{
			name: "invalid regex pattern with time_range_longer_than",
			limitsYAML: `
blocked_queries:
  - pattern: "[a-9}"
    regex: true
    time_range_longer_than: "1h"
    reason: "invalid regex - must bail out to avoid matching all queries"
`,
			query:                  "rate(metric_counter[5m])",
			queryStart:             now.Add(-25 * time.Hour), // Over 1h threshold
			queryEnd:               now,
			expectedBlockedRange:   false, // Must bail out, invalid regex could match anything
			expectedBlockedInstant: false,
		},
		{
			name: "literal pattern with regex metacharacters and regex: true",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: true
    reason: "literal pattern accidentally marked as regex"
`,
			query:                  "rate(metric_counter[5m])",
			expectedBlockedRange:   true,
			expectedBlockedInstant: true,
		},
		{
			name: "time range longer than threshold",
			limitsYAML: `
blocked_queries:
  - time_range_longer_than: "24h"
    reason: "queries longer than 1 day are not allowed"
`,
			query:                  "up",
			queryStart:             now.Add(-48 * time.Hour),
			queryEnd:               now,
			expectedBlockedRange:   true,
			expectedBlockedInstant: false,
		},
		{
			name: "time range under threshold",
			limitsYAML: `
blocked_queries:
  - time_range_longer_than: "24h"
`,
			query:                  "up",
			queryStart:             now.Add(-12 * time.Hour),
			queryEnd:               now,
			expectedBlockedRange:   false,
			expectedBlockedInstant: false,
		},
		{
			name: "pattern matches AND time range longer than threshold",
			limitsYAML: `
blocked_queries:
  - pattern: ".*expensive.*"
    regex: true
    time_range_longer_than: "24h"
    reason: "expensive queries over 1 day are blocked"
`,
			query:                  "rate(expensive_metric[5m])",
			queryStart:             now.Add(-2 * 24 * time.Hour), // 2 days
			queryEnd:               now,
			expectedBlockedRange:   true,
			expectedBlockedInstant: false,
		},
		{
			name: "pattern matches but time range under threshold",
			limitsYAML: `
blocked_queries:
  - pattern: ".*expensive.*"
    regex: true
    time_range_longer_than: "168h"
`,
			query:                  "rate(expensive_metric[5m])",
			queryStart:             now.Add(-2 * 24 * time.Hour), // 2 days - under threshold
			queryEnd:               now,
			expectedBlockedRange:   false,
			expectedBlockedInstant: false,
		},
		{
			name: "different pattern but time range longer than threshold",
			limitsYAML: `
blocked_queries:
  - pattern: ".*expensive.*"
    regex: true
    time_range_longer_than: "168h"
`,
			query:                  "rate(cheap_metric[5m])",
			queryStart:             now.Add(-10 * 24 * time.Hour), // 10 days - over threshold
			queryEnd:               now,
			expectedBlockedRange:   false,
			expectedBlockedInstant: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var limits mockLimits
			if tt.limitsYAML != "" {
				limits.blockedQueries = parseBlockedQueriesYAML(t, tt.limitsYAML)
			}

			// Set default times for range queries if not specified
			start := tt.queryStart
			end := tt.queryEnd
			if start.IsZero() {
				start = now.Add(-1 * time.Hour)
			}
			if end.IsZero() {
				end = now
			}
			reqs := map[string]MetricsQueryRequest{
				"range query": &PrometheusRangeQueryRequest{
					queryExpr: parseQuery(t, tt.query),
					start:     start.UnixMilli(),
					end:       end.UnixMilli(),
				},
				"instant query": &PrometheusInstantQueryRequest{
					queryExpr: parseQuery(t, tt.query),
					time:      now.UnixMilli(),
				},
			}

			for reqType, req := range reqs {
				t.Run(reqType, func(t *testing.T) {
					var expectBlocked bool
					if reqType == "instant query" {
						expectBlocked = tt.expectedBlockedInstant
					} else {
						expectBlocked = tt.expectedBlockedRange
					}

					reg := prometheus.NewPedanticRegistry()
					blockedQueriesCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
						Name: "cortex_query_frontend_rejected_queries_total",
						Help: "Number of queries that were rejected by the cluster administrator.",
					}, []string{"user", "reason"})
					logger := log.NewNopLogger()
					mw := newQueryBlockerMiddleware(limits, logger, blockedQueriesCounter)
					_, err := mw.Wrap(&mockNextHandler{t: t, shouldContinue: !expectBlocked}).Do(user.InjectOrgID(context.Background(), "test"), req)

					if expectBlocked {
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
			name: "different non-regex pattern",
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
			name: "different regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: ".*another_metric.*"
    regex: true
`,
			expectedBlocked: false,
		},
		{
			name: "invalid regex pattern",
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
