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

// runBlockerTest builds the registry/counter/middleware and asserts blocked vs allowed.
func runBlockerTest(t *testing.T, limitsYAML string, makeReq func(*testing.T) MetricsQueryRequest, expectedBlocked bool) {
	t.Helper()

	var limits mockLimits
	if limitsYAML != "" {
		limits.blockedQueries = parseBlockedQueriesYAML(t, limitsYAML)
	}

	reg := prometheus.NewPedanticRegistry()
	blockedQueriesCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_frontend_rejected_queries_total",
		Help: "Number of queries that were rejected by the cluster administrator.",
	}, []string{"user", "reason"})
	mw := newQueryBlockerMiddleware(limits, log.NewNopLogger(), blockedQueriesCounter)
	_, err := mw.Wrap(&mockNextHandler{t: t, shouldContinue: !expectedBlocked}).Do(user.InjectOrgID(context.Background(), "test"), makeReq(t))

	if expectedBlocked {
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
}

// TestQueryBlockerMiddleware_Pattern verifies pattern matching: exact match, canonicalisation,
// regex, and that rules without a pattern are skipped.
func TestQueryBlockerMiddleware_Pattern(t *testing.T) {
	now := time.Now()

	rangeReq := func(query string) func(t *testing.T) MetricsQueryRequest {
		return func(t *testing.T) MetricsQueryRequest {
			return &PrometheusRangeQueryRequest{
				queryExpr: parseQuery(t, query),
				start:     now.Add(-time.Hour).UnixMilli(),
				end:       now.UnixMilli(),
			}
		}
	}
	instantReq := func(query string) func(t *testing.T) MetricsQueryRequest {
		return func(t *testing.T) MetricsQueryRequest {
			return &PrometheusInstantQueryRequest{
				queryExpr: parseQuery(t, query),
				time:      now.UnixMilli(),
			}
		}
	}

	tests := []struct {
		name            string
		limitsYAML      string
		makeReq         func(t *testing.T) MetricsQueryRequest
		expectedBlocked bool
	}{
		{
			name:            "empty limits",
			makeReq:         rangeReq("rate(metric_counter[5m])"),
			expectedBlocked: false,
		},
		{
			name: "no pattern",
			limitsYAML: `
blocked_queries:
  - reason: "should not block without pattern"
`,
			makeReq:         rangeReq("up"),
			expectedBlocked: false,
		},
		{
			name: "single line non-regex pattern (range)",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: false
`,
			makeReq:         rangeReq("rate(metric_counter[5m])"),
			expectedBlocked: true,
		},
		{
			name: "single line non-regex pattern (instant)",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: false
`,
			makeReq:         instantReq("rate(metric_counter[5m])"),
			expectedBlocked: true,
		},
		{
			name: "non-canonical pattern - label order differs",
			limitsYAML: `
blocked_queries:
  - pattern: 'up{pod="test", job="test"}'
    regex: false
`,
			makeReq:         rangeReq(`up{job="test",pod="test"}`),
			expectedBlocked: true,
		},
		{
			name: "non-canonical pattern - extra whitespace and trailing comma",
			limitsYAML: `
blocked_queries:
  - pattern: 'up{ job="test" , pod="test" , }'
    regex: false
`,
			makeReq:         rangeReq(`up{job="test",pod="test"}`),
			expectedBlocked: true,
		},
		{
			name: "non-canonical pattern - function with extra whitespace",
			limitsYAML: `
blocked_queries:
  - pattern: 'rate( metric_counter[ 5m ] )'
    regex: false
`,
			makeReq:         rangeReq(`rate(metric_counter[5m])`),
			expectedBlocked: true,
		},
		{
			name: "non-canonical pattern - aggregation with extra whitespace",
			limitsYAML: `
blocked_queries:
  - pattern: 'sum( rate(metric_counter[5m]) )'
    regex: false
`,
			makeReq:         rangeReq(`sum(rate(metric_counter[5m]))`),
			expectedBlocked: true,
		},
		{
			name: "non-canonical pattern - aggregation with by() and extra whitespace",
			limitsYAML: `
blocked_queries:
  - pattern: 'sum( rate(metric_counter[5m]) ) by ( job , pod )'
    regex: false
`,
			makeReq:         rangeReq(`sum(rate(metric_counter[5m])) by(job,pod)`),
			expectedBlocked: true,
		},
		{
			name: "by() labels not sorted - different order",
			limitsYAML: `
blocked_queries:
  - pattern: 'sum(rate(metric_counter[5m])) by(job,pod)'
    regex: false
`,
			makeReq:         rangeReq(`sum(rate(metric_counter[5m])) by(pod,job)`),
			expectedBlocked: false,
		},
		{
			name: "different pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: false
`,
			makeReq:         rangeReq("rate(metric_counter[15m])"),
			expectedBlocked: false,
		},
		{
			name: "multiple line non-regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m]) / rate(other_counter[5m])"
    regex: false
`,
			makeReq: func(t *testing.T) MetricsQueryRequest {
				return &PrometheusRangeQueryRequest{
					queryExpr: parseQuery(t, `
						rate(metric_counter[5m])
						/
						rate(other_counter[5m])
					`),
					start: now.Add(-time.Hour).UnixMilli(),
					end:   now.UnixMilli(),
				}
			},
			expectedBlocked: true,
		},
		{
			name: "multiple line different pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: false
`,
			makeReq: func(t *testing.T) MetricsQueryRequest {
				return &PrometheusRangeQueryRequest{
					queryExpr: parseQuery(t, `
						rate(metric_counter[15m])
						/
						rate(other_counter[15m])
					`),
					start: now.Add(-time.Hour).UnixMilli(),
					end:   now.UnixMilli(),
				}
			},
			expectedBlocked: false,
		},
		{
			name: "single line regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: ".*metric_counter.*"
    regex: true
`,
			makeReq:         rangeReq("rate(metric_counter[5m])"),
			expectedBlocked: true,
		},
		{
			name: "block all queries with .* regex",
			limitsYAML: `
blocked_queries:
  - pattern: ".*"
    regex: true
    reason: "all queries are blocked"
`,
			makeReq:         rangeReq("up"),
			expectedBlocked: true,
		},
		{
			name: "multiple line regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "(?s).*metric_counter.*"
    regex: true
`,
			makeReq: func(t *testing.T) MetricsQueryRequest {
				return &PrometheusRangeQueryRequest{
					queryExpr: parseQuery(t, `
						rate(other_counter[15m])
						/
						rate(metric_counter[15m])
					`),
					start: now.Add(-time.Hour).UnixMilli(),
					end:   now.UnixMilli(),
				}
			},
			expectedBlocked: true,
		},
		{
			name: "regex not canonicalized - out-of-order labels don't match",
			limitsYAML: `
blocked_queries:
  - pattern: 'up\{pod="test",job="test"\}'
    regex: true
`,
			makeReq:         rangeReq(`up{job="test",pod="test"}`),
			expectedBlocked: false,
		},
		{
			name: "regex not canonicalized - extra whitespace",
			limitsYAML: `
blocked_queries:
  - pattern: 'rate\( metric_counter\[ 5m \] \)'
    regex: true
`,
			makeReq:         rangeReq(`rate(metric_counter[5m])`),
			expectedBlocked: false,
		},
		{
			name: "invalid regex pattern",
			limitsYAML: `
blocked_queries:
  - pattern: "[a-9}"
    regex: true
`,
			makeReq:         rangeReq("rate(metric_counter[5m])"),
			expectedBlocked: false,
		},
		{
			name: "literal pattern with regex metacharacters and regex: true",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: true
    reason: "literal pattern accidentally marked as regex"
`,
			makeReq:         rangeReq("rate(metric_counter[5m])"),
			expectedBlocked: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runBlockerTest(t, tt.limitsYAML, tt.makeReq, tt.expectedBlocked)
		})
	}
}

// TestQueryBlockerMiddleware_UnalignedRangeQueries verifies unaligned_range_queries: when true, a rule
// only blocks range queries where the time range is not aligned to the step; aligned queries, instant
// queries, and remote read requests are not blocked.
func TestQueryBlockerMiddleware_UnalignedRangeQueries(t *testing.T) {
	step := time.Minute
	alignedStart := timestamp.Time(0).Add(100 * step)
	alignedEnd := alignedStart.Add(time.Hour)
	unalignedStart := alignedStart.Add(2 * time.Second)
	unalignedEnd := unalignedStart.Add(time.Hour)

	rangeReq := func(query string, start, end time.Time, stepMs int64) func(t *testing.T) MetricsQueryRequest {
		return func(t *testing.T) MetricsQueryRequest {
			return &PrometheusRangeQueryRequest{
				queryExpr: parseQuery(t, query),
				start:     start.UnixMilli(),
				end:       end.UnixMilli(),
				step:      stepMs,
			}
		}
	}
	instantReq := func(query string) func(t *testing.T) MetricsQueryRequest {
		now := time.Now()
		return func(t *testing.T) MetricsQueryRequest {
			return &PrometheusInstantQueryRequest{
				queryExpr: parseQuery(t, query),
				time:      now.UnixMilli(),
			}
		}
	}
	remoteReadReq := func(matchers ...*prompb.LabelMatcher) func(t *testing.T) MetricsQueryRequest {
		req := mustSucceed(remoteReadToMetricsQueryRequest(remoteReadPathSuffix, &prompb.Query{Matchers: matchers}))
		return func(_ *testing.T) MetricsQueryRequest { return req }
	}

	tests := []struct {
		name            string
		limitsYAML      string
		makeReq         func(t *testing.T) MetricsQueryRequest
		expectedBlocked bool
	}{
		{
			name: "no pattern",
			limitsYAML: `
blocked_queries:
  - unaligned_range_queries: true
    reason: "unaligned range query"
`,
			makeReq:         rangeReq("rate(metric_counter[5m])", unalignedStart, unalignedEnd, step.Milliseconds()),
			expectedBlocked: false,
		},
		{
			name: "unaligned range query",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    unaligned_range_queries: true
`,
			makeReq:         rangeReq("rate(metric_counter[5m])", unalignedStart, unalignedEnd, step.Milliseconds()),
			expectedBlocked: true,
		},
		{
			name: "aligned range query",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    unaligned_range_queries: true
`,
			makeReq:         rangeReq("rate(metric_counter[5m])", alignedStart, alignedEnd, step.Milliseconds()),
			expectedBlocked: false,
		},
		{
			name: "unaligned range query when filter set to false",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    unaligned_range_queries: false
`,
			makeReq:         rangeReq("rate(metric_counter[5m])", unalignedStart, unalignedEnd, step.Milliseconds()),
			expectedBlocked: true,
		},
		{
			name: "aligned range query when filter set to false",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    unaligned_range_queries: false
`,
			makeReq:         rangeReq("rate(metric_counter[5m])", alignedStart, alignedEnd, step.Milliseconds()),
			expectedBlocked: true,
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
			makeReq:         rangeReq("rate(metric_counter[5m])", alignedStart, alignedEnd, step.Milliseconds()),
			expectedBlocked: true,
		},
		{
			name: "instant query",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    unaligned_range_queries: true
`,
			makeReq:         instantReq("rate(metric_counter[5m])"),
			expectedBlocked: false,
		},
		{
			name: "remote read",
			limitsYAML: `
blocked_queries:
  - pattern: '{__name__="metric_counter"}'
    unaligned_range_queries: true
`,
			makeReq: remoteReadReq(
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "metric_counter"},
			),
			expectedBlocked: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runBlockerTest(t, tt.limitsYAML, tt.makeReq, tt.expectedBlocked)
		})
	}
}

// TestQueryBlockerMiddleware_RemoteRead verifies pattern matching for remote-read requests.
//
// Remote read query strings are always in selector form ({label=value,...}) produced by
// LabelMatchersToString — never PromQL syntax. Non-regex matching is a raw string compare
// with no canonicalization on the query side, so matcher order is significant (unlike range/instant
// queries where both sides are canonicalized via the PromQL parser).
// step_size_shorter_than never applies because GetStep always returns 0 for remote reads.
func TestQueryBlockerMiddleware_RemoteRead(t *testing.T) {
	remoteReadReq := func(matchers ...*prompb.LabelMatcher) func(t *testing.T) MetricsQueryRequest {
		req := mustSucceed(remoteReadToMetricsQueryRequest(remoteReadPathSuffix, &prompb.Query{Matchers: matchers}))
		return func(_ *testing.T) MetricsQueryRequest { return req }
	}

	counterMatcher := &prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "metric_counter"}
	podMatcher := &prompb.LabelMatcher{Type: prompb.LabelMatcher_RE, Name: "pod", Value: "app-.*"}

	tests := []struct {
		name            string
		limitsYAML      string
		makeReq         func(t *testing.T) MetricsQueryRequest
		expectedBlocked bool
	}{
		{
			// Query string matches pattern exactly: {__name__="metric_counter",pod=~"app-.*"}
			name: "non-regex pattern matches when matcher order matches",
			limitsYAML: `
blocked_queries:
  - pattern: '{__name__="metric_counter",pod=~"app-.*"}'
    regex: false
`,
			makeReq:         remoteReadReq(counterMatcher, podMatcher),
			expectedBlocked: true,
		},
		{
			// LabelMatchersToString preserves caller order; no canonicalization normalises it.
			// Contrast with range queries where both pattern and query go through the PromQL parser.
			name: "non-regex pattern does not match when matcher order differs",
			limitsYAML: `
blocked_queries:
  - pattern: '{__name__="metric_counter",pod=~"app-.*"}'
    regex: false
`,
			makeReq:         remoteReadReq(podMatcher, counterMatcher),
			expectedBlocked: false,
		},
		{
			// Query string is always selector form; PromQL expressions can never match.
			name: "promql-style non-regex pattern never matches remote read",
			limitsYAML: `
blocked_queries:
  - pattern: "rate(metric_counter[5m])"
    regex: false
`,
			makeReq:         remoteReadReq(counterMatcher),
			expectedBlocked: false,
		},
		{
			name: "regex pattern matches",
			limitsYAML: `
blocked_queries:
  - pattern: ".*metric_counter.*"
    regex: true
`,
			makeReq:         remoteReadReq(counterMatcher, podMatcher),
			expectedBlocked: true,
		},
		{
			// Query string always starts with '{'; anchoring on braces is meaningful here.
			name: "regex anchored on braces matches selector form",
			limitsYAML: `
blocked_queries:
  - pattern: '\{.*metric_counter.*\}'
    regex: true
`,
			makeReq:         remoteReadReq(counterMatcher, podMatcher),
			expectedBlocked: true,
		},
		{
			name: "regex pattern does not match",
			limitsYAML: `
blocked_queries:
  - pattern: ".*another_metric.*"
    regex: true
`,
			makeReq:         remoteReadReq(counterMatcher, podMatcher),
			expectedBlocked: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runBlockerTest(t, tt.limitsYAML, tt.makeReq, tt.expectedBlocked)
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
