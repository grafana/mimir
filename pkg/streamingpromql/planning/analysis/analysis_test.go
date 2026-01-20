// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/middleware"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast/sharding"
)

func TestHandler(t *testing.T) {
	testCases := map[string]struct {
		params url.Values

		expectedResponse   string
		expectedStatusCode int
	}{
		"valid request for instant query": {
			params: url.Values{
				"query": []string{`up`},
				"time":  []string{"2022-01-01T00:00:00Z"},
			},
			expectedResponse: `{
			  "originalExpression": "up",
			  "timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
			  "astStages": [
				{"name": "Parsing", "duration": 1234000000, "outputExpression": "up"},
				{"name": "Pre-processing", "duration": 1234000000, "outputExpression": "up"},
				{"name": "Final expression", "duration": null, "outputExpression": "up"}
			  ],
			  "planningStages": [
				{
				  "name": "Original plan",
				  "duration": 1234000000,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__name__=\"up\"}"}
					],
					"originalExpression": "up",
					"version": 0
				  }
				},
				{
				  "name": "Final plan",
				  "duration": null,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__name__=\"up\"}"}
					],
					"originalExpression": "up",
					"version": 0
				  }
				}
			  ],
			  "planVersion": 0
			}`,
			expectedStatusCode: http.StatusOK,
		},

		"valid request for range query": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"2022-01-01T01:00:00Z"},
				"step":  []string{"10"},
			},
			expectedResponse: `{
			  "originalExpression": "up",
			  "timeRange": {"startT": 1640995200000, "endT": 1640998800000, "intervalMilliseconds": 10000},
			  "astStages": [
				{"name": "Parsing", "duration": 1234000000, "outputExpression": "up"},
				{"name": "Pre-processing", "duration": 1234000000, "outputExpression": "up"},
				{"name": "Final expression", "duration": null, "outputExpression": "up"}
			  ],
			  "planningStages": [
				{
				  "name": "Original plan",
				  "duration": 1234000000,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640998800000, "intervalMilliseconds": 10000},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__name__=\"up\"}"}
					],
					"originalExpression": "up",
					"version": 0
				  }
				},
				{
				  "name": "Final plan",
				  "duration": null,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640998800000, "intervalMilliseconds": 10000},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__name__=\"up\"}"}
					],
					"originalExpression": "up",
					"version": 0
				  }
				}
			  ],
			  "planVersion": 0
			}`,
			expectedStatusCode: http.StatusOK,
		},

		"valid request with non-zero plan version": {
			params: url.Values{
				"query": []string{`up @ start()`},
				"time":  []string{"2022-01-01T00:00:00Z"},
			},
			expectedResponse: `{
			  "originalExpression": "up @ start()",
			  "timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
			  "astStages": [
				{"name": "Parsing", "duration": 1234000000, "outputExpression": "up @ start()"},
				{"name": "Pre-processing", "duration": 1234000000, "outputExpression": "up @ 1640995200.000"},
				{"name": "Final expression", "duration": null, "outputExpression": "up @ 1640995200.000"}
			  ],
			  "planningStages": [
				{
				  "name": "Original plan",
				  "duration": 1234000000,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__name__=\"up\"} @ 1640995200000 (2022-01-01T00:00:00Z)"}
					],
					"originalExpression": "up @ start()",
					"version": 0
				  }
				},
				{
				  "name": "Final plan",
				  "duration": null,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__name__=\"up\"} @ 1640995200000 (2022-01-01T00:00:00Z)"}
					],
					"originalExpression": "up @ start()",
					"version": 0
				  }
				}
			  ],
			  "planVersion": 0
			}`,
			expectedStatusCode: http.StatusOK,
		},

		"no params": {
			expectedResponse:   `missing 'query' parameter`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"no time range": {
			params: url.Values{
				"query": []string{`up`},
			},
			expectedResponse:   `missing 'time' parameter for instant query or 'start', 'end' and 'step' parameters for range query`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"invalid time": {
			params: url.Values{
				"query": []string{`up`},
				"time":  []string{"foo"},
			},
			expectedResponse:   `could not parse 'time' parameter: cannot parse "foo" to a valid timestamp`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"invalid start time": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"foo"},
				"end":   []string{"2022-01-01T00:00:00Z"},
				"step":  []string{"10"},
			},
			expectedResponse:   `could not parse 'start' parameter: cannot parse "foo" to a valid timestamp`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"invalid end time": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"foo"},
				"step":  []string{"10"},
			},
			expectedResponse:   `could not parse 'end' parameter: cannot parse "foo" to a valid timestamp`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"invalid step": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"2022-01-01T01:00:00Z"},
				"step":  []string{"foo"},
			},
			expectedResponse:   `could not parse 'step' parameter: cannot parse "foo" to a valid duration`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"0 step": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"2022-01-01T01:00:00Z"},
				"step":  []string{"0"},
			},
			expectedResponse:   `step must be greater than 0`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"negative step": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"2022-01-01T01:00:00Z"},
				"step":  []string{"-10"},
			},
			expectedResponse:   `step must be greater than 0`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"end before start": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T01:00:00Z"},
				"end":   []string{"2022-01-01T00:00:00Z"},
				"step":  []string{"10s"},
			},
			expectedResponse:   `end time must be not be before start time`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"missing start time": {
			params: url.Values{
				"query": []string{`up`},
				"end":   []string{"2022-01-01T01:00:00Z"},
				"step":  []string{"10s"},
			},
			expectedResponse:   `missing 'time' parameter for instant query or 'start', 'end' and 'step' parameters for range query`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"missing end time": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"step":  []string{"10s"},
			},
			expectedResponse:   `missing 'time' parameter for instant query or 'start', 'end' and 'step' parameters for range query`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"missing step": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"2022-01-01T01:00:00Z"},
			},
			expectedResponse:   `missing 'time' parameter for instant query or 'start', 'end' and 'step' parameters for range query`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"have both instant query time and range query start time": {
			params: url.Values{
				"query": []string{`up`},
				"time":  []string{"2022-01-01T00:00:00Z"},
				"start": []string{"2022-01-01T01:00:00Z"},
			},
			expectedResponse:   `cannot provide a mixture of parameters for instant query ('time') and range query ('start', 'end' and 'step')`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"have both instant query time and range query end time": {
			params: url.Values{
				"query": []string{`up`},
				"time":  []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"2022-01-01T01:00:00Z"},
			},
			expectedResponse:   `cannot provide a mixture of parameters for instant query ('time') and range query ('start', 'end' and 'step')`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"have both instant query time and range query step": {
			params: url.Values{
				"query": []string{`up`},
				"time":  []string{"2022-01-01T00:00:00Z"},
				"step":  []string{"10s"},
			},
			expectedResponse:   `cannot provide a mixture of parameters for instant query ('time') and range query ('start', 'end' and 'step')`,
			expectedStatusCode: http.StatusBadRequest,
		},

		"invalid expression": {
			params: url.Values{
				"query": []string{`-`},
				"time":  []string{"2022-01-01T01:00:00Z"},
			},
			expectedResponse:   `parsing expression failed: 1:2: parse error: unexpected end of input`,
			expectedStatusCode: http.StatusBadRequest,
		},
	}

	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(streamingpromql.NewTestEngineOpts(), streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	planner.TimeSince = func(_ time.Time) time.Duration { return 1234 * time.Millisecond }
	handler := Handler(planner)

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.URL.RawQuery = testCase.params.Encode()
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)

			body := resp.Body.String()

			if testCase.expectedStatusCode == http.StatusOK {
				require.JSONEq(t, testCase.expectedResponse, body)
				require.Equal(t, "application/json", resp.Header().Get("Content-Type"))
			} else {
				require.Equal(t, testCase.expectedResponse, body)
				require.Equal(t, "text/plain", resp.Header().Get("Content-Type"))
			}

			require.Equal(t, testCase.expectedStatusCode, resp.Code)
			require.Equal(t, strconv.Itoa(len(body)), resp.Header().Get("Content-Length"))
		})
	}
}

func TestHandler_PlanningDisabled(t *testing.T) {
	handler := Handler(nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	body := resp.Body.String()

	require.Equal(t, "query planning is disabled, analysis is not available", body)
	require.Equal(t, "text/plain", resp.Header().Get("Content-Type"))
	require.Equal(t, http.StatusNotFound, resp.Code)
	require.Equal(t, strconv.Itoa(len(body)), resp.Header().Get("Content-Length"))
}

func TestHandler_Sharding(t *testing.T) {
	testCases := map[string]struct {
		params  url.Values
		headers http.Header

		expectedResponse string
	}{
		"shardable expression with no Sharding-Control header": {
			params: url.Values{
				"query": []string{`sum(up)`},
				"time":  []string{"2022-01-01T00:00:00Z"},
			},
			headers: http.Header{},
			expectedResponse: `{
			  "originalExpression": "sum(up)",
			  "timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
			  "astStages": [
				{"name": "Parsing", "duration": 1234000000, "outputExpression": "sum(up)"},
				{"name": "Pre-processing", "duration": 1234000000, "outputExpression": "sum(up)"},
				{"name": "Sharding", "duration": 1234000000, "outputExpression": "sum(\n  __sharded_concat__(\n    sum(up{__query_shard__=\"1_of_3\"}),\n    sum(up{__query_shard__=\"2_of_3\"}),\n    sum(up{__query_shard__=\"3_of_3\"})\n  )\n)"},
				{"name": "Final expression", "duration": null, "outputExpression": "sum(\n  __sharded_concat__(\n    sum(up{__query_shard__=\"1_of_3\"}),\n    sum(up{__query_shard__=\"2_of_3\"}),\n    sum(up{__query_shard__=\"3_of_3\"})\n  )\n)"}
			  ],
			  "planningStages": [
				{
				  "name": "Original plan",
				  "duration": 1234000000,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__query_shard__=\"1_of_3\", __name__=\"up\"}"},
					  {"type": "AggregateExpression", "children": [0], "description": "sum", "childrenLabels": [""]},
					  {"type": "VectorSelector", "description": "{__query_shard__=\"2_of_3\", __name__=\"up\"}"},
					  {"type": "AggregateExpression", "children": [2], "description": "sum", "childrenLabels": [""]},
					  {"type": "VectorSelector", "description": "{__query_shard__=\"3_of_3\", __name__=\"up\"}"},
					  {"type": "AggregateExpression", "children": [4], "description": "sum", "childrenLabels": [""]},
					  {"type": "FunctionCall", "children": [1, 3, 5], "description": "__sharded_concat__(...)", "childrenLabels": ["param 0", "param 1", "param 2"]},
					  {"type": "AggregateExpression", "children": [6], "description": "sum", "childrenLabels": [""]}
					],
					"originalExpression": "sum(up)",
					"rootNode": 7,
					"version": 0
				  }
				},
				{
				  "name": "Final plan",
				  "duration": null,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__query_shard__=\"1_of_3\", __name__=\"up\"}"},
					  {"type": "AggregateExpression", "children": [0], "description": "sum", "childrenLabels": [""]},
					  {"type": "VectorSelector", "description": "{__query_shard__=\"2_of_3\", __name__=\"up\"}"},
					  {"type": "AggregateExpression", "children": [2], "description": "sum", "childrenLabels": [""]},
					  {"type": "VectorSelector", "description": "{__query_shard__=\"3_of_3\", __name__=\"up\"}"},
					  {"type": "AggregateExpression", "children": [4], "description": "sum", "childrenLabels": [""]},
					  {"type": "FunctionCall", "children": [1, 3, 5], "description": "__sharded_concat__(...)", "childrenLabels": ["param 0", "param 1", "param 2"]},
					  {"type": "AggregateExpression", "children": [6], "description": "sum", "childrenLabels": [""]}
					],
					"originalExpression": "sum(up)",
					"rootNode": 7,
					"version": 0
				  }
				}
			  ],
			  "planVersion": 0
			}`,
		},

		"shardable expression with Sharding-Control header": {
			params: url.Values{
				"query": []string{`sum(up)`},
				"time":  []string{"2022-01-01T00:00:00Z"},
			},
			headers: http.Header{
				"Sharding-Control": []string{"2"},
			},
			expectedResponse: `{
			  "originalExpression": "sum(up)",
			  "timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
			  "astStages": [
				{"name": "Parsing", "duration": 1234000000, "outputExpression": "sum(up)"},
				{"name": "Pre-processing", "duration": 1234000000, "outputExpression": "sum(up)"},
				{"name": "Sharding", "duration": 1234000000, "outputExpression": "sum(__sharded_concat__(sum(up{__query_shard__=\"1_of_2\"}), sum(up{__query_shard__=\"2_of_2\"})))"},
				{"name": "Final expression", "duration": null, "outputExpression": "sum(__sharded_concat__(sum(up{__query_shard__=\"1_of_2\"}), sum(up{__query_shard__=\"2_of_2\"})))"}
			  ],
			  "planningStages": [
				{
				  "name": "Original plan",
				  "duration": 1234000000,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__query_shard__=\"1_of_2\", __name__=\"up\"}"},
					  {"type": "AggregateExpression", "children": [0], "description": "sum", "childrenLabels": [""]},
					  {"type": "VectorSelector", "description": "{__query_shard__=\"2_of_2\", __name__=\"up\"}"},
					  {"type": "AggregateExpression", "children": [2], "description": "sum", "childrenLabels": [""]},
					  {"type": "FunctionCall", "children": [1, 3], "description": "__sharded_concat__(...)", "childrenLabels": ["param 0", "param 1"]},
					  {"type": "AggregateExpression", "children": [4], "description": "sum", "childrenLabels": [""]}
					],
					"originalExpression": "sum(up)",
					"rootNode": 5,
					"version": 0
				  }
				},
				{
				  "name": "Final plan",
				  "duration": null,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__query_shard__=\"1_of_2\", __name__=\"up\"}"},
					  {"type": "AggregateExpression", "children": [0], "description": "sum", "childrenLabels": [""]},
					  {"type": "VectorSelector", "description": "{__query_shard__=\"2_of_2\", __name__=\"up\"}"},
					  {"type": "AggregateExpression", "children": [2], "description": "sum", "childrenLabels": [""]},
					  {"type": "FunctionCall", "children": [1, 3], "description": "__sharded_concat__(...)", "childrenLabels": ["param 0", "param 1"]},
					  {"type": "AggregateExpression", "children": [4], "description": "sum", "childrenLabels": [""]}
					],
					"originalExpression": "sum(up)",
					"rootNode": 5,
					"version": 0
				  }
				}
			  ],
			  "planVersion": 0
			}`,
		},
	}

	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(streamingpromql.NewTestEngineOpts(), streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	planner.TimeSince = func(_ time.Time) time.Duration { return 1234 * time.Millisecond }
	planner.RegisterASTOptimizationPass(sharding.NewOptimizationPass(&mockLimits{}, 0, nil, log.NewNopLogger()))

	handler := middleware.AuthenticateUser(Handler(planner))

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.URL.RawQuery = testCase.params.Encode()
			req.Header = testCase.headers
			req.Header.Set("X-Scope-OrgID", "tenant-1")
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)

			body := resp.Body.String()

			fmt.Println(body)

			require.JSONEq(t, testCase.expectedResponse, body)
			require.Equal(t, "application/json", resp.Header().Get("Content-Type"))
			require.Equal(t, http.StatusOK, resp.Code)
			require.Equal(t, strconv.Itoa(len(body)), resp.Header().Get("Content-Length"))
		})
	}
}

type mockLimits struct{}

func (m *mockLimits) QueryShardingTotalShards(userID string) int        { return 3 }
func (m *mockLimits) QueryShardingMaxRegexpSizeBytes(userID string) int { return 0 }
func (m *mockLimits) QueryShardingMaxShardedQueries(userID string) int  { return 0 }
func (m *mockLimits) CompactorSplitAndMergeShards(userID string) int    { return 1 }
