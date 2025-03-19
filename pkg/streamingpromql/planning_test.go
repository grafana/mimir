// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestPlanCreationEncodingAndDecoding(t *testing.T) {
	opts := NewTestEngineOpts()
	opts.CommonOpts.NoStepSubqueryIntervalFn = func(_ int64) int64 {
		return (23 * time.Second).Milliseconds()
	}

	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	instantQuery := types.NewInstantQueryTimeRange(timestamp.Time(1000))
	rangeQuery := types.NewRangeQueryTimeRange(timestamp.Time(3000), timestamp.Time(5000), time.Second)

	testCases := map[string]struct {
		expr      string
		timeRange types.QueryTimeRange

		expectedPlan string
	}{
		"instant query with vector selector": {
			expr:      `some_metric{env="prod", cluster!="cluster-2", name=~"foo.*", node!~"small-nodes-.*"}`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"env","value":"prod"},
								{"type":1,"name":"cluster","value":"cluster-2"},
								{"type":2,"name":"name","value":"foo.*"},
								{"type":3,"name":"node","value":"small-nodes-.*"},
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [0,84]
						},
						"description": "{env=\"prod\", cluster!=\"cluster-2\", name=~\"foo.*\", node!~\"small-nodes-.*\", __name__=\"some_metric\"}"
					}
				]
			}`,
		},
		"range query with vector selector": {
			expr:      `some_metric{env="prod", cluster!="cluster-2", name=~"foo.*", node!~"small-nodes-.*"}`,
			timeRange: rangeQuery,

			expectedPlan: `{
				"timeRange": { "start": 3000, "end": 5000, "interval": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"env","value":"prod"},
								{"type":1,"name":"cluster","value":"cluster-2"},
								{"type":2,"name":"name","value":"foo.*"},
								{"type":3,"name":"node","value":"small-nodes-.*"},
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [0,84]
						},
						"description": "{env=\"prod\", cluster!=\"cluster-2\", name=~\"foo.*\", node!~\"small-nodes-.*\", __name__=\"some_metric\"}"
					}
				]
			}`,
		},
		"vector selector with '@ 0'": {
			expr:      `some_metric @ 0`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"timestamp": 0,
							"expressionPosition": [0,15]
						},
						"description": "{__name__=\"some_metric\"} @ 0 (1970-01-01T00:00:00Z)"
					}
				]
			}`,
		},
		"vector selector with '@ start()'": {
			expr:      `some_metric @ start()`,
			timeRange: rangeQuery,

			expectedPlan: `{
				"timeRange": { "start": 3000, "end": 5000, "interval": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"timestamp": 3000,
							"expressionPosition": [0,21]
						},
						"description": "{__name__=\"some_metric\"} @ 3000 (1970-01-01T00:00:03Z)"
					}
				]
			}`,
		},
		"vector selector with '@ end()'": {
			expr:      `some_metric @ end()`,
			timeRange: rangeQuery,

			expectedPlan: `{
				"timeRange": { "start": 3000, "end": 5000, "interval": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"timestamp": 5000,
							"expressionPosition": [0,19]
						},
						"description": "{__name__=\"some_metric\"} @ 5000 (1970-01-01T00:00:05Z)"
					}
				]
			}`,
		},
		"vector selector with offset": {
			expr:      `some_metric offset 30s`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"offset": 30000000000,
							"expressionPosition": [0,22]
						},
						"description": "{__name__=\"some_metric\"} offset 30s"
					}
				]
			}`,
		},
		"matrix selector": {
			expr:      `some_metric[1m]`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "MatrixSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"range": 60000000000,
							"expressionPosition": [0,15]
						},
						"description": "{__name__=\"some_metric\"}[1m0s]"
					}
				]
			}`,
		},
		"matrix selector with '@ 0'": {
			expr:      `some_metric[1m] @ 0`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "MatrixSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"range": 60000000000,
							"timestamp": 0,
							"expressionPosition": [0,19]
						},
						"description": "{__name__=\"some_metric\"}[1m0s] @ 0 (1970-01-01T00:00:00Z)"
					}
				]
			}`,
		},
		"matrix selector with '@ start()'": {
			expr:      `some_metric[1m] @ start()`,
			timeRange: rangeQuery,

			expectedPlan: `{
				"timeRange": { "start": 3000, "end": 5000, "interval": 1000 },
				"nodes": [
					{
						"type": "MatrixSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"range": 60000000000,
							"timestamp": 3000,
							"expressionPosition": [0,25]
						},
						"description": "{__name__=\"some_metric\"}[1m0s] @ 3000 (1970-01-01T00:00:03Z)"
					}
				]
			}`,
		},
		"matrix selector with '@ end()'": {
			expr:      `some_metric[1m] @ end()`,
			timeRange: rangeQuery,

			expectedPlan: `{
				"timeRange": { "start": 3000, "end": 5000, "interval": 1000 },
				"nodes": [
					{
						"type": "MatrixSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"range": 60000000000,
							"timestamp": 5000,
							"expressionPosition": [0,23]
						},
						"description": "{__name__=\"some_metric\"}[1m0s] @ 5000 (1970-01-01T00:00:05Z)"
					}
				]
			}`,
		},
		"expression with parenthesis": {
			expr:      `(some_metric)`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [1,12]
						},
						"description": "{__name__=\"some_metric\"}"
					}
				]
			}`,
		},
		"number literal": {
			expr:      `12`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "NumberLiteral",
						"details": {
							"value": 12,
							"expressionPosition": [0,2]
						},
						"description": "12"
					}
				]
			}`,
		},
		"string literal": {
			expr:      `"abc"`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "StringLiteral",
						"details": {
							"value": "abc",
							"expressionPosition": [0,5]
						},
						"description": "\"abc\""
					}
				]
			}`,
		},
		"function call with no arguments": {
			expr:      `time()`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "FunctionCall",
						"details": {
							"functionName": "time",
							"expressionPosition": [0,6]
						},
						"description": "time(...)"
					}
				]
			}`,
		},
		"function call with optional arguments omitted": {
			expr:      `year()`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "FunctionCall",
						"details": {
							"functionName": "year",
							"expressionPosition": [0,6]
						},
						"description": "year(...)"
					}
				]
			}`,
		},
		"function call with optional arguments provided": {
			expr:      `year(some_metric)`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [5,16]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "FunctionCall",
						"details": {
							"functionName": "year",
							"expressionPosition": [0,17]
						},
						"children": [0],
						"description": "year(...)",
						"childrenLabels": [""]
					}
				]
			}`,
		},
		"unary expression": {
			expr:      `-some_metric`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [1,12]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "UnaryExpression",
						"details": {
							"op": "-",
							"expressionPosition": [0,12]
						},
						"children": [0],
						"childrenLabels": [""],
						"description": "-"
					}
				]
			}`,
		},
		"basic aggregation": {
			expr:      `sum(some_metric)`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [4,15]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "AggregateExpression",
						"details": {
							"op": "sum",
							"expressionPosition": [0,16]
						},
						"children": [0],
						"childrenLabels": [""],
						"description": "sum"
					}
				]
			}`,
		},
		"aggregation with grouping": {
			expr:      `sum by (foo) (some_metric)`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [14,25]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "AggregateExpression",
						"details": {
							"op": "sum",
							"grouping": ["foo"],
							"expressionPosition": [0,26]
						},
						"children": [0],
						"childrenLabels": [""],
						"description": "sum by (foo)"
					}
				]
			}`,
		},
		"aggregation with 'without'": {
			expr:      `sum without (foo) (some_metric)`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [19,30]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "AggregateExpression",
						"details": {
							"op": "sum",
							"grouping": ["foo"],
							"without": true,
							"expressionPosition": [0,31]
						},
						"children": [0],
						"childrenLabels": [""],
						"description": "sum without (foo)"
					}
				]
			}`,
		},
		"aggregation with parameter": {
			expr:      `topk(3, some_metric)`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [8,19]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "NumberLiteral",
						"details": {
							"value": 3,
							"expressionPosition": [5,6]
						},
						"description": "3"
					},
					{
						"type": "AggregateExpression",
						"details": {
							"op": "topk",
							"expressionPosition": [0,20]
						},
						"children": [0,1],
						"childrenLabels": ["expression", "parameter"],
						"description": "topk"
					}
				]
			}`,
		},
		"binary expression with two scalars": {
			expr:      `2 + 3`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "NumberLiteral",
						"details": {
							"value": 2,
							"expressionPosition": [0,1]
						},
						"description": "2"
					},
					{
						"type": "NumberLiteral",
						"details": {
							"value": 3,
							"expressionPosition": [4,5]
						},
						"description": "3"
					},
					{
						"type": "BinaryExpression",
						"details": {
							"op": "+"
						},
						"children": [0,1],
						"childrenLabels": ["LHS", "RHS"],
						"description": "LHS + RHS"
					}
				]
			}`,
		},
		"binary expression with vector and scalar": {
			expr:      `2 * some_metric`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "NumberLiteral",
						"details": {
							"value": 2,
							"expressionPosition": [0,1]
						},
						"description": "2"
					},
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [4,15]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "BinaryExpression",
						"details": {
							"op": "*"
						},
						"children": [0,1],
						"childrenLabels": ["LHS", "RHS"],
						"description": "LHS * RHS"
					}
				]
			}`,
		},
		"binary expression with 'bool'": {
			expr:      `some_metric > bool 2`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [0,11]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "NumberLiteral",
						"details": {
							"value": 2,
							"expressionPosition": [19,20]
						},
						"description": "2"
					},
					{
						"type": "BinaryExpression",
						"details": {
							"op": ">",
							"returnBool": true
						},
						"children": [0,1],
						"childrenLabels": ["LHS", "RHS"],
						"description": "LHS > bool RHS"
					}
				]
			}`,
		},
		"binary expression with two vectors": {
			expr:      `some_metric * some_other_metric`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [0,11]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_other_metric"}
							],
							"expressionPosition": [14,31]
						},
						"description": "{__name__=\"some_other_metric\"}"
					},
					{
						"type": "BinaryExpression",
						"details": {
							"op": "*",
							"vectorMatching": {}
						},
						"children": [0,1],
						"childrenLabels": ["LHS", "RHS"],
						"description": "LHS * RHS"
					}
				]
			}`,
		},
		"binary expression with 'on'": {
			expr:      `some_metric * on (foo) some_other_metric`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [0,11]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_other_metric"}
							],
							"expressionPosition": [23,40]
						},
						"description": "{__name__=\"some_other_metric\"}"
					},
					{
						"type": "BinaryExpression",
						"details": {
							"op": "*",
							"vectorMatching": {
								"matchingLabels": ["foo"],
								"on": true
							}
						},
						"children": [0,1],
						"childrenLabels": ["LHS", "RHS"],
						"description": "LHS * on (foo) RHS"
					}
				]
			}`,
		},
		"binary expression with 'ignoring'": {
			expr:      `some_metric * ignoring (foo) some_other_metric`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [0,11]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_other_metric"}
							],
							"expressionPosition": [29,46]
						},
						"description": "{__name__=\"some_other_metric\"}"
					},
					{
						"type": "BinaryExpression",
						"details": {
							"op": "*",
							"vectorMatching": {
								"matchingLabels": ["foo"]
							}
						},
						"children": [0,1],
						"childrenLabels": ["LHS", "RHS"],
						"description": "LHS * ignoring (foo) RHS"
					}
				]
			}`,
		},
		"binary expression with 'group_left'": {
			expr:      `some_metric * ignoring (foo) group_left (bar) some_other_metric`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [0,11]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_other_metric"}
							],
							"expressionPosition": [46,63]
						},
						"description": "{__name__=\"some_other_metric\"}"
					},
					{
						"type": "BinaryExpression",
						"details": {
							"op": "*",
							"vectorMatching": {
								"card": 1,
								"matchingLabels": ["foo"],
								"include": ["bar"]
							}
						},
						"children": [0,1],
						"childrenLabels": ["LHS", "RHS"],
						"description": "LHS * ignoring (foo) group_left (bar) RHS"
					}
				]
			}`,
		},
		"subquery": {
			expr:      `(some_metric)[1m:1s]`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [1,12]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "Subquery",
						"details": {
							"range": 60000000000,
							"step":   1000000000,
							"expressionPosition": [0,20]
						},
						"children": [0],
						"description": "[1m0s:1s]",
						"childrenLabels": [""]
					}
				]
			}`,
		},
		"subquery without explicit step": {
			expr:      `(some_metric)[1m:]`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [1,12]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "Subquery",
						"details": {
							"range": 60000000000,
							"step":  23000000000,
							"expressionPosition": [0,18]
						},
						"children": [0],
						"description": "[1m0s:23s]",
						"childrenLabels": [""]
					}
				]
			}`,
		},
		"subquery with offset": {
			expr:      `(some_metric)[1m:1s] offset 3s`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [1,12]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "Subquery",
						"details": {
							"range": 60000000000,
							"step":   1000000000,
							"offset": 3000000000,
							"expressionPosition": [0,30]
						},
						"children": [0],
						"description": "[1m0s:1s] offset 3s",
						"childrenLabels": [""]
					}
				]
			}`,
		},
		"subquery with '@'": {
			expr:      `(some_metric)[1m:1s] @ 0`,
			timeRange: instantQuery,

			expectedPlan: `{
				"timeRange": { "at": 1000 },
				"nodes": [
					{
						"type": "VectorSelector",
						"details": {
							"matchers": [
								{"type":0,"name":"__name__","value":"some_metric"}
							],
							"expressionPosition": [1,12]
						},
						"description": "{__name__=\"some_metric\"}"
					},
					{
						"type": "Subquery",
						"details": {
							"range": 60000000000,
							"step":   1000000000,
							"timestamp": 0,
							"expressionPosition": [0,24]
						},
						"children": [0],
						"description": "[1m0s:1s] @ 0 (1970-01-01T00:00:00Z)",
						"childrenLabels": [""]
					}
				]
			}`,
		},
	}

	ctx := context.Background()

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			originalPlan, err := engine.NewQueryPlan(ctx, testCase.expr, testCase.timeRange, NoopPlanningObserver{})
			require.NoError(t, err)

			// Encode plan to JSON, confirm it matches what we expect
			encoded, err := engine.EncodeQueryPlan(originalPlan)
			require.NoError(t, err)

			require.JSONEq(t, testCase.expectedPlan, string(encoded), "marshalled JSON of plan does not match expected JSON payload")

			// Decode plan from JSON, confirm it matches the original plan
			decodedPlan, err := engine.DecodeQueryPlan(encoded)
			require.NoError(t, err)

			require.True(t, originalPlan.Root.Equals(decodedPlan.Root), "root node of unmarshalled plan does not equal root node of original plan")
			require.Equal(t, testCase.timeRange, decodedPlan.TimeRange) // Make sure the all the properties of the time range are correctly decoded, including those not encoded to JSON.

			// Encode plan to JSON again, confirm it still matches what we expect
			reEncodedPlan, err := jsoniter.MarshalToString(decodedPlan)
			require.NoError(t, err)

			require.JSONEq(t, testCase.expectedPlan, reEncodedPlan, "marshalled JSON of decoded plan does not match expected JSON payload")
		})
	}
}
