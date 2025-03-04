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
						}
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
						}
					}
				]
			}`,
		},
	}

	ctx := context.Background()

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			originalPlan, err := engine.NewQueryPlan(ctx, testCase.expr, testCase.timeRange)
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
