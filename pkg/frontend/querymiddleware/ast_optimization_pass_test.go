// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast/testdata"
)

func TestASTOptPassPruneToggles(t *testing.T) {
	testASTOptimizationPassWithData(t, `
		load 1m
			foo{series="1"} 0+1x<num samples>
			foo{series="2"} 0+2x<num samples>
			foo{series="3"} 0+3x<num samples>
			foo{series="4"} 0+4x<num samples>
			foo{series="5"} 0+5x<num samples>
			bar{series="1"} 0+6x<num samples>
			bar{series="2"} 0+7x<num samples>
			bar{series="3"} 0+8x<num samples>
			bar{series="4"} 0+9x<num samples>
			bar{series="5"} 0+10x<num samples>
	`, testdata.TestCasesPruneToggles)
}

// Using querymiddleware code to test streamingpromql/optimize/ast (doing it there instead
// would result in an import cycle).
func testASTOptimizationPassWithData(t *testing.T, loadTemplate string, testCases map[string]string) {
	numSamples := 100
	replacer := strings.NewReplacer("<num samples>", fmt.Sprintf("%d", numSamples))
	data := replacer.Replace(loadTemplate)

	const step = 20 * time.Second

	queryable := promqltest.LoadedStorage(t, data)

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:               promslog.NewNopLogger(),
		Reg:                  nil,
		MaxSamples:           100000000,
		Timeout:              time.Minute,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	})

	downstream := &downstreamHandler{
		engine:    engine,
		queryable: queryable,
	}

	for input, expected := range testCases {
		if input == expected {
			continue
		}
		t.Run(input, func(t *testing.T) {
			inputExpr, err := parser.ParseExpr(input)
			require.NoError(t, err)
			inputReq := NewPrometheusRangeQueryRequest(
				"/query_range",
				nil,
				0,
				int64(numSamples)*time.Minute.Milliseconds(),
				step.Milliseconds(),
				0,
				inputExpr,
				Options{},
				nil,
				"all",
			)

			injectedContext := user.InjectOrgID(context.Background(), "test")

			// Run the original input query.
			expectedRes, err := downstream.Do(injectedContext, inputReq)
			require.Nil(t, err)
			expectedPrometheusResponse, ok := expectedRes.GetPrometheusResponse()
			require.True(t, ok)

			if len(expectedPrometheusResponse.Data.Result) > 0 {
				requireValidSamples(t, expectedPrometheusResponse.Data.Result)
			}

			rewrittenExpr, err := parser.ParseExpr(expected)
			require.NoError(t, err)
			rewrittenReq := NewPrometheusRangeQueryRequest(
				"/query_range",
				nil,
				0,
				int64(numSamples)*time.Minute.Milliseconds(),
				step.Milliseconds(),
				0,
				rewrittenExpr,
				Options{},
				nil,
				"all",
			)

			// Run the rewritten query.
			rewrittenRes, err := downstream.Do(injectedContext, rewrittenReq)
			require.Nil(t, err)
			rewrittenPromethusResponse, ok := rewrittenRes.GetPrometheusResponse()
			require.True(t, ok)

			if len(rewrittenPromethusResponse.Data.Result) > 0 {
				requireValidSamples(t, rewrittenPromethusResponse.Data.Result)
			}

			// Ensure the results are approximately equal.
			approximatelyEqualsSamples(t, expectedPrometheusResponse, rewrittenPromethusResponse)
		})
	}
}
