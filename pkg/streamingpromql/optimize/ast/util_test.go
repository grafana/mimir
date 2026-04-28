// SPDX-License-Identifier: AGPL-3.0-only

package ast_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func preprocessQuery(t *testing.T, expr parser.Expr) (parser.Expr, error) {
	dummyTimeRange := types.NewInstantQueryTimeRange(timestamp.Time(1000))
	return promql.PreprocessExpr(expr, timestamp.Time(dummyTimeRange.StartT), timestamp.Time(dummyTimeRange.EndT), 0)
}

func runASTOptimizationPass(t *testing.T, ctx context.Context, input string, createOptimizerFunc func(prometheus.Registerer) optimize.ASTOptimizationPass) (*prometheus.Registry, parser.Expr) {
	opts := streamingpromql.NewTestEngineOpts()
	reg := prometheus.NewPedanticRegistry()
	opts.CommonOpts.Reg = reg
	optimizer := createOptimizerFunc(opts.CommonOpts.Reg)
	dummyTimeRange := types.NewInstantQueryTimeRange(timestamp.Time(1000))
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	planner.RegisterASTOptimizationPass(optimizer)
	observer := streamingpromql.NoopPlanningObserver{}
	outputExpr, err := planner.ParseAndApplyASTOptimizationPasses(ctx, input, dummyTimeRange, observer)
	require.NoError(t, err)
	return reg, outputExpr
}

func runASTOptimizationPassWithoutMetrics(t *testing.T, ctx context.Context, input string, optimizer optimize.ASTOptimizationPass) parser.Expr {
	_, outputExpr := runASTOptimizationPass(t, ctx, input, func(prometheus.Registerer) optimize.ASTOptimizationPass {
		return optimizer
	})
	return outputExpr
}

func testASTOptimizationPassWithData(t *testing.T, loadTemplate string, testCases map[string]string) {
	numSamples := 100
	replacer := strings.NewReplacer("<num samples>", fmt.Sprintf("%d", numSamples))
	data := replacer.Replace(loadTemplate)

	const step = 20 * time.Second

	queryable := promqltest.LoadedStorage(t, data)

	// Use Prometheus's query engine to execute the queries, ensuring that our query rewriting
	// produces the same results as the original queries without any added optimizations
	// on the query engine.
	engine := promql.NewEngine(streamingpromql.NewTestEngineOpts().CommonOpts)

	ctx := user.InjectOrgID(context.Background(), "test")
	startTime := timestamp.Time(0)
	endTime := startTime.Add(time.Duration(numSamples) * time.Minute)

	makeRangeQuery := func(query string) (promql.Query, error) {
		return engine.NewRangeQuery(ctx, queryable, nil, query, startTime, endTime, step)
	}

	makeInstantQuery := func(query string) (promql.Query, error) {
		return engine.NewInstantQuery(ctx, queryable, nil, query, endTime)
	}

	for input, expected := range testCases {
		if input == expected {
			continue
		}

		t.Run(input, func(t *testing.T) {
			t.Run("range query", func(t *testing.T) {
				runAndCompare(t, input, expected, makeRangeQuery)
			})

			t.Run("instant query", func(t *testing.T) {
				runAndCompare(t, input, expected, makeInstantQuery)
			})
		})
	}
}

func runAndCompare(t *testing.T, input, expected string, makeQuery func(string) (promql.Query, error)) {
	qInput, err := makeQuery(input)
	require.NoError(t, err)
	t.Cleanup(qInput.Close)
	resInput := qInput.Exec(context.Background())
	require.NoError(t, resInput.Err)

	qRewritten, err := makeQuery(expected)
	require.NoError(t, err)
	t.Cleanup(qRewritten.Close)
	resRewritten := qRewritten.Exec(context.Background())
	require.NoError(t, resRewritten.Err)

	testutils.RequireEqualResults(t, "", resInput, resRewritten, true)
}
