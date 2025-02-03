// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
)

func TestRegisterInstantVectorFunctionOperatorFactory(t *testing.T) {
	// Register an already existing function
	err := RegisterInstantVectorFunctionOperatorFactory("acos", InstantVectorLabelManipulationFunctionOperatorFactory("acos", functions.DropSeriesName))
	require.Error(t, err)
	require.Equal(t, "function 'acos' has already been registered", err.Error())

	// Register a new function
	newFunc := InstantVectorLabelManipulationFunctionOperatorFactory("new_function", functions.DropSeriesName)
	err = RegisterInstantVectorFunctionOperatorFactory("new_function", newFunc)
	require.NoError(t, err)
	require.Contains(t, instantVectorFunctionOperatorFactories, "new_function")

	// Register existing function we registered previously
	err = RegisterInstantVectorFunctionOperatorFactory("new_function", newFunc)
	require.Error(t, err)
	require.Equal(t, "function 'new_function' has already been registered", err.Error())

	// Cleanup changes to instantVectorFunctionOperatorFactories
	delete(instantVectorFunctionOperatorFactories, "new_function")
}

func TestRegisterScalarFunctionOperatorFactory(t *testing.T) {
	// Register an already existing function
	err := RegisterScalarFunctionOperatorFactory("pi", piOperatorFactory)
	require.Error(t, err)
	require.Equal(t, "function 'pi' has already been registered", err.Error())

	// Register a new function
	err = RegisterScalarFunctionOperatorFactory("new_function", piOperatorFactory)
	require.NoError(t, err)
	require.Contains(t, scalarFunctionOperatorFactories, "new_function")

	// Register existing function we registered previously
	err = RegisterScalarFunctionOperatorFactory("new_function", piOperatorFactory)
	require.Error(t, err)
	require.Equal(t, "function 'new_function' has already been registered", err.Error())

	// Cleanup changes to instantVectorFunctionOperatorFactories
	delete(scalarFunctionOperatorFactories, "new_function")
}

// This test ensures that all functions correctly merge series after dropping the metric name.
func TestFunctionDeduplicateAndMerge(t *testing.T) {
	data := `
		load 30s
			float_a{env="prod"}      _   0 1                       _ _   _ _   _ _   _ _   _ _   _ _
			float_b{env="prod"}      _   _ _                       _ _   _ _   _ _   _ _   _ _   8 9
			histogram_a{env="prod"}  _   {{count:0}} {{count:1}}   _ _   _ _   _ _   _ _   _ _   _ _
			histogram_b{env="prod"}  _   _ _                       _ _   _ _   _ _   _ _   _ _   {{count:8}} {{count:9}}
	`

	storage := promqltest.LoadedStorage(t, data)
	opts := NewTestEngineOpts()
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	ctx := context.Background()
	start := timestamp.Time(0).Add(time.Minute)
	end := timestamp.Time(0).Add(7 * time.Minute)
	step := time.Minute

	expressions := map[string]string{
		// Please keep this list sorted alphabetically.
		"abs":                `abs({__name__=~"float.*"})`,
		"acos":               `acos({__name__=~"float.*"})`,
		"acosh":              `acosh({__name__=~"float.*"})`,
		"asin":               `asin({__name__=~"float.*"})`,
		"asinh":              `asinh({__name__=~"float.*"})`,
		"atan":               `atan({__name__=~"float.*"})`,
		"atanh":              `atanh({__name__=~"float.*"})`,
		"avg_over_time":      `avg_over_time({__name__=~"float.*"}[1m])`,
		"ceil":               `ceil({__name__=~"float.*"})`,
		"changes":            `changes({__name__=~"float.*"}[1m])`,
		"clamp":              `clamp({__name__=~"float.*"}, -Inf, Inf)`,
		"clamp_max":          `clamp_max({__name__=~"float.*"}, -Inf)`,
		"clamp_min":          `clamp_min({__name__=~"float.*"}, Inf)`,
		"cos":                `cos({__name__=~"float.*"})`,
		"cosh":               `cosh({__name__=~"float.*"})`,
		"count_over_time":    `count_over_time({__name__=~"float.*"}[1m])`,
		"deg":                `deg({__name__=~"float.*"})`,
		"delta":              `delta({__name__=~"float.*"}[1m])`,
		"deriv":              `deriv({__name__=~"float.*"}[1m])`,
		"exp":                `exp({__name__=~"float.*"})`,
		"floor":              `floor({__name__=~"float.*"})`,
		"histogram_avg":      `histogram_avg({__name__=~"histogram.*"})`,
		"histogram_count":    `histogram_count({__name__=~"histogram.*"})`,
		"histogram_fraction": `histogram_fraction(0, 0.1, {__name__=~"histogram.*"})`,
		"histogram_quantile": `histogram_quantile(0.1, {__name__=~"histogram.*"})`,
		"histogram_stddev":   `histogram_stddev({__name__=~"histogram.*"})`,
		"histogram_stdvar":   `histogram_stdvar({__name__=~"histogram.*"})`,
		"histogram_sum":      `histogram_sum({__name__=~"histogram.*"})`,
		"idelta":             `idelta({__name__=~"float.*"}[1m])`,
		"increase":           `increase({__name__=~"float.*"}[1m])`,
		"irate":              `irate({__name__=~"float.*"}[1m])`,
		"label_replace":      `label_replace({__name__=~"float.*"}, "__name__", "$1", "env", "(.*)")`,
		"last_over_time":     `<skip>`, // last_over_time() doesn't drop the metric name, so this test doesn't apply.
		"ln":                 `ln({__name__=~"float.*"})`,
		"log10":              `log10({__name__=~"float.*"})`,
		"log2":               `log2({__name__=~"float.*"})`,
		"max_over_time":      `max_over_time({__name__=~"float.*"}[1m])`,
		"min_over_time":      `min_over_time({__name__=~"float.*"}[1m])`,
		"present_over_time":  `present_over_time({__name__=~"float.*"}[1m])`,
		"rad":                `rad({__name__=~"float.*"})`,
		"rate":               `rate({__name__=~"float.*"}[1m])`,
		"resets":             `resets({__name__=~"float.*"}[1m])`,
		"round":              `round({__name__=~"float.*"})`,
		"sgn":                `sgn({__name__=~"float.*"})`,
		"sin":                `sin({__name__=~"float.*"})`,
		"sinh":               `sinh({__name__=~"float.*"})`,
		"sqrt":               `sqrt({__name__=~"float.*"})`,
		"sum_over_time":      `sum_over_time({__name__=~"float.*"}[1m])`,
		"tan":                `tan({__name__=~"float.*"})`,
		"tanh":               `tanh({__name__=~"float.*"})`,
		"vector":             `<skip>`, // vector() takes a scalar, so this test doesn't apply.
	}

	for name := range instantVectorFunctionOperatorFactories {
		expr, haveExpression := expressions[name]

		if expr == "<skip>" {
			continue
		}

		t.Run(name, func(t *testing.T) {
			require.Truef(t, haveExpression, "no expression defined for '%s' function", name)

			q, err := engine.NewRangeQuery(ctx, storage, nil, expr, start, end, step)
			require.NoError(t, err)
			defer q.Close()

			mimirResult := q.Exec(ctx)
			require.NoError(t, mimirResult.Err)
			m, err := mimirResult.Matrix()
			require.NoError(t, err)

			require.Len(t, m, 1, "expected a single series")
		})
	}
}
