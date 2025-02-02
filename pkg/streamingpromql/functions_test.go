// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"fmt"
	"strings"
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

	preSelectorFunctionArgs := map[string]string{
		"histogram_quantile": "0.1",
		"histogram_fraction": "0, 0.1",
	}

	postSelectorFunctionArgs := map[string]string{
		"clamp":         "-Inf, Inf",
		"clamp_min":     "-Inf",
		"clamp_max":     "Inf",
		"label_replace": `"__name__", "$1", "env", "(.*)"`,
	}

	for name := range instantVectorFunctionOperatorFactories {
		if name == "vector" || name == "last_over_time" {
			// This test doesn't apply to vector() because it takes a scalar parameter.
			// This test doesn't apply to last_over_time() because it doesn't drop the metric name.
			continue
		}

		t.Run(name, func(t *testing.T) {
			metricType := "float"

			if strings.HasPrefix(name, "histogram_") {
				metricType = "histogram"
			}

			selector := fmt.Sprintf(`{__name__=~"%s.*"}`, metricType)

			if isFunctionOverRangeVector(name) {
				selector = selector + "[1m]"
			}

			preSelectorArgs := preSelectorFunctionArgs[name]
			if preSelectorArgs != "" {
				preSelectorArgs = preSelectorArgs + ", "
			}

			postSelectorArgs := postSelectorFunctionArgs[name]
			if postSelectorArgs != "" {
				postSelectorArgs = ", " + postSelectorArgs
			}

			expr := fmt.Sprintf("%s(%s%s%s)", name, preSelectorArgs, selector, postSelectorArgs)

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

func isFunctionOverRangeVector(name string) bool {
	if strings.HasSuffix(name, "_over_time") {
		return true
	}

	switch name {
	case "changes", "delta", "deriv", "idelta", "increase", "irate", "rate", "resets":
		return true
	default:
		return false
	}
}
