// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"slices"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
)

// A function without an entry keeps no labels, so it stops narrowing. Every function that returns
// an instant vector must have an entry, so that a new function can't stop narrowing by mistake.
func TestFunctionLabelBehaviours_ContainsEveryInstantVectorFunction(t *testing.T) {
	// Only plans of sharded queries use these functions, and the optimisation pass doesn't change these plans.
	shardingFunctions := []functions.Function{functions.FUNCTION_SHARDING_AVG, functions.FUNCTION_SHARDING_CONCAT}

	for function, metadata := range functions.RegisteredFunctions {
		if metadata.ReturnType != parser.ValueTypeVector || slices.Contains(shardingFunctions, function) {
			continue
		}

		require.Contains(t, functionLabelBehaviours, function, "function %s must have an entry in functionLabelBehaviours", metadata.Name)
	}

	for function := range functionLabelBehaviours {
		metadata, registered := functions.RegisteredFunctions[function]
		require.True(t, registered, "function %s in functionLabelBehaviours is not registered", function)
		require.Equal(t, parser.ValueTypeVector, metadata.ReturnType, "function %s in functionLabelBehaviours doesn't return an instant vector", metadata.Name)
	}
}
