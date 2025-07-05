// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestRegisterFunction(t *testing.T) {
	// Try to re-register an existing function
	err := RegisterFunction(FUNCTION_ACOS, "acos", parser.ValueTypeVector, InstantVectorLabelManipulationFunctionOperatorFactory("acos", DropSeriesName))
	require.EqualError(t, err, "function with ID 4 has already been registered")

	newFunc := InstantVectorLabelManipulationFunctionOperatorFactory("new_function", DropSeriesName)
	f := Function(1234)

	// Register a new function
	err = RegisterFunction(f, "new_function", parser.ValueTypeVector, newFunc)
	require.NoError(t, err)
	require.Contains(t, RegisteredFunctions, f)
	require.Equal(t, f.PromQLName(), "new_function")

	// Try to re-register function we registered previously
	err = RegisterFunction(f, "new_function", parser.ValueTypeVector, newFunc)
	require.EqualError(t, err, "function with ID 1234 has already been registered")

	// Try to re-register function we registered previously with different name
	err = RegisterFunction(f, "another_new_function", parser.ValueTypeVector, newFunc)
	require.EqualError(t, err, "function with ID 1234 has already been registered")

	// Try to re-register function with different ID but same name
	err = RegisterFunction(Function(5678), "new_function", parser.ValueTypeVector, newFunc)
	require.EqualError(t, err, "function with name 'new_function' has already been registered with a different ID: 1234")

	// Cleanup changes to instantVectorFunctionOperatorFactories
	delete(RegisteredFunctions, f)
	delete(promQLNamesToFunctions, "new_function")
}
