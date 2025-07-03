// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegisterInstantVectorFunctionOperatorFactory(t *testing.T) {
	// Try to re-register an existing function
	err := RegisterInstantVectorFunctionOperatorFactory(FUNCTION_ACOS, "acos", InstantVectorLabelManipulationFunctionOperatorFactory("acos", DropSeriesName))
	require.EqualError(t, err, "function 'acos' (4) has already been registered")

	newFunc := InstantVectorLabelManipulationFunctionOperatorFactory("new_function", DropSeriesName)
	f := Function(1234)

	// Try to register with a name that clashes with an existing scalar function
	err = RegisterInstantVectorFunctionOperatorFactory(f, "pi", newFunc)
	require.EqualError(t, err, "function with name 'pi' has already been registered as a function with ID 71 returning a different type of result")

	// Register a new function
	err = RegisterInstantVectorFunctionOperatorFactory(f, "new_function", newFunc)
	require.NoError(t, err)
	require.Contains(t, InstantVectorFunctionOperatorFactories, f)
	require.Equal(t, f.PromQLName(), "new_function")

	// Try to re-register function we registered previously
	err = RegisterInstantVectorFunctionOperatorFactory(f, "new_function", newFunc)
	require.EqualError(t, err, "function 'new_function' (1234) has already been registered")

	// Try to re-register function we registered previously with different name
	err = RegisterInstantVectorFunctionOperatorFactory(f, "another_new_function", newFunc)
	require.EqualError(t, err, "function with ID 1234 has already been registered with a different name: new_function")

	// Cleanup changes to instantVectorFunctionOperatorFactories
	delete(InstantVectorFunctionOperatorFactories, f)
	delete(promQLNamesToFunctions, "new_function")
	delete(functionsToPromQLNames, f)
}

func TestRegisterScalarFunctionOperatorFactory(t *testing.T) {
	// Try to re-register an existing function
	err := RegisterScalarFunctionOperatorFactory(FUNCTION_PI, "pi", piOperatorFactory)
	require.EqualError(t, err, "function 'pi' (71) has already been registered")

	f := Function(1234)

	// Try to register with a name that clashes with an existing instant vector function
	err = RegisterScalarFunctionOperatorFactory(f, "acos", piOperatorFactory)
	require.EqualError(t, err, "function with name 'acos' has already been registered as a function with ID 4 returning a different type of result")

	// Register a new function
	err = RegisterScalarFunctionOperatorFactory(f, "new_function", piOperatorFactory)
	require.NoError(t, err)
	require.Contains(t, ScalarFunctionOperatorFactories, f)
	require.Equal(t, f.PromQLName(), "new_function")

	// Try to re-register function we registered previously
	err = RegisterScalarFunctionOperatorFactory(f, "new_function", piOperatorFactory)
	require.EqualError(t, err, "function 'new_function' (1234) has already been registered")

	// Try to re-register function we registered previously with different name
	err = RegisterScalarFunctionOperatorFactory(f, "another_new_function", piOperatorFactory)
	require.EqualError(t, err, "function with ID 1234 has already been registered with a different name: new_function")

	// Cleanup changes to instantVectorFunctionOperatorFactories
	delete(ScalarFunctionOperatorFactories, f)
	delete(promQLNamesToFunctions, "new_function")
	delete(functionsToPromQLNames, f)
}
