// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegisterInstantVectorFunctionOperatorFactory(t *testing.T) {
	// Register an already existing function
	err := RegisterInstantVectorFunctionOperatorFactory("acos", InstantVectorLabelManipulationFunctionOperatorFactory("acos", DropSeriesName))
	require.Error(t, err)
	require.Equal(t, "function 'acos' has already been registered", err.Error())

	// Register a new function
	newFunc := InstantVectorLabelManipulationFunctionOperatorFactory("new_function", DropSeriesName)
	err = RegisterInstantVectorFunctionOperatorFactory("new_function", newFunc)
	require.NoError(t, err)
	require.Contains(t, InstantVectorFunctionOperatorFactories, "new_function")

	// Register existing function we registered previously
	err = RegisterInstantVectorFunctionOperatorFactory("new_function", newFunc)
	require.Error(t, err)
	require.Equal(t, "function 'new_function' has already been registered", err.Error())

	// Cleanup changes to instantVectorFunctionOperatorFactories
	delete(InstantVectorFunctionOperatorFactories, "new_function")
}

func TestRegisterScalarFunctionOperatorFactory(t *testing.T) {
	// Register an already existing function
	err := RegisterScalarFunctionOperatorFactory("pi", piOperatorFactory)
	require.Error(t, err)
	require.Equal(t, "function 'pi' has already been registered", err.Error())

	// Register a new function
	err = RegisterScalarFunctionOperatorFactory("new_function", piOperatorFactory)
	require.NoError(t, err)
	require.Contains(t, ScalarFunctionOperatorFactories, "new_function")

	// Register existing function we registered previously
	err = RegisterScalarFunctionOperatorFactory("new_function", piOperatorFactory)
	require.Error(t, err)
	require.Equal(t, "function 'new_function' has already been registered", err.Error())

	// Cleanup changes to instantVectorFunctionOperatorFactories
	delete(ScalarFunctionOperatorFactories, "new_function")
}
