// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
)

func TestRegisterInstantVectorFunctionOperatorFactory(t *testing.T) {
	// Register an already existing function
	err := RegisterInstantVectorFunctionOperatorFactory("acos", LabelManipulationFunctionOperatorFactory("acos", functions.DropSeriesName))
	require.Error(t, err)
	require.Equal(t, "function 'acos' has already been registered", err.Error())

	// Register a new function
	newFunc := LabelManipulationFunctionOperatorFactory("new_function", functions.DropSeriesName)
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
