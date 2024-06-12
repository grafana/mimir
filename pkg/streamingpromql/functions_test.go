// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
)

func TestRegisterInstantVectorFunctionOperator(t *testing.T) {
	// Register an already existing function
	err := RegisterInstantVectorFunctionOperator("acos", LabelManipulationFunctionOperator("acos", functions.DropSeriesName))
	require.Error(t, err)
	require.Equal(t, "function 'acos' has already been registered", err.Error())

	// Register a new function
	newFunc := LabelManipulationFunctionOperator("new_function", functions.DropSeriesName)
	err = RegisterInstantVectorFunctionOperator("new_function", newFunc)
	require.NoError(t, err)
	require.Contains(t, instantVectorFunctionOperatorFactories, "new_function")
}
