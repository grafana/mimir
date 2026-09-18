// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseExplainValues(t *testing.T) {
	t.Run("nil slice", func(t *testing.T) {
		require.Equal(t, []ExplainValue(nil), ParseExplainValues(nil))
	})

	t.Run("empty slice", func(t *testing.T) {
		require.Equal(t, []ExplainValue(nil), ParseExplainValues([]string{}))
	})

	t.Run("mixed valid and invalid values", func(t *testing.T) {
		require.Equal(t, []ExplainValue{ExplainValueCost}, ParseExplainValues([]string{"bogus", "cost"}))
	})

	t.Run("valid value mixed case", func(t *testing.T) {
		require.Equal(t, []ExplainValue{ExplainValueCost}, ParseExplainValues([]string{"Cost"}))
	})
}
