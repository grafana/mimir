// SPDX-License-Identifier: AGPL-3.0-only

package costattributionmodel

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCostAttributionLabels(t *testing.T) {
	tc := map[string]struct {
		input    []string
		expected []Label
	}{
		"no labels": {
			input:    []string{},
			expected: []Label{},
		},
		"single": {
			input:    []string{"team"},
			expected: []Label{{Input: "team", Output: ""}},
		},
		"regular list": {
			input:    []string{"team", "service"},
			expected: []Label{{Input: "team", Output: ""}, {Input: "service", Output: ""}},
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			result := ParseCostAttributionLabels(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestLabel_OutputLabel(t *testing.T) {
	tc := map[string]struct {
		input    Label
		expected string
	}{
		"empty output": {
			input:    Label{Input: "team", Output: ""},
			expected: "team",
		},
		"non-empty output": {
			input:    Label{Input: "team", Output: "eng_team"},
			expected: "eng_team",
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			result := tt.input.OutputLabel()
			require.Equal(t, tt.expected, result)
		})
	}
}
