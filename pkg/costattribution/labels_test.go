// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

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
		"list with renames": {
			input:    []string{"eng_team=team", "eng_service=service"},
			expected: []Label{{Input: "team", Output: "eng_team"}, {Input: "service", Output: "eng_service"}},
		},
		"list with partial renames": {
			input:    []string{"eng_team=team", "service"},
			expected: []Label{{Input: "team", Output: "eng_team"}, {Input: "service", Output: ""}},
		},
		"output=input": {
			input:    []string{"team=team"},
			expected: []Label{{Input: "team", Output: "team"}},
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			result := parseCostAttributionLabels(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestLabel_outputLabel(t *testing.T) {
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
			result := tt.input.outputLabel()
			require.Equal(t, tt.expected, result)
		})
	}
}
