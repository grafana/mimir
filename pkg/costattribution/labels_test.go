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
			expected: []Label{{input: "team", output: ""}},
		},
		"regular list": {
			input:    []string{"team", "service"},
			expected: []Label{{input: "team", output: ""}, {input: "service", output: ""}},
		},
		"list with renames": {
			input:    []string{"eng_team=team", "eng_service=service"},
			expected: []Label{{input: "team", output: "eng_team"}, {input: "service", output: "eng_service"}},
		},
		"list with partial renames": {
			input:    []string{"eng_team=team", "service"},
			expected: []Label{{input: "team", output: "eng_team"}, {input: "service", output: ""}},
		},
		"output=input": {
			input:    []string{"team=team"},
			expected: []Label{{input: "team", output: "team"}},
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			result := ParseCostAttributionLabels(tt.input)
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
			input:    Label{input: "team", output: ""},
			expected: "team",
		},
		"non-empty output": {
			input:    Label{input: "team", output: "eng_team"},
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
