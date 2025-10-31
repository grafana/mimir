// SPDX-License-Identifier: AGPL-3.0-only

package costattributionmodel

import (
	"fmt"
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

func TestLabel_Validate(t *testing.T) {
	tc := map[string]struct {
		input       Label
		expectedErr error
	}{
		"valid label with input and output": {
			input:       Label{Input: "team", Output: "eng_team"},
			expectedErr: nil,
		},
		"valid label with input only": {
			input:       Label{Input: "team", Output: ""},
			expectedErr: nil,
		},
		"valid label with underscores": {
			input:       Label{Input: "team_name", Output: "eng_team_name"},
			expectedErr: nil,
		},
		"valid label with numbers": {
			input:       Label{Input: "team1", Output: "team2"},
			expectedErr: nil,
		},
		"input label with reserved prefix is valid": {
			input:       Label{Input: "__team", Output: "team"},
			expectedErr: nil,
		},
		"invalid output label with reserved prefix": {
			input:       Label{Input: "team", Output: "__team"},
			expectedErr: fmt.Errorf(`invalid cost attribution output label: "team:__team"`),
		},
		"empty input label": {
			input:       Label{Input: "", Output: "team"},
			expectedErr: fmt.Errorf(`cost attribution input label must not be empty: ":team"`),
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			err := tt.input.Validate()
			if tt.expectedErr != nil {
				require.EqualError(t, err, tt.expectedErr.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFromCostAttributionLabelsToOutputLabels(t *testing.T) {
	tc := map[string]struct {
		input    []Label
		expected []string
	}{
		"no labels": {
			input:    []Label{},
			expected: []string{},
		},
		"single": {
			input:    []Label{{Input: "team", Output: "my_team"}},
			expected: []string{"my_team"},
		},
		"regular list": {
			input: []Label{
				{Input: "team", Output: "my_team"},
				{Input: "service", Output: "my_service"},
			},
			expected: []string{"my_team", "my_service"},
		},
	}
	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			result := FromCostAttributionLabelsToOutputLabels(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
