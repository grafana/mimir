// SPDX-License-Identifier: AGPL-3.0-only

package costattributionmodel

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestLabels_OutputLabels(t *testing.T) {
	tc := map[string]struct {
		input    Labels
		expected []string
	}{
		"no labels": {
			input:    Labels{},
			expected: []string{},
		},
		"single": {
			input:    Labels{{Input: "team", Output: "my_team"}},
			expected: []string{"my_team"},
		},
		"regular list": {
			input: Labels{
				{Input: "team", Output: "my_team"},
				{Input: "service", Output: "my_service"},
			},
			expected: []string{"my_team", "my_service"},
		},
	}
	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			result := tt.input.OutputLabels()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestLabels_Validate(t *testing.T) {
	tc := map[string]struct {
		input       Labels
		expectedErr error
	}{
		"empty labels": {
			input:       Labels{},
			expectedErr: nil,
		},
		"single valid label": {
			input:       Labels{{Input: "team", Output: "my_team"}},
			expectedErr: nil,
		},
		"multiple valid labels with different inputs and outputs": {
			input: Labels{
				{Input: "team", Output: "my_team"},
				{Input: "service", Output: "my_service"},
			},
			expectedErr: nil,
		},
		"multiple valid labels with empty outputs": {
			input: Labels{
				{Input: "team", Output: ""},
				{Input: "service", Output: ""},
			},
			expectedErr: nil,
		},
		"duplicate input labels": {
			input: Labels{
				{Input: "team", Output: "my_team"},
				{Input: "team", Output: "other_team"},
			},
			expectedErr: nil,
		},
		"duplicate output labels": {
			input: Labels{
				{Input: "team", Output: "my_team"},
				{Input: "service", Output: "my_team"},
			},
			expectedErr: fmt.Errorf(`duplicate output label: "service:my_team"`),
		},
		"duplicate output when first label has empty output": {
			input: Labels{
				{Input: "team", Output: ""},
				{Input: "service", Output: "team"},
			},
			expectedErr: fmt.Errorf(`duplicate output label: "service:team"`),
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
