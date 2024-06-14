// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

var fakeValidator = func(k string, v float64) error {
	if v < 0 {
		return errors.New("value cannot be negative")
	}
	return nil
}

func TestNewLimitsMap(t *testing.T) {
	lm := NewLimitsMap(fakeValidator)
	lm.data["key1"] = 10
	require.Len(t, lm.data, 1)
}

func TestLimitsMap_SetAndString(t *testing.T) {
	tc := []struct {
		name     string
		input    string
		expected map[string]float64
		error    string
	}{
		{
			name:     "set without error",
			input:    `{"key1":10,"key2":20}`,
			expected: map[string]float64{"key1": 10, "key2": 20},
		},
		{
			name:  "set with parsing error",
			input: `{"key1": 10, "key2": 20`,
			error: "unexpected end of JSON input",
		},
		{
			name:  "set with validation error",
			input: `{"key1": -10, "key2": 20}`,
			error: "value cannot be negative",
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			lm := NewLimitsMap(fakeValidator)
			err := lm.Set(tt.input)
			if tt.error != "" {
				require.Error(t, err)
				require.Equal(t, tt.error, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, lm.data)
				require.Equal(t, tt.input, lm.String())
			}
		})
	}
}

func TestLimitsMap_UnmarshalYAML(t *testing.T) {
	tc := []struct {
		name     string
		input    string
		expected map[string]float64
		error    string
	}{
		{
			name: "unmarshal without error",
			input: `
key1: 10
key2: 20
`,
			expected: map[string]float64{"key1": 10, "key2": 20},
		},
		{
			name: "unmarshal with validation error",
			input: `
key1: -10
key2: 20
`,
			error: "value cannot be negative",
		},
		{
			name: "unmarshal with parsing error",
			input: `
key1: 10
key2: 20
			key3: 30
`,
			error: "yaml: line 3: found a tab character that violates indentation",
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			lm := NewLimitsMap(fakeValidator)
			err := yaml.Unmarshal([]byte(tt.input), &lm)
			if tt.error != "" {
				require.Error(t, err)
				require.Equal(t, tt.error, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, lm.data)
			}
		})
	}
}

func TestLimitsMap_MarshalYAML(t *testing.T) {
	lm := NewLimitsMap(fakeValidator)
	lm.data["key1"] = 10
	lm.data["key2"] = 20

	out, err := yaml.Marshal(&lm)
	require.NoError(t, err)
	require.Equal(t, "key1: 10\nkey2: 20\n", string(out))
}
