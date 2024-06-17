// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
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

func TestLimitsMap_Equal(t *testing.T) {
	tc := []struct {
		name     string
		map1     LimitsMap[float64]
		map2     LimitsMap[float64]
		expected bool
	}{
		{
			name:     "Equal maps with same key-value pairs",
			map1:     LimitsMap[float64]{data: map[string]float64{"key1": 1.1, "key2": 2.2}},
			map2:     LimitsMap[float64]{data: map[string]float64{"key1": 1.1, "key2": 2.2}},
			expected: true,
		},
		{
			name:     "Different maps with different lengths",
			map1:     LimitsMap[float64]{data: map[string]float64{"key1": 1.1}},
			map2:     LimitsMap[float64]{data: map[string]float64{"key1": 1.1, "key2": 2.2}},
			expected: false,
		},
		{
			name:     "Different maps with same keys but different values",
			map1:     LimitsMap[float64]{data: map[string]float64{"key1": 1.1}},
			map2:     LimitsMap[float64]{data: map[string]float64{"key1": 1.2}},
			expected: false,
		},
		{
			name:     "Equal empty maps",
			map1:     LimitsMap[float64]{data: map[string]float64{}},
			map2:     LimitsMap[float64]{data: map[string]float64{}},
			expected: true,
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.map1.Equal(LimitsMap[float64]{data: tt.map2.data}))
			require.Equal(t, tt.expected, cmp.Equal(tt.map1, tt.map2))
		})
	}
}
