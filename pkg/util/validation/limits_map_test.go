// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

var fakeValidator = func(_ string, v float64) error {
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

func TestLimitsMap_IsNil(t *testing.T) {
	tc := map[string]struct {
		input    LimitsMap[float64]
		expected bool
	}{

		"when the map is initialised": {
			input:    LimitsMap[float64]{data: map[string]float64{"key1": 10}},
			expected: true,
		},
		"when the map is not initialised": {
			input:    LimitsMap[float64]{data: nil},
			expected: false,
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tt.input.IsInitialized(), tt.expected)
		})
	}
}

func TestLimitsMap_SetAndString(t *testing.T) {
	tc := map[string]struct {
		input    string
		expected map[string]float64
		error    string
	}{

		"set without error": {
			input:    `{"key1":10,"key2":20}`,
			expected: map[string]float64{"key1": 10, "key2": 20},
		},
		"set with parsing error": {
			input: `{"key1": 10, "key2": 20`,
			error: "unexpected end of JSON input",
		},
		"set with validation error": {
			input: `{"key1": -10, "key2": 20}`,
			error: "value cannot be negative",
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
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
	tc := map[string]struct {
		map1     LimitsMap[float64]
		map2     LimitsMap[float64]
		expected bool
	}{
		"Equal maps with same key-value pairs": {
			map1:     LimitsMap[float64]{data: map[string]float64{"key1": 1.1, "key2": 2.2}},
			map2:     LimitsMap[float64]{data: map[string]float64{"key1": 1.1, "key2": 2.2}},
			expected: true,
		},
		"Different maps with different lengths": {
			map1:     LimitsMap[float64]{data: map[string]float64{"key1": 1.1}},
			map2:     LimitsMap[float64]{data: map[string]float64{"key1": 1.1, "key2": 2.2}},
			expected: false,
		},
		"Different maps with same keys but different values": {
			map1:     LimitsMap[float64]{data: map[string]float64{"key1": 1.1}},
			map2:     LimitsMap[float64]{data: map[string]float64{"key1": 1.2}},
			expected: false,
		},
		"Equal empty maps": {
			map1:     LimitsMap[float64]{data: map[string]float64{}},
			map2:     LimitsMap[float64]{data: map[string]float64{}},
			expected: true,
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.map1.Equal(LimitsMap[float64]{data: tt.map2.data}))
			require.Equal(t, tt.expected, cmp.Equal(tt.map1, tt.map2))
		})
	}
}

func TestLimitsMap_Clone(t *testing.T) {
	// Create an initial LimitsMap with some data.
	original := NewLimitsMap[float64](fakeValidator)
	original.data["limit1"] = 1.0
	original.data["limit2"] = 2.0

	// Clone the original LimitsMap.
	cloned := original.Clone()

	// Check that the cloned LimitsMap is equal to the original.
	require.True(t, original.Equal(cloned), "expected cloned LimitsMap to be different from original")

	// Modify the original LimitsMap and ensure the cloned map is not affected.
	original.data["limit1"] = 10.0
	require.False(t, cloned.data["limit1"] == 10.0, "expected cloned LimitsMap to be unaffected by changes to original")

	// Modify the cloned LimitsMap and ensure the original map is not affected.
	cloned.data["limit3"] = 3.0
	_, exists := original.data["limit3"]
	require.False(t, exists, "expected original LimitsMap to be unaffected by changes to cloned")
}

func TestLimitsMap_updateMap(t *testing.T) {
	initialData := map[string]float64{"a": 1.0, "b": 2.0}
	updateData := map[string]float64{"a": 3.0, "b": -3.0, "c": 5.0}

	limitsMap := LimitsMap[float64]{data: initialData, validator: fakeValidator}

	err := limitsMap.updateMap(updateData)
	require.Error(t, err)

	// Verify that no partial updates were applied.
	// Because maps in Go are accessed in random order, there's a chance that the validation will fail on the first invalid element of the map thus not asserting partial updates.
	expectedData := map[string]float64{"a": 1.0, "b": 2.0}
	require.Equal(t, expectedData, limitsMap.data)
}
