// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions_internal_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package floats

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKahanSumInc(t *testing.T) {
	testCases := map[string]struct {
		first    float64
		second   float64
		expected float64
	}{
		"+Inf + anything = +Inf": {
			first:    math.Inf(1),
			second:   2.0,
			expected: math.Inf(1),
		},
		"-Inf + anything = -Inf": {
			first:    math.Inf(-1),
			second:   2.0,
			expected: math.Inf(-1),
		},
		"+Inf + -Inf = NaN": {
			first:    math.Inf(1),
			second:   math.Inf(-1),
			expected: math.NaN(),
		},
		"NaN + anything = NaN": {
			first:    math.NaN(),
			second:   2,
			expected: math.NaN(),
		},
		"NaN + Inf = NaN": {
			first:    math.NaN(),
			second:   math.Inf(1),
			expected: math.NaN(),
		},
		"NaN + -Inf = NaN": {
			first:    math.NaN(),
			second:   math.Inf(-1),
			expected: math.NaN(),
		},
	}

	runTest := func(t *testing.T, a, b, expected float64) {
		t.Run(fmt.Sprintf("%v + %v = %v", a, b, expected), func(t *testing.T) {
			sum, c := KahanSumInc(b, a, 0)
			result := sum + c

			if math.IsNaN(expected) {
				require.Truef(t, math.IsNaN(result), "expected result to be NaN, but got %v (from %v + %v)", result, sum, c)
			} else {
				require.Equalf(t, expected, result, "expected result to be %v, but got %v (from %v + %v)", expected, result, sum, c)
			}
		})
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			runTest(t, testCase.first, testCase.second, testCase.expected)
			runTest(t, testCase.second, testCase.first, testCase.expected)
		})
	}
}
