// SPDX-License-Identifier: AGPL-3.0-only

package math

import (
	"math"
)

var log10Values []int

func init() {
	for i := 0; i < 1000; i++ {
		log10Values = append(log10Values, int(max(1, float64(int(math.Log10(float64(i)))))))
	}
}

// Log10Func returns a function that produces the Log10 value for some input, scaled for some factor, using cached values when available.
// Input values for the resulting function are rounded to an int.
func Log10Func(factor int) func(value int) int {
	return func(value int) int {
		if value < len(log10Values) {
			return factor * log10Values[value]
		}
		return factor * int(math.Log10(float64(value)))
	}
}
