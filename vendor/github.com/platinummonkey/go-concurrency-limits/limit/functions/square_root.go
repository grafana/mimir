package functions

import (
	"math"
)

var sqrtRootLookup []int

// SqrtRootFunction is a specialized utility function used by limiters to calculate thresholds using square root of the
// current limit.  Here we pre-compute the square root of numbers up to 1000 (or more) because the square root
// operation can be slow.
func SqrtRootFunction(baseline int) func(estimatedLimit int) int {
	return func(estimatedLimit int) int {
		if estimatedLimit < len(sqrtRootLookup) {
			return max(baseline, sqrtRootLookup[estimatedLimit])
		}
		return max(baseline, int(math.Sqrt(float64(estimatedLimit))))
	}
}
