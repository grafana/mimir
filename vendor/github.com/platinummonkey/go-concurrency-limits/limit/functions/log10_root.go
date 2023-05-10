package functions

import (
	"math"
)

var log10RootLookup []int

// Log10RootFunction is a specialized utility function used by limiters to calculate thresholds using log10 of the
// current limit.  Here we pre-compute the log10 root of numbers up to 1000 (or more) because the log10 root
// operation can be slow.
func Log10RootFunction(baseline int) func(estimatedLimit int) int {
	return func(estimatedLimit int) int {
		if estimatedLimit < len(log10RootLookup) {
			return baseline + log10RootLookup[estimatedLimit]
		}
		return baseline + int(math.Log10(float64(estimatedLimit)))
	}
}

// Log10RootFloatFunction is a specialized utility function used by limiters to calculate thresholds using log10 of the
// current limit.  Here we pre-compute the log10 root of numbers up to 1000 (or more) because the log10 root
// operation can be slow.
func Log10RootFloatFunction(baseline float64) func(estimatedLimit float64) float64 {
	return func(estimatedLimit float64) float64 {
		if int(estimatedLimit) < len(log10RootLookup) {
			return baseline + float64(log10RootLookup[int(estimatedLimit)])
		}
		return baseline + math.Log10(estimatedLimit)
	}
}
