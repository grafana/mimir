// SPDX-License-Identifier: AGPL-3.0-only

package math

import (
	"math"
	"math/bits"
)

// Digits returns the number of digits required to represent value with base 10.
func Digits[T int32 | int64](value T) int {
	if value == 0 {
		return 1
	}

	// Handle negative numbers by considering the minus sign.
	num := 0
	if value < 0 {
		num++
		value = -value
	}

	num += int(math.Log10(float64(value))) + 1
	return num
}

// EstimatedDigitsInt32 returns the estimated number of digits required to represent value with base 10.
// The returned estimation should always be >= that the actual number of digits, but never lower.
func EstimatedDigitsInt32(value int32) int {
	if value == 0 {
		return 1
	}

	// Handle negative numbers by considering the minus sign.
	sign := 0
	if value < 0 {
		value = -value
		sign++
	}

	// Each decimal digit corresponds to approximately log2(10) bits, which is around 3.32.
	return sign + int(math.Ceil(float64(bits.Len32(uint32(value)))/3.3))
}

// EstimatedDigitsInt64 returns the estimated number of digits required to represent value with base 10.
// The returned estimation should always be >= that the actual number of digits, but never lower.
func EstimatedDigitsInt64(value int64) int {
	if value == 0 {
		return 1
	}

	// Handle negative numbers by considering the minus sign.
	sign := 0
	if value < 0 {
		value = -value
		sign++
	}

	// Each decimal digit corresponds to approximately log2(10) bits, which is around 3.32.
	return sign + int(math.Ceil(float64(bits.Len64(uint64(value)))/3.3))
}
