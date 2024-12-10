// SPDX-License-Identifier: AGPL-3.0-only

package math

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDigits(t *testing.T) {
	assert.Equal(t, 1, Digits[int64](0))
	assert.Equal(t, 1, Digits[int64](1))
	assert.Equal(t, 2, Digits[int64](-1))
	assert.Equal(t, 2, Digits[int64](10))
	assert.Equal(t, 3, Digits[int64](-10))
	assert.Equal(t, 6, Digits[int64](123456))
	assert.Equal(t, 7, Digits[int64](-123456))
	assert.Equal(t, len(strconv.Itoa(math.MaxInt)), Digits[int64](math.MaxInt))
	assert.Equal(t, len(strconv.Itoa(math.MinInt+1)), Digits[int64](math.MinInt+1))
}

func TestEstimatedDigitsInt32(t *testing.T) {
	// Small numbers.
	for i := int32(-10000); i < 10000; i++ {
		assert.GreaterOrEqual(t, EstimatedDigitsInt32(i), Digits(i))
		assert.LessOrEqual(t, EstimatedDigitsInt32(i), Digits(i)+1)
	}

	// Large negative numbers.
	for i := int32(-10000); i > math.MinInt32/2; i *= 2 {
		assert.GreaterOrEqual(t, EstimatedDigitsInt32(i), Digits(i))
		assert.LessOrEqual(t, EstimatedDigitsInt32(i), Digits(i)+1)
	}

	// Large positive numbers.
	for i := int32(10000); i < math.MaxInt32/2; i *= 2 {
		assert.GreaterOrEqual(t, EstimatedDigitsInt32(i), Digits(i))
		assert.LessOrEqual(t, EstimatedDigitsInt32(i), Digits(i)+1)
	}
}

func TestEstimatedDigitsInt64(t *testing.T) {
	// Small numbers.
	for i := int64(-10000); i < 10000; i++ {
		assert.GreaterOrEqual(t, EstimatedDigitsInt64(i), Digits(i))
		assert.LessOrEqual(t, EstimatedDigitsInt64(i), Digits(i)+1)
	}

	// Large negative numbers.
	for i := int64(-10000); i > math.MinInt64/2; i *= 2 {
		assert.GreaterOrEqual(t, EstimatedDigitsInt64(i), Digits(i))
		assert.LessOrEqual(t, EstimatedDigitsInt64(i), Digits(i)+1)
	}

	// Large positive numbers.
	for i := int64(10000); i < math.MaxInt64/2; i *= 2 {
		assert.GreaterOrEqual(t, EstimatedDigitsInt64(i), Digits(i))
		assert.LessOrEqual(t, EstimatedDigitsInt64(i), Digits(i)+1)
	}
}
