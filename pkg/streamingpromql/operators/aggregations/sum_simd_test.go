// SPDX-License-Identifier: AGPL-3.0-only

//go:build goexperiment.simd

package aggregations

import (
	"fmt"
	"simd"
	"slices"
	"testing"
)

func TestAccumulateFloat64sSIMD(t *testing.T) {
	var zero simd.Float64s
	width := zero.Len()
	t.Logf("vector bits=%d, float64 lanes=%d, emulated=%t", simd.VectorBitSize(), width, simd.Emulated())
	lengths := []int{0, 1, 2, 3, 7, 8, 9, 31, 32, 33, width - 1, width, width + 1, 2*width - 1, 2 * width, 2*width + 1}
	slices.Sort(lengths)
	lengths = slices.Compact(lengths)
	for _, offset := range []int{1, width} {
		t.Run(fmt.Sprintf("offset=%d", offset), func(t *testing.T) {
			testAccumulateFloat64s(t, accumulateFloat64sSIMD, offset, lengths...)
		})
	}
}

func TestAccumulateFloat64sSIMDRandom(t *testing.T) {
	testAccumulateFloat64sRandom(t, accumulateFloat64sSIMD)
}
