// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

//nolint //Since this was copied from Prometheus leave it as is
package encoding

import "github.com/prometheus/common/model"

var (
	// bit masks for consecutive bits in a byte at various offsets.
	bitMask = [][]byte{
		{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // 0 bit
		{0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01}, // 1 bit
		{0xC0, 0x60, 0x30, 0x18, 0x0C, 0x06, 0x03, 0x01}, // 2 bit
		{0xE0, 0x70, 0x38, 0x1C, 0x0E, 0x07, 0x03, 0x01}, // 3 bit
		{0xF0, 0x78, 0x3C, 0x1E, 0x0F, 0x07, 0x03, 0x01}, // 4 bit
		{0xF8, 0x7C, 0x3E, 0x1F, 0x0F, 0x07, 0x03, 0x01}, // 5 bit
		{0xFC, 0x7E, 0x3F, 0x1F, 0x0F, 0x07, 0x03, 0x01}, // 6 bit
		{0xFE, 0x7F, 0x3F, 0x1F, 0x0F, 0x07, 0x03, 0x01}, // 7 bit
		{0xFF, 0x7F, 0x3F, 0x1F, 0x0F, 0x07, 0x03, 0x01}, // 8 bit
	}
)

// isInt32 returns true if v can be represented as an int32.
func isInt32(v model.SampleValue) bool {
	return model.SampleValue(int32(v)) == v
}

// countBits returns the number of leading zero bits and the number of
// significant bits after that in the given bit pattern. The maximum number of
// leading zeros is 31 (so that it can be represented by a 5bit number). Leading
// zeros beyond that are considered part of the significant bits.
func countBits(pattern uint64) (leading, significant byte) {
	// TODO(beorn7): This would probably be faster with ugly endless switch
	// statements.
	if pattern == 0 {
		return
	}
	for pattern < 1<<63 {
		leading++
		pattern <<= 1
	}
	for pattern > 0 {
		significant++
		pattern <<= 1
	}
	if leading > 31 { // 5 bit limit.
		significant += leading - 31
		leading = 31
	}
	return
}

// isSignedIntN returns if n can be represented as a signed int with the given
// bit length.
func isSignedIntN(i int64, n byte) bool {
	upper := int64(1) << (n - 1)
	if i >= upper {
		return false
	}
	lower := upper - (1 << n)
	if i < lower {
		return false
	}
	return true
}
