// SPDX-License-Identifier: AGPL-3.0-only

package hyperloglog

import (
	"fmt"
	"math"
)

// HyperLogLog is a probabilistic cardinality estimator.
// This implementation is based on the DataDog hyperloglog library:
// https://github.com/DataDog/hyperloglog
type HyperLogLog struct {
	M         uint32  // Number of registers (2^precision)
	B         uint8   // Precision (number of bits for register indexing)
	Alpha     float64 // Bias correction constant
	Registers []byte  // Register values (one byte per register)
}

// New creates a new HyperLogLog with the given precision.
// precision is the number of bits used for register indexing (log2 of number of registers).
// Valid range: 4-18. Precision of 11 gives 2048 registers (~2KB) with ~5-6% error.
func New(precision uint8) *HyperLogLog {
	if precision < 4 || precision > 18 {
		panic(fmt.Sprintf("precision must be between 4 and 18, got %d", precision))
	}

	m := uint32(1) << precision // 2^precision

	// Calculate alpha constant for bias correction
	var alpha float64
	switch precision {
	case 4:
		alpha = 0.673
	case 5:
		alpha = 0.697
	case 6:
		alpha = 0.709
	default:
		alpha = 0.7213 / (1 + 1.079/float64(m))
	}

	return &HyperLogLog{
		M:         m,
		B:         precision,
		Alpha:     alpha,
		Registers: make([]byte, m),
	}
}

// Add adds a 32-bit hash value to the HyperLogLog.
// The hash should be computed from the series labels using a good hash function.
func (h *HyperLogLog) Add(hash uint32) {
	// Extract register index from the first B bits
	registerIndex := hash & ((1 << h.B) - 1)

	// Count leading zeros in the remaining (32 - B) bits
	// We need to count the position of the first 1 bit (rho + 1)
	w := hash >> h.B
	var leadingZeros uint8

	if w == 0 {
		// All remaining bits are 0
		leadingZeros = uint8(32 - h.B + 1)
	} else {
		// Shift w left by B bits so the (32-B) bits we care about
		// are in the upper portion of the 32-bit value.
		// This allows countLeadingZeros32 to work correctly.
		leadingZeros = uint8(countLeadingZeros32(w<<h.B)) + 1
	}

	// Update register if new value is larger (maximum operation)
	if leadingZeros > h.Registers[registerIndex] {
		h.Registers[registerIndex] = leadingZeros
	}
}

// Merge merges another HyperLogLog into this one.
// Both HyperLogLogs must have the same precision (same M).
// This operation takes the maximum value for each register.
func (h *HyperLogLog) Merge(other *HyperLogLog) {
	if other == nil {
		return
	}

	if h.M != other.M {
		panic(fmt.Sprintf("cannot merge HyperLogLogs with different precisions: %d != %d", h.M, other.M))
	}

	// Take maximum for each register
	for i := range h.Registers {
		if other.Registers[i] > h.Registers[i] {
			h.Registers[i] = other.Registers[i]
		}
	}
}

// Count returns the estimated cardinality.
// This implements the HyperLogLog algorithm with bias correction.
func (h *HyperLogLog) Count() uint64 {
	// Sum of 2^(-register_value) for all registers
	var sum float64
	var zeros uint32

	for _, val := range h.Registers {
		if val == 0 {
			zeros++
		}
		sum += 1.0 / float64(uint32(1)<<val)
	}

	// Raw estimate
	estimate := h.Alpha * float64(h.M) * float64(h.M) / sum

	// Apply small range correction if estimate is small and there are zero registers
	if estimate <= 2.5*float64(h.M) && zeros > 0 {
		// Small range correction
		estimate = float64(h.M) * math.Log(float64(h.M)/float64(zeros))
	}

	// We don't apply large range correction as mentioned in the proposal
	// to avoid overcounting issues

	if estimate < 0 {
		return 0
	}

	return uint64(estimate)
}

// Clone creates a deep copy of the HyperLogLog.
// This is useful for creating snapshots without holding locks.
func (h *HyperLogLog) Clone() *HyperLogLog {
	clone := &HyperLogLog{
		M:         h.M,
		B:         h.B,
		Alpha:     h.Alpha,
		Registers: make([]byte, len(h.Registers)),
	}
	copy(clone.Registers, h.Registers)
	return clone
}

// countLeadingZeros32 counts the number of leading zero bits in a 32-bit value.
// Returns 32 if the value is 0.
func countLeadingZeros32(x uint32) int {
	if x == 0 {
		return 32
	}

	n := 0
	if x&0xFFFF0000 == 0 {
		n += 16
		x <<= 16
	}
	if x&0xFF000000 == 0 {
		n += 8
		x <<= 8
	}
	if x&0xF0000000 == 0 {
		n += 4
		x <<= 4
	}
	if x&0xC0000000 == 0 {
		n += 2
		x <<= 2
	}
	if x&0x80000000 == 0 {
		n++
	}

	return n
}
