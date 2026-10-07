// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/dolthub/swiss/blob/main/bits_amd64.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Dolthub, Inc.

//go:build amd64.v3 && !nosimd

package v2

import (
	"math/bits"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

// The group operations in this file are implemented in map_simd_amd64.s, which asmgen generates.
// The build constraint matches GOAMD64=v3, which the Makefile builds with by default. It guarantees AVX2
// and POPCNT, so there is no CPU feature check at runtime. Every other build uses map_nonsimd.go.

// groupSize is 16, so that a whole group fits in one 128-bit register.
const groupSize = 16

// bitset has bit i set when slot i of a group matches.
type bitset uint16

// match searches the given index for slots that hold the given prefix.
func (m *index) match(p prefix) bitset {
	return matchAVX2(m, p)
}

// matchEmptyOrSpillmark searches the given index for slots that hold no data, i.e. that are either
// empty or hold a spillmark.
func (m *index) matchEmptyOrSpillmark() bitset {
	return matchEmptyOrSpillmarkAVX2(m)
}

// matchOccupied matches the slots that hold data, i.e. neither empty nor spillmarks.
func (m *index) matchOccupied() bitset {
	return ^m.matchEmptyOrSpillmark()
}

// nextMatch clears and returns the index corresponding to the next set bit in
// the given bitset. It is assumed that the given bitset is nonzero.
func nextMatch(b *bitset) uint32 {
	s := uint32(bits.TrailingZeros16(uint16(*b)))
	*b &= ^(1 << s) // clear bit s.
	return s
}

// cleanupGroup removes the entries of the group that expired at watermark, and returns how many it removed.
func cleanupGroup(idx *index, d *data, watermark clock.Minutes) int {
	return cleanupGroupAVX2(idx, d, watermark)
}

// matchAVX2 returns a bitset with bit i set when idx[i] == p.
//
//go:noescape
func matchAVX2(idx *index, p prefix) bitset

// matchEmptyOrSpillmarkAVX2 returns a bitset with bit i set when idx[i] is empty or a spillmark.
//
//go:noescape
func matchEmptyOrSpillmarkAVX2(idx *index) bitset

// cleanupGroupAVX2 does what the scalar cleanupGroup does, for the whole group at once: it computes
// watermark.GreaterOrEqualThan for every slot that holds data, with the same steps as clock.Minutes.
// The bytes wrap where Go uses int64, but that does not change the result for any watermark or value.
// Removed slots become empty in both idx and d, except for the last one, which becomes a spillmark.
// When nothing expires, neither idx nor d are written.
//
//go:noescape
func cleanupGroupAVX2(idx *index, d *data, watermark clock.Minutes) int
